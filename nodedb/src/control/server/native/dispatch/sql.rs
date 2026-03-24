//! SQL dispatch: DataFusion planning + Data Plane execution.

use std::sync::Arc;

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::planner::physical::PhysicalTask;
use crate::control::server::pgwire::session::TransactionState;
use crate::data::executor::response_codec;

use super::super::super::dispatch_utils;
use super::pgwire_bridge::pgwire_result_to_native;
use super::transaction::{handle_begin, handle_commit, handle_rollback};
use super::{DispatchCtx, error_to_native};

/// Handle a SQL statement: transaction control, SET/SHOW, DDL, or DataFusion.
pub(crate) async fn handle_sql(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
    let sql_trimmed = sql.trim();
    let upper = sql_trimmed.to_uppercase();

    ctx.sessions.ensure_session(*ctx.peer_addr);

    if sql_trimmed.is_empty() || sql_trimmed == ";" {
        return NativeResponse::ok(seq);
    }

    // Transaction control.
    if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
        return handle_begin(ctx, seq);
    }
    if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
        return handle_commit(ctx, seq).await;
    }
    if upper == "ROLLBACK" || upper == "ABORT" {
        return handle_rollback(ctx, seq);
    }
    if upper.starts_with("SAVEPOINT ") {
        return NativeResponse::status_row(seq, "SAVEPOINT");
    }
    if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
        return NativeResponse::status_row(seq, "RELEASE");
    }
    if upper.starts_with("ROLLBACK TO ") {
        return NativeResponse::status_row(seq, "ROLLBACK");
    }

    if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::Failed {
        return NativeResponse::error(
            seq,
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        );
    }

    // SET / SHOW / RESET.
    if upper.starts_with("SET ") {
        return handle_set_sql(ctx, seq, sql_trimmed);
    }
    if upper.starts_with("SHOW ") && is_session_show(&upper) {
        return handle_show_sql(ctx, seq, sql_trimmed);
    }
    if upper.starts_with("RESET ") {
        let param = sql_trimmed[6..].trim().to_lowercase();
        ctx.sessions
            .set_parameter(ctx.peer_addr, param, String::new());
        return NativeResponse::status_row(seq, "RESET");
    }
    if upper == "DISCARD ALL" {
        ctx.sessions.remove(ctx.peer_addr);
        ctx.sessions.ensure_session(*ctx.peer_addr);
        return NativeResponse::status_row(seq, "DISCARD ALL");
    }

    // EXPLAIN.
    if upper.starts_with("EXPLAIN ") {
        return handle_explain(ctx, seq, sql_trimmed).await;
    }

    // DDL: try DDL router first.
    if let Some(result) =
        super::super::super::pgwire::ddl::dispatch(ctx.state, ctx.identity, sql_trimmed).await
    {
        return pgwire_result_to_native(seq, result).await;
    }

    // Quota check.
    if let Err(e) = ctx.state.check_tenant_quota(ctx.tenant_id()) {
        return error_to_native(seq, &e);
    }

    // DataFusion planning.
    ctx.state.tenant_request_start(ctx.tenant_id());
    let result = execute_planned(ctx, seq, sql_trimmed).await;
    ctx.state.tenant_request_end(ctx.tenant_id());

    if result.status == nodedb_types::protocol::ResponseStatus::Error {
        ctx.sessions.fail_transaction(ctx.peer_addr);
    }

    result
}

/// Plan SQL via DataFusion and dispatch tasks to the Data Plane.
async fn execute_planned(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
    let tasks = match ctx.query_ctx.plan_sql(sql, ctx.tenant_id()).await {
        Ok(t) => t,
        Err(e) => return error_to_native(seq, &e),
    };

    if tasks.is_empty() {
        return NativeResponse::status_row(seq, "OK");
    }

    let mut all_columns: Option<Vec<String>> = None;
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut last_lsn = 0u64;
    let mut total_affected = 0u64;

    for task in tasks {
        if task.tenant_id != ctx.tenant_id() {
            return NativeResponse::error(seq, "42501", "tenant isolation violation");
        }

        // In transaction: buffer writes.
        if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock {
            let is_write = crate::control::wal_replication::to_replicated_entry(
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            )
            .is_some();
            if is_write {
                ctx.sessions.buffer_write(ctx.peer_addr, task);
                total_affected += 1;
                continue;
            }
        }

        let resp = match dispatch_task(ctx, task).await {
            Ok(r) => r,
            Err(e) => return error_to_native(seq, &e),
        };

        if resp.status == Status::Error {
            let msg = if resp.payload.is_empty() {
                resp.error_code
                    .as_ref()
                    .map(|c| format!("{c:?}"))
                    .unwrap_or_else(|| "unknown error".into())
            } else {
                String::from_utf8_lossy(&resp.payload).into_owned()
            };
            return NativeResponse::error(seq, "XX000", msg);
        }

        last_lsn = resp.watermark_lsn.as_u64();

        if resp.payload.is_empty() {
            total_affected += 1;
        } else {
            let json_text = response_codec::decode_payload_to_json(&resp.payload);
            let (cols, rows) = super::parse_json_to_columns_rows(&json_text);
            if !cols.is_empty() && all_columns.is_none() {
                all_columns = Some(cols);
            }
            all_rows.extend(rows);
        }
    }

    if all_rows.is_empty() {
        let mut r = NativeResponse::ok(seq);
        r.rows_affected = Some(total_affected);
        r.watermark_lsn = last_lsn;
        r
    } else {
        NativeResponse {
            seq,
            status: nodedb_types::protocol::ResponseStatus::Ok,
            columns: all_columns,
            rows: Some(all_rows),
            rows_affected: Some(total_affected),
            watermark_lsn: last_lsn,
            error: None,
            auth: None,
        }
    }
}

/// Check if a plan is a read scan that should broadcast to all cores.
fn is_broadcast_scan(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::DocumentScan { .. }
            | PhysicalPlan::Aggregate { .. }
            | PhysicalPlan::PartialAggregate { .. }
            | PhysicalPlan::GraphHop { .. }
            | PhysicalPlan::GraphNeighbors { .. }
            | PhysicalPlan::GraphPath { .. }
            | PhysicalPlan::GraphSubgraph { .. }
            | PhysicalPlan::VectorSearch { .. }
            | PhysicalPlan::TextSearch { .. }
            | PhysicalPlan::HybridSearch { .. }
            | PhysicalPlan::GraphRagFusion { .. }
    )
}

/// Dispatch a single PhysicalTask (WAL + Data Plane, or Raft).
/// Scan operations are broadcast to all cores; point operations use single-core dispatch.
async fn dispatch_task(ctx: &DispatchCtx<'_>, task: PhysicalTask) -> crate::Result<Response> {
    // Broadcast scans to all cores so we find data regardless of which core stored it.
    if is_broadcast_scan(&task.plan) {
        return dispatch_utils::broadcast_to_all_cores(ctx.state, task.tenant_id, task.plan, 0)
            .await;
    }
    // Raft path for replicated writes.
    if let (Some(proposer), Some(tracker)) = (&ctx.state.raft_proposer, &ctx.state.propose_tracker)
        && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
            task.tenant_id,
            task.vshard_id,
            &task.plan,
        )
    {
        let data = entry.to_bytes();
        let vshard_id = entry.vshard_id;

        let (group_id, log_index) =
            proposer(vshard_id, data).map_err(|e| crate::Error::Dispatch {
                detail: format!("raft propose failed: {e}"),
            })?;

        let rx = tracker.register(group_id, log_index);
        let result = tokio::time::timeout(std::time::Duration::from_secs(30), rx)
            .await
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("raft commit timeout for group {group_id} index {log_index}"),
            })?
            .map_err(|_| crate::Error::Dispatch {
                detail: "propose waiter channel closed".into(),
            })?;

        return match result {
            Ok(payload) => Ok(Response {
                request_id: crate::types::RequestId::new(0),
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: payload.into(),
                watermark_lsn: crate::types::Lsn::new(log_index),
                error_code: None,
            }),
            Err(err_msg) => {
                let err_str = err_msg.to_string();
                Ok(Response {
                    request_id: crate::types::RequestId::new(0),
                    status: Status::Error,
                    attempt: 1,
                    partial: false,
                    payload: crate::bridge::envelope::Payload::from_arc(Arc::from(
                        err_str.as_bytes(),
                    )),
                    watermark_lsn: crate::types::Lsn::new(0),
                    error_code: Some(crate::bridge::envelope::ErrorCode::Internal {
                        detail: err_str,
                    }),
                })
            }
        };
    }

    // Local path: WAL append + Data Plane dispatch.
    dispatch_utils::wal_append_if_write(
        &ctx.state.wal,
        task.tenant_id,
        task.vshard_id,
        &task.plan,
    )?;

    dispatch_utils::dispatch_to_data_plane(
        ctx.state,
        task.tenant_id,
        task.vshard_id,
        task.plan,
        0, // trace_id
    )
    .await
}

// ─── SET / SHOW / RESET (SQL form) ─────────────────────────────────

fn handle_set_sql(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
    let after_set = sql[4..].trim();
    let after_set = after_set
        .strip_prefix("SESSION ")
        .or_else(|| after_set.strip_prefix("LOCAL "))
        .unwrap_or(after_set);

    let (key, value) = if let Some(eq_pos) = after_set.find('=') {
        (
            after_set[..eq_pos].trim().to_lowercase(),
            after_set[eq_pos + 1..]
                .trim()
                .trim_matches('\'')
                .to_string(),
        )
    } else if let Some(to_pos) = after_set.to_uppercase().find(" TO ") {
        (
            after_set[..to_pos].trim().to_lowercase(),
            after_set[to_pos + 4..]
                .trim()
                .trim_matches('\'')
                .to_string(),
        )
    } else {
        return NativeResponse::error(seq, "42601", "invalid SET syntax");
    };

    ctx.sessions.set_parameter(ctx.peer_addr, key, value);
    NativeResponse::status_row(seq, "SET")
}

fn is_session_show(upper: &str) -> bool {
    !upper.starts_with("SHOW USERS")
        && !upper.starts_with("SHOW TENANTS")
        && !upper.starts_with("SHOW SESSION")
        && !upper.starts_with("SHOW CLUSTER")
        && !upper.starts_with("SHOW RAFT")
        && !upper.starts_with("SHOW MIGRATIONS")
        && !upper.starts_with("SHOW PEER")
        && !upper.starts_with("SHOW NODES")
        && !upper.starts_with("SHOW NODE ")
        && !upper.starts_with("SHOW COLLECTIONS")
        && !upper.starts_with("SHOW AUDIT")
        && !upper.starts_with("SHOW PERMISSIONS")
        && !upper.starts_with("SHOW GRANTS")
        && upper != "SHOW CONNECTIONS"
        && !upper.starts_with("SHOW INDEXES")
}

fn handle_show_sql(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
    let param = sql[5..].trim().to_lowercase();
    if param == "all" {
        let params = ctx.sessions.all_parameters(ctx.peer_addr);
        let columns = vec!["name".into(), "setting".into()];
        let rows: Vec<Vec<Value>> = params
            .into_iter()
            .map(|(k, v)| vec![Value::String(k), Value::String(v)])
            .collect();
        return NativeResponse {
            seq,
            status: nodedb_types::protocol::ResponseStatus::Ok,
            columns: Some(columns),
            rows: Some(rows),
            rows_affected: None,
            watermark_lsn: 0,
            error: None,
            auth: None,
        };
    }

    let value = ctx
        .sessions
        .get_parameter(ctx.peer_addr, &param)
        .unwrap_or_default();
    NativeResponse {
        seq,
        status: nodedb_types::protocol::ResponseStatus::Ok,
        columns: Some(vec!["setting".into()]),
        rows: Some(vec![vec![Value::String(value)]]),
        rows_affected: None,
        watermark_lsn: 0,
        error: None,
        auth: None,
    }
}

// ─── Explain ───────────────────────────────────────────────────────

async fn handle_explain(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
    let inner_sql = sql.strip_prefix("EXPLAIN ").unwrap_or(sql).trim();
    let inner_upper = inner_sql.to_uppercase();

    if inner_upper.starts_with("CREATE ")
        || inner_upper.starts_with("DROP ")
        || inner_upper.starts_with("ALTER ")
        || inner_upper.starts_with("SHOW ")
        || inner_upper.starts_with("SEARCH ")
    {
        return NativeResponse {
            seq,
            status: nodedb_types::protocol::ResponseStatus::Ok,
            columns: Some(vec!["plan".into()]),
            rows: Some(vec![vec![Value::String(format!("DDL: {inner_sql}"))]]),
            rows_affected: None,
            watermark_lsn: 0,
            error: None,
            auth: None,
        };
    }

    match ctx.query_ctx.plan_sql(inner_sql, ctx.tenant_id()).await {
        Ok(tasks) => {
            let plan_text = tasks
                .iter()
                .map(|t| format!("{:?}", t.plan))
                .collect::<Vec<_>>()
                .join("\n");
            NativeResponse {
                seq,
                status: nodedb_types::protocol::ResponseStatus::Ok,
                columns: Some(vec!["plan".into()]),
                rows: Some(vec![vec![Value::String(plan_text)]]),
                rows_affected: None,
                watermark_lsn: 0,
                error: None,
                auth: None,
            }
        }
        Err(e) => error_to_native(seq, &e),
    }
}
