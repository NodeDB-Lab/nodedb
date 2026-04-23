//! SQL dispatch: DataFusion planning + Data Plane execution.

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use crate::bridge::envelope::{Response, Status};
use crate::control::planner::physical::PhysicalTask;
use crate::control::server::pgwire::session::TransactionState;
use crate::data::executor::response_codec;

use super::super::super::dispatch_utils;
use super::pgwire_bridge::pgwire_result_to_native;
use super::sql_gateway::dispatch_task_via_gateway;
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
    // Extract per-query ON DENY override (e.g., SELECT ... ON DENY ERROR 'CODE' MESSAGE '...').
    let mut auth_ctx = ctx.auth_context.clone();
    let clean_sql =
        crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);

    let perm_cache = ctx.state.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: ctx.identity,
        auth: &auth_ctx,
        rls_store: &ctx.state.rls,
        permissions: &ctx.state.permissions,
        roles: &ctx.state.roles,
        permission_cache: Some(&*perm_cache),
    };
    let tasks = match ctx
        .query_ctx
        .plan_sql_with_rls(&clean_sql, ctx.tenant_id(), &sec)
        .await
    {
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

/// Dispatch a single PhysicalTask.
///
/// Broadcast plans (scans, InsertSelect) are handled locally; all other tasks
/// flow through `dispatch_task_via_gateway` which routes via the gateway when
/// available, or falls back to the local SPSC path on single-node boot.
async fn dispatch_task(ctx: &DispatchCtx<'_>, task: PhysicalTask) -> crate::Result<Response> {
    if matches!(
        task.plan,
        crate::bridge::envelope::PhysicalPlan::Document(
            crate::bridge::physical_plan::DocumentOp::InsertSelect { .. }
        )
    ) {
        return dispatch_utils::broadcast_count_to_all_cores(
            ctx.state,
            task.tenant_id,
            task.plan,
            0,
            "inserted",
        )
        .await;
    }

    // Broadcast scans must fan-out to all cores regardless of gateway state.
    if task.plan.is_broadcast_scan() {
        return dispatch_utils::broadcast_to_all_cores(ctx.state, task.tenant_id, task.plan, 0)
            .await;
    }

    // All other tasks — point ops, writes, Raft-replicated writes — route
    // through the gateway when available (cluster-aware routing + retry),
    // or via the local SPSC path when the gateway is not yet wired.
    dispatch_task_via_gateway(ctx, task).await
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
        && !upper.starts_with("SHOW RANGES")
        && !upper.starts_with("SHOW ROUTING")
        && !upper.starts_with("SHOW SCHEMA VERSION")
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

    let perm_cache = ctx.state.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: ctx.identity,
        auth: ctx.auth_context,
        rls_store: &ctx.state.rls,
        permissions: &ctx.state.permissions,
        roles: &ctx.state.roles,
        permission_cache: Some(&*perm_cache),
    };
    match ctx
        .query_ctx
        .plan_sql_with_rls(inner_sql, ctx.tenant_id(), &sec)
        .await
    {
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

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::bridge::physical_plan::{ColumnarOp, DocumentOp};

    #[test]
    fn columnar_scan_is_broadcast() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: "metrics".into(),
            projection: Vec::new(),
            limit: 10,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            sort_keys: Vec::new(),
        });
        assert!(plan.is_broadcast_scan());
    }

    #[test]
    fn document_scan_is_still_broadcast() {
        let plan = PhysicalPlan::Document(DocumentOp::Scan {
            collection: "docs".into(),
            filters: Vec::new(),
            limit: 10,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
        });
        assert!(plan.is_broadcast_scan());
    }
}
