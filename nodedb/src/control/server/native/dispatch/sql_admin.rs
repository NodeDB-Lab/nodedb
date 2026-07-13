// SPDX-License-Identifier: BUSL-1.1

//! SET / SHOW / RESET (SQL form) and EXPLAIN handling for the native SQL
//! dispatch path. Split out of `sql.rs` to keep that file under the
//! file-size limit; behavior is unchanged.

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use super::{DispatchCtx, error_to_native};

// ─── SET / SHOW / RESET (SQL form) ─────────────────────────────────

pub(super) fn handle_set_sql(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
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

pub(super) fn is_session_show(upper: &str) -> bool {
    !upper.starts_with("SHOW USERS")
        && !upper.starts_with("SHOW TENANTS")
        && !upper.starts_with("SHOW TENANT ")
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

pub(super) fn handle_show_sql(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
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
            warnings: Vec::new(),
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
        warnings: Vec::new(),
    }
}

// ─── Explain ───────────────────────────────────────────────────────

pub(super) async fn handle_explain(ctx: &DispatchCtx<'_>, seq: u64, sql: &str) -> NativeResponse {
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
            warnings: Vec::new(),
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
    let database_id = ctx.database_id();
    match ctx
        .query_ctx
        .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
            sql: inner_sql,
            tenant_id: ctx.tenant_id(),
            database_id,
            sec: &sec,
        })
        .await
    {
        Ok((mut tasks, _output_schema)) => {
            drop(perm_cache);
            if let Err(error) = crate::control::planner::implicit_edges::append_implicit_edge_tasks(
                ctx.state,
                &mut tasks,
                ctx.tenant_id(),
                database_id,
                crate::types::TraceId::ZERO,
            )
            .await
            {
                return error_to_native(seq, &error);
            }
            let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
                &ctx.state.audit,
            ));
            if let Err(error) = crate::control::server::shared::authorization::authorize_task_set(
                ctx.identity,
                &tasks,
                &ctx.state.permissions,
                &ctx.state.roles,
                &emitter,
            ) {
                return error_to_native(seq, &crate::Error::from(error));
            }
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
                warnings: Vec::new(),
            }
        }
        Err(e) => error_to_native(seq, &e),
    }
}
