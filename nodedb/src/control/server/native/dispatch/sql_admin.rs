// SPDX-License-Identifier: BUSL-1.1

//! SET / SHOW / RESET (SQL form) and EXPLAIN handling for the native SQL
//! dispatch path. Split out of `sql.rs` to keep that file under the
//! file-size limit; behavior is unchanged.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use super::{DispatchCtx, error_to_native};
use crate::control::server::native::sqlstate_code::sqlstate_error;

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
    } else if let Some(to_pos) = find_ascii_case_insensitive(after_set, " TO ") {
        (
            after_set[..to_pos].trim().to_lowercase(),
            after_set[to_pos + 4..]
                .trim()
                .trim_matches('\'')
                .to_string(),
        )
    } else {
        return sqlstate_error(seq, "42601", "invalid SET syntax");
    };

    // The SQL form and the opcode form share one contract, so a statement
    // refused as `SET x = 'junk'` is refused as the `Set` opcode too.
    super::handle_set(ctx, seq, &key, &value)
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
        return super::show_all(ctx, seq);
    }
    super::handle_show(ctx, seq, &param)
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
            command: None,
            watermark_lsn: 0,
            error: None,
            auth: None,
            warnings: Vec::new(),
        };
    }

    let perm_cache = ctx.state.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: ctx.identity,
        auth: ctx.auth_context(),
        rls_store: &ctx.state.rls,
        redaction_store: &ctx.state.redaction,
        permissions: &ctx.state.permissions,
        roles: &ctx.state.roles,
        permission_cache: Some(&*perm_cache),
    };
    let database_id = ctx.database_id();
    match ctx
        .query_ctx
        .plan_sql_with_rls_metadata(crate::control::planner::context::PlanSqlWithRlsParams {
            sql: inner_sql,
            tenant_id: ctx.tenant_id(),
            database_id,
            sec: &sec,
        })
        .await
    {
        Ok((tasks, _output_schema)) => {
            drop(perm_cache);
            // EXPLAIN is metadata-only. Authorize the original plan to protect
            // metadata, but never materialize implicit edges while describing it.
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
                command: None,
                watermark_lsn: 0,
                error: None,
                auth: None,
                warnings: Vec::new(),
            }
        }
        Err(e) => error_to_native(seq, &e),
    }
}
