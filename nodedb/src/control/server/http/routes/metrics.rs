//! Prometheus-compatible metrics endpoint.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use super::super::auth::{ApiError, AppState, resolve_identity};

/// GET /metrics — Prometheus-format metrics.
///
/// Requires monitor role or superuser.
pub async fn metrics(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    if !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::Monitor)
    {
        return Err(ApiError::Forbidden(
            "monitor or superuser role required".into(),
        ));
    }

    let mut output = String::with_capacity(4096);

    // WAL metrics.
    let wal_lsn = state.shared.wal.next_lsn().as_u64();
    output.push_str("# HELP nodedb_wal_next_lsn Next WAL log sequence number.\n");
    output.push_str("# TYPE nodedb_wal_next_lsn gauge\n");
    output.push_str(&format!("nodedb_wal_next_lsn {wal_lsn}\n\n"));

    // Node ID.
    output.push_str("# HELP nodedb_node_id This node's cluster ID.\n");
    output.push_str("# TYPE nodedb_node_id gauge\n");
    output.push_str(&format!("nodedb_node_id {}\n\n", state.shared.node_id));

    // Tenant metrics.
    if let Ok(tenants) = state.shared.tenants.lock() {
        // Collect active tenants from credential store.
        for user in state.shared.credentials.list_user_details() {
            let tid = user.tenant_id.as_u32();
            if let Some(usage) = tenants.usage(user.tenant_id) {
                output.push_str(&format!(
                    "nodedb_tenant_active_requests{{tenant_id=\"{tid}\"}} {}\n",
                    usage.active_requests
                ));
                output.push_str(&format!(
                    "nodedb_tenant_total_requests{{tenant_id=\"{tid}\"}} {}\n",
                    usage.total_requests
                ));
                output.push_str(&format!(
                    "nodedb_tenant_rejected_requests{{tenant_id=\"{tid}\"}} {}\n",
                    usage.rejected_requests
                ));
                output.push_str(&format!(
                    "nodedb_tenant_active_connections{{tenant_id=\"{tid}\"}} {}\n",
                    usage.active_connections
                ));
            }
        }
    }

    // Audit metrics.
    if let Ok(audit) = state.shared.audit.lock() {
        output.push_str("# HELP nodedb_audit_total_entries Total audit entries ever recorded.\n");
        output.push_str("# TYPE nodedb_audit_total_entries counter\n");
        output.push_str(&format!(
            "nodedb_audit_total_entries {}\n\n",
            audit.total_recorded()
        ));
    }

    // Credential metrics.
    let user_count = state.shared.credentials.list_user_details().len();
    output.push_str("# HELP nodedb_users_active Number of active users.\n");
    output.push_str("# TYPE nodedb_users_active gauge\n");
    output.push_str(&format!("nodedb_users_active {user_count}\n\n"));

    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    ))
}
