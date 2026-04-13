//! `ALTER TENANT <id> SET QUOTA <field> = <value>` handler.
//!
//! Tenant quotas live in the in-memory `TenantStore` and are not part
//! of `StoredTenant`. Quota replication is handled separately from
//! the tenant identity record.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::types::sqlstate_error;

pub fn alter_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can alter tenants",
        ));
    }

    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER TENANT <id> SET QUOTA <field> = <value>",
        ));
    }

    let tid: u32 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
    let tenant_id = TenantId::new(tid);

    if !parts[3].eq_ignore_ascii_case("SET") || !parts[4].eq_ignore_ascii_case("QUOTA") {
        return Err(sqlstate_error(
            "42601",
            "expected SET QUOTA after tenant ID",
        ));
    }

    let field = parts[5].to_lowercase();
    let value_idx = if parts.len() > 7 && parts[6] == "=" {
        7
    } else {
        6
    };
    if value_idx >= parts.len() {
        return Err(sqlstate_error("42601", "expected value after field name"));
    }

    let value: u64 = parts[value_idx]
        .parse()
        .map_err(|_| sqlstate_error("42601", "quota value must be a positive integer"))?;

    let mut tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let mut quota = tenants.quota(tenant_id).clone();
    match field.as_str() {
        "max_memory_bytes" => quota.max_memory_bytes = value,
        "max_storage_bytes" => quota.max_storage_bytes = value,
        "max_concurrent_requests" => quota.max_concurrent_requests = value as u32,
        "max_qps" => quota.max_qps = value as u32,
        "max_vector_dim" => quota.max_vector_dim = value as u32,
        "max_graph_depth" => quota.max_graph_depth = value as u32,
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!(
                    "unknown quota field: {other}. Valid: max_memory_bytes, max_storage_bytes, max_concurrent_requests, max_qps, max_vector_dim, max_graph_depth"
                ),
            ));
        }
    }
    tenants.set_quota(tenant_id, quota);

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("altered tenant {tenant_id}: set {field} = {value}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TENANT"))])
}
