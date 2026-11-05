// SPDX-License-Identifier: BUSL-1.1

//! `PURGE TENANT <id|name> CONFIRM` — Data Plane meta op that deletes
//! ALL tenant data across every engine. Superuser-only, requires
//! the literal `CONFIRM` keyword.
//!
//! The tenant reference accepts either a numeric id or a tenant name
//! (single-quoted optional), parallel to `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>`.
//!
//! The `PhysicalPlan::Meta(MetaOp::PurgeTenant)` plan runs on every local
//! Data Plane core with a 300s timeout. Each core purges only the state its
//! own vShards home to, so a purge on one core leaves the tenant's data on
//! every other core.

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::gather_all_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};

use super::super::super::result::{DdlError, DdlResult};
use super::support::{ddl_err, resolve_tenant_ref, status, tenant_exists};

pub async fn purge_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can purge tenants",
        ));
    }

    if parts.len() < 4 {
        return Err(ddl_err("42601", "syntax: PURGE TENANT <id|name> CONFIRM"));
    }

    // Accept either a numeric id or a tenant name (mirrors CREATE/SHOW/DROP).
    let tenant_id = resolve_tenant_ref(state, parts[2])?
        .ok_or_else(|| ddl_err("42704", format!("tenant '{}' does not exist", parts[2])))?;
    let tid = tenant_id.as_u64();

    if tid == 0 {
        return Err(ddl_err("42501", "cannot purge system tenant (0)"));
    }

    // Existence gate, uniform across numeric ids and resolved names: refuse to
    // dispatch the destructive meta op for a tenant that does not exist.
    if !tenant_exists(state, tenant_id)? {
        return Err(ddl_err(
            "42704",
            format!("tenant '{}' does not exist", parts[2]),
        ));
    }

    if !parts[3].eq_ignore_ascii_case("CONFIRM") {
        return Err(ddl_err(
            "42601",
            "PURGE TENANT requires CONFIRM keyword to prevent accidental data destruction",
        ));
    }

    super::super::audit_intent::record_action_intent(
        state,
        identity,
        Some(tenant_id),
        &format!("PURGE TENANT {tid} CONFIRM — deleting all data across all engines"),
    )?;

    let plan = crate::bridge::envelope::PhysicalPlan::Meta(
        nodedb_physical::physical_plan::MetaOp::PurgeTenant { tenant_id: tid },
    );

    let timeout = std::time::Duration::from_secs(300);
    let purged = match tokio::time::timeout(
        timeout,
        gather_all_cores(
            state,
            tenant_id,
            database_id,
            plan,
            TraceId::generate(),
            None,
        ),
    )
    .await
    {
        Ok(result) => result.map(|_| ()),
        Err(_) => Err(crate::Error::Dispatch {
            detail: format!("PURGE TENANT {tid} did not finish on every core within {timeout:?}"),
        }),
    };
    match purged {
        Ok(_) => {
            state.audit_record(
                AuditEvent::AdminAction,
                Some(tenant_id),
                &identity.username,
                &format!("PURGE TENANT {tid} completed successfully"),
            );
            Ok(status("PURGE TENANT"))
        }
        Err(e) => Err(DdlError::from_error_in_context("purge failed", &e)),
    }
}
