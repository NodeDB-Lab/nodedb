// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP CONTINUOUS AGGREGATE` handler.
//!
//! Ported from the pgwire `ddl::continuous_agg::drop` handler. The catalog path
//! (`propose_and_apply` for the `DeleteContinuousAggregate` entry, then the
//! `log_index == 0` single-node `UnregisterContinuousAggregate` sync dispatch),
//! the `parts[3]` name extraction, and the arity check are preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError` to
//! the protocol-neutral [`DdlResult`] / [`DdlError`]. The
//! [`continuous_aggregate_exists`] helper (moved from the pgwire router's
//! `ast::exists`) backs the router's IF EXISTS short-circuit guard.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Whether a continuous aggregate exists in the in-memory registry for the
/// identity tenant. Used by the router's IF EXISTS short-circuit guard.
pub fn continuous_aggregate_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    state
        .credentials
        .catalog()
        .get_continuous_aggregate(database_id.as_u64(), identity.tenant_id.as_u64(), name)
        .ok()
        .flatten()
        .is_some()
}

/// `DROP CONTINUOUS AGGREGATE <name>`.
///
/// `parts` is the whitespace-tokenised statement; positions 0..=2 are
/// `["DROP", "CONTINUOUS", "AGGREGATE"]` and position 3 is the name.
pub async fn drop_continuous_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 {
        return Err(err(
            "42601",
            "syntax: DROP CONTINUOUS AGGREGATE <name>".to_string(),
        ));
    }

    let name = parts
        .last()
        .ok_or_else(|| err("42601", "missing continuous aggregate name".to_string()))?
        .trim_matches(['\'', '"'])
        .to_lowercase();
    let tenant_id = identity.tenant_id;
    let stored = state
        .credentials
        .catalog()
        .get_continuous_aggregate(database_id.as_u64(), tenant_id.as_u64(), &name)
        .map_err(|e| err("XX000", format!("catalog read: {e}")))?
        .ok_or_else(|| {
            err(
                "42704",
                format!("continuous aggregate '{name}' does not exist"),
            )
        })?;
    let is_owner = stored.owner == identity.username;
    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);
    if !is_owner && !is_admin {
        return Err(err(
            "42501",
            format!("permission denied to drop continuous aggregate '{name}'"),
        ));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteContinuousAggregate {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
    };
    let log_index = propose_and_apply(state, &entry)?;

    // Single-node / no-applier path: mirror the unregister dispatch the
    // raft-applier path would have done so the local manager forgets the
    // aggregate immediately.
    if log_index == 0 {
        state
            .permissions
            .install_replicated_remove_owner_in_database(
                crate::control::security::catalog::auth_types::object_type::CONTINUOUS_AGGREGATE,
                database_id.as_u64(),
                tenant_id.as_u64(),
                &name,
            );
        let plan = PhysicalPlan::Meta(MetaOp::UnregisterContinuousAggregate { name: name.clone() });
        sync_dispatch::dispatch_async(
            state,
            tenant_id,
            database_id,
            &stored.source,
            plan,
            Duration::from_secs(5),
        )
        .await
        .map_err(|e| err("XX000", format!("dispatch failed: {e}")))?;
    }

    tracing::info!(name, "continuous aggregate dropped");

    Ok(vec![DdlResult::Status {
        command: "DROP CONTINUOUS AGGREGATE".to_string(),
        rows_affected: None,
    }])
}
