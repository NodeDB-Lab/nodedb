// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP MATERIALIZED VIEW [IF EXISTS]` handler.
//!
//! The DIRECT catalog path (`propose_catalog_entry` for the compound
//! `DeleteMaterializedView` definition+target deletion), the token-based name / IF EXISTS
//! extraction, and the pre-check existence gate are shared by every protocol.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Whether a materialized view exists in the in-memory registry for the
/// identity tenant. Used by the router's IF EXISTS short-circuit guard.
pub fn materialized_view_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.mv_registry.get_def(database_id, tid, name).is_some()
}

pub async fn drop_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 {
        return Err(err(
            "42601",
            "syntax: DROP MATERIALIZED VIEW [IF EXISTS] <name>".to_string(),
        ));
    }

    let tenant_id = identity.tenant_id;

    let (name, if_exists) = if parts.len() >= 6
        && parts[3].to_uppercase() == "IF"
        && parts[4].to_uppercase() == "EXISTS"
    {
        (parse_ident_token(parts[5])?, true)
    } else {
        (parse_ident_token(parts[3])?, false)
    };

    // The view's recorded owner may drop it. Anyone else must own the
    // same-named collection or hold an admin role.
    let owns_view = state
        .credentials
        .catalog()
        .get_materialized_view(database_id.as_u64(), tenant_id.as_u64(), &name)
        .ok()
        .flatten()
        .is_some_and(|view| view.owner == identity.username);
    if !owns_view {
        crate::control::server::shared::ddl::neutral::object_permission::require_drop_permission(
            state,
            identity,
            database_id,
            &name,
            "drop materialized views",
        )?;
    }

    // Streaming MVs live in the Event-Plane registry (`mv_registry`), not the
    // periodic MV catalog. Handle them first: delete the catalog record and
    // unregister from the live registry. Falls through to the periodic path
    // below when no streaming MV of this name exists, preserving IF EXISTS.
    if state
        .mv_registry
        .get_def(database_id, tenant_id.as_u64(), &name)
        .is_some()
    {
        let entry = crate::control::catalog_entry::CatalogEntry::DeleteStreamingMaterializedView {
            database_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            name: name.clone(),
        };
        crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
            .await
            .map_err(|error| DdlError::from_error_in_context("metadata propose", &error))?;
        tracing::info!(view = name, "streaming materialized view dropped");
        return Ok(vec![DdlResult::Status {
            command: "DROP MATERIALIZED VIEW".to_string(),
            rows_affected: None,
        }]);
    }

    // Pre-check existence so `IF EXISTS` + missing is a no-op
    // that never touches raft.
    let exists_before = matches!(
        state.credentials.catalog().get_materialized_view(
            database_id.as_u64(),
            tenant_id.as_u64(),
            &name
        ),
        Ok(Some(_))
    );
    if !exists_before && !if_exists {
        return Err(err(
            "42P01",
            format!("materialized view '{name}' does not exist"),
        ));
    }
    if !exists_before {
        return Ok(vec![DdlResult::Status {
            command: "DROP MATERIALIZED VIEW".to_string(),
            rows_affected: None,
        }]);
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteMaterializedView {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
        // Frozen by the proposer's stamp.
        target_descriptor_version: 0,
        target_hlc: nodedb_types::Hlc::ZERO,
    };
    // The apply deletes the definition and its target, and reclaims the
    // target's storage in post-apply on every node, this one included.
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|error| DdlError::from_error_in_context("metadata propose", &error))?;

    tracing::info!(view = name, "materialized view dropped");

    Ok(vec![DdlResult::Status {
        command: "DROP MATERIALIZED VIEW".to_string(),
        rows_affected: None,
    }])
}
