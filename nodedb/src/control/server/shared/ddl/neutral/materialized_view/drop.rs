// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP MATERIALIZED VIEW [IF EXISTS]` handler.
//!
//! Ported from the pgwire `ddl::materialized_view::drop` handler. The DIRECT
//! catalog path (`propose_catalog_entry` for the `DeleteMaterializedView` entry
//! with a manual `catalog.delete_materialized_view` fallback on the `log_index
//! == 0` bypass branch, then a `DeactivateCollection` propose for the view's
//! target collection), the token-based name / IF EXISTS extraction, and the
//! pre-check existence gate are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Whether a materialized view exists in the in-memory registry for the
/// identity tenant. Used by the router's IF EXISTS short-circuit guard.
pub fn materialized_view_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.mv_registry.get_def(tid, name).is_some()
}

pub fn drop_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
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
        (parts[5].to_lowercase(), true)
    } else {
        (parts[3].to_lowercase(), false)
    };

    // Streaming MVs live in the Event-Plane registry (`mv_registry`), not the
    // periodic MV catalog. Handle them first: delete the catalog record and
    // unregister from the live registry. Falls through to the periodic path
    // below when no streaming MV of this name exists, preserving IF EXISTS.
    if state
        .mv_registry
        .get_def(tenant_id.as_u64(), &name)
        .is_some()
    {
        {
            let catalog = state.credentials.catalog();
            catalog
                .delete_streaming_mv(tenant_id.as_u64(), &name)
                .map_err(|e| err("XX000", e.to_string()))?;
        }
        state.mv_registry.unregister(tenant_id.as_u64(), &name);
        tracing::info!(view = name, "streaming materialized view dropped");
        return Ok(vec![DdlResult::Status {
            command: "DROP MATERIALIZED VIEW".to_string(),
            rows_affected: None,
        }]);
    }

    // Pre-check existence so `IF EXISTS` + missing is a no-op
    // that never touches raft.
    let exists_before = matches!(
        state
            .credentials
            .catalog()
            .get_materialized_view(tenant_id.as_u64(), &name),
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
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    if log_index == 0 {
        let catalog = state.credentials.catalog();
        catalog
            .delete_materialized_view(tenant_id.as_u64(), &name)
            .map_err(|e| err("XX000", e.to_string()))?;
    }

    // Also drop the view's target collection created by CREATE MATERIALIZED VIEW.
    // The target lives as a normal collection to support INSERT...SELECT refresh
    // and SELECT reads; leaving it behind would leak storage and shadow any
    // later CREATE COLLECTION with the same name.
    let catalog = state.credentials.catalog();
    if matches!(
        catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &name),
        Ok(Some(_))
    ) {
        let coll_entry = crate::control::catalog_entry::CatalogEntry::DeactivateCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: tenant_id.as_u64(),
            name: name.clone(),
        };
        let _ = crate::control::metadata_proposer::propose_catalog_entry(state, &coll_entry)
            .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    }

    tracing::info!(view = name, "materialized view dropped");

    Ok(vec![DdlResult::Status {
        command: "DROP MATERIALIZED VIEW".to_string(),
        rows_affected: None,
    }])
}
