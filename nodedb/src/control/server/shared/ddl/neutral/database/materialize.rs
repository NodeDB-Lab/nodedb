// SPDX-License-Identifier: BUSL-1.1

//! Handler for `ALTER DATABASE <name> MATERIALIZE`.
//!
//! The catalog lookup, `DatabaseOwner`-or-higher gate, awaited
//! force-materialization (with `BadRequest` → `0A000` mapping), and
//! `DatabaseMaterialized` audit record run here. The result is the
//! protocol-neutral [`DdlResult`].

use crate::control::maintenance::clone_materializer::{CloneMaterializerHandle, force_materialize};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_database_owner_or_higher;
use super::support::{ddl_err, status};

/// Handle `ALTER DATABASE <name> MATERIALIZE`.
///
/// Required role: `DatabaseOwner(db)`, `ClusterAdmin`, or `Superuser`.
///
/// Forces full materialization of all clone collections in the named
/// database. Returns once all collections are in `Materialized` state.
pub async fn alter_database_materialize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    require_database_owner_or_higher(
        state,
        identity,
        db_id,
        &format!("ALTER DATABASE {name} MATERIALIZE"),
    )?;

    // A resumed copy writes rows with no status write first, so the catalog
    // chokepoint comes too late to refuse it.
    super::super::audit_intent::record_action_intent(
        state,
        identity,
        None,
        &format!("ALTER DATABASE {name} MATERIALIZE"),
    )?;

    // Build a completion handle so callers can observe progress if needed.
    let handle = CloneMaterializerHandle::new(db_id);

    // The materialization is awaited on this handler's runtime.
    //
    // `BadRequest` from the gating walker is surfaced as SQLSTATE `0A000`
    // (`feature_not_supported`) so clients can distinguish it from generic
    // failures and retry strategy is unambiguous (don't retry — wait for the
    // per-engine bulk-copy implementation to land).
    force_materialize(db_id, state, catalog, Some(&handle))
        .await
        .map_err(|e| match e {
            crate::Error::BadRequest { detail } => ddl_err("0A000", detail),
            // Any other error keeps the class the SQLSTATE table gives it.
            other => DdlError::from_error_in_context(
                &format!("clone materialization of '{name}' failed"),
                &other,
            ),
        })?;

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseMaterialized,
        None,
        Some(db_id),
        &identity.username,
        &format!("ALTER DATABASE {name} MATERIALIZE"),
    );

    Ok(status("ALTER DATABASE"))
}
