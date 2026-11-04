// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DROP [IF EXISTS] DATABASE <name> [CASCADE | FORCE]`.
//!
//! Every catalog removal of the drop, the objects inside the database and the
//! descriptor itself, travels in one metadata commit built by
//! [`plan_database_teardown`]. Each node applies it, reclaims the dropped
//! collections' storage, and replays it as one unit after a restart.

use nodedb_types::DatabaseId;

use crate::control::maintenance::clone_materializer::{CloneMaterializerHandle, force_materialize};
use crate::control::metadata_proposer::propose_catalog_batch_async;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_superuser;
use super::support::{ddl_err, status};
use super::teardown::plan_database_teardown;

/// Handle `DROP [IF EXISTS] DATABASE <name> [CASCADE | FORCE]`.
///
/// Required role: `Superuser`.
pub async fn drop_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_exists: bool,
    cascade: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    // `default` is immutable — cannot be dropped.
    if name.eq_ignore_ascii_case("default") {
        return Err(DdlError::cannot_drop_default_database(
            "cannot drop the built-in 'default' database",
        ));
    }

    let catalog = state.credentials.catalog();

    let db_id = match catalog
        .get_database_id_by_name(name)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?
    {
        Some(id) => id,
        None => {
            // If the database does not exist and if_exists=true, no actor to record.
            if if_exists {
                return Ok(status("DROP DATABASE"));
            }
            return Err(ddl_err(
                "3D000",
                format!("database '{name}' does not exist"),
            ));
        }
    };

    // Gate: Superuser required. Resolving db_id first so we can include it in the
    // audit record on denial.
    require_superuser(
        state,
        identity,
        Some(db_id),
        &format!("DROP DATABASE {name}"),
    )?;

    // Guard: `default` identity check by id (rename resilience).
    if db_id == DatabaseId::DEFAULT {
        return Err(DdlError::cannot_drop_default_database(
            "cannot drop the built-in 'default' database",
        ));
    }

    // ── Orphan protection ─────────────────────────────────────────────────────
    //
    // Check whether any live clones depend on this database as their source.
    // If dependents exist and `cascade` is false, reject immediately.
    // If dependents exist and `cascade` is true, block-materialize each one
    // before proceeding.
    let dependent_ids = catalog
        .get_live_clone_children(db_id)
        .map_err(|e| DdlError::from_error_in_context("lineage check failed", &e))?;

    if !dependent_ids.is_empty() {
        if !cascade {
            let id_list: Vec<String> = dependent_ids
                .iter()
                .map(|id| id.as_u64().to_string())
                .collect();
            return Err(DdlError::clone_dependency(format!(
                "database '{}' cannot be dropped: {} clone(s) depend on it \
                 (database ids: {}); use FORCE or CASCADE to materialize them first",
                name,
                dependent_ids.len(),
                id_list.join(", ")
            )));
        }

        // FORCE path: materialize each dependent clone so it is no longer
        // backed by this source, then proceed with the drop.
        //
        // Crash safety: if the server dies mid-force-drop, the dependents
        // retain their `Materializing { .. }` status and finish on restart.
        // The original DROP command is retried by the caller, which will
        // succeed once the dependents are fully materialized.
        for dep_id in &dependent_ids {
            let handle = CloneMaterializerHandle::new(*dep_id);
            // Awaited on this handler's runtime.
            force_materialize(*dep_id, state, catalog, Some(&handle))
                .await
                .map_err(|e| match e {
                    // Gated until per-engine row copy lands — surface `0A000`
                    // (`feature_not_supported`) so clients know not to retry.
                    crate::Error::BadRequest { detail } => ddl_err("0A000", detail),
                    // Any other error keeps the class the SQLSTATE table
                    // gives it.
                    other => DdlError::from_error_in_context(
                        &format!(
                            "force materialization of dependent clone {} failed",
                            dep_id.as_u64()
                        ),
                        &other,
                    ),
                })?;
        }
    }

    // ── Teardown ──────────────────────────────────────────────────────────────
    let collections = catalog
        .load_all_collections(db_id)
        .map_err(|e| DdlError::from_error_in_context("catalog scan failed", &e))?;

    if !cascade && !collections.is_empty() {
        return Err(ddl_err(
            "2BP01",
            format!(
                "database '{name}' has {} collection(s); \
                 use CASCADE to drop all collections automatically",
                collections.len()
            ),
        ));
    }
    // Arrays register per node, so this node's array catalog is the one to
    // read. Every node drops its own arrays when it applies the drop.
    let arrays = catalog
        .load_all_arrays()
        .map_err(|e| DdlError::from_error_in_context("array catalog scan failed", &e))?
        .into_iter()
        .filter(|entry| entry.array_id.database_id == db_id)
        .count();
    if !cascade && arrays > 0 {
        return Err(ddl_err(
            "2BP01",
            format!(
                "database '{name}' has {arrays} array(s); \
                 use CASCADE to drop them automatically"
            ),
        ));
    }

    // Emit audit BEFORE the catalog mutation so the record is durable even
    // if the catalog delete fails (the database still exists in that case,
    // but the attempt is documented).
    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseDropped,
        None,
        Some(db_id),
        &identity.username,
        &format!("DROP DATABASE {name}"),
    );

    // One metadata commit removes every object of the database and then the
    // database, on every node. The plan is rebuilt under the DDL preparation
    // lease, so a collection created after the check above is still refused
    // without CASCADE.
    propose_catalog_batch_async(state, |catalog| {
        if !cascade {
            refuse_if_collections(catalog, db_id, name)?;
        }
        plan_database_teardown(catalog, db_id)
    })
    .await
    .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))?;

    // Remove every per-database metric series.
    state.database_metrics.remove_database(db_id);

    Ok(status("DROP DATABASE"))
}

/// Refuse a drop without CASCADE of a database that holds collections.
fn refuse_if_collections(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    name: &str,
) -> crate::Result<()> {
    let collections = catalog.load_all_collections(db_id)?;
    let Some(first) = collections.first() else {
        return Ok(());
    };
    Err(crate::Error::DependentObjectsExist {
        tenant_id: first.tenant_id,
        root_kind: "database",
        root_name: name.to_string(),
        dependent_count: collections.len(),
        dependents: collections
            .iter()
            .map(|c| ("collection".to_string(), c.name.clone()))
            .collect(),
    })
}
