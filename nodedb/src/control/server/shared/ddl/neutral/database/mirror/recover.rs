// SPDX-License-Identifier: BUSL-1.1

//! Handlers for `ALTER DATABASE <name> MIRROR RESUME` and `MIRROR REBOOTSTRAP`.
//!
//! A failed mirror keeps serving reads at its last ready cut.
//!
//! - `RESUME` retries the link from its replicated positions. It needs a failure
//!   that leaves those positions good: the operator fixed the cause.
//! - `REBOOTSTRAP` starts over, because a position no longer names the source's
//!   log, or a re-issue stopped part way. One metadata commit holds the delete of
//!   every object in the database and the status step. The step clears the cut
//!   state, keeping the fence, so a copy of a former epoch skips on every replica.
//!   Every replica then reclaims each collection's storage before it applies the
//!   next metadata entry, and the new capture's writes name a floor past that
//!   commit. The database's grants stay: they are this cluster's.

use nodedb_types::{MirrorStatus, MirrorStatusStep};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::{propose_catalog_batch_async, propose_catalog_entry_async};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::gate::require_superuser;
use super::super::support::{ddl_err, status};
use super::super::teardown::plan_database_objects_teardown;

/// The status of `name`, once the statement passed its gate.
fn gated_status(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    db_id: DatabaseId,
    operation: &str,
) -> Result<MirrorStatus, DdlError> {
    require_superuser(
        state,
        identity,
        Some(db_id),
        &format!("ALTER DATABASE {name} MIRROR {operation}"),
    )?;
    state
        .credentials
        .catalog()
        .get_database(db_id)
        .map_err(|e| DdlError::from_error_in_context("catalog read failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?
        .mirror_origin
        .map(|origin| origin.status)
        .ok_or_else(|| {
            ddl_err(
                "0A000",
                format!("database '{name}' is not a mirror database"),
            )
        })
}

/// Handle `ALTER DATABASE <name> MIRROR RESUME`. Required role: `Superuser`.
pub async fn resume_mirror(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    db_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    match gated_status(state, identity, name, db_id, "RESUME")? {
        MirrorStatus::Failed { failure } if failure.is_resumable() => {}
        MirrorStatus::Failed { failure } => {
            return Err(ddl_err(
                "55000",
                format!(
                    "mirror database '{name}' cannot resume after: {failure}; run ALTER DATABASE \
                     {name} MIRROR REBOOTSTRAP"
                ),
            ));
        }
        other => {
            return Err(ddl_err(
                "55000",
                format!("mirror database '{name}' has not failed; its status is {other:?}"),
            ));
        }
    }
    propose_catalog_entry_async(
        state,
        &CatalogEntry::StepMirrorStatus {
            database_id: db_id.as_u64(),
            step: MirrorStatusStep::Resume,
        },
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))?;
    Ok(status("ALTER DATABASE"))
}

/// Handle `ALTER DATABASE <name> MIRROR REBOOTSTRAP`. Required role: `Superuser`.
pub async fn rebootstrap_mirror(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    db_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    match gated_status(state, identity, name, db_id, "REBOOTSTRAP")? {
        MirrorStatus::Failed { .. } => {}
        other => {
            return Err(ddl_err(
                "55000",
                format!(
                    "mirror database '{name}' has not failed, and a rebuild removes its rows; \
                     its status is {other:?}"
                ),
            ));
        }
    }
    propose_catalog_batch_async(state, |catalog| {
        let mut plan = plan_database_objects_teardown(catalog, db_id)?;
        plan.push(CatalogEntry::StepMirrorStatus {
            database_id: db_id.as_u64(),
            step: MirrorStatusStep::Rebootstrap,
        });
        Ok(plan)
    })
    .await
    .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))?;
    Ok(status("ALTER DATABASE"))
}
