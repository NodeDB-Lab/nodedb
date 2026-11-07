// SPDX-License-Identifier: BUSL-1.1

//! Handler for `ALTER DATABASE <name> PROMOTE`.
//!
//! The catalog lookup, the superuser gate (after db-id resolution), the
//! idempotent already-promoted answer, the not-a-mirror and first-capture
//! refusals, and the `DatabasePromoted` audit run here. The steps of the
//! promotion live in `control::mirror::applier::promote`. The result is the
//! protocol-neutral [`DdlResult`].

use nodedb_types::MirrorStatus;

use crate::control::mirror::applier::{finish_promote, seal_mirror};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::gate::require_superuser;
use super::super::support::{ddl_err, status};

/// Handle `ALTER DATABASE <name> PROMOTE`.
///
/// Required role: `Superuser`.
///
/// A statement that ends in an error after the seal leaves a sealed mirror. The
/// metadata leader finishes it, and running the statement again waits for it.
pub async fn promote_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    // Gate after db_id resolution so the audit record carries the database id.
    require_superuser(
        state,
        identity,
        Some(db_id),
        &format!("ALTER DATABASE {name} PROMOTE"),
    )?;

    // A name row whose descriptor is gone is a database a concurrent DROP
    // removed between the two reads.
    let descriptor = catalog
        .get_database(db_id)
        .map_err(|e| DdlError::from_error_in_context("catalog read failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    match descriptor
        .mirror_origin
        .as_ref()
        .map(|origin| &origin.status)
    {
        // A promoted mirror keeps its origin row, so a real promotion never
        // lands here: a repeat takes the `Promoted` arm.
        None => {
            return Err(ddl_err(
                "0A000",
                format!("database '{name}' is not a mirror database"),
            ));
        }
        Some(MirrorStatus::Promoted) => return Ok(status("ALTER DATABASE")),
        Some(MirrorStatus::Bootstrapping { .. }) => {
            return Err(ddl_err(
                "55000",
                format!(
                    "mirror database '{name}' is taking its first capture and holds no \
                     consistent state; wait for it to finish, or DROP DATABASE"
                ),
            ));
        }
        Some(
            MirrorStatus::Following
            | MirrorStatus::Degraded
            | MirrorStatus::Disconnected
            | MirrorStatus::Failed { .. }
            | MirrorStatus::Promoting { .. },
        ) => {}
    }

    let final_cut = seal_mirror(state, db_id)
        .await
        .map_err(|e| DdlError::from_error_in_context("PROMOTE did not seal the mirror", &e))?;
    finish_promote(state, db_id)
        .await
        .map_err(|e| DdlError::from_error_in_context("PROMOTE did not finish", &e))?;

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabasePromoted,
        None,
        Some(db_id),
        &identity.username,
        &format!("ALTER DATABASE {name} PROMOTE at source cut {final_cut}"),
    );

    Ok(status("ALTER DATABASE"))
}
