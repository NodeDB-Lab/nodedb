// SPDX-License-Identifier: BUSL-1.1

//! Handler for `ALTER DATABASE <name> PROMOTE`.
//!
//! The catalog lookup, superuser gate (after db-id resolution), idempotent
//! already-promoted short-circuit, not-a-mirror rejection, observer-link
//! teardown BEFORE the descriptor mutation, status flip, Raft propose /
//! single-node fallback, and `DatabasePromoted` audit run here. The result is
//! the protocol-neutral [`DdlResult`].

use nodedb_types::MirrorStatus;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::database_types::DatabaseStatus;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::gate::require_superuser;
use super::super::support::{ddl_err, status};

/// Handle `ALTER DATABASE <name> PROMOTE`.
///
/// Required role: `Superuser`.
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
    let mut descriptor = catalog
        .get_database(db_id)
        .map_err(|e| DdlError::from_error_in_context("catalog read failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    // Idempotent: if already promoted (or Active without any mirror_origin),
    // return success immediately.
    let already_promoted = match &descriptor.mirror_origin {
        Some(origin) => matches!(origin.status, MirrorStatus::Promoted),
        None => descriptor.status == DatabaseStatus::Active,
    };
    if already_promoted {
        return Ok(status("ALTER DATABASE"));
    }

    // Reject PROMOTE on a database that is not a mirror at all
    // (not Mirroring status and has no mirror_origin).
    if descriptor.mirror_origin.is_none() && descriptor.status != DatabaseStatus::Mirroring {
        return Err(ddl_err(
            "0A000",
            format!("database '{name}' is not a mirror database"),
        ));
    }

    // Tear down the cross-cluster observer link BEFORE the descriptor
    // mutation lands. This ensures there is no window where the database
    // is Promoted (writes accepted) but the observer is still streaming
    // entries from the source.
    //
    // If the teardown fails — e.g. the source is unreachable and the link
    // was never established, or it already dropped due to a disconnect —
    // we log and continue. The operator's intent is to stop following; the
    // link will be garbage-collected when the Arc reference count reaches
    // zero. The database must become writable regardless.
    state.mirror.links.teardown_link(db_id);

    // Flip status to Promoted in the mirror_origin record and set
    // the database status to Active so writes are accepted.
    if let Some(ref mut origin) = descriptor.mirror_origin {
        origin.status = MirrorStatus::Promoted;
    }
    descriptor.status = DatabaseStatus::Active;

    // Persist atomically through Raft. On restart the descriptor is reloaded
    // with status=Active + origin.status=Promoted, so the database remains
    // writable without any further intervention. Every node's apply of this
    // entry removes the cut state. `mirror_origin` stays as lineage.
    propose_catalog_entry_async(
        state,
        &CatalogEntry::PutDatabase(Box::new(descriptor.clone())),
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))?;

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabasePromoted,
        None,
        Some(db_id),
        &identity.username,
        &format!("ALTER DATABASE {name} PROMOTE"),
    );

    Ok(status("ALTER DATABASE"))
}
