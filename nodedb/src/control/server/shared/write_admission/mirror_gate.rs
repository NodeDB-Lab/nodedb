// SPDX-License-Identifier: BUSL-1.1

//! The one read-only decision every write gate calls for a mirror database.
//!
//! A mirror takes its rows and its schema from its source. Every other writer
//! is refused, so the mirror never holds what its source does not.
//!
//! Gates:
//! - the write funnel, for a write with no replicated form;
//! - the leader proposal gate, for every replicated entry;
//! - the Calvin submit, for a sequenced transaction;
//! - the authorized dispatch door and CONVERT, which skip the funnel;
//! - the metadata proposer, for object DDL.
//!
//! Each gate runs before its write has an order. A committed entry applies on
//! every replica with no gate, so no replica refuses what another applied.
//!
//! The mirror applier stamps its writes [`EventSource::RaftFollower`], the one
//! source that passes.

use nodedb_cluster::{METADATA_GROUP_ID, WaitOutcome};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::mirror::MirrorFence;
use crate::control::server::pgwire::ddl::database::{
    MirrorReadOutcome, check_mirror_read_consistency,
};
use crate::control::server::shared::session::read_scope;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{DatabaseId, RequestId};

/// Refuse a write of `source` to a mirror that is not promoted. `is_write`
/// runs only for such a database, so a write elsewhere never classifies its
/// plan.
pub(crate) fn check_mirror_writable(
    state: &SharedState,
    database_id: DatabaseId,
    source: EventSource,
    is_write: impl FnOnce() -> crate::Result<bool>,
) -> crate::Result<()> {
    decide_write(&state.mirror.fence, database_id, source, is_write)
}

fn decide_write(
    fence: &MirrorFence,
    database_id: DatabaseId,
    source: EventSource,
    is_write: impl FnOnce() -> crate::Result<bool>,
) -> crate::Result<()> {
    if !source_is_gated(source) {
        return Ok(());
    }
    let Some(database) = fence.read_only_name(database_id) else {
        return Ok(());
    };
    if is_write()? {
        Err(crate::Error::MirrorReadOnly {
            database: database.to_string(),
        })
    } else {
        Ok(())
    }
}

/// Whether writes of `source` pass the mirror gate.
///
/// A restore differs from the storage quota's rule: it reinstates rows the
/// mirror's source never wrote, so it is refused too.
fn source_is_gated(source: EventSource) -> bool {
    match source {
        EventSource::User
        | EventSource::Trigger
        | EventSource::CrdtSync
        | EventSource::Deferred
        | EventSource::ImplicitClient
        | EventSource::Restore => true,
        EventSource::RaftFollower => false,
    }
}

/// Refuse a catalog commit that changes objects in a mirror.
///
/// A commit that drops the database passes for that database: `DROP DATABASE`
/// carries a delete for each object inside it, ahead of the descriptor's.
pub(crate) fn check_mirror_ddl(state: &SharedState, entries: &[CatalogEntry]) -> crate::Result<()> {
    decide_ddl(&state.mirror.fence, entries)
}

fn decide_ddl(fence: &MirrorFence, entries: &[CatalogEntry]) -> crate::Result<()> {
    for entry in entries {
        for database_id in entry.mirror_scope().into_iter().flatten() {
            let Some(database) = fence.read_only_name(database_id) else {
                continue;
            };
            if !drops_database(entries, database_id) {
                return Err(crate::Error::MirrorReadOnly {
                    database: database.to_string(),
                });
            }
        }
    }
    Ok(())
}

fn drops_database(entries: &[CatalogEntry], database_id: DatabaseId) -> bool {
    entries.iter().any(|entry| {
        matches!(entry, CatalogEntry::DeleteDatabase { db_id } if *db_id == database_id.as_u64())
    })
}

/// Refuse a read the running statement's consistency level cannot take from a
/// mirror that is not promoted.
pub(crate) fn check_mirror_read(state: &SharedState, database_id: DatabaseId) -> crate::Result<()> {
    if state.mirror.fence.read_only_name(database_id).is_none() {
        return Ok(());
    }
    let catalog = state.credentials.catalog();
    let Some(descriptor) = catalog.get_database(database_id)? else {
        return Ok(());
    };
    let Some(origin) = descriptor.mirror_origin.as_ref() else {
        return Ok(());
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis() as u64;
    match check_mirror_read_consistency(catalog, database_id, origin, read_scope::current(), now_ms)
    {
        MirrorReadOutcome::ServeLocally => Ok(()),
        MirrorReadOutcome::Reject { message, .. } => Err(crate::Error::StaleReadNotLeader {
            database: descriptor.name.clone(),
            source_cluster: origin.source_cluster.clone(),
            detail: message,
        }),
    }
}

/// Wait until this node applied the catalog an entry's proposer planned
/// against, so the gate judges the entry by that catalog.
///
/// Without the wait, a leader behind its proposer admits a write to a mirror
/// it has not applied yet, and refuses a write to one already promoted. The
/// wait holds no key. The entry's apply waits for the same floor, so it adds
/// no dependency the entry does not already have.
pub(crate) async fn await_entry_metadata_floor(
    state: &SharedState,
    floor: u64,
    deadline: tokio::time::Instant,
) -> crate::Result<()> {
    let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
    if floor == 0 || watcher.current() >= floor {
        return Ok(());
    }
    match watcher.wait_for_async(floor, deadline.into_std()).await {
        WaitOutcome::Reached => Ok(()),
        WaitOutcome::TimedOut => Err(crate::Error::DeadlineExceeded {
            request_id: RequestId::new(0),
        }),
        WaitOutcome::GroupGone => Err(crate::Error::Internal {
            detail: format!(
                "the metadata group left this node before it applied index {floor}, the catalog \
                 a proposed write was planned against; retry the write on another node"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{MirrorMode, MirrorOrigin, MirrorStatus};

    use super::*;
    use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};

    const MIRROR: u64 = 1100;
    const PLAIN: u64 = 1101;

    fn descriptor(id: u64, status: MirrorStatus) -> DatabaseDescriptor {
        DatabaseDescriptor {
            id: DatabaseId::new(id),
            name: "mirror_db".to_string(),
            status: DatabaseStatus::Mirroring,
            created_at_lsn: 0,
            quota_ref: 0,
            parent_clone: None,
            mirror_origin: Some(MirrorOrigin {
                source_cluster: "source".to_string(),
                source_database: DatabaseId::new(0),
                source_database_name: "source_db".to_string(),
                mode: MirrorMode::Async,
                status,
            }),
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
    }

    fn fence() -> MirrorFence {
        let fence = MirrorFence::new();
        fence.apply_descriptor(&descriptor(MIRROR, MirrorStatus::Following));
        fence
    }

    fn write_to(fence: &MirrorFence, id: u64, source: EventSource) -> crate::Result<()> {
        decide_write(fence, DatabaseId::new(id), source, || Ok(true))
    }

    fn drop_collection(id: u64) -> CatalogEntry {
        CatalogEntry::DeactivateCollection {
            database_id: id,
            tenant_id: 1,
            name: "c".into(),
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn every_client_source_is_refused_naming_the_database() {
        for source in [
            EventSource::User,
            EventSource::Trigger,
            EventSource::CrdtSync,
            EventSource::Deferred,
            EventSource::ImplicitClient,
            EventSource::Restore,
        ] {
            let error = write_to(&fence(), MIRROR, source).unwrap_err();
            assert!(
                matches!(&error, crate::Error::MirrorReadOnly { database } if database == "mirror_db"),
                "{source:?}: {error:?}"
            );
        }
    }

    #[test]
    fn the_applier_source_passes() {
        assert!(write_to(&fence(), MIRROR, EventSource::RaftFollower).is_ok());
    }

    #[test]
    fn a_plain_database_passes_and_never_classifies() {
        let result = decide_write(&fence(), DatabaseId::new(PLAIN), EventSource::User, || {
            panic!("classified a write to a writable database")
        });
        assert!(result.is_ok());
    }

    #[test]
    fn a_non_write_passes() {
        let result = decide_write(&fence(), DatabaseId::new(MIRROR), EventSource::User, || {
            Ok(false)
        });
        assert!(result.is_ok());
    }

    #[test]
    fn a_classification_error_surfaces() {
        let result = decide_write(&fence(), DatabaseId::new(MIRROR), EventSource::User, || {
            Err(crate::Error::Internal {
                detail: "decode".into(),
            })
        });
        assert!(matches!(result, Err(crate::Error::Internal { .. })));
    }

    #[test]
    fn a_promoted_mirror_passes() {
        let fence = fence();
        fence.apply_descriptor(&descriptor(MIRROR, MirrorStatus::Promoted));
        assert!(write_to(&fence, MIRROR, EventSource::User).is_ok());
    }

    #[test]
    fn object_ddl_on_a_mirror_is_refused() {
        let result = decide_ddl(&fence(), &[drop_collection(MIRROR)]);
        assert!(matches!(result, Err(crate::Error::MirrorReadOnly { .. })));
    }

    #[test]
    fn object_ddl_elsewhere_passes() {
        assert!(decide_ddl(&fence(), &[drop_collection(PLAIN)]).is_ok());
    }

    #[test]
    fn a_database_drop_carries_its_objects_past_the_gate() {
        let entries = [
            drop_collection(MIRROR),
            CatalogEntry::DeleteDatabase { db_id: MIRROR },
        ];
        assert!(decide_ddl(&fence(), &entries).is_ok());
    }

    #[test]
    fn dropping_another_database_does_not_carry_a_mirror_object() {
        let entries = [
            drop_collection(MIRROR),
            CatalogEntry::DeleteDatabase { db_id: PLAIN },
        ];
        assert!(decide_ddl(&fence(), &entries).is_err());
    }

    #[test]
    fn a_tenant_move_into_a_mirror_is_refused() {
        let entry = CatalogEntry::MoveTenantCutover {
            tenant_id: 1,
            source_db_id: PLAIN,
            target_db_id: MIRROR,
            collections: Vec::new(),
        };
        assert!(decide_ddl(&fence(), &[entry]).is_err());
    }
}
