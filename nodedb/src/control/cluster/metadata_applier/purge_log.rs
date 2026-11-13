// SPDX-License-Identifier: BUSL-1.1

//! Purge-log rows of the catalog entries that hard-delete a collection, and
//! the online `CollectionPurged` notice of each.
//!
//! The row is written in the entry's apply, ahead of its post-apply reclaim.
//! A crash between the two re-delivers the entry, and the row write repeats
//! with the same key. Every node applies the entry at the same metadata log
//! index, so every node writes the same row.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::collection_purge_log::{CollectionPurge, StoredPurgeRecord};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Nanoseconds per millisecond, to turn an entry stamp into a purge time.
const NANOS_PER_MILLI: u64 = 1_000_000;

/// The collections `entry` hard-deletes, as purge-log rows at `raft_index`.
///
/// Read after the entry's catalog apply: a purged collection's row stays,
/// inactive, until its reclaim finalizes it. A purge of a name with no row
/// deletes nothing a client can hold, so it has no purge-log row.
pub(super) fn purged_by(
    entry: &CatalogEntry,
    raft_index: u64,
    purged_at_ms: u64,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<CollectionPurge>> {
    let row = |database_id: u64, tenant_id: u64, name: &str| -> crate::Result<_> {
        Ok(catalog
            .get_collection(DatabaseId::new(database_id), tenant_id, name)?
            .map(|stored| CollectionPurge {
                database_id,
                tenant_id,
                purge_index: raft_index,
                name: name.to_string(),
                record: StoredPurgeRecord {
                    incarnation: stored.incarnation,
                    purged_at_ms,
                },
            }))
    };
    Ok(match entry {
        CatalogEntry::PurgeCollection {
            database_id,
            tenant_id,
            name,
            ..
        }
        | CatalogEntry::DeleteMaterializedView {
            database_id,
            tenant_id,
            name,
            ..
        } => row(*database_id, *tenant_id, name)?.into_iter().collect(),
        // The moved collections leave the source database: a client of it
        // holds rows the source no longer has.
        CatalogEntry::MoveTenantCutover {
            tenant_id,
            source_db_id,
            collections,
            ..
        } => collections
            .iter()
            .map(|stored| CollectionPurge {
                database_id: *source_db_id,
                tenant_id: *tenant_id,
                purge_index: raft_index,
                name: stored.name.clone(),
                record: StoredPurgeRecord {
                    incarnation: stored.incarnation,
                    purged_at_ms,
                },
            })
            .collect(),
        _ => Vec::new(),
    })
}

/// Record the purge-log rows of `entry` at `raft_index`. Returns the rows on
/// record after the write: a row a prune already passed is not.
pub(super) fn record_purges(
    shared: &SharedState,
    entry: &CatalogEntry,
    raft_index: u64,
) -> crate::Result<Vec<CollectionPurge>> {
    let catalog = shared.credentials.catalog();
    let purges = purged_by(entry, raft_index, purge_time_ms(), catalog)?;
    let mut recorded = Vec::with_capacity(purges.len());
    for purge in purges {
        if catalog.record_collection_purge(&purge)? {
            recorded.push(purge);
        }
    }
    Ok(recorded)
}

/// Send the online `CollectionPurged` notice of each recorded purge to the
/// sessions of this node that hold the collection. A session whose queue
/// drops the notice gets it from the purge log at its next handshake.
pub(super) fn broadcast_purges(shared: &SharedState, purges: &[CollectionPurge]) {
    for purge in purges {
        shared.crdt_sync_delivery.broadcast_collection_purged(
            purge.tenant_id,
            DatabaseId::new(purge.database_id),
            &purge.name,
            purge.purge_index,
            purge.record.incarnation,
        );
    }
}

/// The purge time of the entry being applied: the wall time of the
/// leader's stamp, the same on every node. An unstamped entry takes this
/// node's clock.
fn purge_time_ms() -> u64 {
    match crate::wal::manager::effect_stamp::effect_stamp() {
        Some(stamp_ns) => stamp_ns / NANOS_PER_MILLI,
        None => crate::control::security::time::now_ms(),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Hlc;

    use super::*;
    use crate::control::security::catalog::StoredCollection;

    fn purge_entry(name: &str) -> CatalogEntry {
        CatalogEntry::PurgeCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: name.to_string(),
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        }
    }

    #[test]
    fn a_purge_of_a_held_row_names_its_incarnation() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        let stored = StoredCollection::stamped_for_test(1, "orders", "admin");
        catalog
            .put_collection(DatabaseId::DEFAULT, &stored)
            .unwrap();

        let purges = purged_by(&purge_entry("orders"), 42, 7, &catalog).unwrap();
        assert_eq!(purges.len(), 1);
        assert_eq!(purges[0].purge_index, 42);
        assert_eq!(purges[0].name, "orders");
        assert_eq!(purges[0].record.incarnation, stored.incarnation);
        assert_eq!(purges[0].record.purged_at_ms, 7);
    }

    #[test]
    fn a_purge_of_an_absent_row_records_nothing() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        assert!(
            purged_by(&purge_entry("never"), 42, 7, &catalog)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_tenant_move_purges_every_moved_collection_from_the_source() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        let entry = CatalogEntry::MoveTenantCutover {
            tenant_id: 1,
            source_db_id: 5,
            target_db_id: 6,
            collections: vec![
                StoredCollection::stamped_for_test(1, "a", "admin"),
                StoredCollection::stamped_for_test(1, "b", "admin"),
            ],
        };
        let purges = purged_by(&entry, 9, 7, &catalog).unwrap();
        let names: Vec<_> = purges
            .iter()
            .map(|p| (p.database_id, p.name.as_str()))
            .collect();
        assert_eq!(names, vec![(5, "a"), (5, "b")]);
    }
}
