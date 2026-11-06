// SPDX-License-Identifier: BUSL-1.1

//! What the applier checks and changes on each source entry.
//!
//! The applier runs every check before the entry enters the mirror's log, so a
//! poison entry never reaches a replica.
//!
//! The rewrite touches the event source, the metadata floor and the stamp. The
//! rest stays byte for byte: the key dedupes a re-proposed copy, the ordinal and
//! the write's HLC are the source's, and each carried surrogate installs as it
//! came. The applier mints no surrogate: nothing here draws from the assigner.

use std::collections::HashMap;

use nodedb_types::{DatabaseId, MirrorEntryStamp, MirrorFailure};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::security::catalog::SystemCatalog;
use crate::control::wal_replication::decode::{DecodedTransactionRedo, decode_transaction_redo};
use crate::control::wal_replication::types::{MirrorBarrier, ReplicatedEventSource};
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::event::EventSource;

/// What one source entry is to the applier.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum WriteClass<'a> {
    /// The entry applies on the mirror.
    Applies,
    /// A mirror cut's barrier.
    Barrier(&'a MirrorBarrier),
    /// The source's filter must have dropped the entry.
    Refused(&'static str),
}

/// Sort `write`. The match is exhaustive, so a new write decides its place here.
pub(super) fn classify_write(write: &ReplicatedWrite) -> WriteClass<'_> {
    match write {
        ReplicatedWrite::CutBarrier {
            mirror: Some(barrier),
            ..
        } => WriteClass::Barrier(barrier),
        ReplicatedWrite::CutBarrier { mirror: None, .. } => WriteClass::Refused("a backup's cut"),
        ReplicatedWrite::CalvinReadResult { .. } => WriteClass::Refused("a Calvin read result"),
        ReplicatedWrite::CalvinReadTimeout { .. } => WriteClass::Refused("a Calvin read timeout"),
        ReplicatedWrite::MirrorSeal { .. } => WriteClass::Refused("another mirror's seal"),
        ReplicatedWrite::PointPut { .. }
        | ReplicatedWrite::PointInsert { .. }
        | ReplicatedWrite::PointDelete { .. }
        | ReplicatedWrite::PointUpdate { .. }
        | ReplicatedWrite::DocUpsert { .. }
        | ReplicatedWrite::DocBatchUpsert { .. }
        | ReplicatedWrite::DocBatchInsert { .. }
        | ReplicatedWrite::VectorInsert { .. }
        | ReplicatedWrite::VectorBatchInsert { .. }
        | ReplicatedWrite::VectorDelete { .. }
        | ReplicatedWrite::SetVectorParams { .. }
        | ReplicatedWrite::SparseInsert { .. }
        | ReplicatedWrite::SparseDelete { .. }
        | ReplicatedWrite::MultiVectorInsert { .. }
        | ReplicatedWrite::MultiVectorDelete { .. }
        | ReplicatedWrite::DeleteBySurrogate { .. }
        | ReplicatedWrite::DirectUpsert { .. }
        | ReplicatedWrite::CrdtApply { .. }
        | ReplicatedWrite::ColumnarIngest { .. }
        | ReplicatedWrite::TimeseriesIngest { .. }
        | ReplicatedWrite::FtsIndex { .. }
        | ReplicatedWrite::FtsDelete { .. }
        | ReplicatedWrite::SpatialInsert { .. }
        | ReplicatedWrite::SpatialDelete { .. }
        | ReplicatedWrite::EdgePut { .. }
        | ReplicatedWrite::EdgeDelete { .. }
        | ReplicatedWrite::SetNodeLabels { .. }
        | ReplicatedWrite::RemoveNodeLabels { .. }
        | ReplicatedWrite::EdgePutBatch { .. }
        | ReplicatedWrite::EdgeDeleteBatch { .. }
        | ReplicatedWrite::KvPut { .. }
        | ReplicatedWrite::KvDelete { .. }
        | ReplicatedWrite::KvInsert { .. }
        | ReplicatedWrite::KvInsertOnConflictUpdate { .. }
        | ReplicatedWrite::KvBatchPut { .. }
        | ReplicatedWrite::KvExpire { .. }
        | ReplicatedWrite::KvPersist { .. }
        | ReplicatedWrite::KvIncr { .. }
        | ReplicatedWrite::KvIncrFloat { .. }
        | ReplicatedWrite::KvCas { .. }
        | ReplicatedWrite::KvGetSet { .. }
        | ReplicatedWrite::KvRegisterSortedIndex { .. }
        | ReplicatedWrite::KvDropSortedIndex { .. }
        | ReplicatedWrite::KvRegisterIndex { .. }
        | ReplicatedWrite::KvDropIndex { .. }
        | ReplicatedWrite::KvFieldSet { .. }
        | ReplicatedWrite::KvTransfer { .. }
        | ReplicatedWrite::KvTransferItem { .. }
        | ReplicatedWrite::ArrayOp { .. }
        | ReplicatedWrite::ArraySchema { .. }
        | ReplicatedWrite::BulkDml { .. }
        | ReplicatedWrite::ColumnarBulkDml { .. }
        | ReplicatedWrite::CrdtImportCollection { .. }
        | ReplicatedWrite::CrdtListInsert { .. }
        | ReplicatedWrite::CrdtListDelete { .. }
        | ReplicatedWrite::CrdtListMove { .. }
        | ReplicatedWrite::CrdtDocUpsert { .. }
        | ReplicatedWrite::CrdtDocDelete { .. }
        | ReplicatedWrite::DocTruncate { .. }
        | ReplicatedWrite::KvTruncate { .. }
        | ReplicatedWrite::ConstraintChange { .. }
        | ReplicatedWrite::ArrayCellPut { .. }
        | ReplicatedWrite::ArrayCellDelete { .. }
        | ReplicatedWrite::CrdtApplyFenced { .. }
        | ReplicatedWrite::CrdtApplyAuthenticated { .. }
        | ReplicatedWrite::DropVectorIndex { .. }
        | ReplicatedWrite::ApplyBalanceDelta { .. }
        | ReplicatedWrite::ColumnarBulkDmlResolved { .. }
        | ReplicatedWrite::ColumnarTruncate { .. }
        | ReplicatedWrite::TimeseriesTruncate { .. }
        | ReplicatedWrite::TimeseriesAggregateRefresh { .. }
        | ReplicatedWrite::KvResolvedWrite { .. }
        | ReplicatedWrite::DocumentResolvedWrite { .. }
        | ReplicatedWrite::VectorDirectDelete { .. }
        | ReplicatedWrite::VectorDirectTruncate { .. }
        | ReplicatedWrite::VectorDirectUpdate { .. }
        | ReplicatedWrite::VectorResolvedDirectWrite { .. }
        | ReplicatedWrite::MergeApply { .. }
        | ReplicatedWrite::UpdateFromJoinApply { .. }
        | ReplicatedWrite::TransactionRedo { .. }
        | ReplicatedWrite::TopicPublish { .. }
        | ReplicatedWrite::SurrogateBind { .. }
        | ReplicatedWrite::RedoChunk { .. }
        | ReplicatedWrite::RedoAbandon { .. } => WriteClass::Applies,
    }
}

/// What one fetched source entry holds.
#[derive(Debug)]
pub(super) enum CheckedEntry {
    Data(Box<ReplicatedEntry>),
    Barrier {
        hlc: u64,
        barrier: Box<MirrorBarrier>,
    },
}

fn poison(group: u64, index: u64, detail: impl Into<String>) -> MirrorFailure {
    MirrorFailure::PoisonEntry {
        source_group: group,
        source_index: index,
        detail: detail.into(),
    }
}

/// Decode and check one fetched entry of `group`. A failure here stops the link
/// with the entry outside the mirror's log.
pub(super) fn check_data_entry(
    data: &[u8],
    group: u64,
    index: u64,
    database: DatabaseId,
) -> Result<CheckedEntry, MirrorFailure> {
    let entry = ReplicatedEntry::from_bytes(data)
        .ok_or_else(|| poison(group, index, "the bytes are not a replicated entry"))?;
    match classify_write(&entry.write) {
        WriteClass::Applies => {}
        WriteClass::Barrier(_) => {
            let ReplicatedWrite::CutBarrier {
                hlc,
                mirror: Some(barrier),
                ..
            } = entry.write
            else {
                return Err(poison(group, index, "a barrier with no mark"));
            };
            return Ok(CheckedEntry::Barrier {
                hlc,
                barrier: Box::new(barrier),
            });
        }
        WriteClass::Refused(what) => {
            return Err(poison(
                group,
                index,
                format!("the source sent {what}, which its filter drops"),
            ));
        }
    }
    if entry.database_id != database.as_u64() {
        return Err(poison(
            group,
            index,
            format!(
                "the entry names database {}, not the link's {}",
                entry.database_id,
                database.as_u64()
            ),
        ));
    }
    if entry.mirror.is_some() {
        return Err(poison(group, index, "the entry already carries a stamp"));
    }
    if matches!(entry.write, ReplicatedWrite::TransactionRedo { .. }) {
        match decode_transaction_redo(&entry.write) {
            Ok(DecodedTransactionRedo::Inline(_) | DecodedTransactionRedo::Chunked(_)) => {}
            Err(error) => return Err(poison(group, index, error.to_string())),
        }
    }
    Ok(CheckedEntry::Data(Box::new(entry)))
}

/// Make `entry` the mirror's: it applies as a follower's write, once its cut is
/// ready. `metadata_floor` is the mirror's own applied metadata index. A
/// source's index names nothing in the mirror's metadata log.
pub(super) fn rewrite_entry(
    entry: &mut ReplicatedEntry,
    stamp: MirrorEntryStamp,
    metadata_floor: u64,
) {
    let follower = ReplicatedEventSource::from(EventSource::RaftFollower);
    entry.event_source = follower;
    if let ReplicatedWrite::TransactionRedo { event_source, .. } = &mut entry.write {
        *event_source = follower;
    }
    entry.metadata_floor = metadata_floor;
    entry.mirror = Some(stamp);
}

/// Checks a cut's catalog entries against the mirror's catalog, in log order.
pub(super) struct CatalogCheck<'a> {
    catalog: &'a SystemCatalog,
    database: DatabaseId,
    /// Each collection's incarnation and type as this batch left them.
    seen: HashMap<(u64, String), HeldCollection>,
}

#[derive(Clone)]
struct HeldCollection {
    incarnation: nodedb_types::Hlc,
    collection_type: nodedb_types::CollectionType,
}

impl<'a> CatalogCheck<'a> {
    pub(super) fn new(catalog: &'a SystemCatalog, database: DatabaseId) -> Self {
        Self {
            catalog,
            database,
            seen: HashMap::new(),
        }
    }

    /// Check one source catalog payload.
    ///
    /// A drop or a tenant move stops the link: neither reaches the mirror's
    /// rows. A changed collection type stops it too: CONVERT re-encodes rows on
    /// the source's cores, with no log entry a mirror can follow.
    pub(super) fn check(&mut self, payload: &[u8], index: u64) -> Result<(), MirrorFailure> {
        let entry = catalog_entry::decode(payload)
            .map_err(|error| poison(nodedb_cluster::METADATA_GROUP_ID, index, error.to_string()))?;
        if matches!(entry, CatalogEntry::DeleteDatabase { .. }) {
            return Err(MirrorFailure::SourceDropped);
        }
        if matches!(entry, CatalogEntry::MoveTenantCutover { .. }) {
            return Err(MirrorFailure::UnsupportedSourceOp {
                detail: "MOVE TENANT names this database".to_string(),
            });
        }
        if let CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) =
            &entry
        {
            return self.check_type(
                &stored.name,
                stored.tenant_id,
                HeldCollection {
                    incarnation: stored.incarnation,
                    collection_type: stored.collection_type.clone(),
                },
            );
        }
        // Every other entry names an object the mirror installs as stamped.
        Ok(())
    }

    /// One incarnation with two types is a CONVERT. A drop and a same-name
    /// create start a new incarnation, which holds any type.
    fn check_type(
        &mut self,
        name: &str,
        tenant_id: u64,
        incoming: HeldCollection,
    ) -> Result<(), MirrorFailure> {
        let key = (tenant_id, name.to_string());
        let held = match self.seen.get(&key) {
            Some(held) => Some(held.clone()),
            None => self
                .catalog
                .get_collection(self.database, tenant_id, name)
                .map_err(|error| MirrorFailure::ApplyError {
                    source_group: nodedb_cluster::METADATA_GROUP_ID,
                    source_index: 0,
                    detail: error.to_string(),
                })?
                .map(|stored| HeldCollection {
                    incarnation: stored.incarnation,
                    collection_type: stored.collection_type,
                }),
        };
        if let Some(held) = held
            && held.incarnation == incoming.incarnation
            && held.collection_type != incoming.collection_type
        {
            return Err(MirrorFailure::UnsupportedSourceOp {
                detail: format!("CONVERT changed the storage type of collection '{name}'"),
            });
        }
        self.seen.insert(key, incoming);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::types::MirrorCalvinFloor;

    const DB: DatabaseId = DatabaseId::new(1400);

    fn put(database: u64) -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            database,
            2,
            ReplicatedWrite::PointPut {
                collection: "docs".into(),
                document_id: "d".into(),
                value: vec![1],
                surrogate: 77,
                resolved_sum_targets: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
        )
    }

    fn stamp() -> MirrorEntryStamp {
        MirrorEntryStamp {
            source_group: 3,
            source_index: 9,
            cut: 5,
            epoch: 6,
        }
    }

    #[test]
    fn the_rewrite_keeps_what_the_source_decided() {
        let mut entry = put(DB.as_u64());
        entry.system_ordinal = 41;
        entry.write_hlc = 42;
        entry.metadata_floor = 43;
        entry.user_roles = vec!["role".into()];
        let key = entry.idempotency_key;

        rewrite_entry(&mut entry, stamp(), 8);

        assert_eq!(entry.idempotency_key, key);
        assert_eq!(entry.system_ordinal, 41);
        assert_eq!(entry.write_hlc, 42);
        assert_eq!(entry.user_roles, vec!["role".to_string()]);
        assert_eq!(entry.metadata_floor, 8, "the mirror's own floor");
        assert_eq!(entry.mirror, Some(stamp()));
        assert_eq!(
            entry.event_source,
            ReplicatedEventSource::from(EventSource::RaftFollower)
        );
        let ReplicatedWrite::PointPut { surrogate, .. } = entry.write else {
            panic!("the write changed shape");
        };
        assert_eq!(surrogate, 77, "the carried surrogate installs as it came");
    }

    #[test]
    fn bytes_that_are_no_entry_are_poison() {
        assert!(matches!(
            check_data_entry(b"not an entry", 3, 9, DB),
            Err(MirrorFailure::PoisonEntry {
                source_group: 3,
                source_index: 9,
                ..
            })
        ));
    }

    #[test]
    fn another_databases_entry_is_poison() {
        let bytes = put(1401).encode().expect("encode");
        assert!(matches!(
            check_data_entry(&bytes, 3, 9, DB),
            Err(MirrorFailure::PoisonEntry { .. })
        ));
    }

    #[test]
    fn what_the_filter_drops_is_poison() {
        let timeout = ReplicatedEntry::new(
            1,
            DB.as_u64(),
            2,
            ReplicatedWrite::CalvinReadTimeout {
                epoch: 1,
                position: 2,
                tenant_id: 1,
            },
        );
        let bytes = timeout.encode().expect("encode");
        assert!(matches!(
            check_data_entry(&bytes, 3, 9, DB),
            Err(MirrorFailure::PoisonEntry { .. })
        ));
    }

    #[test]
    fn a_stamped_entry_is_poison() {
        let mut entry = put(DB.as_u64());
        entry.mirror = Some(stamp());
        let bytes = entry.encode().expect("encode");
        assert!(matches!(
            check_data_entry(&bytes, 3, 9, DB),
            Err(MirrorFailure::PoisonEntry { .. })
        ));
    }

    #[test]
    fn a_barrier_gives_its_mark() {
        let mark = nodedb_types::MirrorCutMark {
            seq: 11,
            cap_ordinal: 12,
            metadata_index: 13,
        };
        let entry = ReplicatedEntry::new(
            0,
            0,
            2,
            ReplicatedWrite::CutBarrier {
                hlc: 14,
                restore_point: 0,
                capture: None,
                mirror: Some(MirrorBarrier {
                    mark,
                    calvin_floors: vec![MirrorCalvinFloor {
                        vshard: 2,
                        epoch: 15,
                    }],
                }),
            },
        );
        let bytes = entry.encode().expect("encode");
        match check_data_entry(&bytes, 3, 9, DB).expect("a barrier") {
            CheckedEntry::Barrier { hlc, barrier } => {
                assert_eq!(hlc, 14);
                assert_eq!(barrier.mark, mark);
                assert_eq!(barrier.calvin_floors.len(), 1);
            }
            CheckedEntry::Data(entry) => panic!("expected a barrier, got {entry:?}"),
        }
    }

    #[test]
    fn a_data_entry_passes() {
        let bytes = put(DB.as_u64()).encode().expect("encode");
        assert!(matches!(
            check_data_entry(&bytes, 3, 9, DB),
            Ok(CheckedEntry::Data(_))
        ));
    }

    #[test]
    fn a_dropped_source_and_a_tenant_move_stop_the_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let mut check = CatalogCheck::new(&catalog, DB);
        let drop = catalog_entry::encode(&CatalogEntry::DeleteDatabase { db_id: DB.as_u64() })
            .expect("encode");
        assert_eq!(check.check(&drop, 4), Err(MirrorFailure::SourceDropped));
        assert!(matches!(
            check.check(b"not a payload", 4),
            Err(MirrorFailure::PoisonEntry { .. })
        ));
    }
}
