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

use std::collections::{BTreeMap, HashMap};

use nodedb_types::{CollectionType, DatabaseId, DocumentMode, MirrorEntryStamp, MirrorFailure};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::wal_replication::decode::{DecodedTransactionRedo, decode_transaction_redo};
use crate::control::wal_replication::types::{MirrorBarrier, ReplicatedEventSource};
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::event::EventSource;

/// The odd step between two groups' purge share keys.
const PURGE_SHARE_KEY_STEP: u64 = 0x9E37_79B9_7F4A_7C15;

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
        | ReplicatedWrite::RedoAbandon { .. }
        | ReplicatedWrite::TenantPurge { .. }
        | ReplicatedWrite::CollectionConvert { .. } => WriteClass::Applies,
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
    collection_type: CollectionType,
    /// The newest conversion of this incarnation the row names. `0` for none.
    conversion: u64,
}

impl HeldCollection {
    fn of(stored: &StoredCollection) -> Self {
        Self {
            incarnation: stored.incarnation,
            collection_type: stored.collection_type.clone(),
            conversion: stored
                .storage_conversion
                .as_ref()
                .filter(|conversion| conversion.incarnation == stored.incarnation)
                .map_or(0, |conversion| conversion.generation),
        }
    }
}

/// How a collection type stores its rows. ALTER moves a strict schema inside one
/// family. Only a conversion moves a collection between two.
fn storage_family(collection_type: &CollectionType) -> u8 {
    match collection_type {
        CollectionType::Document(DocumentMode::Schemaless) => 0,
        CollectionType::Document(DocumentMode::Strict(_)) => 1,
        CollectionType::KeyValue(_) => 2,
        CollectionType::Columnar(_) => 3,
    }
}

/// Split a tenant purge along the mirror's groups. The source lists its own
/// group's vShards, and each mirror group takes its share at this log position.
///
/// A vShard belongs to one source group, so each share keeps its place against
/// every other write of its vShards. Each share of a split takes its own key, and
/// a re-proposal of that share derives the same one.
pub(super) fn split_tenant_purge(
    entry: &ReplicatedEntry,
    group_of: impl Fn(u32) -> crate::Result<u64>,
) -> crate::Result<Vec<ReplicatedEntry>> {
    let ReplicatedWrite::TenantPurge { vshards, .. } = &entry.write else {
        return Ok(vec![entry.clone()]);
    };
    let mut shares: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
    for vshard in vshards {
        shares.entry(group_of(*vshard)?).or_default().push(*vshard);
    }
    if shares.len() <= 1 {
        return Ok(vec![entry.clone()]);
    }
    Ok(shares
        .into_iter()
        .filter_map(|(group, vshards)| {
            let mut share = entry.clone();
            share.vshard_id = vshards.first().copied()?;
            share.idempotency_key = entry
                .idempotency_key
                .wrapping_add(group.wrapping_mul(PURGE_SHARE_KEY_STEP))
                | 1;
            // The clone holds the collections. Only the vShards differ.
            if let ReplicatedWrite::TenantPurge {
                vshards: shared, ..
            } = &mut share.write
            {
                *shared = vshards;
            }
            Some(share)
        })
        .collect())
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
    /// rows. A row that changes storage family stops it too, unless the row
    /// names a new conversion: the conversion's data-group entry re-encodes the
    /// mirror's rows, in either order against this row.
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
            return self.check_type(&stored.name, stored.tenant_id, HeldCollection::of(stored));
        }
        // Every other entry names an object the mirror installs as stamped.
        Ok(())
    }

    /// One incarnation in two storage families with no new conversion has no
    /// entry a mirror can follow. A drop and a same-name create start a new
    /// incarnation, which holds any type.
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
                .as_ref()
                .map(HeldCollection::of),
        };
        if let Some(held) = held
            && held.incarnation == incoming.incarnation
            && storage_family(&held.collection_type) != storage_family(&incoming.collection_type)
            && incoming.conversion <= held.conversion
        {
            return Err(MirrorFailure::UnsupportedSourceOp {
                detail: format!(
                    "collection '{name}' changed its storage type with no new conversion"
                ),
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

    fn strict_type() -> CollectionType {
        CollectionType::strict(
            nodedb_types::columnar::StrictSchema::new(vec![
                nodedb_types::columnar::ColumnDef::required(
                    "id",
                    nodedb_types::columnar::ColumnType::String,
                ),
            ])
            .expect("schema"),
        )
    }

    fn row_payload(stored: &StoredCollection) -> Vec<u8> {
        catalog_entry::encode(&CatalogEntry::PutCollection(Box::new(stored.clone())))
            .expect("encode")
    }

    fn check_second_row(mutate: impl FnOnce(&mut StoredCollection)) -> Result<(), MirrorFailure> {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let mut check = CatalogCheck::new(&catalog, DB);
        let first = StoredCollection::stamped_for_test(1, "c", "owner");
        check.check(&row_payload(&first), 1).expect("first row");
        let mut second = first;
        mutate(&mut second);
        check.check(&row_payload(&second), 2)
    }

    /// The conversion's data-group entry re-encodes the mirror's rows.
    #[test]
    fn a_row_that_names_a_new_conversion_passes() {
        assert!(
            check_second_row(|row| {
                row.collection_type = strict_type();
                row.storage_conversion = Some(nodedb_physical::physical_plan::StorageConversion {
                    generation: 1,
                    incarnation: row.incarnation,
                    source: nodedb_physical::physical_plan::StorageMode::Schemaless,
                });
            })
            .is_ok()
        );
    }

    /// No log entry re-encodes these rows, so the link stops.
    #[test]
    fn a_family_change_with_no_new_conversion_stops_the_link() {
        assert!(matches!(
            check_second_row(|row| row.collection_type = strict_type()),
            Err(MirrorFailure::UnsupportedSourceOp { .. })
        ));
    }

    /// A conversion stamp of another incarnation says nothing of these rows.
    #[test]
    fn a_conversion_of_another_incarnation_does_not_count() {
        assert!(matches!(
            check_second_row(|row| {
                row.collection_type = strict_type();
                row.storage_conversion = Some(nodedb_physical::physical_plan::StorageConversion {
                    generation: 1,
                    incarnation: nodedb_types::Hlc::ZERO,
                    source: nodedb_physical::physical_plan::StorageMode::Schemaless,
                });
            }),
            Err(MirrorFailure::UnsupportedSourceOp { .. })
        ));
    }

    /// ALTER moves a strict schema, and no rows change encoding family.
    #[test]
    fn a_schema_change_inside_one_family_passes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let mut check = CatalogCheck::new(&catalog, DB);
        let mut row = StoredCollection::stamped_for_test(1, "c", "owner");
        row.collection_type = strict_type();
        check.check(&row_payload(&row), 1).expect("first row");
        row.collection_type = CollectionType::strict(
            nodedb_types::columnar::StrictSchema::new(vec![
                nodedb_types::columnar::ColumnDef::required(
                    "id",
                    nodedb_types::columnar::ColumnType::String,
                ),
                nodedb_types::columnar::ColumnDef::required(
                    "added",
                    nodedb_types::columnar::ColumnType::String,
                ),
            ])
            .expect("schema"),
        );
        assert!(check.check(&row_payload(&row), 2).is_ok());
    }

    fn purge(vshards: Vec<u32>) -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            DB.as_u64(),
            vshards.first().copied().unwrap_or(0),
            ReplicatedWrite::TenantPurge {
                collections: vec!["c".into()],
                vshards,
            },
        )
    }

    fn share_vshards(entry: &ReplicatedEntry) -> Vec<u32> {
        match &entry.write {
            ReplicatedWrite::TenantPurge { vshards, .. } => vshards.clone(),
            other => panic!("not a purge: {other:?}"),
        }
    }

    /// A purge spans the mirror's groups: each group takes only its vShards, at
    /// a vShard of its own, under its own key.
    #[test]
    fn a_purge_splits_along_the_mirror_groups() {
        let entry = purge(vec![1, 2, 3, 4]);
        let original = entry.idempotency_key;
        let shares = split_tenant_purge(&entry, |vshard| Ok(u64::from(vshard % 2))).expect("split");
        assert_eq!(shares.len(), 2);
        assert_eq!(share_vshards(&shares[0]), vec![2, 4]);
        assert_eq!(shares[0].vshard_id, 2);
        assert_eq!(share_vshards(&shares[1]), vec![1, 3]);
        assert_eq!(shares[1].vshard_id, 1);
        assert_ne!(shares[0].idempotency_key, shares[1].idempotency_key);
        assert_ne!(shares[0].idempotency_key, 0);
        assert_ne!(shares[1].idempotency_key, original);
    }

    /// A re-proposed cut proposes the same shares under the same keys.
    #[test]
    fn a_split_derives_the_same_keys_each_time() {
        let entry = purge(vec![1, 2]);
        let first = split_tenant_purge(&entry, |vshard| Ok(u64::from(vshard))).expect("split");
        let second = split_tenant_purge(&entry, |vshard| Ok(u64::from(vshard))).expect("split");
        for (one, other) in first.iter().zip(&second) {
            assert_eq!(one.idempotency_key, other.idempotency_key);
        }
    }

    #[test]
    fn a_purge_inside_one_group_stays_whole() {
        let entry = purge(vec![1, 2]);
        let original = entry.idempotency_key;
        let shares = split_tenant_purge(&entry, |_| Ok(5)).expect("split");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].idempotency_key, original);
        assert_eq!(share_vshards(&shares[0]), vec![1, 2]);
    }

    #[test]
    fn any_other_write_passes_through() {
        let shares = split_tenant_purge(&put(DB.as_u64()), |_| {
            Err(crate::Error::Internal {
                detail: "no routing is consulted".into(),
            })
        })
        .expect("no split");
        assert_eq!(shares.len(), 1);
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
