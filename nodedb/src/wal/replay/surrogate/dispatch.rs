// SPDX-License-Identifier: BUSL-1.1

//! Dispatch loop for replaying `SurrogateAlloc` / `SurrogateBind` WAL records.

use nodedb_types::{DatabaseId, TenantId};
use nodedb_wal::record::{RecordType, SurrogateBindPayload};
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::control::security::catalog::SystemCatalog;
use crate::control::surrogate::SurrogateRegistryHandle;

use super::{apply_surrogate_alloc, apply_surrogate_bind};

/// Replay every `SurrogateAlloc` and `SurrogateBind` record in `records`
/// into the live `SurrogateRegistry` + `SystemCatalog`. Run once at
/// startup, after the registry has been seeded from the catalog hwm row
/// and after the catalog has been opened — replay then advances both
/// past the WAL's tail so any binding emitted before the crash is
/// durable on the next allocation.
///
/// `tombstones` gates the bind arm exactly as it gates every engine arm: a
/// collection dropped at or after a bind's LSN had its `(pk → surrogate)` rows
/// deleted from the catalog, and re-writing them here would resurrect
/// pre-drop identities into a collection that has since been recreated —
/// making a fresh row's PK resolve to a dead surrogate.
///
/// The alloc arm is deliberately NOT gated: it carries no collection to gate
/// on, and its only effect is raising the allocator watermark, which must
/// happen whether or not the collection it was allocated for still exists.
pub fn replay_surrogate_records(
    records: &[WalRecord],
    catalog: &SystemCatalog,
    registry: &SurrogateRegistryHandle,
    tombstones: &TombstoneSet,
) -> crate::Result<ReplayStats> {
    let mut stats = ReplayStats::default();
    for record in records {
        let raw = record.logical_record_type();
        let Some(rt) = RecordType::from_raw(raw) else {
            continue;
        };
        match rt {
            RecordType::SurrogateAlloc => {
                apply_surrogate_alloc(&record.payload, registry)?;
                stats.allocs += 1;
            }
            RecordType::SurrogateBind => {
                let db = DatabaseId::new(record.header.database_id);
                let tenant = TenantId::new(record.header.tenant_id);
                // Decoded here only to read the collection name for the gate;
                // `apply_surrogate_bind` decodes the payload itself so the
                // authoritative parse stays in one place.
                let parsed = SurrogateBindPayload::from_bytes(&record.payload)
                    .map_err(crate::Error::Wal)?;
                if tombstones.is_tombstoned(
                    record.header.database_id,
                    record.header.tenant_id,
                    &parsed.collection,
                    record.header.lsn,
                ) {
                    stats.binds_skipped += 1;
                    continue;
                }
                apply_surrogate_bind(&record.payload, db, tenant, catalog, registry)?;
                stats.binds += 1;
            }
            RecordType::Noop
            | RecordType::Put
            | RecordType::Delete
            | RecordType::VectorPut
            | RecordType::VectorDelete
            | RecordType::VectorParams
            | RecordType::VectorIndexDrop
            | RecordType::VectorDirectUpsert
            | RecordType::VectorDirectDelete
            | RecordType::VectorDirectUpdate
            | RecordType::VectorDirectTruncate
            | RecordType::ColumnarTruncate
            | RecordType::TimeseriesTruncate
            | RecordType::VectorResolvedDirectWrite
            | RecordType::SparseVectorPut
            | RecordType::SparseVectorDelete
            | RecordType::MultiVectorPut
            | RecordType::MultiVectorDelete
            | RecordType::CrdtDelta
            // CrdtListOp carries no surrogate; the list ops it replays never
            // read `surrogate` (see `CrdtOp::ListInsert`/`ListDelete`/
            // `ListMove`'s dispatch arms, all `surrogate: _`).
            | RecordType::CrdtListOp
            // CrdtDocOp carries the row's own surrogate, but that surrogate was
            // allocated upstream and made durable via SurrogateAlloc/Bind — this
            // record never allocates one, so it does not advance the hwm here.
            | RecordType::CrdtDocOp
            | RecordType::TimeseriesBatch
            | RecordType::LogBatch
            | RecordType::ArrayPut
            | RecordType::ArrayDelete
            | RecordType::ArrayFlush
            | RecordType::Transaction
            | RecordType::TransactionRedo
            | RecordType::Checkpoint
            | RecordType::CollectionTombstoned
            | RecordType::LsnMsAnchor
            | RecordType::TemporalPurge
            | RecordType::CalvinApplied
            // SyncSeqAdvance: not relevant to surrogate replay; the sync
            // idempotency replay pass handles HWM reconstruction.
            | RecordType::SyncSeqAdvance
            | RecordType::FtsIndex
            | RecordType::FtsDelete
            | RecordType::SpatialPut
            | RecordType::SpatialDelete
            | RecordType::GraphNodeLabelSet
            | RecordType::GraphNodeLabelRemove
            // WriteAborted only names a refused write's LSN; the record it
            // names is already gone from this stream (the replay source drops
            // it), and the marker itself binds no surrogate.
            | RecordType::WriteAborted => {}
        }
    }
    Ok(stats)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    pub allocs: usize,
    pub binds: usize,
    /// Binds skipped because their collection was dropped at or after the
    /// record's LSN.
    pub binds_skipped: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::surrogate::SurrogateRegistry;
    use nodedb_wal::record::WalRecordArgs;
    use std::sync::{Arc, RwLock};

    fn bind_record(lsn: u64, collection: &str, pk: &[u8], surrogate: u32) -> WalRecord {
        let payload = SurrogateBindPayload::new(surrogate, collection, pk.to_vec())
            .to_bytes()
            .expect("encode bind payload");
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::SurrogateBind as u32,
            lsn,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    fn open_test() -> (tempfile::TempDir, SystemCatalog, SurrogateRegistryHandle) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).expect("open catalog");
        let reg: SurrogateRegistryHandle = Arc::new(RwLock::new(SurrogateRegistry::new()));
        (dir, cat, reg)
    }

    #[test]
    fn untombstoned_bind_is_applied() {
        let (_dir, cat, reg) = open_test();
        let stats = replay_surrogate_records(
            &[bind_record(10, "users", b"alice", 7)],
            &cat,
            &reg,
            &TombstoneSet::new(),
        )
        .expect("replay");
        assert_eq!(stats.binds, 1);
        assert_eq!(stats.binds_skipped, 0);
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, TenantId::new(0), "users", b"alice")
                .unwrap(),
            Some(nodedb_types::Surrogate::new(7))
        );
    }

    /// A dropped-and-recreated collection must not have its pre-drop
    /// `(pk → surrogate)` rows written back: the drop deleted them, and
    /// resurrecting them makes a fresh row's PK resolve to a dead surrogate.
    #[test]
    fn bind_for_a_tombstoned_collection_is_not_resurrected() {
        let (_dir, cat, reg) = open_test();
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(0, 0, "users".to_string(), 50);

        let stats = replay_surrogate_records(
            &[bind_record(10, "users", b"alice", 7)],
            &cat,
            &reg,
            &tombstones,
        )
        .expect("replay");
        assert_eq!(stats.binds, 0);
        assert_eq!(stats.binds_skipped, 1);
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, TenantId::new(0), "users", b"alice")
                .unwrap(),
            None,
            "a pre-drop binding must stay deleted"
        );
    }

    /// A bind written AFTER the drop belongs to the recreated collection and
    /// must still apply.
    #[test]
    fn bind_after_the_drop_still_applies() {
        let (_dir, cat, reg) = open_test();
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(0, 0, "users".to_string(), 50);

        let stats = replay_surrogate_records(
            &[bind_record(60, "users", b"bob", 11)],
            &cat,
            &reg,
            &tombstones,
        )
        .expect("replay");
        assert_eq!(stats.binds, 1);
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, TenantId::new(0), "users", b"bob")
                .unwrap(),
            Some(nodedb_types::Surrogate::new(11))
        );
    }

    /// Replaying the same tail twice must leave the catalog and the watermark
    /// exactly where one pass did.
    #[test]
    fn replaying_the_same_records_twice_is_a_no_op() {
        let (_dir, cat, reg) = open_test();
        let records = vec![bind_record(10, "users", b"alice", 7)];
        replay_surrogate_records(&records, &cat, &reg, &TombstoneSet::new()).expect("first");
        let hwm_after_first = reg.read().expect("read lock").current_hwm();
        replay_surrogate_records(&records, &cat, &reg, &TombstoneSet::new()).expect("second");
        assert_eq!(
            reg.read().expect("read lock").current_hwm(),
            hwm_after_first
        );
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, TenantId::new(0), "users", b"alice")
                .unwrap(),
            Some(nodedb_types::Surrogate::new(7))
        );
    }
}
