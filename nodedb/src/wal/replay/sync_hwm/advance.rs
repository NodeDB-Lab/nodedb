// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for `RecordType::SyncSeqAdvance`: monotonically advance the
//! per-core sync HWM maps from WAL records.
//!
//! Mirrors the surrogate alloc replay pattern in
//! `wal/replay/surrogate/alloc.rs`: iterate records, decode the fixed-width
//! payload, fold into in-memory maps with max-wins semantics, and return the
//! populated maps to the caller for installation onto `CoreLoop` state.

use std::collections::HashMap;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;
use nodedb_wal::record::SyncSeqAdvancePayload;

/// Reconstructed sync HWM state produced by replaying WAL records.
///
/// Handed to the caller (e.g. `spawn_core`) which installs the maps onto the
/// freshly-opened `CoreLoop` before the core enters its event loop.
#[derive(Debug, Default, Clone)]
pub struct SyncHwmReplayMaps {
    /// Max `seq` seen per `(producer_id, stream_id)`.
    pub sync_hwm: HashMap<(u64, u64), u64>,
    /// Max `epoch` seen per `producer_id`.
    pub producer_epoch_floor: HashMap<u64, u64>,
}

/// Statistics returned by [`replay_sync_hwm_records`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncHwmReplayStats {
    /// Number of `SyncSeqAdvance` records processed.
    pub records: usize,
}

/// Apply a single decoded [`SyncSeqAdvancePayload`] into the mutable maps.
///
/// Uses max-wins semantics: the HWM and epoch floor can only move forward.
/// Called by [`replay_sync_hwm_records`] for each `SyncSeqAdvance` record and
/// also directly from tests.
pub fn apply_sync_seq_advance(payload: &SyncSeqAdvancePayload, maps: &mut SyncHwmReplayMaps) {
    // Advance seq HWM: max(current, payload.seq).
    let hwm_entry = maps
        .sync_hwm
        .entry((payload.producer_id, payload.stream_id))
        .or_insert(0);
    if payload.seq > *hwm_entry {
        *hwm_entry = payload.seq;
    }

    // Advance epoch floor: max(current, payload.epoch).
    let epoch_entry = maps
        .producer_epoch_floor
        .entry(payload.producer_id)
        .or_insert(0);
    if payload.epoch > *epoch_entry {
        *epoch_entry = payload.epoch;
    }
}

/// Replay every `SyncSeqAdvance` record in `records`, folding payloads into the
/// returned [`SyncHwmReplayMaps`] with max-wins semantics.
///
/// Run once at startup before the core enters its event loop so
/// post-restart deduplication is correct. Other record types are ignored
/// (this pass is deliberately narrow — surrogate replay and engine WAL replay
/// are separate passes).
pub fn replay_sync_hwm_records(
    records: &[WalRecord],
) -> crate::Result<(SyncHwmReplayMaps, SyncHwmReplayStats)> {
    let mut maps = SyncHwmReplayMaps::default();
    let mut stats = SyncHwmReplayStats::default();

    for record in records {
        let raw = record.logical_record_type();
        let Some(rt) = RecordType::from_raw(raw) else {
            continue;
        };
        match rt {
            RecordType::SyncSeqAdvance => {
                let payload = SyncSeqAdvancePayload::from_bytes(&record.payload)
                    .map_err(crate::Error::Wal)?;
                apply_sync_seq_advance(&payload, &mut maps);
                stats.records += 1;
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
            // CrdtListOp / CrdtDocOp carry no sync HWM data; not relevant to
            // this idempotency replay pass.
            | RecordType::CrdtListOp
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
            | RecordType::SurrogateAlloc
            | RecordType::SurrogateBind
            | RecordType::FtsIndex
            | RecordType::FtsDelete
            | RecordType::SpatialPut
            | RecordType::SpatialDelete
            | RecordType::GraphNodeLabelSet
            | RecordType::GraphNodeLabelRemove
            // WriteAborted carries only a refused write's LSN, no sync HWM.
            | RecordType::WriteAborted
            // ProposalApplied carries only an applied Raft proposal's identity.
            | RecordType::ProposalApplied => {}
        }
    }

    Ok((maps, stats))
}

#[cfg(test)]
mod tests {
    use nodedb_wal::WalRecord;
    use nodedb_wal::record::{RecordType, SyncSeqAdvancePayload};

    use super::*;

    fn make_record(producer_id: u64, epoch: u64, stream_id: u64, seq: u64) -> WalRecord {
        let payload_bytes = SyncSeqAdvancePayload::new(producer_id, epoch, stream_id, seq)
            .to_bytes()
            .to_vec();
        WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::SyncSeqAdvance as u32,
            lsn: 0,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: payload_bytes,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("WalRecord::new")
    }

    #[test]
    fn empty_records_produces_empty_maps() {
        let (maps, stats) = replay_sync_hwm_records(&[]).unwrap();
        assert!(maps.sync_hwm.is_empty());
        assert!(maps.producer_epoch_floor.is_empty());
        assert_eq!(stats.records, 0);
    }

    #[test]
    fn single_record_populates_maps() {
        let records = [make_record(1, 3, 7, 42)];
        let (maps, stats) = replay_sync_hwm_records(&records).unwrap();
        assert_eq!(maps.sync_hwm.get(&(1, 7)).copied().unwrap_or(0), 42);
        assert_eq!(maps.producer_epoch_floor.get(&1).copied().unwrap_or(0), 3);
        assert_eq!(stats.records, 1);
    }

    #[test]
    fn max_wins_semantics_for_seq() {
        // Two records for the same (producer, stream): higher seq wins.
        let records = [make_record(1, 1, 1, 5), make_record(1, 1, 1, 10)];
        let (maps, _) = replay_sync_hwm_records(&records).unwrap();
        assert_eq!(maps.sync_hwm.get(&(1, 1)).copied().unwrap_or(0), 10);
    }

    #[test]
    fn max_wins_semantics_for_seq_reverse_order() {
        // Higher seq appears first — result must still be max.
        let records = [make_record(1, 1, 1, 10), make_record(1, 1, 1, 3)];
        let (maps, _) = replay_sync_hwm_records(&records).unwrap();
        assert_eq!(maps.sync_hwm.get(&(1, 1)).copied().unwrap_or(0), 10);
    }

    #[test]
    fn max_wins_semantics_for_epoch() {
        // Two producers; epoch should be max per producer.
        let records = [
            make_record(10, 2, 1, 1),
            make_record(10, 7, 1, 2),
            make_record(20, 5, 1, 1),
        ];
        let (maps, stats) = replay_sync_hwm_records(&records).unwrap();
        assert_eq!(maps.producer_epoch_floor.get(&10).copied().unwrap_or(0), 7);
        assert_eq!(maps.producer_epoch_floor.get(&20).copied().unwrap_or(0), 5);
        assert_eq!(stats.records, 3);
    }

    #[test]
    fn multiple_streams_same_producer() {
        let records = [
            make_record(1, 1, 10, 100),
            make_record(1, 1, 20, 200),
            make_record(1, 1, 10, 150),
        ];
        let (maps, _) = replay_sync_hwm_records(&records).unwrap();
        assert_eq!(maps.sync_hwm.get(&(1, 10)).copied().unwrap_or(0), 150);
        assert_eq!(maps.sync_hwm.get(&(1, 20)).copied().unwrap_or(0), 200);
    }

    #[test]
    fn apply_sync_seq_advance_is_max_wins() {
        let mut maps = SyncHwmReplayMaps::default();
        let payload = SyncSeqAdvancePayload::new(5, 3, 9, 50);
        apply_sync_seq_advance(&payload, &mut maps);
        assert_eq!(maps.sync_hwm.get(&(5, 9)).copied().unwrap_or(0), 50);
        // Applying a lower seq should not regress.
        let payload_lower = SyncSeqAdvancePayload::new(5, 3, 9, 20);
        apply_sync_seq_advance(&payload_lower, &mut maps);
        assert_eq!(maps.sync_hwm.get(&(5, 9)).copied().unwrap_or(0), 50);
        // Applying a higher seq should advance.
        let payload_higher = SyncSeqAdvancePayload::new(5, 4, 9, 99);
        apply_sync_seq_advance(&payload_higher, &mut maps);
        assert_eq!(maps.sync_hwm.get(&(5, 9)).copied().unwrap_or(0), 99);
        assert_eq!(maps.producer_epoch_floor.get(&5).copied().unwrap_or(0), 4);
    }
}
