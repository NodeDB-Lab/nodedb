// SPDX-License-Identifier: BUSL-1.1

//! Ordered startup replay for all CRDT WAL record classes.
//!
//! CRDT deltas, document intents, and list intents all mutate the same Loro
//! state. They must therefore be replayed in one global WAL order rather than
//! in separate record-type passes.

use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};
use crate::wal::{CrdtDeltaWalPayload, CrdtDocOpWalRecord, CrdtListOpWalRecord};

/// The collection a CRDT record writes, `None` when its payload does not
/// decode or names none.
fn crdt_record_collection(record: &nodedb_wal::WalRecord) -> Option<String> {
    match RecordType::from_raw(record.logical_record_type())? {
        RecordType::CrdtDelta => {
            CrdtDeltaWalPayload::decode(&record.payload)
                .ok()?
                .collection
        }
        RecordType::CrdtListOp => {
            match zerompk::from_msgpack::<CrdtListOpWalRecord>(&record.payload).ok()? {
                CrdtListOpWalRecord::Insert { collection, .. }
                | CrdtListOpWalRecord::Delete { collection, .. }
                | CrdtListOpWalRecord::Move { collection, .. } => Some(collection),
            }
        }
        RecordType::CrdtDocOp => {
            match zerompk::from_msgpack::<CrdtDocOpWalRecord>(&record.payload).ok()? {
                CrdtDocOpWalRecord::Upsert { collection, .. }
                | CrdtDocOpWalRecord::Delete { collection, .. } => Some(collection),
            }
        }
        _ => None,
    }
}

impl CoreLoop {
    /// The committed-redo step before a CRDT record's write. In the validate
    /// pass it claims a record that names its collection and returns `false`.
    /// In the install pass it records the collection's Loro pre-image and
    /// returns whether the write proceeds. A record naming no collection is
    /// left unclaimed, so the validate pass refuses the record.
    fn redo_crdt_prelude(&mut self, record: &nodedb_wal::WalRecord) -> bool {
        let Some(collection) = crdt_record_collection(record) else {
            return false;
        };
        if self.claim_for_validation() {
            return false;
        }
        let captured = self.capture_crdt_collection_undo(
            DatabaseId::new(record.header.database_id),
            TenantId::new(record.header.tenant_id),
            &collection,
        );
        self.record_redo_capture(captured.map(std::iter::once))
    }

    /// Replay CRDT WAL records in stable global LSN order.
    ///
    /// `records` holds the standalone CRDT records and the CRDT intents a
    /// committed transaction journalled as `TransactionRedo` sub-records. A
    /// transaction never journals a raw delta apply: that is refused inside a
    /// transaction, because a raw apply requires the serialized admission
    /// boundary.
    pub fn replay_crdt_wal_ordered(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let mut crdt_records: Vec<(usize, &nodedb_wal::WalRecord)> = records
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                matches!(
                    RecordType::from_raw(record.logical_record_type()),
                    Some(RecordType::CrdtDelta | RecordType::CrdtListOp | RecordType::CrdtDocOp)
                )
            })
            .collect();
        crdt_records.sort_by_key(|(original_index, record)| (record.header.lsn, *original_index));

        for (_, record) in crdt_records {
            if self.applying_committed_redo() && !self.redo_crdt_prelude(record) {
                continue;
            }
            match RecordType::from_raw(record.logical_record_type()) {
                Some(RecordType::CrdtDelta) => {
                    let _ = self.try_replay_crdt_delta(record, num_cores, tombstones);
                }
                Some(RecordType::CrdtListOp) => {
                    let _ = self.try_replay_crdt_list(record, num_cores, tombstones);
                }
                Some(RecordType::CrdtDocOp) => {
                    let _ = self.try_replay_crdt_doc(record, num_cores, tombstones);
                }
                // The filter above admits only these three record types. Keep
                // this defensive no-op rather than panicking on a future enum
                // change or malformed logical type.
                _ => continue,
            }
        }
    }
}
