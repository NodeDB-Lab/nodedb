// SPDX-License-Identifier: BUSL-1.1

//! Which replicated position each locally applied WAL record reproduces.
//!
//! A Data-Plane change event names its write by the local WAL LSN of the
//! write's record. That LSN differs on every replica. The Raft log position
//! of the entry the record applies is the same on every replica, so the CDC
//! router positions events by it. A committed Calvin slice installs from a
//! data-group entry, so its records take that entry's position too.
//!
//! The write path records `record LSN -> position` before it hands the write
//! to a core, so the entry exists before any event of the write can reach
//! the Event Plane. Boot rebuilds the ledger from the WAL: a Raft entry's
//! records link to its `ChangePosition` marker by proposal key. A WAL
//! catch-up that rebuilds an event after a restart therefore positions it as
//! the ring did.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::marker::{ChangePositionMarker, ReplicatedPosition};

/// Records the ledger keeps before it evicts the lowest LSN. An event whose
/// record was evicted before the Event Plane routed it takes its partition's
/// current position instead (see `CdcRouter`).
pub const CHANGE_POSITION_CAPACITY: usize = 1 << 18;

/// Local record LSN to replicated position, bounded.
#[derive(Debug)]
pub struct ChangePositionLedger {
    positions: RwLock<BTreeMap<u64, ReplicatedPosition>>,
    /// Record LSN to the commit HLC (wall nanoseconds) of the write the
    /// record belongs to. Every replica holds the same value for a
    /// replicated write, so change events date by it.
    commit_hlcs: RwLock<BTreeMap<u64, u64>>,
    capacity: usize,
    /// Set once this node applies Raft data-group entries. A node that never
    /// does positions its other events by local WAL LSN.
    replicated: AtomicBool,
}

impl Default for ChangePositionLedger {
    fn default() -> Self {
        Self::new(CHANGE_POSITION_CAPACITY)
    }
}

impl ChangePositionLedger {
    pub fn new(capacity: usize) -> Self {
        Self {
            positions: RwLock::new(BTreeMap::new()),
            commit_hlcs: RwLock::new(BTreeMap::new()),
            capacity: capacity.max(1),
            replicated: AtomicBool::new(false),
        }
    }

    /// Record that the WAL record at `record_lsn` applies the Raft entry at
    /// `position`.
    pub fn record(&self, record_lsn: u64, position: ReplicatedPosition) {
        self.mark_replicated();
        let mut positions = self.positions.write().unwrap_or_else(|p| p.into_inner());
        positions.insert(record_lsn, position);
        while positions.len() > self.capacity {
            positions.pop_first();
        }
    }

    /// Record that the WAL record at `record_lsn` belongs to a write that
    /// committed at `commit_hlc`.
    pub fn record_commit_hlc(&self, record_lsn: u64, commit_hlc: u64) {
        let mut hlcs = self.commit_hlcs.write().unwrap_or_else(|p| p.into_inner());
        hlcs.insert(record_lsn, commit_hlc);
        while hlcs.len() > self.capacity {
            hlcs.pop_first();
        }
    }

    /// The commit HLC of the write the record at `record_lsn` belongs to, if
    /// known.
    pub fn commit_hlc(&self, record_lsn: u64) -> Option<u64> {
        self.commit_hlcs
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&record_lsn)
            .copied()
    }

    /// The replicated position of the record at `record_lsn`, if known.
    pub fn get(&self, record_lsn: u64) -> Option<ReplicatedPosition> {
        self.positions
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&record_lsn)
            .copied()
    }

    /// Declare that this node positions change events by replicated
    /// position. The data-group apply loop calls this before it applies an
    /// entry.
    pub fn mark_replicated(&self) {
        self.replicated.store(true, Ordering::Release);
    }

    pub fn is_replicated(&self) -> bool {
        self.replicated.load(Ordering::Acquire)
    }

    /// Rebuild the ledger from `records`, the node's replayed WAL in LSN
    /// order. Returns how many records it positioned.
    ///
    /// A record whose header key names a preceding `ChangePosition` marker
    /// takes that marker's Raft position.
    pub fn recover(&self, records: &[WalRecord]) -> usize {
        let mut by_key: HashMap<u64, ReplicatedPosition> = HashMap::new();
        let mut positioned = 0;
        for record in records {
            let record_type = RecordType::from_raw(record.logical_record_type());
            if record_type == Some(RecordType::ChangePosition) {
                match ChangePositionMarker::from_bytes(&record.payload) {
                    Ok(marker) if marker.apply_key != 0 => {
                        by_key.insert(marker.apply_key, marker.position);
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(
                        lsn = record.header.lsn,
                        %error,
                        "skipping an undecodable ChangePosition WAL marker"
                    ),
                }
                continue;
            }
            if record.header.commit_hlc != 0 {
                self.record_commit_hlc(record.header.lsn, record.header.commit_hlc);
            }
            let key = record.apply_key();
            if key != 0
                && let Some(position) = by_key.get(&key)
            {
                self.record(record.header.lsn, *position);
                positioned += 1;
            }
        }
        if !by_key.is_empty() {
            self.mark_replicated();
        }
        positioned
    }

    pub fn len(&self) -> usize {
        self.positions
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(log_index: u64) -> ReplicatedPosition {
        ReplicatedPosition {
            epoch: 0,
            group_id: 3,
            log_index,
        }
    }

    fn raft(log_index: u64) -> Option<ReplicatedPosition> {
        Some(position(log_index))
    }

    #[test]
    fn records_and_reads_back_a_position() {
        let ledger = ChangePositionLedger::new(8);
        assert!(!ledger.is_replicated());
        ledger.record(100, position(7));
        assert!(ledger.is_replicated());
        assert_eq!(ledger.get(100), raft(7));
        assert_eq!(ledger.get(101), None);
    }

    fn record(record_type: RecordType, lsn: u64, apply_key: u64, payload: Vec<u8>) -> WalRecord {
        WalRecord::new_stamped(
            nodedb_wal::record::WalRecordArgs {
                record_type: record_type as u32,
                lsn,
                tenant_id: 1,
                vshard_id: 0,
                database_id: 0,
                payload,
                encryption_key: None,
                preamble_bytes: None,
            },
            nodedb_wal::record::RecordStamp {
                apply_key,
                event_source: nodedb_wal::NO_EVENT_SOURCE,
                commit_hlc: 0,
            },
        )
        .expect("build WAL record")
    }

    #[test]
    fn recovery_links_each_keyed_record_to_its_marker() {
        let marker = |apply_key, log_index| {
            ChangePositionMarker {
                apply_key,
                position: position(log_index),
            }
            .to_bytes()
            .to_vec()
        };
        let records = vec![
            record(RecordType::ChangePosition, 1, 0, marker(0xA, 40)),
            record(RecordType::Put, 2, 0xA, b"row".to_vec()),
            record(RecordType::Put, 3, 0, b"local".to_vec()),
            record(RecordType::ChangePosition, 4, 0, marker(0xB, 41)),
            record(RecordType::Put, 5, 0xB, b"row".to_vec()),
            record(RecordType::Put, 6, 0xB, b"row".to_vec()),
        ];
        let ledger = ChangePositionLedger::new(16);
        assert_eq!(ledger.recover(&records), 3);
        assert!(ledger.is_replicated());
        assert_eq!(ledger.get(2), raft(40));
        assert_eq!(ledger.get(3), None);
        assert_eq!(ledger.get(5), raft(41));
        assert_eq!(ledger.get(6), raft(41));
        // The markers themselves name no event.
        assert_eq!(ledger.get(1), None);
    }

    #[test]
    fn a_record_dates_by_its_commit_hlc() {
        let ledger = ChangePositionLedger::new(2);
        ledger.record_commit_hlc(10, 7_000_000);
        assert_eq!(ledger.commit_hlc(10), Some(7_000_000));
        assert_eq!(ledger.commit_hlc(11), None);
        let stamped = WalRecord::new_stamped(
            nodedb_wal::record::WalRecordArgs {
                record_type: RecordType::Put as u32,
                lsn: 12,
                tenant_id: 1,
                vshard_id: 0,
                database_id: 0,
                payload: b"row".to_vec(),
                encryption_key: None,
                preamble_bytes: None,
            },
            nodedb_wal::record::RecordStamp {
                apply_key: 0,
                event_source: nodedb_wal::NO_EVENT_SOURCE,
                commit_hlc: 9_000_000,
            },
        )
        .expect("build WAL record");
        ledger.recover(&[stamped]);
        assert_eq!(ledger.commit_hlc(12), Some(9_000_000));
    }

    #[test]
    fn capacity_evicts_the_lowest_lsn() {
        let ledger = ChangePositionLedger::new(2);
        ledger.record(30, position(3));
        ledger.record(10, position(1));
        ledger.record(20, position(2));
        assert_eq!(ledger.len(), 2);
        assert_eq!(ledger.get(10), None);
        assert_eq!(ledger.get(20), raft(2));
        assert_eq!(ledger.get(30), raft(3));
    }
}
