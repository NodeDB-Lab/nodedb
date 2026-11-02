// SPDX-License-Identifier: BUSL-1.1

//! The stamps of the WAL records the replay arms apply.
//!
//! A replay arm sees a record's payload and LSN, not its header. Restart
//! replay registers every record of the WAL here before the arms run, and a
//! committed redo install registers the one record it applies. A write an
//! arm applies then takes its record's vShard.

use std::collections::HashMap;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::types::{Lsn, VShardId};

use super::keys::WriteStamp;

/// WAL record LSN to the stamp the record's writes take.
#[derive(Debug, Default)]
pub struct RecordStamps {
    by_lsn: HashMap<u64, WriteStamp>,
}

impl RecordStamps {
    /// The stamps of `records`, a node's WAL. A `ChangePosition` marker
    /// writes no row and takes no stamp.
    pub fn from_wal(records: &[WalRecord]) -> Self {
        let mut stamps = Self::default();
        for record in records {
            if RecordType::from_raw(record.logical_record_type())
                == Some(RecordType::ChangePosition)
                || record.header.vshard_id >= VShardId::COUNT
            {
                continue;
            }
            stamps.by_lsn.insert(
                record.header.lsn,
                WriteStamp {
                    vshard: VShardId::new(record.header.vshard_id),
                    lsn: Lsn::new(record.header.lsn),
                },
            );
        }
        stamps
    }

    /// Register the record a committed redo install applies.
    pub fn insert(&mut self, stamp: WriteStamp) {
        self.by_lsn.insert(stamp.lsn.as_u64(), stamp);
    }

    /// Forget the record at `lsn`.
    pub fn remove(&mut self, lsn: Lsn) {
        self.by_lsn.remove(&lsn.as_u64());
    }

    /// Forget every record: restart replay finished.
    pub fn clear(&mut self) {
        self.by_lsn.clear();
    }

    /// The stamp of the record at `lsn`, if registered.
    pub fn get(&self, lsn: u64) -> Option<WriteStamp> {
        self.by_lsn.get(&lsn).copied()
    }

    /// `stamp`, or the stamp of its record when the record is registered.
    pub fn resolve(&self, stamp: WriteStamp) -> WriteStamp {
        self.get(stamp.lsn.as_u64()).unwrap_or(stamp)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_wal::record::WalRecordArgs;

    use super::*;
    use crate::event::cdc::position::{ChangePositionMarker, ReplicatedPosition};

    fn record(record_type: RecordType, lsn: u64, vshard: u32) -> WalRecord {
        let payload = match record_type {
            RecordType::ChangePosition => ChangePositionMarker {
                apply_key: 0xAB,
                position: ReplicatedPosition {
                    epoch: 3,
                    group_id: 7,
                    log_index: 100 + lsn,
                },
            }
            .to_bytes()
            .to_vec(),
            _ => vec![1],
        };
        WalRecord::new(WalRecordArgs {
            record_type: record_type as u32,
            lsn,
            tenant_id: 1,
            vshard_id: vshard,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("build record")
    }

    #[test]
    fn a_record_takes_its_header_vshard_and_a_marker_takes_none() {
        let records = vec![
            record(RecordType::ChangePosition, 10, 4),
            record(RecordType::Put, 11, 4),
        ];
        let stamps = RecordStamps::from_wal(&records);
        assert_eq!(
            stamps.get(11),
            Some(WriteStamp {
                vshard: VShardId::new(4),
                lsn: Lsn::new(11),
            })
        );
        assert_eq!(stamps.get(10), None, "a marker writes no row");
    }

    #[test]
    fn resolve_keeps_an_unregistered_stamp() {
        let stamps = RecordStamps::default();
        let stamp = WriteStamp {
            vshard: VShardId::new(2),
            lsn: Lsn::new(5),
        };
        assert_eq!(stamps.resolve(stamp), stamp);
    }
}
