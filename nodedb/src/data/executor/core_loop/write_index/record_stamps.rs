// SPDX-License-Identifier: BUSL-1.1

//! The stamps of the WAL records the replay arms apply.
//!
//! A replay arm sees a record's payload and LSN, not its header. Restart
//! replay registers every record of the WAL here before the arms run, and a
//! committed redo install registers the one record it applies. A write an
//! arm applies then takes its record's vShard and the log position of the
//! data-group entry the record applies.
//!
//! A replicated write's records follow a `ChangePosition` marker. The marker
//! names the entry's position and the proposal key every record of the entry
//! carries in its header. Restart replay recovers each record's position by
//! that key, so a recovered write records the version its live apply did.

use std::collections::HashMap;

use nodedb_types::WriteVersion;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::event::cdc::position::ChangePositionMarker;
use crate::types::{Lsn, VShardId};

use super::keys::WriteStamp;

/// WAL record LSN to the stamp the record's writes take.
#[derive(Debug, Default)]
pub struct RecordStamps {
    by_lsn: HashMap<u64, WriteStamp>,
    /// The highest version each vShard's registered records hold.
    latest: HashMap<VShardId, WriteVersion>,
}

impl RecordStamps {
    /// The stamps of `records`, a node's WAL. A record that follows the
    /// `ChangePosition` marker of its proposal key takes the marker's entry
    /// position. Every other record names no entry.
    pub fn from_wal(records: &[WalRecord]) -> Self {
        let mut ordered: Vec<&WalRecord> = records.iter().collect();
        ordered.sort_by_key(|record| record.header.lsn);

        let mut stamps = Self::default();
        let mut entry_of_key: HashMap<u64, WriteVersion> = HashMap::new();
        for record in ordered {
            if RecordType::from_raw(record.logical_record_type())
                == Some(RecordType::ChangePosition)
            {
                match ChangePositionMarker::from_bytes(&record.payload) {
                    Ok(marker) if marker.apply_key != 0 => {
                        entry_of_key.insert(
                            marker.apply_key,
                            WriteVersion::logged(marker.position.epoch, marker.position.log_index),
                        );
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
            if record.header.vshard_id >= VShardId::COUNT {
                continue;
            }
            let vshard = VShardId::new(record.header.vshard_id);
            let key = record.apply_key();
            let entry = (key != 0)
                .then(|| entry_of_key.get(&key).copied())
                .flatten();
            let latest = stamps.latest.entry(vshard).or_default();
            *latest = (*latest).max(
                entry.unwrap_or_else(|| WriteVersion::local_after(*latest, record.header.lsn)),
            );
            stamps.by_lsn.insert(
                record.header.lsn,
                WriteStamp {
                    vshard,
                    lsn: Lsn::new(record.header.lsn),
                    entry,
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
        self.latest.clear();
    }

    /// The stamp of the record at `lsn`, if registered.
    pub fn get(&self, lsn: u64) -> Option<WriteStamp> {
        self.by_lsn.get(&lsn).copied()
    }

    /// `stamp`, or the stamp of its record when the record is registered.
    pub fn resolve(&self, stamp: WriteStamp) -> WriteStamp {
        self.get(stamp.lsn.as_u64()).unwrap_or(stamp)
    }

    /// The highest version each vShard's registered WAL records hold.
    pub fn latest_by_vshard(&self) -> impl Iterator<Item = (VShardId, WriteVersion)> + '_ {
        self.latest
            .iter()
            .map(|(vshard, version)| (*vshard, *version))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_wal::record::WalRecordArgs;

    use super::*;
    use crate::event::cdc::position::ReplicatedPosition;

    fn record(record_type: RecordType, lsn: u64, vshard: u32, apply_key: u64) -> WalRecord {
        let payload = match record_type {
            RecordType::ChangePosition => ChangePositionMarker {
                apply_key,
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
        let header_key = match record_type {
            RecordType::ChangePosition => 0,
            _ => apply_key,
        };
        WalRecord::new_stamped(
            WalRecordArgs {
                record_type: record_type as u32,
                lsn,
                tenant_id: 1,
                vshard_id: vshard,
                database_id: 0,
                payload,
                encryption_key: None,
                preamble_bytes: None,
            },
            nodedb_wal::record::RecordStamp {
                apply_key: header_key,
                event_source: nodedb_wal::NO_EVENT_SOURCE,
                commit_hlc: 0,
            },
        )
        .expect("build record")
    }

    #[test]
    fn a_record_after_its_marker_takes_the_entry_position() {
        let records = vec![
            record(RecordType::ChangePosition, 10, 4, 0xAB),
            record(RecordType::Put, 11, 4, 0xAB),
            record(RecordType::Put, 12, 4, 0),
        ];
        let stamps = RecordStamps::from_wal(&records);

        let replicated = stamps.get(11).expect("registered");
        assert_eq!(replicated.vshard, VShardId::new(4));
        assert_eq!(replicated.entry, Some(WriteVersion::logged(3, 110)));

        let local = stamps.get(12).expect("registered");
        assert_eq!(local.entry, None);
        assert_eq!(stamps.get(10), None, "a marker writes no row");

        let latest: HashMap<VShardId, WriteVersion> = stamps.latest_by_vshard().collect();
        assert_eq!(
            latest.get(&VShardId::new(4)),
            Some(&WriteVersion::local_after(WriteVersion::logged(3, 110), 12))
        );
    }

    #[test]
    fn resolve_keeps_an_unregistered_stamp() {
        let stamps = RecordStamps::default();
        let stamp = WriteStamp {
            vshard: VShardId::new(2),
            lsn: Lsn::new(5),
            entry: None,
        };
        assert_eq!(stamps.resolve(stamp), stamp);
    }
}
