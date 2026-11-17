// SPDX-License-Identifier: BUSL-1.1

//! Boot's read of the WAL for the entries whose change events a restart
//! stages again.
//!
//! Every replicated write the funnel applies appends its entry's
//! `ChangePosition` marker before its first record. The marker's tail names
//! the rows the write yields, or that its `TransactionRedo` record names them
//! (see `MarkerChanges`). Boot keeps an entry when:
//!
//! - a record of the entry's proposal key, other than a `WriteAborted`
//!   marker, outlived replay: the write's effect replays, and a restart
//!   skips its entry as a repeat. A final refusal leaves only its keyed
//!   `WriteAborted` marker, so it publishes nothing. A versioned point write
//!   cancels its own forward record with a keyed marker too, and keeps the
//!   group parts that carry its rows, so it counts as applied;
//! - no marker of the key withholds its events: a refusal that kept its
//!   records publishes nothing.
//!
//! A chunked redo's marker names its entry in its stream reference. Its row
//! changes stay in the stream's chunks, so the entry keeps the reference
//! when the WAL still holds the first chunk.
//!
//! Boot reads every such entry, whatever its group's saved floor. The end of
//! boot's data-group replay releases each one no delivery restaged.

use std::collections::{HashMap, HashSet};

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::event::cdc::position::{ChangePositionMarker, MarkerChanges};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalFloorHolds;
use crate::wal::{RedoRecord, RedoRowChange, RedoStreamRef};

use super::store::{RecoveredChanges, UnjournaledEntries};

/// Where a marker's rows are.
enum MarkedTail {
    Rows(Vec<RedoRowChange>),
    OfRedo,
}

/// The last marker of one proposal key whose tail names its rows.
struct MarkedRows {
    marker: ChangePositionMarker,
    lsn: u64,
    tenant_id: u64,
    database_id: u64,
    vshard_id: u32,
    commit_hlc: u64,
    tail: MarkedTail,
}

/// What one pass over the WAL found.
#[derive(Default)]
struct WalScan {
    marked: HashMap<u64, MarkedRows>,
    withheld: HashSet<u64>,
    applied: HashSet<u64>,
    chunk_lsns: HashSet<u64>,
    /// The position in the records of each key's last `TransactionRedo`
    /// record.
    redo_records: HashMap<u64, usize>,
}

impl WalScan {
    fn of(records: &[WalRecord]) -> crate::Result<Self> {
        let mut scan = Self::default();
        for (at, record) in records.iter().enumerate() {
            let key = record.apply_key();
            match RecordType::from_raw(record.logical_record_type()) {
                Some(RecordType::ChangePosition) => scan.note_marker(record)?,
                // An abort marker is no effect of its write.
                Some(RecordType::WriteAborted) => {}
                Some(RecordType::RedoChunk) => {
                    scan.chunk_lsns.insert(record.header.lsn);
                    if key != 0 {
                        scan.applied.insert(key);
                    }
                }
                Some(RecordType::TransactionRedo) if key != 0 => {
                    scan.applied.insert(key);
                    scan.redo_records.insert(key, at);
                }
                _ if key != 0 => {
                    scan.applied.insert(key);
                }
                _ => {}
            }
        }
        Ok(scan)
    }

    fn note_marker(&mut self, record: &WalRecord) -> crate::Result<()> {
        let marker = ChangePositionMarker::from_bytes(&record.payload).map_err(|error| {
            crate::Error::Serialization {
                format: "change position marker".into(),
                detail: format!("WAL record at lsn {}: {error}", record.header.lsn),
            }
        })?;
        if marker.apply_key == 0 {
            return Ok(());
        }
        let tail = match MarkerChanges::of_payload(&record.payload)? {
            // A marker a boot journalled again for a write whose origin a
            // crash cut, when its plan named no row.
            None => return Ok(()),
            Some(MarkerChanges::Withheld) => {
                self.withheld.insert(marker.apply_key);
                return Ok(());
            }
            Some(MarkerChanges::Rows(rows)) => MarkedTail::Rows(rows),
            Some(MarkerChanges::OfRedo) => MarkedTail::OfRedo,
        };
        self.marked.insert(
            marker.apply_key,
            MarkedRows {
                marker,
                lsn: record.header.lsn,
                tenant_id: record.header.tenant_id,
                database_id: record.header.database_id,
                vshard_id: record.header.vshard_id,
                commit_hlc: record.header.commit_hlc,
                tail,
            },
        );
        Ok(())
    }
}

impl UnjournaledEntries {
    /// Keep every applied entry of `records`, the WAL replay reads at boot,
    /// whose change events a delivery stages again. Each holds its records
    /// in the WAL through `holds`. Returns how many it kept.
    ///
    /// Runs at boot, before the data-group apply loop starts.
    pub fn recover(&self, records: &[WalRecord], holds: &WalFloorHolds) -> crate::Result<usize> {
        let scan = WalScan::of(records)?;
        let mut streams: HashMap<(u64, u64), RedoStreamRef> = HashMap::new();
        for (_, reference) in crate::wal::redo::markers_in(records)? {
            if let (Some(entry), Some(first)) = (reference.entry, reference.first_lsn())
                && scan.chunk_lsns.contains(&first)
            {
                streams.insert((entry.group_id, entry.log_index), reference);
            }
        }
        let mut kept = 0usize;
        for (key, marked) in scan.marked {
            if !scan.applied.contains(&key) || scan.withheld.contains(&key) {
                continue;
            }
            let rows = match marked.tail {
                MarkedTail::Rows(rows) => rows,
                MarkedTail::OfRedo => redo_rows(records, &scan.redo_records, key, marked.lsn)?,
            };
            let at = (
                marked.marker.position.group_id,
                marked.marker.position.log_index,
            );
            let stream = streams.remove(&at);
            let mut entry_holds = vec![holds.hold(Lsn::new(marked.lsn))];
            if let Some(first) = stream.as_ref().and_then(RedoStreamRef::first_lsn) {
                entry_holds.push(holds.hold(Lsn::new(first)));
            }
            self.insert_recovered(
                at.0,
                at.1,
                RecoveredChanges {
                    lsn: Lsn::new(marked.lsn),
                    tenant_id: TenantId::new(marked.tenant_id),
                    database_id: DatabaseId::new(marked.database_id),
                    vshard_id: VShardId::new(marked.vshard_id),
                    commit_hlc: marked.commit_hlc,
                    rows,
                    stream,
                },
                entry_holds,
            );
            kept += 1;
        }
        Ok(kept)
    }
}

/// The row changes the `TransactionRedo` record of proposal `key` names:
/// the applied write whose marker at `marker_lsn` says its record holds
/// them.
fn redo_rows(
    records: &[WalRecord],
    redo_records: &HashMap<u64, usize>,
    key: u64,
    marker_lsn: u64,
) -> crate::Result<Vec<RedoRowChange>> {
    let record = redo_records
        .get(&key)
        .and_then(|at| records.get(*at))
        .filter(|record| record.header.lsn > marker_lsn)
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "the change marker at lsn {marker_lsn} names its write's TransactionRedo \
                 record, and the WAL holds no such record after it for proposal {key:#x}"
            ),
        })?;
    Ok(RedoRecord::from_bytes(&record.payload)?.row_changes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::position::ReplicatedPosition;
    use crate::wal::RedoRowKind;
    use crate::wal::WalManager;
    use crate::wal::manager::NO_APPLY_KEY;

    const TENANT: TenantId = TenantId::new(1);
    const VSHARD: VShardId = VShardId::new(0);
    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn row(id: &str) -> RedoRowChange {
        RedoRowChange {
            collection: "orders".into(),
            row: id.into(),
            kind: RedoRowKind::Insert,
        }
    }

    /// Append the marker of the entry at `log_index` of group 4 under
    /// `key`, with `changes` as its tail. Returns its LSN.
    fn marker(wal: &WalManager, key: u64, log_index: u64, changes: &MarkerChanges) -> Lsn {
        let head = ChangePositionMarker {
            apply_key: key,
            position: ReplicatedPosition {
                epoch: 0,
                group_id: 4,
                log_index,
            },
        };
        wal.appender(NO_APPLY_KEY)
            .with_commit_hlc(77)
            .append_change_position(
                TENANT,
                VSHARD,
                DB,
                &head.with_changes(changes).expect("encode"),
            )
            .expect("append marker")
    }

    fn forward(wal: &WalManager, key: u64) -> Lsn {
        wal.appender(key)
            .with_event_source(crate::event::EventSource::User)
            .append_put(TENANT, VSHARD, DB, b"row")
            .expect("append put")
    }

    /// An applied entry keeps its marker's rows and LSN. A marker whose
    /// write never applied, one refused for good, and one withheld are not
    /// kept.
    #[test]
    fn boot_keeps_each_applied_entry_that_publishes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let rows = MarkerChanges::Rows(vec![row("a"), row("b")]);

        let applied = marker(&wal, 0xA, 9, &rows);
        forward(&wal, 0xA);

        marker(&wal, 0xB, 10, &rows);

        marker(&wal, 0xC, 11, &rows);
        let refused = forward(&wal, 0xC);
        wal.appender(0xC)
            .append_write_aborted(TENANT, VSHARD, DB, refused)
            .expect("abort");

        marker(&wal, 0xD, 12, &rows);
        forward(&wal, 0xD);
        marker(&wal, 0xD, 12, &MarkerChanges::Withheld);
        wal.sync().expect("sync");

        let entries = UnjournaledEntries::new();
        let kept = entries
            .recover(&wal.replay().expect("replay"), wal.floor_holds())
            .expect("recover");
        assert_eq!(kept, 1);
        assert_eq!(
            entries.take_recovered(4, 9),
            Some(RecoveredChanges {
                lsn: applied,
                tenant_id: TENANT,
                database_id: DB,
                vshard_id: VSHARD,
                commit_hlc: 77,
                rows: vec![row("a"), row("b")],
                stream: None,
            })
        );
        for log_index in [10, 11, 12] {
            assert_eq!(entries.take_recovered(4, log_index), None);
        }
        assert_eq!(wal.floor_holds().lowest(), Some(applied));
    }

    /// A versioned point write cancels its own forward record with a keyed
    /// marker and keeps the part that carries its row: it applied, and its
    /// events stage again. A redo's marker reads its rows from the redo
    /// record.
    #[test]
    fn boot_keeps_a_cancelled_forward_with_parts_and_reads_redo_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");

        let point = marker(&wal, 0xE, 20, &MarkerChanges::Rows(vec![row("p")]));
        let origin = forward(&wal, 0xE);
        forward(&wal, 0xE);
        wal.appender(0xE)
            .append_write_aborted(TENANT, VSHARD, DB, origin)
            .expect("abort");

        let redo_marker = marker(&wal, 0xF, 21, &MarkerChanges::OfRedo);
        let redo = crate::wal::RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: vec![row("r1"), row("r2")],
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        }
        .to_bytes()
        .expect("encode redo");
        wal.appender(0xF)
            .with_event_source(crate::event::EventSource::User)
            .append_transaction_redo_bytes(TENANT, VSHARD, DB, &redo)
            .expect("append redo");
        wal.sync().expect("sync");

        let entries = UnjournaledEntries::new();
        let kept = entries
            .recover(&wal.replay().expect("replay"), wal.floor_holds())
            .expect("recover");
        assert_eq!(kept, 2);
        let point_changes = entries.take_recovered(4, 20).expect("the point write");
        assert_eq!(point_changes.lsn, point);
        assert_eq!(point_changes.rows, vec![row("p")]);
        let redo_changes = entries.take_recovered(4, 21).expect("the redo");
        assert_eq!(redo_changes.lsn, redo_marker);
        assert_eq!(redo_changes.rows, vec![row("r1"), row("r2")]);
    }
}
