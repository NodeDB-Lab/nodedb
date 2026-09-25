// SPDX-License-Identifier: BUSL-1.1

//! One committed record installs into a timeseries collection as one unit.
//!
//! A committed transaction's redo record can carry several timeseries
//! sub-records into one collection, all at the record's LSN. A partition
//! stamp names whole records only, so no flush may run between two of those
//! sub-records: the partition would hold part of the record, and restart
//! replay would either append that part again or lose the rest.
//!
//! So the replay arm measures the whole record before it writes any of it:
//! the rows and bytes each collection receives from every sub-record at that
//! LSN (`ts_record_loads`). A memtable that cannot take the record whole
//! flushes once, before the record's first row lands
//! (`CoreLoop::flush_ts_for_record`). No sub-record flushes. A record that
//! alone passes the memtable budget installs over it: it is committed, so it
//! must install, and the governor charge of the resident bytes reports the
//! overshoot as memory pressure. One flush after the record settles brings
//! the memtable back under budget, with a stamp that names the whole record
//! (`settle_redo_timeseries` live, `CoreLoop::settle_ts_replayed_record` in
//! restart replay).

use std::collections::HashMap;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::ilp;
use crate::types::{DatabaseId, TenantId};
use crate::wal::decode_batch_record;

/// `(database, tenant, collection)`.
pub(in crate::data::executor) type TsKey = (DatabaseId, TenantId, String);

/// What one record writes into one timeseries collection.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(in crate::data::executor) struct TsRecordLoad {
    /// Rows the record writes.
    pub rows: usize,
    /// Payload bytes the rows arrive as: the measure the budget test adds to
    /// the memtable's resident bytes.
    pub bytes: usize,
    /// Every ILP line the record writes, one per line, for the tag-headroom
    /// test. Empty for a sample batch, which carries no tags.
    pub ilp: String,
}

/// Every timeseries collection the record at `records[start]` writes on
/// `core_id`, and the load it writes into each, summed over every
/// `TimeseriesBatch` at that record's LSN. `records` is in LSN order, so the
/// record's sub-records follow `start` directly.
pub(in crate::data::executor) fn ts_record_loads(
    records: &[WalRecord],
    start: usize,
    num_cores: usize,
    core_id: usize,
) -> HashMap<TsKey, TsRecordLoad> {
    let mut loads: HashMap<TsKey, TsRecordLoad> = HashMap::new();
    let Some(first) = records.get(start) else {
        return loads;
    };
    let lsn = first.header.lsn;
    for record in records[start..]
        .iter()
        .take_while(|record| record.header.lsn == lsn)
    {
        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::TimeseriesBatch) {
            continue;
        }
        let target_core = if num_cores > 0 {
            record.header.vshard_id as usize % num_cores
        } else {
            0
        };
        if target_core != core_id {
            continue;
        }
        let Ok(decoded) = decode_batch_record(&record.payload) else {
            continue;
        };
        if decoded.kind.as_deref() == Some("columnar") {
            continue;
        }
        let key = (
            DatabaseId::new(record.header.database_id),
            TenantId::new(record.header.tenant_id),
            decoded.collection,
        );
        let load = loads.entry(key).or_default();
        add_payload(load, &decoded.payload, decoded.format.as_deref());
    }
    loads
}

/// Add one sub-record's rows to `load`, decoded the way the replay arm
/// decodes them (`replay_timeseries_payload`).
fn add_payload(load: &mut TsRecordLoad, payload: &[u8], format: Option<&str>) {
    load.bytes = load.bytes.saturating_add(payload.len());
    let lines: Vec<String> = match format {
        Some("ilp-msgpack") => zerompk::from_msgpack::<Vec<String>>(payload).unwrap_or_default(),
        Some("ilp") => utf8_lines(payload),
        _ => {
            if let Ok(batch) =
                zerompk::from_msgpack::<nodedb_types::timeseries::TimeseriesWalBatch>(payload)
            {
                load.rows = load.rows.saturating_add(batch.samples.len());
                return;
            }
            match format {
                None => utf8_lines(payload),
                // A JSON or msgpack row payload: its rows are counted by the
                // ingest. Its bytes still count toward the budget test.
                Some(_) => Vec::new(),
            }
        }
    };
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        load.rows += 1;
        load.ilp.push_str(&line);
        load.ilp.push('\n');
    }
}

fn utf8_lines(payload: &[u8]) -> Vec<String> {
    std::str::from_utf8(payload)
        .map(|text| text.lines().map(str::to_string).collect())
        .unwrap_or_default()
}

/// The record the timeseries replay arm is applying: its LSN and the
/// collections it writes on this core.
#[derive(Debug)]
pub(in crate::data::executor) struct TsRecordInProgress {
    lsn: u64,
    keys: Vec<TsKey>,
}

impl CoreLoop {
    /// Called by the timeseries replay arm at `records[index]`, a record
    /// routed to this core. When it starts a new record, restart replay
    /// settles the previous one, and every writing pass flushes the
    /// memtables that cannot take the new record whole. The validate pass of
    /// a committed-redo apply writes nothing and passes through.
    pub(in crate::data::executor) fn begin_ts_record(
        &mut self,
        records: &[WalRecord],
        index: usize,
        num_cores: usize,
        current: &mut Option<TsRecordInProgress>,
    ) {
        let Some(record) = records.get(index) else {
            return;
        };
        let lsn = record.header.lsn;
        if self.validating_committed_redo() || current.as_ref().is_some_and(|c| c.lsn == lsn) {
            return;
        }
        if let Some(previous) = current.take() {
            self.end_ts_record(previous);
        }
        let loads = ts_record_loads(records, index, num_cores, self.core_id);
        if let Err(e) = self.flush_ts_for_record(&loads) {
            self.report_ts_record_flush_error(lsn, e);
        }
        *current = Some(TsRecordInProgress {
            lsn,
            keys: loads.into_keys().collect(),
        });
    }

    /// The timeseries replay arm applied every sub-record of `record`. In
    /// restart replay, flush each memtable it left at or over its budget. A
    /// committed-redo install settles in `settle_redo_timeseries` instead.
    pub(in crate::data::executor) fn end_ts_record(&mut self, record: TsRecordInProgress) {
        if self.applying_committed_redo() {
            return;
        }
        if let Err(e) = self.settle_ts_replayed_record(&record.keys) {
            self.report_ts_record_flush_error(record.lsn, e);
        }
    }

    /// A flush around one record failed. A committed-redo install fails and
    /// rolls back: it can retry. Restart replay must apply the committed
    /// record all the same; its rows stay in the memtable over budget, and
    /// the next checkpoint flush retries and clamps its reported LSN.
    fn report_ts_record_flush_error(&mut self, lsn: u64, error: crate::Error) {
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.record_error(error);
            return;
        }
        tracing::error!(
            core = self.core_id,
            lsn,
            error = %error,
            "timeseries flush around a replayed record failed; its rows stay in the \
             memtable until the next checkpoint flush"
        );
    }

    /// Flush every memtable in `loads` that cannot take its record whole:
    /// its resident bytes plus the record's reach the memtable budget, its
    /// tag dictionaries lack room for the record's values, or the engine
    /// budget is under pressure. An empty or absent memtable flushes nothing:
    /// a flush cannot make room there.
    ///
    /// Runs before the record's first row lands, and before a committed-redo
    /// install records any pre-image, so the undo never restores rows the
    /// flush moved to disk.
    fn flush_ts_for_record(&mut self, loads: &HashMap<TsKey, TsRecordLoad>) -> crate::Result<()> {
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        for (key, load) in loads {
            let Some(resident) = self
                .columnar_memtables
                .get(key)
                .filter(|mt| !mt.is_empty())
                .map(|mt| mt.memory_bytes())
            else {
                continue;
            };
            let lines = ilp::parse_batch(&load.ilp)
                .map(|batch| batch.into_lines())
                .unwrap_or_default();
            let fits = resident.saturating_add(load.bytes) < soft_limit
                && !self.ts_ingest_needs_flush(key, &lines);
            if fits {
                continue;
            }
            let now_ms = self.ingest_now_ms();
            self.flush_ts_collection(key.1, key.0, &key.2, now_ms)?;
        }
        Ok(())
    }

    /// After restart replay applied a whole record: flush each collection it
    /// wrote whose memtable is at or over its budget. The replay cursor has
    /// passed the record, so the flush's stamp names all of it.
    fn settle_ts_replayed_record(&mut self, keys: &[TsKey]) -> crate::Result<()> {
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        for key in keys {
            let over = self
                .columnar_memtables
                .get(key)
                .is_some_and(|mt| mt.memory_bytes() >= soft_limit);
            if over {
                let now_ms = self.ingest_now_ms();
                self.flush_ts_collection(key.1, key.0, &key.2, now_ms)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_wal::record::WalRecordArgs;

    fn record(lsn: u64, collection: &str, lines: &[&str]) -> WalRecord {
        let lines: Vec<String> = lines.iter().map(|l| l.to_string()).collect();
        let resolved = zerompk::to_msgpack_vec(&lines).expect("encode lines");
        let payload =
            crate::control::server::wal_dispatch::encode_timeseries_batch_payload_with_format(
                collection,
                &resolved,
                None,
                "ilp-msgpack",
            )
            .expect("encode sub-record");
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TimeseriesBatch as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn a_record_load_sums_every_sub_record_at_its_lsn() {
        let records = [
            record(7, "m", &["m,host=a value=1i 1"]),
            record(7, "m", &["m,host=b value=2i 2", "m,host=c value=3i 3"]),
            record(7, "n", &["n,host=a value=1i 1"]),
            record(8, "m", &["m,host=d value=4i 4"]),
        ];
        let loads = ts_record_loads(&records, 0, 1, 0);
        let m = (DatabaseId::new(0), TenantId::new(1), "m".to_string());
        let n = (DatabaseId::new(0), TenantId::new(1), "n".to_string());
        assert_eq!(loads.len(), 2);
        assert_eq!(loads[&m].rows, 3, "the lsn-8 record is not part of it");
        assert_eq!(loads[&n].rows, 1);
        assert_eq!(loads[&m].ilp.lines().count(), 3);
        assert!(loads[&m].bytes > 0);
    }

    // ── One record installs as one unit ──────────────────────────────────────

    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::transaction::redo_apply::CommittedRedo;
    use crate::types::Lsn;
    use crate::wal::{RedoRecord, RedoSubRecord};

    const COLL: &str = "metrics";

    fn key() -> TsKey {
        (DatabaseId::DEFAULT, TenantId::new(1), COLL.to_string())
    }

    /// A committed redo record whose sub-records each ingest one ILP line
    /// into `metrics`.
    fn redo(lines: &[&str]) -> Vec<u8> {
        let ops = lines
            .iter()
            .map(|line| {
                let resolved =
                    zerompk::to_msgpack_vec(&vec![line.to_string()]).expect("encode lines");
                RedoSubRecord {
                    record_type: RecordType::TimeseriesBatch as u32,
                    payload:
                        crate::control::server::wal_dispatch::encode_timeseries_batch_payload_with_format(
                            COLL,
                            &resolved,
                            None,
                            "ilp-msgpack",
                        )
                        .expect("encode sub-record"),
                }
            })
            .collect();
        RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo")
    }

    fn redo_wal_record(lsn: u64, redo: &[u8]) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: redo.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    fn install(core: &mut CoreLoop, lsn: u64, redo: &[u8]) {
        let mut task = make_default_task();
        task.wal_lsn = Some(Lsn::new(lsn));
        let response = core.install_committed_redo(
            &task,
            1,
            CommittedRedo {
                redo,
                collections: &[COLL.to_string()],
                sum_targets: &[],
            },
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }

    /// Row counts of the collection's partitions, sorted.
    fn partition_rows(core: &CoreLoop) -> Vec<u64> {
        let mut rows: Vec<u64> = core
            .ts_registries
            .get(&key())
            .map(|registry| registry.iter().map(|(_, e)| e.meta.row_count).collect())
            .unwrap_or_default();
        rows.sort_unstable();
        rows
    }

    fn memtable_rows(core: &CoreLoop) -> u64 {
        core.columnar_memtables
            .get(&key())
            .map_or(0, |mt| mt.row_count())
    }

    /// A core and the bridge ends that must outlive it.
    type Booted = (
        CoreLoop,
        nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
    );

    /// Boot over `dir` with a one-byte memtable budget, and replay
    /// `records` as restart replay does.
    fn restart_tiny_budget(dir: &std::path::Path, records: &[WalRecord]) -> Booted {
        let (mut core, req, resp) = make_core_with_dir(dir);
        core.ts_tuning.memtable_budget_bytes = 1;
        core.load_ts_registries().expect("load");
        let wal_end = records.iter().map(|r| r.header.lsn).max().unwrap_or(0);
        core.floors
            .applied_prefix
            .seed_replayed_through(Lsn::new(wal_end));
        core.replay_transaction_redo_wal(records, 1, &nodedb_wal::TombstoneSet::new())
            .expect("replay");
        (core, req, resp)
    }

    const SEED: &str = "metrics,host=s value=1 1000000000";
    const FIRST: &str = "metrics,host=a value=2 2000000000";
    const SECOND: &str = "metrics,host=b value=3 3000000000";

    /// Live: the memtable holds a row and is over its budget once the first
    /// of two sub-records lands. The record flushes once before it and once
    /// after it, never between: one partition holds the seed, one holds the
    /// whole record. A restart applies neither again.
    #[test]
    fn a_live_install_never_flushes_between_its_sub_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let seed = redo(&[SEED]);
        let pair = redo(&[FIRST, SECOND]);
        {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            install(&mut core, 5, &seed);
            assert_eq!(memtable_rows(&core), 1);
            core.ts_tuning.memtable_budget_bytes = 1;
            install(&mut core, 20, &pair);
            assert_eq!(
                partition_rows(&core),
                vec![1, 2],
                "one flush before the record and one after it"
            );
            assert_eq!(memtable_rows(&core), 0);
            let stamp = &core.ts_replay_stamps.get(&key()).expect("stamp").rows;
            assert!(stamp.skips(20), "the settle flush names the whole record");
        }

        let records = [redo_wal_record(5, &seed), redo_wal_record(20, &pair)];
        let (core, _req, _resp) = restart_tiny_budget(dir.path(), &records);
        assert_eq!(partition_rows(&core), vec![1, 2]);
        assert_eq!(
            memtable_rows(&core),
            0,
            "every record is named: none replays"
        );
    }

    /// Restart replay: the record's sub-records are only in the WAL. The
    /// memtable is over its budget once the first one lands, yet the second
    /// lands in the same generation, and the record flushes whole. A second
    /// restart applies nothing again.
    #[test]
    fn restart_replay_never_flushes_between_a_records_sub_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let seed = redo(&[SEED]);
        let pair = redo(&[FIRST, SECOND]);
        {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            install(&mut core, 5, &seed);
            install(&mut core, 20, &pair);
            assert_eq!(memtable_rows(&core), 3);
            assert!(
                partition_rows(&core).is_empty(),
                "nothing flushed before the crash"
            );
        }

        let records = [redo_wal_record(5, &seed), redo_wal_record(20, &pair)];
        let (core, req, resp) = restart_tiny_budget(dir.path(), &records);
        assert_eq!(
            partition_rows(&core),
            vec![1, 2],
            "the seed record and the pair each flush whole"
        );
        assert_eq!(memtable_rows(&core), 0);
        drop((core, req, resp));

        let (again, _req, _resp) = restart_tiny_budget(dir.path(), &records);
        assert_eq!(partition_rows(&again), vec![1, 2]);
        assert_eq!(
            memtable_rows(&again),
            0,
            "both records applied exactly once"
        );
    }
}
