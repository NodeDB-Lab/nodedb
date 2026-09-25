// SPDX-License-Identifier: BUSL-1.1

//! The coordinated checkpoint's timeseries contributor.

use tracing::{info, warn};

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, Lsn, TenantId};

impl CoreLoop {
    /// Flush every timeseries memtable on this core to an L1 partition and
    /// return the LSN the timeseries engine is now durable through.
    ///
    /// Returns `Ok(watermark)` only once every collection's partition — columns,
    /// symbol dictionaries, sparse index, and the `partition.meta` that commits
    /// them — has landed. Any failure returns `Err`; the caller must then clamp
    /// the reported checkpoint LSN to the last LSN timeseries was known durable
    /// through, so a failed flush costs WAL growth instead of the rows it could
    /// not write.
    ///
    /// One collection's failure does not abandon the others: every collection is
    /// flushed and the first error is reported at the end. The reported LSN is
    /// clamped either way, so stopping early would buy nothing and cost the
    /// healthy collections their durability — and, with a persistently broken
    /// collection, cost it on every cycle from then on.
    ///
    /// Collections with no memtable on this core are not skipped state: a core
    /// only ever ingested rows into memtables it created, so a collection absent
    /// from `columnar_memtables` has nothing here to lose. Its partitions are on
    /// disk and its registry is rebuilt at boot by `load_ts_registries`.
    pub(in crate::data::executor) fn checkpoint_timeseries_memtables(
        &mut self,
    ) -> crate::Result<Lsn> {
        let durable_through = self.watermark;

        // Collected first: `flush_ts_collection` takes `&mut self`, so the
        // memtable-map iterator cannot stay borrowed across the loop. Empty
        // memtables are filtered out here rather than flushed as no-ops so the
        // `flushed` count reports what actually reached disk.
        let pending: Vec<(DatabaseId, TenantId, String)> = self
            .columnar_memtables
            .iter()
            .filter(|(_, mt)| !mt.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        if pending.is_empty() {
            return Ok(durable_through);
        }

        // The continuous-aggregate hook the flush fires needs a wall-clock
        // watermark. The checkpoint is not a Calvin epoch, so there is no
        // deterministic timestamp to take — and none is needed: `on_flush` uses
        // it only to timestamp the aggregate's refresh watermark, never as row
        // data.
        // no-determinism: aggregate refresh timing, not replicated row state
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let mut flushed = 0usize;
        let mut first_error: Option<crate::Error> = None;
        for (db, tid, collection) in &pending {
            match self.flush_ts_collection(*tid, *db, collection, now_ms) {
                Ok(()) => flushed += 1,
                Err(e) => {
                    // Every failure is logged where it happened; only the first
                    // is returned, since one clamp is all the caller can apply.
                    warn!(
                        core = self.core_id,
                        collection = %collection,
                        error = %e,
                        "timeseries checkpoint flush failed for one collection; continuing \
                         with the rest and clamping this core's checkpoint LSN"
                    );
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }

        info!(
            core = self.core_id,
            collections = pending.len(),
            flushed,
            durable_through_lsn = durable_through.as_u64(),
            "timeseries checkpoint flushed"
        );
        Ok(durable_through)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::TimeseriesOp;
    use nodedb_types::QualifiedCollection;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
    use crate::types::*;

    const TID: u64 = 1;
    const COLL: &str = "metrics";

    /// A core over a caller-owned data dir, so a restart can be modelled by
    /// dropping one and opening the next over the same directory.
    struct Core {
        core: CoreLoop,
        req_tx: Producer<BridgeRequest>,
        resp_rx: Consumer<BridgeResponse>,
        next_id: u64,
    }

    impl Core {
        fn open_at(dir: &std::path::Path) -> Self {
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
            let core = CoreLoop::open(
                0,
                req_rx,
                resp_tx,
                dir,
                Arc::new(nodedb_types::OrdinalClock::new()),
                crate::data::executor::core_loop::test_governor(),
            )
            .expect("CoreLoop::open");
            Self {
                core,
                req_tx,
                resp_rx,
                next_id: 1,
            }
        }

        fn send(&mut self, plan: PhysicalPlan, wal_lsn: Option<u64>) -> Response {
            let id = self.next_id;
            self.next_id += 1;
            self.req_tx
                .try_push(BridgeRequest::unfloored(Request {
                    request_id: RequestId::new(id),
                    tenant_id: TenantId::new(TID),
                    database_id: DatabaseId::DEFAULT,
                    vshard_id: VShardId::new(0),
                    plan,
                    deadline: Instant::now() + Duration::from_secs(5),
                    priority: Priority::Normal,
                    trace_id: TraceId::ZERO,
                    consistency: ReadConsistency::Strong,
                    idempotency_key: None,
                    event_source: crate::event::EventSource::User,
                    user_roles: Vec::new(),
                    user_id: None,
                    statement_digest: None,
                    txn_id: None,
                    wal_lsn: wal_lsn.map(crate::types::Lsn::new),
                    resolved_now_ms: None,
                    admission: crate::bridge::envelope::Admission::Admitted,
                }))
                .expect("push request");
            self.core.tick();
            self.resp_rx.try_pop().expect("response").inner
        }

        /// Ingest one ILP line through the real dispatch path, exactly as the
        /// Control Plane does. `wal_lsn` is threaded BOTH on the op (the
        /// record the partition's replay stamp names) and on the request (what
        /// `note_collection_write_lsn` raises the watermark from).
        fn ingest(&mut self, host: &str, value: f64, ts_ms: i64, wal_lsn: u64) {
            let line = format!("{COLL},host={host} value={value} {}\n", ts_ms * 1_000_000);
            let r = self.send(
                PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                    payload: line.into_bytes(),
                    format: "ilp".to_string(),
                    wal_lsn: Some(wal_lsn),
                    surrogates: Vec::new(),
                    provenance: None,
                    rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                    returning: None,
                    rls_filters: Vec::new(),
                }),
                Some(wal_lsn),
            );
            assert_eq!(r.status, Status::Ok, "ts ingest: {r:?}");
        }

        /// The `host` tag of every row an unbounded scan returns, sorted.
        ///
        /// Goes through the real scan handler, which reads the registered
        /// partitions and the live memtable together — so this asserts the rows
        /// are QUERYABLE, not merely that a file exists.
        fn scan_hosts(&mut self) -> Vec<String> {
            let r = self.send(
                PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                    time_range: (0, i64::MAX),
                    projection: Vec::new(),
                    limit: usize::MAX,
                    filters: Vec::new(),
                    sort_keys: Vec::new(),
                    bucket_interval_ms: 0,
                    group_by: Vec::new(),
                    aggregates: Vec::new(),
                    gap_fill: String::new(),
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                    computed_columns: Vec::new(),
                }),
                None,
            );
            assert_eq!(r.status, Status::Ok, "ts scan: {r:?}");
            decode_hosts(r.payload.as_bytes())
        }
    }

    /// Decode a raw scan response into its `host` tag values, sorted.
    fn decode_hosts(bytes: &[u8]) -> Vec<String> {
        let json_str = crate::data::executor::response_codec::decode_payload_to_json(bytes);
        let value: serde_json::Value =
            serde_json::from_str(&json_str).expect("scan payload must be decodable");
        let rows = value
            .get("rows")
            .and_then(|r| r.as_array())
            .cloned()
            .or_else(|| value.as_array().cloned())
            .unwrap_or_else(|| panic!("scan response must carry rows: {json_str}"));
        let mut out: Vec<String> = rows
            .iter()
            .map(|row| {
                row.get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scan row must carry its host tag: {row}"))
                    .to_string()
            })
            .collect();
        out.sort();
        out
    }

    /// The whole point of this checkpoint: rows ingested but never explicitly
    /// flushed must still answer a SCAN after a restart. Drives the real ingest
    /// and scan dispatch paths, and models the restart by dropping the core and
    /// opening a second one over the same data dir — its memtable starts empty,
    /// exactly as it does once truncation has deleted the `TimeseriesBatch`
    /// records, so only the checkpoint's flush can make these assertions hold.
    #[test]
    fn checkpointed_rows_answer_a_scan_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut before = Core::open_at(dir.path());
        before.ingest("a", 1.0, 1_000, 10);
        before.ingest("b", 2.0, 2_000, 20);
        assert_eq!(
            before.scan_hosts(),
            vec!["a".to_string(), "b".to_string()],
            "both rows must be live in the memtable before any flush"
        );

        let reported = before
            .core
            .checkpoint_timeseries_memtables()
            .expect("flush to a writable dir must succeed");
        assert_eq!(
            reported,
            Lsn::new(20),
            "the flush must report exactly the LSN it made durable — the manager \
             deletes WAL segments below whatever this returns"
        );

        drop(before);

        let mut after = Core::open_at(dir.path());
        after
            .core
            .load_ts_registries()
            .expect("valid partitions must load");
        assert_eq!(
            after.scan_hosts(),
            vec!["a".to_string(), "b".to_string()],
            "every checkpointed row must come back from its on-disk partition"
        );
    }

    /// A core with no timeseries memtables reports the watermark rather than
    /// clamping — it holds no timeseries state at all, so it can never be the
    /// reason the WAL must be kept.
    #[test]
    fn no_memtables_reports_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = Core::open_at(dir.path());
        core.core.advance_watermark(Lsn::new(42));
        assert_eq!(
            core.core
                .checkpoint_timeseries_memtables()
                .expect("flush with nothing to flush"),
            Lsn::new(42)
        );
    }

    /// A flush with an empty memtable must still report the watermark: every row
    /// it held is already in a partition, so clamping there would pin WAL
    /// truncation for no reason.
    #[test]
    fn empty_memtable_reports_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = Core::open_at(dir.path());
        core.ingest("a", 1.0, 1_000, 5);
        core.core
            .checkpoint_timeseries_memtables()
            .expect("first flush");

        core.core.advance_watermark(Lsn::new(900));
        assert_eq!(
            core.core
                .checkpoint_timeseries_memtables()
                .expect("second flush"),
            Lsn::new(900),
            "nothing was ingested since the last flush, so the timeseries engine \
             is durable through the current watermark"
        );
    }

    /// A row ingested AFTER the checkpoint stays in the memtable and must still
    /// be readable — the flush drains, it does not discard, and the partition
    /// plus memtable are scanned together.
    #[test]
    fn rows_ingested_after_the_flush_are_still_live() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = Core::open_at(dir.path());
        core.ingest("a", 1.0, 1_000, 10);
        core.core.checkpoint_timeseries_memtables().expect("flush");
        core.ingest("b", 2.0, 2_000, 11);

        assert_eq!(
            core.scan_hosts(),
            vec!["a".to_string(), "b".to_string()],
            "the flushed partition and the live memtable must scan as one collection"
        );
    }

    /// A flush that cannot write its partition must leave the memtable intact.
    /// Draining first would take the rows out of memory without putting them
    /// anywhere — the scan would stop returning them until a restart replayed
    /// the WAL, which this checkpoint's clamped LSN is what keeps.
    #[test]
    fn failed_flush_leaves_the_memtable_intact_and_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = Core::open_at(dir.path());
        core.ingest("a", 1.0, 1_000, 10);

        // Occupy the collection's segment directory path with a FILE so the
        // partition write has nowhere to land.
        let tenant_dir = dir
            .path()
            .join("ts")
            .join(DatabaseId::DEFAULT.as_u64().to_string())
            .join(TID.to_string());
        std::fs::create_dir_all(&tenant_dir).expect("create tenant dir");
        std::fs::write(tenant_dir.join(COLL), b"not a directory").expect("write blocking file");

        assert!(
            core.core.checkpoint_timeseries_memtables().is_err(),
            "a flush that cannot write its partition must report the failure, not \
             swallow it — the caller clamps its checkpoint LSN on this Err"
        );
        // Read straight from the memtable rather than through a scan: this test
        // sabotaged the very directory the scan's registry load reads, so a scan
        // here would measure that sabotage rather than the drain ordering.
        assert_eq!(
            core.core
                .columnar_memtable_row_count(DatabaseId::DEFAULT.as_u64(), TID, COLL),
            1,
            "the row must still be live in the memtable after the failed flush — \
             draining before the write would have discarded it outright"
        );
    }

    /// The ILP line `ingest` writes for one row.
    fn ilp_line(host: &str, value: f64, ts_ms: i64) -> String {
        format!("{COLL},host={host} value={value} {}\n", ts_ms * 1_000_000)
    }

    /// The `TimeseriesBatch` WAL record a live ingest of one row appends.
    fn ingest_record(host: &str, value: f64, ts_ms: i64, lsn: u64) -> nodedb_wal::WalRecord {
        let payload = zerompk::to_msgpack_vec(&(
            "timeseries".to_string(),
            COLL.to_string(),
            ilp_line(host, value, ts_ms).into_bytes(),
            Option::<nodedb_types::sync::wire::SyncProvenance>::None,
            "ilp".to_string(),
        ))
        .expect("encode timeseries tuple");
        wal_record(
            nodedb_wal::record::RecordType::TimeseriesBatch,
            lsn,
            payload,
        )
    }

    /// The `TimeseriesTruncate` WAL record a live truncate appends.
    fn truncate_record(lsn: u64) -> nodedb_wal::WalRecord {
        let payload = zerompk::to_msgpack_vec(&nodedb_types::columnar::ColumnarTruncateWalRecord {
            collection: COLL.to_string(),
        })
        .expect("encode truncate");
        wal_record(
            nodedb_wal::record::RecordType::TimeseriesTruncate,
            lsn,
            payload,
        )
    }

    fn wal_record(
        record_type: nodedb_wal::record::RecordType,
        lsn: u64,
        payload: Vec<u8>,
    ) -> nodedb_wal::WalRecord {
        nodedb_wal::WalRecord::new(nodedb_wal::record::WalRecordArgs {
            record_type: record_type as u32,
            lsn,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// Restart the way boot does: load the registries and stamps, raise the
    /// floor to the WAL end, then replay `records`.
    fn restart_and_replay(dir: &std::path::Path, records: &[nodedb_wal::WalRecord]) -> Core {
        let mut core = Core::open_at(dir);
        core.core.load_ts_registries().expect("load");
        let wal_end = records.iter().map(|r| r.header.lsn).max().unwrap_or(0);
        core.core
            .floors
            .applied_prefix
            .seed_replayed_through(Lsn::new(wal_end));
        core.core
            .replay_timeseries_wal(records, 1, &nodedb_wal::TombstoneSet::new());
        core
    }

    fn observe_floor(core: &mut Core, lsn: u64) {
        core.core
            .floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(lsn));
    }

    fn truncate(core: &mut Core, lsn: u64) {
        let r = core.send(
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                restart_identity: false,
            }),
            Some(lsn),
        );
        assert_eq!(r.status, Status::Ok, "ts truncate: {r:?}");
    }

    /// Floor 10. B at lsn 30 applies, a checkpoint flushes it, then A at lsn
    /// 20 applies. A restart replays A once and never re-applies B, and the
    /// collection scans as it did live — across a second restart too.
    #[test]
    fn a_timeseries_write_in_flight_at_a_checkpoint_replays_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let records = [
            ingest_record("a", 1.0, 1_000, 20),
            ingest_record("b", 2.0, 2_000, 30),
        ];

        let mut before = Core::open_at(dir.path());
        observe_floor(&mut before, 10);
        before.ingest("b", 2.0, 2_000, 30);
        before
            .core
            .checkpoint_timeseries_memtables()
            .expect("flush B");
        before.ingest("a", 1.0, 1_000, 20);
        let live = before.scan_hosts();
        assert_eq!(live, vec!["a".to_string(), "b".to_string()]);
        drop(before);

        let mut after = restart_and_replay(dir.path(), &records);
        assert_eq!(
            after.scan_hosts(),
            live,
            "replay equals live: A once, B once"
        );
        after
            .core
            .checkpoint_timeseries_memtables()
            .expect("flush A");
        drop(after);

        let mut again = restart_and_replay(dir.path(), &records);
        assert_eq!(
            again.scan_hosts(),
            live,
            "every record is named now: a second restart applies nothing"
        );
    }

    /// A live ingest below the highest flushed LSN is a record that was still
    /// on its way when the flush ran: it applies. A redelivery of a flushed
    /// record is named by the stamp and writes nothing.
    #[test]
    fn a_live_ingest_below_the_highest_flushed_lsn_applies() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = Core::open_at(dir.path());
        observe_floor(&mut core, 10);
        core.ingest("b", 2.0, 2_000, 30);
        core.core
            .checkpoint_timeseries_memtables()
            .expect("flush B");

        core.ingest("a", 1.0, 1_000, 20);
        assert_eq!(core.scan_hosts(), vec!["a".to_string(), "b".to_string()]);
        core.ingest("b", 2.0, 2_000, 30);
        assert_eq!(
            core.scan_hosts(),
            vec!["a".to_string(), "b".to_string()],
            "the redelivered flushed record writes nothing"
        );
    }

    /// Restart replay asks the collection stamp, record by record.
    #[test]
    fn the_collection_stamp_gates_replay_record_by_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = Core::open_at(dir.path());
        let key = (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string());
        core.core.ts_replay_stamps.insert(
            key,
            super::super::stamp::TsReplayStamp {
                rows: crate::types::replay_stamp::ReplayStamp::through(5)
                    .union(&crate::types::replay_stamp::ReplayStamp::naming(10)),
                truncates: crate::types::replay_stamp::ReplayStamp::default(),
            },
        );
        let records = [
            ingest_record("h05", 1.0, 1_000, 5),
            ingest_record("h07", 1.0, 2_000, 7),
            ingest_record("h10", 1.0, 3_000, 10),
            ingest_record("h11", 1.0, 4_000, 11),
        ];
        core.core
            .replay_timeseries_wal(&records, 1, &nodedb_wal::TombstoneSet::new());
        assert_eq!(
            core.scan_hosts(),
            vec!["h07".to_string(), "h11".to_string()]
        );
    }

    /// X at lsn 10 applies, a truncate at lsn 20 removes it, then A at lsn 15
    /// applies. A restart must not re-apply the truncate over A, and must not
    /// bring X back.
    #[test]
    fn a_write_in_flight_at_a_truncate_survives_replay() {
        let dir = tempfile::tempdir().expect("tempdir");
        let records = [
            ingest_record("x", 1.0, 1_000, 10),
            ingest_record("a", 2.0, 2_000, 15),
            truncate_record(20),
        ];

        let mut before = Core::open_at(dir.path());
        before.ingest("x", 1.0, 1_000, 10);
        truncate(&mut before, 20);
        before.ingest("a", 2.0, 2_000, 15);
        let live = before.scan_hosts();
        assert_eq!(live, vec!["a".to_string()]);
        drop(before);

        let mut after = restart_and_replay(dir.path(), &records);
        assert_eq!(
            after.scan_hosts(),
            live,
            "the truncate took effect before the crash: replay skips it and \
             everything it removed, and applies the write that followed it"
        );
    }
}
