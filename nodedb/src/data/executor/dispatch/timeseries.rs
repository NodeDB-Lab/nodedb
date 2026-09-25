// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for TimeseriesOp variants (scan, ingest).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::TimeseriesOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::{TimeseriesIngestExec, TimeseriesScanParams};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_timeseries(
        &mut self,
        task: &ExecutionTask,
        op: &TimeseriesOp,
    ) -> Response {
        match op {
            // `projection` is not destructured: the raw scan emits every
            // stored column and the aggregate branch derives its own column
            // set from GROUP BY, the aggregates, and the predicates.
            TimeseriesOp::Scan {
                collection,
                time_range,
                projection: _,
                limit,
                filters,
                sort_keys,
                bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
                computed_columns,
                rls_filters,
                system_time,
                valid_at_ms,
            } => self.execute_timeseries_scan(TimeseriesScanParams {
                task,
                tid: task.request.tenant_id,
                collection: collection.as_str(),
                time_range: *time_range,
                limit: *limit,
                filters,
                rls_filters,
                sort_keys,
                bucket_interval_ms: *bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
                computed_columns,
                system_time: *system_time,
                valid_at_ms: *valid_at_ms,
            }),

            TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn,
                surrogates: _,
                provenance,
                rls_write_check,
                returning,
                rls_filters,
            } => self.execute_timeseries_ingest(TimeseriesIngestExec {
                task,
                tid: task.request.tenant_id,
                collection: collection.as_str(),
                payload,
                format,
                // The envelope LSN wins: SQL/ILP planners leave the plan's copy
                // `None`, and a stamp of 0 there would defeat replay's dedup gate.
                wal_lsn: task.wal_lsn().map(|lsn| lsn.as_u64()).or(*wal_lsn),
                provenance: provenance.as_ref(),
                mode: crate::data::executor::handlers::timeseries::TimeseriesApplyMode::Immediate,
                rls_write_check,
                returning: returning.as_ref(),
                rls_filters,
            }),

            TimeseriesOp::ResolveIngest(inner) => {
                self.execute_timeseries_resolve_ingest(task, inner)
            }

            TimeseriesOp::Truncate {
                collection,
                restart_identity: _,
            } => self.execute_timeseries_truncate(task, collection.as_str(), None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::TimeseriesOp;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{Admission, PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    const TENANT: u64 = 1;
    const COLLECTION: &str = "metrics";

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime; the
    /// test drives the handler directly and never ticks the event loop.
    struct Harness {
        core: CoreLoop,
        _req_tx: Producer<BridgeRequest>,
        _resp_rx: Consumer<BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        Harness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// An autocommit ILP ingest exactly as the SQL and ILP planners build it:
    /// the plan carries NO LSN, the request envelope carries the minted one.
    fn autocommit_ingest_task(envelope_lsn: Option<u64>) -> ExecutionTask {
        ingest_task(
            format!("{COLLECTION},host=h0 value=1i\n").into_bytes(),
            envelope_lsn,
        )
    }

    fn ingest_task(payload: Vec<u8>, envelope_lsn: Option<u64>) -> ExecutionTask {
        task_for(
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                payload,
                format: "ilp".to_string(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            envelope_lsn,
        )
    }

    /// A read of every stored row, carrying `rls_filters` exactly as the
    /// planner ships a read policy. `aggregates` empty is a raw scan.
    fn scan_task(rls_filters: Vec<u8>, aggregates: Vec<(String, String)>) -> ExecutionTask {
        task_for(
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                time_range: nodedb_physical::physical_plan::UNBOUNDED_TIME_RANGE,
                projection: Vec::new(),
                limit: usize::MAX,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates,
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters,
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            None,
        )
    }

    /// A read policy `owner = <owner>`, encoded the way the planner ships it.
    fn policy_owner_eq(owner: &str) -> Vec<u8> {
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String(owner.into()),
            clauses: vec![],
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy")
    }

    fn decode_rows(payload: &[u8]) -> Vec<nodedb_types::Value> {
        match nodedb_types::value_from_msgpack(payload).expect("decode scan payload") {
            nodedb_types::Value::Array(rows) => rows,
            other => panic!("scan payload is not an array: {other:?}"),
        }
    }

    /// Ingest one row owned by `mine` and two owned by `theirs`.
    fn seed_owned_rows(h: &mut Harness) {
        let task = ingest_task(
            format!(
                "{COLLECTION},owner=mine value=1i\n\
                 {COLLECTION},owner=theirs value=2i\n\
                 {COLLECTION},owner=theirs value=3i\n"
            )
            .into_bytes(),
            Some(1),
        );
        let PhysicalPlan::Timeseries(op) = task.request.plan.clone() else {
            panic!("timeseries plan");
        };
        let response = h.core.dispatch_timeseries(&task, &op);
        assert_eq!(response.status, Status::Ok, "seed ingest must succeed");
    }

    fn task_for(plan: PhysicalPlan, envelope_lsn: Option<u64>) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TENANT),
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
            wal_lsn: envelope_lsn.map(Lsn::new),
            resolved_now_ms: None,
            admission: Admission::Admitted,
        })
    }

    /// A flushed partition's stamp must name the record it holds, or boot
    /// replay's skip gate never fires and the record replays on top of rows
    /// already on disk.
    #[test]
    fn autocommit_ingest_stamps_the_envelope_lsn_on_the_partition_it_flushes() {
        let mut h = make_core();
        let task = autocommit_ingest_task(Some(42));
        let PhysicalPlan::Timeseries(op) = task.request.plan.clone() else {
            panic!("timeseries plan");
        };

        let response = h.core.dispatch_timeseries(&task, &op);
        assert_eq!(response.status, Status::Ok, "ingest must succeed");

        h.core
            .flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        let stamp = &h.core.ts_replay_stamps.get(&key).expect("stamp").rows;
        assert!(
            stamp.skips(42) && !stamp.skips(41),
            "the flushed partition's stamp must name the record's WAL LSN and \
             nothing else: {stamp:?}"
        );
        let registry = h.core.ts_registries.get(&key).expect("registry");
        let dirs: Vec<String> = registry.iter().map(|(_, e)| e.dir_name.clone()).collect();
        assert_eq!(dirs.len(), 1);
        let dir = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &h.core.data_dir,
            DatabaseId::DEFAULT.as_u64(),
            TENANT,
            COLLECTION,
        )
        .join(&dirs[0]);
        let on_disk = crate::data::executor::timeseries_checkpoint::stamp::read_ts_stamp(&dir)
            .expect("read")
            .expect("the partition carries its stamp");
        assert_eq!(&on_disk.rows, stamp);
    }

    /// A read policy governs a raw timeseries scan: only the rows it admits
    /// come back, from the live memtable and from a flushed partition alike.
    #[test]
    fn a_read_policy_admits_only_matching_rows_from_memtable_and_partition() {
        let mut h = make_core();
        seed_owned_rows(&mut h);

        let scan = scan_task(policy_owner_eq("mine"), Vec::new());
        let PhysicalPlan::Timeseries(op) = scan.request.plan.clone() else {
            panic!("timeseries plan");
        };
        let live = h.core.dispatch_timeseries(&scan, &op);
        assert_eq!(live.status, Status::Ok);
        let rows = decode_rows(live.payload.as_bytes());
        assert_eq!(rows.len(), 1, "one live row is admitted: {rows:?}");

        h.core
            .flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");
        let flushed = h.core.dispatch_timeseries(&scan, &op);
        assert_eq!(flushed.status, Status::Ok);
        let rows = decode_rows(flushed.payload.as_bytes());
        assert_eq!(rows.len(), 1, "one flushed row is admitted: {rows:?}");
    }

    /// A governed `COUNT(*)` leaves the metadata fast path and counts only
    /// the rows the policy admits.
    #[test]
    fn a_read_policy_governs_a_count_star_aggregate() {
        let mut h = make_core();
        seed_owned_rows(&mut h);

        let scan = scan_task(
            policy_owner_eq("mine"),
            vec![("count".to_string(), "*".to_string())],
        );
        let PhysicalPlan::Timeseries(op) = scan.request.plan.clone() else {
            panic!("timeseries plan");
        };
        let response = h.core.dispatch_timeseries(&scan, &op);
        assert_eq!(response.status, Status::Ok);
        let rows = decode_rows(response.payload.as_bytes());
        let count_key = nodedb_query::agg_key::canonical_agg_key("count", "*");
        let counted = match rows.first() {
            Some(nodedb_types::Value::Object(map)) => map.get(&count_key).cloned(),
            other => panic!("expected one aggregate row, got {other:?}"),
        };
        assert_eq!(
            counted,
            Some(nodedb_types::Value::Integer(1)),
            "the policy admits one row for this caller: {rows:?}"
        );
    }

    /// A read policy the grouped scan cannot lower fails the aggregate
    /// instead of aggregating the rows the policy governs.
    #[test]
    fn an_unlowerable_read_policy_fails_an_aggregate() {
        let mut h = make_core();
        seed_owned_rows(&mut h);

        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Like,
            value: nodedb_types::Value::String("mi%".into()),
            clauses: vec![],
            expr: None,
        };
        let policy = zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy");
        let scan = scan_task(policy, vec![("count".to_string(), "*".to_string())]);
        let PhysicalPlan::Timeseries(op) = scan.request.plan.clone() else {
            panic!("timeseries plan");
        };
        let response = h.core.dispatch_timeseries(&scan, &op);
        assert_eq!(
            response.status,
            Status::Error,
            "a policy the grouped scan cannot evaluate must not aggregate rows"
        );
        assert!(response.payload.is_empty());
    }

    /// A read policy payload the Data Plane cannot decode fails the scan.
    /// Treating it as "no policy" would return every row the policy hides.
    #[test]
    fn a_malformed_read_policy_payload_fails_the_scan() {
        let mut h = make_core();
        seed_owned_rows(&mut h);

        // `0xC1` is the one byte MessagePack reserves and never emits.
        let scan = scan_task(vec![0xC1], Vec::new());
        let PhysicalPlan::Timeseries(op) = scan.request.plan.clone() else {
            panic!("timeseries plan");
        };
        let response = h.core.dispatch_timeseries(&scan, &op);
        assert_eq!(
            response.status,
            Status::Error,
            "an unreadable policy must not admit rows"
        );
        assert!(
            response.payload.is_empty(),
            "a failed scan carries no rows, got {} bytes",
            response.payload.len()
        );
    }

    /// Nothing minted an LSN, so nothing may be claimed as flushed: a stamp
    /// naming a record here would gate away records that are un-flushed.
    #[test]
    fn an_ingest_with_no_lsn_anywhere_stamps_nothing() {
        let mut h = make_core();
        let task = autocommit_ingest_task(None);
        let PhysicalPlan::Timeseries(op) = task.request.plan.clone() else {
            panic!("timeseries plan");
        };

        let response = h.core.dispatch_timeseries(&task, &op);
        assert_eq!(response.status, Status::Ok);

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        h.core
            .flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");
        assert_eq!(
            h.core.ts_replay_stamps.get(&key).map(|s| s.rows.clone()),
            Some(crate::types::replay_stamp::ReplayStamp::default())
        );
    }

    /// A `RETURNING` ingest whose tags overflow the cardinality limit is
    /// refused before the first row lands. The refusal code claims nothing
    /// applied, so no row and no memtable can exist afterwards.
    #[test]
    fn a_returning_ingest_over_the_tag_limit_writes_no_row() {
        use nodedb_physical::physical_plan::document::{ReturningColumns, ReturningSpec};

        let mut h = make_core();
        h.core.ts_tuning.max_tag_cardinality = 2;
        let mut task = ingest_task(
            format!(
                "{COLLECTION},host=h0 value=1i\n\
                 {COLLECTION},host=h1 value=2i\n\
                 {COLLECTION},host=h2 value=3i\n"
            )
            .into_bytes(),
            Some(7),
        );
        if let PhysicalPlan::Timeseries(TimeseriesOp::Ingest { returning, .. }) =
            &mut task.request.plan
        {
            *returning = Some(ReturningSpec {
                columns: ReturningColumns::Star,
            });
        }
        let PhysicalPlan::Timeseries(op) = task.request.plan.clone() else {
            panic!("timeseries plan");
        };

        let response = h.core.dispatch_timeseries(&task, &op);
        assert_eq!(response.status, Status::Error);
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. })
            ),
            "got {:?}",
            response.error_code
        );

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        assert!(
            !h.core.columnar_memtables.contains_key(&key),
            "a refused ingest must not create the memtable"
        );
    }
}
