// SPDX-License-Identifier: BUSL-1.1

//! `TimeseriesOp::ReadAggregate`: the finalized rows of one continuous
//! aggregate, read on the core that owns its root source.
//!
//! The read answers from complete buckets: everything the aggregate owes is
//! folded first, the partitions an `OnFlush` / `OnSeal` aggregate lacks and
//! the partitions its pending refresh cuts name. A `Manual` or `Periodic`
//! aggregate answers from its last refresh cut.
//!
//! A read whose root owes nothing answers at once. Otherwise the core holds
//! the read, and every `tick()` folds the backlog under the per-tick budget
//! (`drive_cagg_waiters`), so the core keeps serving other requests. The
//! tick answers the read once nothing is owed, or with `DeadlineExceeded` at
//! its deadline.

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::convert::rmpv_time_cell;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::TimeKind;
use crate::engine::timeseries::continuous_agg::{
    ColumnLayout, FoldScope, GroupCell, PartialAggregate,
};
use crate::types::{DatabaseId, TenantId};
use crate::util::rmpv_value::value_to_rmpv;
use nodedb_types::SystemTimeScope;

use super::cagg_fold::{CaggRoot, FoldFilter};

/// A continuous-aggregate read waiting for its root's backlog to fold.
pub struct CaggReadWaiter {
    task: ExecutionTask,
    root: String,
    aggregate: String,
    system_time: SystemTimeScope,
}

impl CoreLoop {
    /// Hold a continuous-aggregate read while its root owes folding.
    /// Returns the task unchanged when it is anything else, or when nothing
    /// is owed.
    pub(in crate::data::executor) fn hold_cagg_read(
        &mut self,
        task: ExecutionTask,
    ) -> Option<ExecutionTask> {
        let read = match task.plan() {
            PhysicalPlan::Timeseries(TimeseriesOp::ReadAggregate {
                collection,
                aggregate,
                system_time,
            }) => Some((
                collection.as_str().to_string(),
                aggregate.clone(),
                *system_time,
            )),
            _ => None,
        };
        let Some((root, aggregate, system_time)) = read else {
            return Some(task);
        };
        let db = task.request.database_id.as_u64();
        let tid = task.request.tenant_id.as_u64();
        if !self.cagg_root_owes(db, tid, &root) {
            return Some(task);
        }
        // The tick works this root until a pass finds nothing left.
        self.continuous_agg_mgr.mark_backfill(db, tid, &root);
        self.maintenance.cagg_read_waiters.push(CaggReadWaiter {
            task,
            root,
            aggregate,
            system_time,
        });
        None
    }

    /// Whether some aggregate rooted at `root` lacks a partition its `Tick`
    /// scope folds. Reads `root`'s image first, so the answer covers every
    /// registered tier.
    fn cagg_root_owes(&mut self, database_id: u64, tenant_id: u64, root: &str) -> bool {
        if !self.owns_cagg_root(database_id, root) {
            return false;
        }
        self.load_cagg_images(database_id, tenant_id, root);
        if self
            .ensure_ts_registry(TenantId::new(tenant_id), DatabaseId::new(database_id), root)
            .is_err()
        {
            // The read reports the registry error itself.
            return false;
        }
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tenant_id),
            root.to_string(),
        );
        let mgr = &self.continuous_agg_mgr;
        self.ts_registries.get(&key).is_some_and(|registry| {
            registry.current_partitions().any(|entry| {
                mgr.needs_fold(
                    database_id,
                    tenant_id,
                    root,
                    &entry.dir_name,
                    FoldScope::Tick,
                )
            })
        })
    }

    /// Answer every held read whose root owes nothing more, or whose
    /// deadline passed. Called from `tick()` and after each fold pass.
    pub fn answer_cagg_read_waiters(&mut self) {
        if self.maintenance.cagg_read_waiters.is_empty() {
            return;
        }
        let waiters = std::mem::take(&mut self.maintenance.cagg_read_waiters);
        for waiter in waiters {
            // no-determinism: the deadline bounds only when the answer is
            // sent; the fold does not depend on it.
            let response = if std::time::Instant::now() > waiter.task.request.deadline {
                self.response_error(&waiter.task, ErrorCode::DeadlineExceeded)
            } else if self.cagg_root_owes(
                waiter.task.request.database_id.as_u64(),
                waiter.task.request.tenant_id.as_u64(),
                &waiter.root,
            ) {
                self.maintenance.cagg_read_waiters.push(waiter);
                continue;
            } else {
                self.execute_ts_read_aggregate(
                    &waiter.task,
                    &waiter.root,
                    &waiter.aggregate,
                    waiter.system_time,
                )
            };
            if let Err(e) = self
                .response_tx
                .try_push(BridgeResponse { inner: response })
            {
                tracing::warn!(
                    core = self.core_id,
                    error = %e,
                    "failed to send a continuous aggregate read: response queue full"
                );
            }
        }
    }
    /// Finalized rows of continuous aggregate `aggregate` rooted at `root`:
    /// a msgpack array of maps, one per bucket and group, holding `bucket`,
    /// the GROUP BY columns and the output columns. A read at a cut returns
    /// the rows the aggregate held at the cut (`read_aggregate_at.rs`).
    pub(in crate::data::executor) fn execute_ts_read_aggregate(
        &mut self,
        task: &ExecutionTask,
        root: &str,
        aggregate: &str,
        system_time: SystemTimeScope,
    ) -> Response {
        let db = task.request.database_id.as_u64();
        let tid = task.request.tenant_id.as_u64();
        let now_ms = self.ingest_now_ms();
        let at = match system_time {
            SystemTimeScope::Current => None,
            SystemTimeScope::AsOf(ms) => Some(nodedb_types::ms_to_ordinal_upper(ms)),
            SystemTimeScope::Pinned { ordinal, .. }
            | SystemTimeScope::TxnSnapshot { ordinal, .. } => Some(ordinal),
            SystemTimeScope::AllVersions => {
                return self.response_error(
                    task,
                    crate::Error::BadRequest {
                        detail: format!(
                            "continuous aggregate '{aggregate}' keeps no version history: \
                             AS OF SYSTEM TIME NULL cannot read it"
                        ),
                    },
                );
            }
        };
        let read = super::read_aggregate_at::CaggReadAt {
            database_id: task.request.database_id,
            tid: task.request.tenant_id,
            root,
            aggregate,
            at: at.unwrap_or(i64::MAX),
        };
        match self
            .complete_cagg_root(db, tid, root, now_ms)
            .and_then(|()| self.encode_cagg_rows(read, at.is_some()))
        {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Load `root`'s image and fold every partition its aggregates owe:
    /// the backfill of the flush tiers and the pending refresh cuts. A root
    /// whose partitions cannot be read is quarantined.
    pub(in crate::data::executor) fn complete_cagg_root(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        now_ms: i64,
    ) -> crate::Result<()> {
        self.load_cagg_images(database_id, tenant_id, root);
        let pass = self.fold_cagg_root(
            CaggRoot {
                database_id,
                tenant_id,
                root,
            },
            FoldScope::Tick,
            FoldFilter::Every,
            usize::MAX,
            now_ms,
        );
        match pass {
            Ok(pass) => {
                if pass.complete {
                    self.continuous_agg_mgr
                        .finish_tick(database_id, tenant_id, root);
                }
                Ok(())
            }
            Err(e) => {
                self.quarantine_cagg_root(database_id, tenant_id, root, &e);
                Err(e)
            }
        }
    }

    /// Encode the rows of `read.aggregate`: its current buckets, or the
    /// buckets it held at `read.at` when `at_cut`.
    fn encode_cagg_rows(
        &self,
        read: super::read_aggregate_at::CaggReadAt<'_>,
        at_cut: bool,
    ) -> crate::Result<Vec<u8>> {
        let database_id = read.database_id.as_u64();
        let tenant_id = read.tid.as_u64();
        let aggregate = read.aggregate;
        let mgr = &self.continuous_agg_mgr;
        let Some(def) = mgr.get_definition(database_id, tenant_id, aggregate) else {
            return Err(crate::Error::UndefinedObject {
                kind: "continuous aggregate",
                name: aggregate.to_string(),
            });
        };
        if def.stale {
            return Err(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("continuous aggregate '{aggregate}'"),
                detail: "it is stale: its state or a source partition could not be read; \
                         drop and re-create it"
                    .into(),
            });
        }
        let layout = ColumnLayout::of(def);
        let kind = mgr
            .time_kind(database_id, tenant_id, aggregate)
            .unwrap_or(TimeKind::Millis);
        let at_snapshot = if at_cut {
            Some(self.cagg_buckets_at(read)?)
        } else {
            None
        };
        let buckets: Vec<&PartialAggregate> = match &at_snapshot {
            Some(held) => {
                let mut sorted: Vec<&PartialAggregate> = held.values().collect();
                sorted
                    .sort_by(|a, b| (a.bucket_ts, &a.group_key).cmp(&(b.bucket_ts, &b.group_key)));
                sorted
            }
            None => mgr
                .get_materialized(database_id, tenant_id, aggregate)
                .unwrap_or_default(),
        };
        let mut rows = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let mut fields = Vec::with_capacity(1 + def.group_by.len() + def.aggregates.len());
            fields.push((
                rmpv::Value::from("bucket"),
                rmpv_time_cell(kind, bucket.bucket_ts)?,
            ));
            for (column, cell) in def.group_by.iter().zip(&bucket.group_key) {
                fields.push((rmpv::Value::from(column.as_str()), group_cell_value(cell)));
            }
            for expr in &def.aggregates {
                let value = bucket.finalize(expr, &layout)?;
                fields.push((
                    rmpv::Value::from(expr.output_column.as_str()),
                    value_to_rmpv(&value),
                ));
            }
            rows.push(rmpv::Value::Map(fields));
        }
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &rmpv::Value::Array(rows)).map_err(|e| {
            crate::Error::Codec {
                detail: format!("encode rows of continuous aggregate '{aggregate}': {e}"),
            }
        })?;
        Ok(payload)
    }
}

/// The value a GROUP BY key cell renders as.
fn group_cell_value(cell: &GroupCell) -> rmpv::Value {
    match cell {
        GroupCell::Null => rmpv::Value::Nil,
        GroupCell::Int(i) => rmpv::Value::Integer((*i).into()),
        GroupCell::Float(bits) => rmpv::Value::F64(f64::from_bits(*bits)),
        GroupCell::Text(s) => rmpv::Value::String(s.as_str().into()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::cagg_test_support::{
        COLLECTION, ROWS_A, ROWS_B, TENANT, flush, host_def, ingest, register, task_at,
    };
    use super::{DatabaseId, ErrorCode, PhysicalPlan, TenantId, TimeseriesOp};
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::continuous_agg::RefreshPolicy;
    use crate::types::VShardId;
    use nodedb_physical::physical_plan::RefreshCut;

    /// `(host, count, sum)` of every row the read returns, sorted.
    fn read(core: &mut crate::data::executor::core_loop::CoreLoop) -> Vec<(String, i64, i64)> {
        let resp = core.execute_ts_read_aggregate(
            &task_at(20),
            COLLECTION,
            "by_host",
            nodedb_types::SystemTimeScope::Current,
        );
        rows_of(&resp)
    }

    /// `(host, count, sum)` of every row in a read's response, sorted.
    fn rows_of(resp: &crate::bridge::envelope::Response) -> Vec<(String, i64, i64)> {
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        let value = rmpv::decode::read_value(&mut resp.payload.as_ref()).expect("decode rows");
        let mut rows: Vec<(String, i64, i64)> = value
            .as_array()
            .expect("an array of rows")
            .iter()
            .map(|row| {
                let map = row.as_map().expect("a row map");
                let get = |key: &str| {
                    map.iter()
                        .find(|(k, _)| k.as_str() == Some(key))
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| panic!("row lacks '{key}': {row:?}"))
                };
                assert!(get("bucket").is_i64() || get("bucket").is_ext());
                let int = |key: &str| get(key).as_i64().unwrap_or(0);
                let host = get("host").as_str().unwrap_or_default().to_string();
                (host, int("c"), int("s"))
            })
            .collect();
        rows.sort();
        rows
    }

    #[test]
    fn a_read_folds_what_the_aggregate_owes_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        assert_eq!(
            read(&mut core),
            vec![("a".to_string(), 2, 101), ("b".to_string(), 1, 10)]
        );
    }

    #[test]
    fn a_manual_aggregate_reads_its_last_cut() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::Manual));
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        assert!(read(&mut core).is_empty(), "no cut yet");
        let cut = RefreshCut {
            aggregate: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::Manual).incarnation,
            refresh_id: 1,
            periodic: false,
        };
        let resp = core.execute_cagg_refresh_cut(&task_at(9), COLLECTION, &cut);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        flush(&mut core);
        assert_eq!(
            read(&mut core),
            vec![("a".to_string(), 2, 101), ("b".to_string(), 1, 10)],
            "the read completes the pending cut and nothing after it"
        );
    }

    /// A `ReadAggregate` task for `by_host`, as the Control Plane sends it.
    fn read_task() -> crate::data::executor::task::ExecutionTask {
        crate::data::executor::core_loop::CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Timeseries(TimeseriesOp::ReadAggregate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                aggregate: "by_host".into(),
                system_time: nodedb_types::SystemTimeScope::Current,
            }),
            None,
        )
    }

    /// One row in a later second of the same hour as `ROWS_A`.
    const ROWS_C: &str = "metrics,host=c value=7i 1700000005000000000\n";

    #[test]
    fn a_read_owing_a_backlog_waits_for_the_budgeted_fold() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, mut rx) = make_core_with_dir(dir.path());
        core.ts_tuning.cagg_fold_partitions_per_tick = 1;
        for (rows, lsn) in [(ROWS_A, 1), (ROWS_B, 2), (ROWS_C, 3)] {
            assert_eq!(ingest(&mut core, rows, lsn), Status::Ok);
            flush(&mut core);
        }
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));

        assert!(
            core.hold_cagg_read(read_task()).is_none(),
            "three partitions owed, one folds per tick: the read is held"
        );
        assert_eq!(core.maintenance.cagg_read_waiters.len(), 1);

        let mut ticks = 0;
        while !core.maintenance.cagg_read_waiters.is_empty() {
            ticks += 1;
            assert!(ticks <= 3, "the backlog folds within three ticks");
            assert_eq!(core.fold_pending_cagg_partitions(0), 1);
            if core.maintenance.cagg_read_waiters.is_empty() {
                break;
            }
            assert!(rx.try_pop().is_err(), "no answer while a backlog is owed");
            // The core serves other requests between ticks: this row stays
            // in the memtable, so the aggregate does not include it.
            let row = format!("metrics,host=d value=5i 170000000{ticks}500000000\n");
            assert_eq!(ingest(&mut core, &row, 10 + ticks), Status::Ok);
        }
        assert_eq!(ticks, 3, "one partition per tick, answered after the last");

        let resp = rx.try_pop().expect("the held read is answered").inner;
        assert_eq!(
            rows_of(&resp),
            vec![
                ("a".to_string(), 3, 10_101),
                ("b".to_string(), 2, 1_010),
                ("c".to_string(), 1, 7),
            ]
        );
        assert!(
            core.hold_cagg_read(read_task()).is_some(),
            "a read owing nothing runs at once"
        );
    }

    #[test]
    fn a_held_read_past_its_deadline_is_answered_with_deadline_exceeded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, mut rx) = make_core_with_dir(dir.path());
        core.ts_tuning.cagg_fold_partitions_per_tick = 1;
        for (rows, lsn) in [(ROWS_A, 1), (ROWS_B, 2)] {
            assert_eq!(ingest(&mut core, rows, lsn), Status::Ok);
            flush(&mut core);
        }
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        let mut task = read_task();
        task.request.deadline = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(1))
            .expect("an instant one millisecond ago");
        assert!(core.hold_cagg_read(task).is_none());
        core.answer_cagg_read_waiters();
        let resp = rx.try_pop().expect("the expired read is answered").inner;
        assert_ne!(resp.status, Status::Ok);
        assert!(
            matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::DeadlineExceeded)
            ),
            "{:?}",
            resp.error_code
        );
        assert!(core.maintenance.cagg_read_waiters.is_empty());
    }

    #[test]
    fn reading_an_unknown_aggregate_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let resp = core.execute_ts_read_aggregate(
            &task_at(20),
            COLLECTION,
            "missing",
            nodedb_types::SystemTimeScope::Current,
        );
        assert_ne!(resp.status, Status::Ok);
    }
}
