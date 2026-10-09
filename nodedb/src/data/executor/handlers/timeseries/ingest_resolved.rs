// SPDX-License-Identifier: BUSL-1.1

//! Install a resolved timeseries ingest: store exactly the rows its record
//! carries.
//!
//! The rows were resolved against a schema. When the live memtable evolves
//! to that same schema, each row's values land as resolved and its image is
//! the stored row. Otherwise a concurrent write changed the schema since the
//! resolve:
//! - a gate-admitted live install under [`TsDriftPolicy::Refuse`] stores
//!   nothing and answers `OllpRetryRequired`. Its record is cancelled, and
//!   the writer resolves again;
//! - every other install stores each value under its column name. It is a
//!   committed entry, a committed redo, or a replayed record, and it never
//!   refuses.
//!
//! A refusable install also refuses while a staged transaction holds the
//! collection between its resolve and its install, so that install finds
//! the schema it resolved against. The log orders every other install.
//!
//! An install that stores by name rejects each row whose value conflicts
//! with its column's type in the live schema. The live schema is the schema
//! in force at the install's log position on every replica
//! (`ingest_resolved_fit`), so every replica rejects the same rows. The
//! record is still applied: its other rows land, and the rejected rows count
//! in the answer's `rejected`. Only landed rows emit events. What such an
//! install stored is recorded before its first row lands
//! (`ingest_resolved_outcome`), so WAL catch-up and a committed redo's
//! writer read what landed, not what the record carries.
//!
//! A node-local limit never rejects a row. An install flushes first when the
//! memtable cannot take the record whole: a live install here, a committed
//! redo and a replayed record before their record. Every landing row is then
//! checked before any lands. A row that still cannot land names a condition
//! this node cannot satisfy, and the core fail-stops rather than store less
//! than its peers.

use std::borrow::Cow;

use crate::bridge::envelope::{Admission, ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::event_emit::RowWriteEvent;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
use crate::engine::timeseries::ilp_ingest;
use crate::engine::timeseries::resolved_ingest::{ResolvedTsBatch, ResolvedTsRow, TsDriftPolicy};
use crate::event::WriteOp;
use crate::event::types::RowId;
use nodedb_types::timeseries::SeriesKey;

use super::admission;
use super::ingest_dispatch::{TimeseriesApplyMode, TimeseriesIngestParams};
use super::ingest_resolved_fit::{CollKey, SchemaFit, landing_values};
use super::ingest_resolved_returning::decode_returning_images;
use crate::data::executor::response_codec::IngestRejection;
use crate::engine::timeseries::install_counts::TsInstallCount;

impl CoreLoop {
    pub(super) fn execute_resolved_ingest(
        &mut self,
        params: TimeseriesIngestParams<'_>,
    ) -> Response {
        let TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload,
            wal_lsn,
            now_ms: _,
            mode,
            rls_write_check: _,
            returning,
            rls_filters,
        } = params;
        let batch = match ResolvedTsBatch::from_bytes(payload) {
            Ok(batch) => batch,
            Err(error) => return self.response_error(task, error),
        };
        let now_ms = batch.now_ms;
        let key: CollKey = (task.request.database_id, tid, collection.to_string());
        let live = mode == TimeseriesApplyMode::Immediate;
        let fit = self.schema_fit(&key, &batch);
        // Only a gate-admitted live install can refuse: its record is
        // cancelled, and the writer resolves again. A committed entry, a
        // committed redo and a replayed record install on every node alike.
        let refusable = live
            && batch.drift == TsDriftPolicy::Refuse
            && matches!(task.request.admission, Admission::Admitted);

        if refusable {
            let held = self
                .ts_resolve_holds
                .get(&key)
                .is_some_and(|holders| !holders.is_empty());
            if held || matches!(fit, SchemaFit::ByName) {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        if mode == TimeseriesApplyMode::RedoInstall {
            self.record_redo_ts_pre_image(task.request.database_id, tid, collection);
        }

        // A live install flushes before the first row lands when the memtable
        // cannot take the batch whole. A replayed or installed sub-record
        // never flushes here: the replay arm flushed before the record.
        if live
            && self.ts_resolved_needs_flush(&key, &batch)
            && let Err(e) =
                self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
        {
            if refusable {
                return self.response_error(task, ErrorCode::from(e));
            }
            // A committed entry every other replica applies cannot make room
            // on this node.
            let detail = format!(
                "a resolved timeseries record for '{collection}' cannot make room on this \
                 node: its pre-ingest flush failed: {e}"
            );
            return self.unfit_resolved_ingest(task, false, &detail);
        }

        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        let cols_before = self
            .columnar_memtables
            .get(&key)
            .map_or(0, |mt| mt.schema().columns.len());
        if is_new_memtable {
            let schema = self.resolved_memtable_schema(&key, &batch);
            let config = ColumnarMemtableConfig::from_tuning(&self.ts_tuning);
            self.columnar_memtables
                .insert(key.clone(), ColumnarMemtable::new(schema, config));
        }
        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after init: {collection}"),
                },
            );
        };
        for (name, column_type) in &batch.columns {
            if !column_type.is_time() {
                mt.add_column(name.clone(), *column_type);
            }
        }
        let schema_changed = !is_new_memtable && mt.schema().columns.len() != cols_before;
        let schema = mt.schema().clone();

        // A row whose value conflicts with its column's type in the live
        // schema has no values: every replica rejects it at this log
        // position. A refusable install never gets here with one, since only
        // a by-name fit conflicts.
        let row_values = landing_values(fit, &schema, &batch);
        let conflicts = row_values.iter().filter(|values| values.is_none()).count();
        // Every landing row is checked before any lands: the tag
        // dictionaries hold its tags after the flush before the record.
        let landing_rows: Cow<'_, [ResolvedTsRow]> = if conflicts == 0 {
            Cow::Borrowed(batch.rows.as_slice())
        } else {
            Cow::Owned(
                batch
                    .rows
                    .iter()
                    .zip(&row_values)
                    .filter(|(_, values)| values.is_some())
                    .map(|(row, _)| row.clone())
                    .collect(),
            )
        };
        if !admission::rows_have_tag_headroom(
            mt,
            &batch.columns,
            &landing_rows,
            self.ts_tuning.max_tag_cardinality,
        ) {
            let detail = format!(
                "a resolved timeseries record for '{collection}' does not fit this node: its \
                 tags exceed the tag cardinality limit ({}) of an emptied memtable",
                self.ts_tuning.max_tag_cardinality
            );
            return self.unfit_resolved_ingest(task, refusable, &detail);
        }
        if conflicts > 0 {
            tracing::warn!(
                collection,
                rejected = conflicts,
                "committed timeseries rows rejected: a value conflicts with its column's type"
            );
        }

        // A by-name install stores values its resolve did not see, so its
        // landing rows' images are rendered from the live schema. When some
        // consumer reads the collection, its outcome is durable before the
        // first row lands, so WAL catch-up rebuilds exactly these events. An
        // install no reader asks for images of renders none.
        let wants_images =
            batch.emits_events || returning.is_some() || mode == TimeseriesApplyMode::RedoInstall;
        let by_name_images = match fit {
            SchemaFit::Exact => None,
            SchemaFit::ByName if !wants_images => None,
            SchemaFit::ByName => {
                let rendered = self
                    .by_name_images(&schema, &row_values)
                    .and_then(|images| match wal_lsn.filter(|_| batch.emits_events) {
                        Some(lsn) => self
                            .write_ts_install_outcome(lsn, collection, &batch, &row_values, &images)
                            .map(|()| images),
                        None => Ok(images),
                    });
                match rendered {
                    Ok(images) => Some(images),
                    Err(e) => {
                        let detail = format!(
                            "a resolved timeseries record for '{collection}' cannot record its \
                             install on this node: {e}"
                        );
                        return self.unfit_resolved_ingest(task, false, &detail);
                    }
                }
            }
        };

        let mut landed: Vec<usize> = Vec::with_capacity(batch.rows.len());
        let mut failed: Option<String> = None;
        if let Some(mt) = self.columnar_memtables.get_mut(&key) {
            let catalog = self.ts_series_catalogs.entry(key.clone()).or_default();
            let mut lvc = self.ts_last_value_caches.get_mut(&key);
            for (position, (row, values)) in batch.rows.iter().zip(&row_values).enumerate() {
                let Some(values) = values else {
                    continue;
                };
                let series_key = SeriesKey::new(batch.measurement.as_str(), row.tags.clone());
                let series_id = ilp_ingest::resolve_series(catalog, &series_key);
                match mt.ingest_row(series_id, values) {
                    Err(e) => {
                        failed = Some(format!("row {position}: {e}"));
                        break;
                    }
                    Ok(_) => {
                        landed.push(position);
                        if let Some(cache) = lvc.as_deref_mut() {
                            ilp_ingest::update_last_value(
                                cache,
                                series_id,
                                row.timestamp_ms,
                                values,
                            );
                        }
                    }
                }
            }
        }
        if let Some(reason) = failed {
            // Every landing row was checked, so this is a memtable fault, and
            // part of the record landed: this core's state is unknown.
            let detail = format!(
                "a resolved timeseries record for '{collection}' landed {} of {} rows: {reason}",
                landed.len(),
                batch.rows.len() - conflicts
            );
            return self.unfit_resolved_ingest(task, false, &detail);
        }
        // The record's rows landed. A committed-redo install is noted by the
        // apply once the whole record installed.
        if mode != TimeseriesApplyMode::RedoInstall
            && let Some(lsn) = wal_lsn
        {
            self.note_ts_record_applied(lsn);
        }

        // Each landed row's image is the row as stored. An exact fit stored
        // the values its resolve imaged. A by-name fit's images were
        // rendered from the live schema above.
        let emits = batch.emits_events && mode != TimeseriesApplyMode::Replay;
        let images: Vec<&[u8]> = match &by_name_images {
            None => landed
                .iter()
                .filter_map(|position| batch.rows.get(*position))
                .map(|row| row.image.as_slice())
                .collect(),
            Some(rendered) => rendered.iter().map(Vec::as_slice).collect(),
        };
        if emits {
            self.emit_resolved_ts_events(task, collection, &images);
        }
        // The ingest's rejected count: the lines its resolve rejected, and
        // the rows this install rejected for a type conflict.
        let accepted = landed.len();
        let rejected = batch.rejected.saturating_add(conflicts as u64);
        if mode == TimeseriesApplyMode::RedoInstall {
            let stored_images = images
                .iter()
                .filter(|image| !image.is_empty())
                .map(|image| image.to_vec())
                .collect();
            self.note_redo_ts_install(
                TsInstallCount {
                    collection: collection.to_string(),
                    accepted: accepted as u64,
                    rejected,
                },
                stored_images,
            );
        }

        // An image that does not decode refuses the statement below, once the
        // landed rows' bookkeeping has run.
        let returned_rows = match returning {
            Some(_) => decode_returning_images(collection, &images),
            None => Ok(Vec::new()),
        };

        if mode != TimeseriesApplyMode::RedoInstall {
            let needs_flush = self
                .columnar_memtables
                .get(&key)
                .is_some_and(|mt| mt.memory_bytes() >= self.ts_tuning.memtable_budget_bytes);
            if live
                && needs_flush
                && let Err(e) =
                    self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
            {
                return self.response_error(task, ErrorCode::from(e));
            }
            if accepted > 0 {
                // no-determinism: Instant::now runs only for the operational idle/checkpoint timer, outside a committed-redo install.
                self.last_ts_ingest = Some(std::time::Instant::now());
            }
            self.checkpoint_coordinator
                .mark_dirty("timeseries", accepted);
            self.recharge_ts_memtable_budget(tid, task.request.database_id, collection);
        }

        if let Some(spec) = returning {
            let returned_rows = match returned_rows {
                Ok(rows) => rows,
                Err(e) => return self.response_error(task, e),
            };
            // A row set has no place for a rejected row, so the rows the
            // install rejected travel beside it as the rejected-lines notice.
            let rejection = (rejected > 0).then(|| IngestRejection {
                collection: collection.to_string(),
                lines: rejected,
            });
            return self.timeseries_stored_returning_response(
                task,
                spec,
                rls_filters,
                &returned_rows,
                rejection,
            );
        }
        let result = if is_new_memtable || schema_changed {
            let schema_columns: Vec<serde_json::Value> = schema
                .columns
                .iter()
                .map(|(name, col_type)| serde_json::json!([name, col_type.ddl_type_name()]))
                .collect();
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
                "schema_columns": schema_columns,
            })
        } else {
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
            })
        };
        match response_codec::encode_json_as_msgpack(&result) {
            Ok(json) => Response {
                request_id: task.request.request_id,
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: Payload::from_vec(json),
                watermark_lsn: self.watermark,
                error_code: None,
                stage_vote: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            },
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// The answer to a resolved record whose rows cannot land as carried.
    ///
    /// A refusable install stores nothing and answers `OllpRetryRequired`:
    /// its record is cancelled and the writer resolves again. Every other
    /// install is a record whose landing rows every node stores, so skipping
    /// one for a node-local reason diverges this node from its peers.
    /// The core fail-stops instead: it refuses every request until a restart
    /// replays the record.
    fn unfit_resolved_ingest(
        &mut self,
        task: &ExecutionTask,
        refusable: bool,
        detail: &str,
    ) -> Response {
        if refusable {
            return self.response_error(task, ErrorCode::OllpRetryRequired);
        }
        self.fail_stop_core(FailStopCause::CommittedRowUnfit, detail);
        self.response_error(
            task,
            ErrorCode::RetryableRefusal {
                reason: detail.to_string(),
            },
        )
    }

    /// Whether the live memtable of `key` must flush before `batch` lands
    /// whole: it is at its soft or hard limit, the engine budget is under
    /// pressure, it cannot also hold the bytes the batch took at resolve, or
    /// its tag dictionaries have no room for the batch's tags.
    fn ts_resolved_needs_flush(&self, key: &CollKey, batch: &ResolvedTsBatch) -> bool {
        let governor_pressure = self
            .governor
            .try_reserve(key.0, key.1, nodedb_mem::EngineId::Timeseries, 0)
            .is_err();
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        let hard_limit = self.ts_tuning.memtable_hard_limit_bytes;
        self.columnar_memtables.get(key).is_some_and(|mt| {
            let resident = mt.memory_bytes();
            let with_batch = resident
                .saturating_add(usize::try_from(batch.resolved_bytes).unwrap_or(usize::MAX));
            resident >= soft_limit
                || resident >= hard_limit
                || (resident > 0 && with_batch >= hard_limit)
                || governor_pressure
                || !admission::rows_have_tag_headroom(
                    mt,
                    &batch.columns,
                    &batch.rows,
                    self.ts_tuning.max_tag_cardinality,
                )
        })
    }

    /// Emit one Insert event per landed row's image, in row order.
    fn emit_resolved_ts_events(
        &mut self,
        task: &crate::data::executor::task::ExecutionTask,
        collection: &str,
        images: &[&[u8]],
    ) {
        for image in images {
            self.emit_event_with_row_id_as(
                task,
                task.request.event_source,
                RowWriteEvent {
                    collection,
                    op: WriteOp::Insert,
                    row_id: RowId::Batch,
                    new_value: Some(*image),
                    old_value: None,
                    image_fault: None,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TimeseriesOp;

    use super::super::{TimeseriesApplyMode, TimeseriesIngestExec, TsResolveInput};
    use crate::bridge::envelope::{
        Admission, ErrorCode, ExemptReason, PhysicalPlan, Response, Status,
    };
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::timeseries::raw_scan::emit_memtable_rows_at;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::timeseries::resolved_ingest::{
        RESOLVED_INGEST_FORMAT, ResolvedTsBatch, TsDriftPolicy,
    };
    use crate::event::bus::{EventConsumerRx, create_event_bus_with_capacity};
    use crate::event::interest::{EventInterest, Interest, InterestSlice, InterestSources};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    const TENANT: u64 = 1;
    const COLLECTION: &str = "metrics";

    fn task_at(lsn: u64) -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                restart_identity: false,
            }),
            Some(Lsn::new(lsn)),
        )
    }

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        )
    }

    /// Wire `core` to an event bus, with a consumer on the collection.
    fn wire_events(core: &mut CoreLoop) -> EventConsumerRx {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        core.set_event_producer(producers.pop().expect("producer"));
        let triggers = InterestSlice::new();
        let mut interest = Interest::default();
        interest.insert(DatabaseId::DEFAULT, COLLECTION);
        triggers.publish(interest);
        let set = EventInterest::new();
        set.install(InterestSources {
            triggers,
            ..InterestSources::default()
        });
        core.set_event_interest(set);
        consumers.pop().expect("consumer")
    }

    fn resolve(core: &CoreLoop, line: &str, now_ms: i64) -> ResolvedTsBatch {
        resolve_lines(core, &[line], now_ms)
    }

    fn resolve_lines(core: &CoreLoop, lines: &[&str], now_ms: i64) -> ResolvedTsBatch {
        let lines: Vec<String> = lines.iter().map(|line| (*line).to_string()).collect();
        core.resolve_ts_batch(
            &task_at(0),
            TsResolveInput {
                tid: TenantId::new(TENANT),
                collection: COLLECTION,
                lines: &lines,
                now_ms,
                drift: TsDriftPolicy::Refuse,
                needs_images: false,
                base: None,
            },
        )
        .expect("resolve")
    }

    /// Install `batch` as a live write that passed the admission gate.
    fn install(core: &mut CoreLoop, batch: &ResolvedTsBatch, lsn: u64) -> Response {
        install_as(core, batch, lsn, Admission::Admitted)
    }

    /// Install `batch` as a committed entry's apply: its order was decided
    /// by the log.
    fn install_committed(core: &mut CoreLoop, batch: &ResolvedTsBatch, lsn: u64) -> Response {
        install_as(
            core,
            batch,
            lsn,
            Admission::Exempt(ExemptReason::AlreadyOrdered),
        )
    }

    fn install_as(
        core: &mut CoreLoop,
        batch: &ResolvedTsBatch,
        lsn: u64,
        admission: Admission,
    ) -> Response {
        let payload = batch.to_bytes().expect("encode batch");
        let mut task = task_at(lsn);
        task.request.admission = admission;
        core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid: TenantId::new(TENANT),
            collection: COLLECTION,
            payload: &payload,
            format: RESOLVED_INGEST_FORMAT,
            wal_lsn: Some(lsn),
            provenance: None,
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: &[],
        })
    }

    /// The stored row at `index`, as a scan reads it.
    fn stored_row(core: &CoreLoop, index: usize) -> rmpv::Value {
        let mt = core.columnar_memtables.get(&key()).expect("memtable");
        emit_memtable_rows_at(mt, &[index])
            .expect("read row")
            .pop()
            .expect("one row")
    }

    /// The images of every event the core emitted since the last drain.
    fn drained_images(events: &mut EventConsumerRx) -> Vec<rmpv::Value> {
        std::iter::from_fn(|| events.try_recv())
            .map(|event| {
                let image = event.new_value.expect("insert image");
                crate::util::bounded_msgpack::read_value(&image).expect("decode image")
            })
            .collect()
    }

    /// Two ingests resolve against the same schema and add different
    /// columns. The first installs. The second finds the schema changed,
    /// stores nothing and refuses, then resolves again and installs. Each
    /// ingest's event equals the row it stored.
    #[test]
    fn interleaved_ingests_that_add_columns_emit_their_stored_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core);

        let first = resolve(&core, "metrics alpha=1 1000000000", 1_000);
        let second = resolve(&core, "metrics beta=2 2000000000", 2_000);

        assert_eq!(install(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(drained_images(&mut events), vec![stored_row(&core, 0)]);

        let refused = install(&mut core, &second, 2);
        assert_eq!(refused.status, Status::Error);
        assert_eq!(
            refused.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert!(
            drained_images(&mut events).is_empty(),
            "a refusal stores and emits nothing"
        );
        assert_eq!(
            core.columnar_memtables.get(&key()).map(|mt| mt.row_count()),
            Some(1)
        );

        let second = resolve(&core, "metrics beta=2 2000000000", 2_000);
        assert_eq!(install(&mut core, &second, 3).status, Status::Ok);
        assert_eq!(drained_images(&mut events), vec![stored_row(&core, 1)]);
    }

    /// A committed apply cannot refuse, whatever drift its batch names: it
    /// stores each value under its column name, and a column the batch does
    /// not name takes its empty value.
    #[test]
    fn an_apply_by_name_install_stores_values_under_their_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let first = resolve(&core, "metrics alpha=1 1000000000", 1_000);
        let second = resolve(&core, "metrics beta=2 2000000000", 2_000);
        let by_name = second.clone().with_drift(TsDriftPolicy::ApplyByName);
        assert_eq!(install(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(install_committed(&mut core, &second, 2).status, Status::Ok);
        assert_eq!(install(&mut core, &by_name, 3).status, Status::Ok);
        assert_eq!(
            core.columnar_memtables.get(&key()).map(|mt| mt.row_count()),
            Some(3)
        );

        let rmpv::Value::Map(fields) = stored_row(&core, 1) else {
            panic!("a stored row is a map");
        };
        let field = |name: &str| {
            fields
                .iter()
                .find(|(key, _)| key.as_str() == Some(name))
                .map(|(_, value)| value.clone())
        };
        assert_eq!(field("beta"), Some(rmpv::Value::F64(2.0)));
        assert!(
            matches!(field("alpha"), Some(rmpv::Value::Nil)),
            "a column the batch does not name is empty"
        );
    }

    /// A staged transaction that resolved into the collection holds it until
    /// its overlay drops. An autocommit install into it refuses meanwhile.
    #[test]
    fn a_held_collection_refuses_a_live_install() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.ts_resolve_holds
            .entry(key())
            .or_default()
            .insert(crate::types::TxnId::new(7));

        let batch = resolve(&core, "metrics alpha=1 1000000000", 1_000);
        let refused = install(&mut core, &batch, 1);
        assert_eq!(
            refused.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );

        core.drop_overlay_entry(crate::types::TxnId::new(7));
        assert_eq!(install(&mut core, &batch, 2).status, Status::Ok);
    }

    /// A line whose field conflicts with its column's type is rejected at
    /// resolve. The batch carries the count and the reason, and the install
    /// reports it as the ingest's `rejected` count.
    #[test]
    fn a_type_conflicting_line_is_reported_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let batch = resolve_lines(
            &core,
            &[
                "metrics value=1 1000000000",
                "metrics value=\"text\" 2000000000",
            ],
            1_000,
        );
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rejected, 1);
        assert!(
            batch.first_rejection.is_some(),
            "the rejection names its reason"
        );

        let response = install_committed(&mut core, &batch, 1);
        assert_eq!(response.status, Status::Ok);
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
        assert_eq!(body["accepted"], serde_json::json!(1));
        assert_eq!(body["rejected"], serde_json::json!(1));
    }

    /// Two ingests resolve against the same schema and give a new column
    /// conflicting types. The first installs. The second is a committed
    /// entry: its conflicting row is rejected and counted, its other row
    /// lands with the column empty, the core stays up, and its one event
    /// equals the row it stored.
    #[test]
    fn a_committed_row_that_conflicts_is_rejected_and_the_rest_land() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core);

        let first = resolve(&core, "metrics extra=1.5 1000000000", 1_000);
        let second = resolve_lines(
            &core,
            &[
                "metrics extra=\"text\" 2000000000",
                "metrics value=2 3000000000",
            ],
            2_000,
        );
        assert_eq!(second.rows.len(), 2, "the resolve saw no conflict");

        assert_eq!(install_committed(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(drained_images(&mut events), vec![stored_row(&core, 0)]);

        let response = install_committed(&mut core, &second, 2);
        assert_eq!(response.status, Status::Ok);
        assert!(!core.is_fail_stopped());
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
        assert_eq!(body["accepted"], serde_json::json!(1));
        assert_eq!(body["rejected"], serde_json::json!(1));
        assert_eq!(
            core.columnar_memtables.get(&key()).map(|mt| mt.row_count()),
            Some(2)
        );
        let stored = stored_row(&core, 1);
        assert_eq!(drained_images(&mut events), vec![stored.clone()]);
        let rmpv::Value::Map(fields) = stored else {
            panic!("a stored row is a map");
        };
        let field = |name: &str| {
            fields
                .iter()
                .find(|(key, _)| key.as_str() == Some(name))
                .map(|(_, value)| value.clone())
        };
        assert_eq!(field("value"), Some(rmpv::Value::F64(2.0)));
        assert!(
            matches!(field("extra"), Some(rmpv::Value::Nil)),
            "a column the row does not name is empty, not a conflict"
        );
    }

    /// A flush drains the memtable and keeps its schema on disk. A restart
    /// seeds the memtable with it, so a committed row that conflicts with
    /// the flushed schema is rejected after the restart as before it.
    #[test]
    fn a_restart_keeps_the_flushed_schema_for_conflicts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let first = resolve(&core, "metrics extra=1.5 1000000000", 1_000);
        assert_eq!(install_committed(&mut core, &first, 1).status, Status::Ok);
        core.flush_ts_collection(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            COLLECTION,
            1_000,
        )
        .expect("flush");
        drop(core);

        let (mut restarted, _tx, _rx) = make_core_with_dir(dir.path());
        restarted.load_ts_registries().expect("load registries");
        let second = resolve(&restarted, "metrics extra=\"text\" 2000000000", 2_000);
        assert_eq!(second.rejected, 1, "the resolve sees the flushed schema");

        // A proposer that never held the column resolves the row whole.
        let unseeded = {
            let scratch = tempfile::tempdir().expect("tempdir");
            let (other, _tx, _rx) = make_core_with_dir(scratch.path());
            resolve(&other, "metrics extra=\"text\" 2000000000", 2_000)
        };
        assert_eq!(unseeded.rows.len(), 1);
        let response = install_committed(&mut restarted, &unseeded, 2);
        assert_eq!(response.status, Status::Ok);
        assert!(!restarted.is_fail_stopped());
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
        assert_eq!(body["accepted"], serde_json::json!(0));
        assert_eq!(body["rejected"], serde_json::json!(1));
    }

    /// The ring drops the events of a committed by-name install that
    /// rejected a row. WAL catch-up rebuilds them from the record and the
    /// install's outcome: one event per landed row, with its stored image,
    /// and none for the rejected row.
    #[test]
    fn catch_up_after_an_apply_time_rejection_rebuilds_only_landed_rows() {
        use crate::control::server::wal_dispatch::{
            TimeseriesIngestRecord, encode_timeseries_ingest_payload,
        };
        use crate::engine::timeseries::install_outcome::{TsOutcomeIndex, outcome_dir};
        use crate::event::wal_replay_scope::{ReplayScope, RowSources};

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core);

        let first = resolve(&core, "metrics extra=1.5 1000000000", 1_000);
        let second = resolve_lines(
            &core,
            &[
                "metrics extra=\"text\" 2000000000",
                "metrics value=2 3000000000",
            ],
            2_000,
        );
        assert_eq!(install_committed(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(install_committed(&mut core, &second, 2).status, Status::Ok);
        assert_eq!(
            drained_images(&mut events).len(),
            2,
            "one live event per landed row"
        );

        // The ring lost both events. Catch-up reads the logged record.
        let logged = encode_timeseries_ingest_payload(TimeseriesIngestRecord {
            collection: COLLECTION,
            payload: &second.to_bytes().expect("encode batch"),
            provenance: None,
            format: RESOLVED_INGEST_FORMAT,
            default_timestamp_ms: second.now_ms,
        })
        .expect("encode record");
        let outcomes =
            TsOutcomeIndex::load(&outcome_dir(dir.path(), core.core_id)).expect("load outcomes");
        let scope = ReplayScope {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TENANT),
            vshard_id: VShardId::new(0),
            lsn: Lsn::new(2),
            sources: RowSources::uniform(crate::event::EventSource::User),
            commit_hlc: None,
        };
        let mut sequence = 0;
        let rebuilt = crate::event::wal_replay_timeseries::replayed_timeseries_events(
            &logged,
            &scope,
            &outcomes,
            &mut sequence,
        );
        let images: Vec<rmpv::Value> = rebuilt
            .iter()
            .map(|event| {
                let image = event.new_value.as_deref().expect("insert image");
                crate::util::bounded_msgpack::read_value(image).expect("decode image")
            })
            .collect();
        assert_eq!(images, vec![stored_row(&core, 1)]);
    }

    /// A follower catches up by a snapshot taken after the leader flushed a
    /// column's first rows. It installs the leader's schema, so a committed
    /// row that conflicts with it is rejected on the follower as on the
    /// leader, before and after the follower restarts.
    #[test]
    fn a_follower_restored_from_a_snapshot_rejects_as_the_leader_does() {
        let leader_dir = tempfile::tempdir().expect("tempdir");
        let (mut leader, _tx, _rx) = make_core_with_dir(leader_dir.path());
        let first = resolve(&leader, "metrics extra=1.5 1000000000", 1_000);
        assert_eq!(install_committed(&mut leader, &first, 1).status, Status::Ok);
        leader
            .flush_ts_collection(
                TenantId::new(TENANT),
                DatabaseId::DEFAULT,
                COLLECTION,
                1_000,
            )
            .expect("flush");
        let snapshot = leader.execute_create_tenant_snapshot(&task_at(1), TENANT, false);
        assert_eq!(snapshot.status, Status::Ok, "{:?}", snapshot.error_code);

        let follower_dir = tempfile::tempdir().expect("tempdir");
        let (mut follower, _ftx, _frx) = make_core_with_dir(follower_dir.path());
        let restored = follower.execute_restore_tenant_snapshot(
            &task_at(1),
            TENANT,
            snapshot.payload.as_bytes(),
            true,
            &[],
            &[],
        );
        assert_eq!(restored.status, Status::Ok, "{:?}", restored.error_code);

        // A proposer that never held the column resolves the row whole.
        let conflicting = {
            let scratch = tempfile::tempdir().expect("tempdir");
            let (other, _otx, _orx) = make_core_with_dir(scratch.path());
            resolve(&other, "metrics extra=\"text\" 2000000000", 2_000)
        };
        let rejected = |core: &mut CoreLoop| {
            let response = install_committed(core, &conflicting, 2);
            assert_eq!(response.status, Status::Ok);
            let json = crate::data::executor::response_codec::decode_payload_to_json(
                response.payload.as_bytes(),
            );
            let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
            body["rejected"].clone()
        };
        assert_eq!(rejected(&mut leader), serde_json::json!(1));
        assert_eq!(rejected(&mut follower), serde_json::json!(1));
        drop(follower);
        let (mut restarted, _rtx, _rrx) = make_core_with_dir(follower_dir.path());
        restarted.load_ts_registries().expect("load registries");
        assert_eq!(rejected(&mut restarted), serde_json::json!(1));
    }

    /// The log already orders a committed entry after a staged
    /// transaction's install, so a hold never refuses it.
    #[test]
    fn a_held_collection_installs_a_committed_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.ts_resolve_holds
            .entry(key())
            .or_default()
            .insert(crate::types::TxnId::new(7));

        let batch = resolve(&core, "metrics alpha=1 1000000000", 1_000);
        assert_eq!(install_committed(&mut core, &batch, 1).status, Status::Ok);
        assert_eq!(
            core.columnar_memtables.get(&key()).map(|mt| mt.row_count()),
            Some(1)
        );
    }

    /// A record whose tags this node's dictionaries cannot hold, even after
    /// a flush. A live install refuses it and stores nothing. A committed
    /// install stores nothing either, and fail-stops the core rather than
    /// skip a row every other node stores.
    #[test]
    fn a_record_that_cannot_fit_refuses_live_and_fail_stops_committed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.set_metrics(std::sync::Arc::clone(&metrics));
        let batch = resolve_lines(
            &core,
            &[
                "metrics,host=a value=1 1000000000",
                "metrics,host=b value=2 2000000000",
            ],
            1_000,
        );
        assert_eq!(batch.rows.len(), 2);
        core.ts_tuning.max_tag_cardinality = 1;

        let refused = install(&mut core, &batch, 1);
        assert_eq!(
            refused.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert!(!core.is_fail_stopped());

        let halted = install_committed(&mut core, &batch, 2);
        assert!(matches!(
            halted.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ));
        assert!(core.is_fail_stopped());
        // The node-wide record `/healthz` and the native `STATUS` read names
        // the cause, and readiness answers 503.
        let report = metrics
            .core_fail_stops
            .report()
            .expect("the fail-stop reaches the node-wide record");
        assert_eq!(report.cause, "committed_row_unfit");
        let (status, body) = crate::control::metrics::system::core_fail_stop::to_http_response(
            report,
            metrics.core_fail_stops.stopped_cores(),
        );
        assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["cause"], serde_json::json!("committed_row_unfit"));
        assert_eq!(
            core.columnar_memtables
                .get(&key())
                .map_or(0, |mt| mt.row_count()),
            0,
            "no row of an unfit record lands"
        );
    }
}
