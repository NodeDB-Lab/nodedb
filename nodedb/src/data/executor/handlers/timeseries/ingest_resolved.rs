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
use crate::data::executor::response_codec;
use crate::engine::timeseries::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
use crate::engine::timeseries::ilp_ingest;
use crate::engine::timeseries::resolved_ingest::{ResolvedTsBatch, ResolvedTsRow, TsDriftPolicy};

use super::admission;
use super::ingest_dispatch::{TimeseriesApplyMode, TimeseriesIngestParams};
use super::ingest_resolved_fit::{CollKey, SchemaFit, landing_values};
use super::ingest_resolved_preview::{landed_images, stamp_system_ordinal};
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
            provenance: _,
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
        // The resolved rows carry the stamp their resolve took. It folds into
        // the shared clock on every replica and every replay.
        self.observe_ingest_stamp_ms(now_ms);
        // Every landing row stores the batch's system-time ordinal, the one
        // this apply stamps, not the placeholder its resolve took. A clone
        // copy's rows keep the ordinals they carry.
        let system_ordinal = self.ts_batch_ordinal(mode, now_ms);
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
        let mut row_values = landing_values(fit, &schema, &batch);
        // A clone copy keeps its source version's ordinal, which folds into
        // the shared clock so a later write stamps above.
        for carried in stamp_system_ordinal(
            &schema,
            batch.carried_system,
            &mut row_values,
            system_ordinal,
        ) {
            self.hlc.update_from_remote(carried);
        }
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
        let wants_images = batch.emits_events || returning.is_some();
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
        let horizon = self.maintenance.version_purge.safe_horizon;
        if let Some(mt) = self.columnar_memtables.get_mut(&key) {
            let catalog = self.ts_series_catalogs.entry(key.clone()).or_default();
            let mut lvc = self.ts_last_value_caches.get_mut(&key);
            if let Some(cache) = lvc.as_deref_mut() {
                cache.set_horizon(horizon);
            }
            for (position, (row, values)) in batch.rows.iter().zip(&row_values).enumerate() {
                let Some(values) = values else {
                    continue;
                };
                let series_key = match ilp_ingest::row_series_key(
                    task.request.database_id,
                    batch.measurement.as_str(),
                    &schema,
                    values,
                ) {
                    Ok(series_key) => series_key,
                    Err(e) => {
                        failed = Some(format!("row {position}: {e}"));
                        break;
                    }
                };
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
                                &schema,
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
        let images: Vec<&[u8]> = landed_images(&batch, &landed, by_name_images.as_deref());
        if emits {
            self.emit_resolved_ts_events(task, collection, &images);
        }
        // The ingest's rejected count: the lines its resolve rejected, and
        // the rows this install rejected for a type conflict.
        let accepted = landed.len();
        let rejected = batch.rejected.saturating_add(conflicts as u64);
        if mode == TimeseriesApplyMode::RedoInstall {
            self.note_redo_ts_install(TsInstallCount {
                collection: collection.to_string(),
                accepted: accepted as u64,
                rejected,
            });
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
                read_vshards: crate::types::ReadShards::new(),
                write_set: Vec::new(),
            },
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ingest_resolved_test_support::{
        install, install_committed, key, resolve, resolve_lines,
    };
    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;

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

        core.drop_overlay_entry(crate::types::TxnId::new(7))
            .expect("drop overlay");
        assert_eq!(install(&mut core, &batch, 2).status, Status::Ok);
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
