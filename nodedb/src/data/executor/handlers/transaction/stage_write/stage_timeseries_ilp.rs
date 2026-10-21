// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the timeseries ingests the row-keyed paths in
//! `stage_timeseries` do not take:
//!
//! - A raw line-protocol payload (`format = "ilp"`) that carries one
//!   surrogate per line is rewritten into the canonical line list and staged
//!   row by row through the canonical path, so a same-transaction read
//!   observes its rows.
//! - A payload in any format that carries no surrogates has no overlay key
//!   for its rows, as a native bulk ingest sends it. It is decided the way
//!   the canonical path decides a batch: normalized into line protocol,
//!   parsed, matched to its routed collection, admitted by the write policy
//!   and prevalidated against the memtable. It stages no row. COMMIT resolve
//!   serializes the ingest from the plan node, so the install still writes
//!   every line.
//! - A resolved payload (`format = "ts-resolved"`) is a sequenced
//!   transaction's ingest, resolved on the node that submitted it, its write
//!   policy decided there. Each row it carries stages under the surrogate of
//!   its line, so a later plan of the transaction reads it, as a
//!   non-sequenced transaction reads its own rows. COMMIT resolve serializes
//!   its rows from the plan node as they are.

use nodedb_types::RowIdentity;
use nodedb_types::timeseries::{SeriesCatalog, SeriesKey};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::StampedIngest;
use crate::data::executor::handlers::timeseries::raw_scan::emit_memtable_rows_at;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{
    ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::engine::timeseries::ilp_ingest;
use crate::engine::timeseries::resolved_ingest::ResolvedTsBatch;
use crate::types::{TenantId, TxnId};
use crate::util::rmpv_value::rmpv_to_value;

use super::context::StageCtx;
use super::stage_timeseries::CanonicalIlpStage;

impl CoreLoop {
    /// Stage a resolved ingest. Each row the batch carries stages one overlay
    /// `Put` under the surrogate of the line it came from, its body the row
    /// as a scan reads it, so a later plan of the transaction reads it. An
    /// ingest with no surrogates has no overlay key and stages no row.
    /// Answers the rows the batch carries and the lines its resolve rejected.
    pub(super) fn stage_resolved_timeseries(&mut self, args: CanonicalIlpStage<'_>) -> Response {
        let task = args.task;
        match self.stage_resolved_rows(&args) {
            Ok((rows, rejected)) => self.stage_ts_count_response(task, rows, rejected),
            Err(error) => self.response_error(task, error),
        }
    }

    fn stage_resolved_rows(
        &mut self,
        args: &CanonicalIlpStage<'_>,
    ) -> Result<(usize, usize), ErrorCode> {
        let batch =
            ResolvedTsBatch::from_bytes(args.payload).map_err(|error| ErrorCode::Internal {
                detail: format!("timeseries insert: invalid resolved batch: {error}"),
            })?;
        let rejected = usize::try_from(batch.rejected).unwrap_or(usize::MAX);
        if args.surrogates.is_empty() {
            return Ok((batch.rows.len(), rejected));
        }
        let images = resolved_row_images(&batch)?;
        for (row, image) in batch.rows.iter().zip(&images) {
            let surrogate = usize::try_from(row.line)
                .ok()
                .and_then(|line| args.surrogates.get(line).copied())
                .ok_or_else(|| ErrorCode::Internal {
                    detail: format!(
                        "timeseries insert: resolved row of line {} has no surrogate among {}",
                        row.line,
                        args.surrogates.len()
                    ),
                })?;
            let body = nodedb_types::value_to_msgpack(&rmpv_to_value(image)).map_err(|e| {
                ErrorCode::Internal {
                    detail: format!("timeseries insert: row encode failed: {e}"),
                }
            })?;
            let ctx = StageCtx::new(
                args.task,
                args.tid,
                args.txn_id,
                args.collection,
                RowIdentity::for_surrogate(surrogate),
                surrogate,
            );
            self.stage_put_capped(&ctx, body)?;
        }
        Ok((batch.rows.len(), rejected))
    }

    /// Stage a raw line-protocol ingest that carries surrogates. Answers the
    /// number of lines.
    pub(super) fn stage_raw_ilp_rows(&mut self, args: CanonicalIlpStage<'_>) -> Response {
        let task = args.task;
        let text = match std::str::from_utf8(args.payload) {
            Ok(text) => text,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("line protocol is not UTF-8: {error}"),
                    },
                );
            }
        };
        let lines: Vec<String> = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect();
        if lines.len() != args.surrogates.len() {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "line-protocol payload carries {} lines but {} surrogates",
                        lines.len(),
                        args.surrogates.len()
                    ),
                },
            );
        }
        let canonical = match zerompk::to_msgpack_vec(&lines) {
            Ok(bytes) => bytes,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("canonical ILP payload encode failed: {error}"),
                    },
                );
            }
        };
        self.stage_canonical_ilp_rows(CanonicalIlpStage {
            payload: &canonical,
            ..args
        })
    }

    /// Restore the exact overlay state from before canonical ILP row staging.
    /// When this batch created the overlay, remove the now-empty representation
    /// and balance the creation gauge exactly once.
    pub(super) fn rollback_canonical_ilp_stage(
        &mut self,
        txn_id: TxnId,
        prior_marker: Option<usize>,
    ) {
        match prior_marker {
            Some(marker) => {
                if let Some(overlay) = self.txn_overlays.get_mut(&txn_id) {
                    overlay.rollback_to(marker);
                }
            }
            None => {
                let remove_empty = self.txn_overlays.get_mut(&txn_id).is_some_and(|overlay| {
                    overlay.rollback_to(0);
                    overlay.is_empty()
                });
                if remove_empty
                    && self.txn_overlays.remove(&txn_id).is_some()
                    && let Some(metrics) = &self.metrics
                {
                    metrics
                        .active_txn_overlays
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    /// Preview `payload` in `format` against the schema in force, through
    /// the scratch ingest a `RETURNING` preview uses. Mutates nothing. The
    /// COMMIT-time resolve is authoritative.
    ///
    /// The preview resolves against the schema the transaction's previous
    /// staged ingest into the collection previewed to, or the live schema
    /// for its first: the chain the COMMIT resolve follows. The chain
    /// advances only when the caller stages the statement
    /// ([`Self::note_stage_preview`]).
    pub(super) fn stage_preview(
        &self,
        target: &PreviewTarget<'_>,
        payload: &[u8],
        format: &str,
        now_ms: i64,
    ) -> Result<StagePreview, ErrorCode> {
        let lines = self.stamped_ingest_lines(StampedIngest {
            database_id: target.task.request.database_id,
            tid: TenantId::new(target.tid),
            collection: target.collection,
            payload,
            format,
            now_ms,
        })?;
        let source = lines.join("\n");
        let parsed = crate::engine::timeseries::ilp::parse_batch(&source).map_err(|error| {
            ErrorCode::RejectedPrevalidation {
                reason: format!("invalid line-protocol row: {error}"),
            }
        })?;
        Ok(self.preview_lines(target, parsed.lines(), now_ms))
    }

    /// Preview parsed `lines` against the chained schema. See
    /// [`Self::stage_preview`].
    fn preview_lines(
        &self,
        target: &PreviewTarget<'_>,
        lines: &[crate::engine::timeseries::ilp::IlpLine<'_>],
        now_ms: i64,
    ) -> StagePreview {
        let tenant = TenantId::new(target.tid);
        let key = (
            target.task.request.database_id,
            tenant,
            target.collection.to_string(),
        );
        let base = self
            .txn_overlays
            .get(&target.txn_id)
            .and_then(|overlay| overlay.ts_preview_schema(&key));
        let (outcome, scratch) =
            self.scratch_ilp_ingest(target.task, tenant, target.collection, lines, now_ms, base);
        StagePreview {
            accepted: outcome.accepted_line_indices.into_iter().collect(),
            rejected: outcome.rejected,
            schema: scratch.schema().clone(),
        }
    }

    /// Advance the transaction's preview chain for `target`'s collection to
    /// `schema`, the schema a staged statement previewed to. A savepoint
    /// rollback restores the schema it replaced.
    pub(super) fn note_stage_preview(
        &mut self,
        target: &PreviewTarget<'_>,
        schema: ColumnarSchema,
    ) {
        let key = (
            target.task.request.database_id,
            TenantId::new(target.tid),
            target.collection.to_string(),
        );
        self.txn_overlay_mut(target.txn_id)
            .note_ts_preview_schema(&key, schema);
    }

    /// The answer of a staged timeseries ingest: the rows its preview
    /// accepts, as `affected`, and the lines it rejects, as `rejected`.
    pub(super) fn stage_ts_count_response(
        &self,
        task: &ExecutionTask,
        affected: usize,
        rejected: usize,
    ) -> Response {
        let counts = serde_json::json!({ "affected": affected, "rejected": rejected });
        match crate::data::executor::response_codec::encode_json_as_msgpack(&counts) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(error) => self.response_error(task, error),
        }
    }

    /// Decide an ingest that carries no surrogates, in `format`. Stages no
    /// row and answers the lines its preview accepts and rejects.
    pub(super) fn stage_unkeyed_timeseries(
        &mut self,
        args: CanonicalIlpStage<'_>,
        format: &str,
    ) -> Response {
        match self.decide_unkeyed_timeseries(&args, format) {
            Ok((lines, preview, now_ms)) => {
                let rejected = preview.rejected;
                self.note_stage_preview(&preview_target(&args), preview.schema);
                // COMMIT resolve stamps the untimed rows with the instant the
                // statement read, not its own.
                let coll_key = (
                    args.task.request.database_id,
                    TenantId::new(args.tid),
                    args.collection.to_string(),
                );
                self.txn_overlay_mut(args.txn_id)
                    .note_unkeyed_ingest_now(&coll_key, now_ms);
                self.stage_ts_count_response(args.task, lines.saturating_sub(rejected), rejected)
            }
            Err(error) => self.response_error(args.task, error),
        }
    }

    /// Normalize, parse, route-check, admit and prevalidate an unkeyed
    /// ingest. Mutates nothing. Returns the number of lines, the preview of
    /// the lines against the transaction's chained schema, and the instant
    /// the ingest read as its default row timestamp.
    fn decide_unkeyed_timeseries(
        &self,
        args: &CanonicalIlpStage<'_>,
        format: &str,
    ) -> Result<(usize, StagePreview, i64), ErrorCode> {
        let tenant = TenantId::new(args.tid);
        let now_ms = self.ingest_stamp_ms();
        let lines = self.stamped_ingest_lines(StampedIngest {
            database_id: args.task.request.database_id,
            tid: tenant,
            collection: args.collection,
            payload: args.payload,
            format,
            now_ms,
        })?;
        let source = lines.join("\n");
        let parsed = match crate::engine::timeseries::ilp::parse_batch(&source) {
            Ok(parsed) if parsed.lines().len() == lines.len() => parsed,
            _ => {
                return Err(ErrorCode::RejectedPrevalidation {
                    reason: "invalid line-protocol row".into(),
                });
            }
        };
        let measurement = args
            .collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(args.collection);
        if parsed
            .lines()
            .iter()
            .any(|row| row.measurement.as_ref() != measurement)
        {
            return Err(ErrorCode::RejectedPrevalidation {
                reason: "line-protocol measurement does not match routed collection".into(),
            });
        }
        crate::data::executor::handlers::timeseries::admit_ilp_lines(
            args.rls_write_check,
            parsed.lines(),
            self.declared_ts_time_key(args.task.request.database_id, tenant, args.collection),
            now_ms,
            args.tid,
            args.collection,
            &self.crs_context(),
        )?;
        self.prevalidate_deferred_ilp_ingest(args.task, tenant, args.collection, parsed.lines())?;
        let preview = self.preview_lines(&preview_target(args), parsed.lines(), now_ms);
        Ok((lines.len(), preview, now_ms))
    }
}

/// The preview target of the staged statement `args`.
pub(super) fn preview_target<'a>(args: &CanonicalIlpStage<'a>) -> PreviewTarget<'a> {
    PreviewTarget {
        task: args.task,
        tid: args.tid,
        txn_id: args.txn_id,
        collection: args.collection,
    }
}

/// The staged statement a preview is for.
pub(super) struct PreviewTarget<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
}

/// What a stage-time preview decides about an ingest's lines, against the
/// transaction's chained schema. The COMMIT-time resolve is authoritative.
pub(super) struct StagePreview {
    /// Input line indices the preview accepts.
    accepted: std::collections::HashSet<usize>,
    /// The number of lines the preview rejects.
    pub rejected: usize,
    /// The schema the preview resolved to: the next preview's base once the
    /// statement stages.
    pub schema: ColumnarSchema,
}

impl StagePreview {
    /// Whether the preview accepts input line `line`.
    pub(super) fn accepts(&self, line: usize) -> bool {
        self.accepted.contains(&line)
    }
}

/// Each row of `batch` as a scan reads it. A row that carries its image
/// reads as that image. Otherwise the rows land in a scratch memtable of the
/// batch's schema and read back through the raw-scan row emitter, the
/// emitter a base scan uses, so a staged row reads like a stored one.
fn resolved_row_images(batch: &ResolvedTsBatch) -> Result<Vec<rmpv::Value>, ErrorCode> {
    if batch.rows.iter().all(|row| !row.image.is_empty()) {
        return batch
            .rows
            .iter()
            .map(|row| {
                crate::util::bounded_msgpack::read_value(&row.image).map_err(|e| {
                    ErrorCode::Internal {
                        detail: format!("timeseries insert: invalid resolved row image: {e}"),
                    }
                })
            })
            .collect();
    }
    let schema = ColumnarSchema {
        columns: batch.columns.clone(),
        timestamp_idx: usize::try_from(batch.timestamp_idx).unwrap_or(0),
        codecs: vec![nodedb_codec::ColumnCodec::Auto; batch.columns.len()],
    };
    // The rows were accepted at resolve, so the scratch memtable takes every
    // one: no limit of its own applies.
    let config = ColumnarMemtableConfig {
        max_memory_bytes: usize::MAX,
        hard_memory_limit: usize::MAX,
        max_tag_cardinality: u32::MAX,
    };
    let mut scratch = ColumnarMemtable::new(schema, config);
    let mut catalog = SeriesCatalog::default();
    for row in &batch.rows {
        let series_key = SeriesKey::new(batch.measurement.as_str(), row.tags.clone());
        let series_id = ilp_ingest::resolve_series(&mut catalog, &series_key);
        scratch
            .ingest_row(series_id, &row.values)
            .map_err(|e| ErrorCode::Internal {
                detail: format!("timeseries insert: resolved row does not read back: {e}"),
            })?;
    }
    let indices: Vec<usize> = (0..batch.rows.len()).collect();
    emit_memtable_rows_at(&scratch, &indices).map_err(ErrorCode::from)
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::task::ExecutionTask;
    use crate::types::TxnId;

    const TID: u64 = 1;

    fn unkeyed_ingest() -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics,host=a value=1".to_vec(),
            format: "ilp".to_owned(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// Stage the unkeyed ingest at `stage_ms` and resolve it at
    /// `resolve_ms`. Returns the resolved redo record.
    fn stage_then_resolve(stage_ms: i64, resolve_ms: i64) -> Vec<u8> {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let txn_id = TxnId::new(5);
        let mut request = make_default_task().request;
        request.txn_id = Some(txn_id);
        let task = ExecutionTask::new(request);
        let plan = unkeyed_ingest();

        core.epoch_system_ms = Some(stage_ms);
        let staged = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);

        core.epoch_system_ms = Some(resolve_ms);
        let resolved = core.execute_resolve_txn(&task, TID, txn_id, std::slice::from_ref(&plan));
        assert_eq!(resolved.status, Status::Ok, "{:?}", resolved.error_code);
        resolved.payload.as_bytes().to_vec()
    }

    /// A sequenced transaction stages its resolved ingest, then a later plan
    /// of the same transaction scans the collection: the scan reads the
    /// staged row, as a non-sequenced transaction reads its own rows.
    #[test]
    fn a_staged_resolved_ingest_reads_back_in_its_transaction() {
        use crate::data::executor::handlers::timeseries::TsResolveInput;
        use crate::data::executor::handlers::transaction::overlay::TimeseriesOverlayMergeParams;
        use crate::engine::timeseries::resolved_ingest::{RESOLVED_INGEST_FORMAT, TsDriftPolicy};
        use crate::types::TenantId;

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, "metrics");
        let txn_id = TxnId::new(9);
        let mut request = make_default_task().request;
        request.txn_id = Some(txn_id);
        let task = ExecutionTask::new(request);

        let batch = core
            .resolve_ts_batch(
                &task,
                TsResolveInput {
                    tid: TenantId::new(TID),
                    collection: collection.as_str(),
                    lines: &["metrics,host=a value=2.5 1000000000".to_string()],
                    now_ms: 1_000,
                    drift: TsDriftPolicy::ApplyByName,
                    needs_images: false,
                    base: None,
                },
            )
            .expect("resolve");
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.clone(),
            payload: batch.to_bytes().expect("encode batch"),
            format: RESOLVED_INGEST_FORMAT.to_owned(),
            wal_lsn: None,
            surrogates: vec![nodedb_types::Surrogate::new(41)],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
            returning: None,
            rls_filters: Vec::new(),
        });
        let staged = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);

        let coll_key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.as_str().to_string(),
        );
        let mut rows = Vec::new();
        core.merge_overlay_into_timeseries_scan(
            TimeseriesOverlayMergeParams {
                txn_id,
                coll_key: &coll_key,
                time_range: (i64::MIN, i64::MAX),
                filter_predicates: &[],
                has_filters: false,
                rls_predicates: &[],
                limit: usize::MAX,
            },
            &mut rows,
        )
        .expect("merge the overlay");
        assert_eq!(rows.len(), 1, "the staged row reads back: {rows:?}");
        let rmpv::Value::Map(fields) = &rows[0] else {
            panic!("a staged row reads as a map: {:?}", rows[0]);
        };
        let value = fields
            .iter()
            .find(|(key, _)| key.as_str() == Some("value"))
            .map(|(_, value)| value.clone());
        assert_eq!(value, Some(rmpv::Value::F64(2.5)));
    }

    /// A staged statement reports the lines its stage-time preview rejects:
    /// `affected` counts the lines the preview accepts, `rejected` the rest.
    #[test]
    fn a_staged_ingest_reports_the_lines_its_preview_rejects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let mut request = make_default_task().request;
        request.txn_id = Some(TxnId::new(6));
        let task = ExecutionTask::new(request);
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics value=1 1000000000\nmetrics value=\"text\" 2000000000".to_vec(),
            format: "ilp".to_owned(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });

        let staged = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            staged.payload.as_bytes(),
        );
        let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
        assert_eq!(body["affected"], serde_json::json!(1));
        assert_eq!(body["rejected"], serde_json::json!(1));
    }

    /// A staged statement previews against the schema the transaction's
    /// earlier staged ingest into the collection previewed to, not the live
    /// one: a field that conflicts with a column only the earlier statement
    /// added is rejected at the statement, as COMMIT rejects it.
    #[test]
    fn a_staged_preview_chains_the_transactions_earlier_ingests() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let mut request = make_default_task().request;
        request.txn_id = Some(TxnId::new(8));
        let task = ExecutionTask::new(request);
        let ingest = |payload: &[u8]| {
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
                payload: payload.to_vec(),
                format: "ilp".to_owned(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            })
        };
        let rejected = |core: &mut CoreLoop, payload: &[u8]| {
            let staged = core.execute_stage_write(&task, TID, &ingest(payload));
            assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
            let json = crate::data::executor::response_codec::decode_payload_to_json(
                staged.payload.as_bytes(),
            );
            let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
            body["rejected"].clone()
        };

        assert_eq!(
            rejected(&mut core, b"metrics extra=1.5 1000000000"),
            serde_json::json!(0)
        );
        assert_eq!(
            rejected(&mut core, b"metrics extra=\"text\" 2000000000"),
            serde_json::json!(1),
            "the second statement previews against the column the first one added"
        );
    }

    #[test]
    fn an_unkeyed_ingest_resolves_with_the_instant_its_statement_read() {
        let staged_early = stage_then_resolve(1_000_000, 9_000_000);
        assert_eq!(
            staged_early,
            stage_then_resolve(1_000_000, 1_000_000),
            "the untimed row carries the stage instant"
        );
        assert_ne!(
            staged_early,
            stage_then_resolve(9_000_000, 9_000_000),
            "the commit instant does not reach the row"
        );
    }
}
