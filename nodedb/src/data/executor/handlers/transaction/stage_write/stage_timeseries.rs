// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `TimeseriesOp::Ingest`.
//!
//! A timeseries INSERT issued inside a `BEGIN..COMMIT` block is staged here,
//! one overlay `Put` per row, so a later same-transaction RAW timeseries
//! SELECT observes the newly inserted rows (read-your-own-writes) before
//! COMMIT. COMMIT resolves the buffered `TimeseriesOp::Ingest` plan into a
//! redo sub-record, which is the sole durable apply. The staged batch records
//! the instant it read as its default row timestamp, and resolve stamps the
//! batch's untimed rows with that instant, so every replica stores the rows
//! the statement decided.
//!
//! No memtable mutation at statement time: staging writes ONLY into the
//! per-transaction overlay (`txn_overlays`), never into `columnar_memtables`.
//! ROLLBACK is therefore handled entirely by dropping / rewinding the
//! transaction overlay (`TxnOverlay`'s journal, `MetaOp::DropTxnOverlay`) —
//! no undo-log entry is required, exactly like the columnar statement-time
//! staging path.
//!
//! Row-level security: the write policy decides every row here, at the
//! statement, before any overlay entry is written. Both payload shapes are
//! decided through the same helpers the Data-Plane ingest gate uses, on the
//! NORMALIZED row — the values that will be stored, not the values that were
//! submitted — so the statement-time decision and the COMMIT-time one are made
//! on a byte-identical image and can never disagree.
//!
//! Row identity: a timeseries row is identified internally by its `series_id`
//! (a hash of measurement + tags), which is not a cross-engine surrogate. For
//! staging, each row is keyed by the per-row `Surrogate` the planner minted
//! via `assign_fresh` (`convert_timeseries_ingest`) — a fresh unique id per
//! row so every staged INSERT occupies its own overlay slot. The overlay's
//! identity side-map holds that surrogate's decimal `RowIdentity`.
//!
//! Row body encoding: each row's `{field => value}` map is stored VERBATIM
//! (the exact column names the INSERT used) and encoded via
//! `nodedb_types::value_to_msgpack` — the same shape
//! `merge_overlay_into_timeseries_scan` decodes and re-emits. Verbatim keys are
//! deliberate: the residual time predicate the planner extracts into the
//! scan's `time_range` is NOT stripped from the serialized field-filters, so a
//! `WHERE ts >= …` scan still carries a `ts` `ScanFilter`. `ScanFilter::matches_binary`
//! returns `false` for a field absent from the row, so renaming the timestamp
//! column would make that residual filter drop every staged row. Keeping the
//! original key lets both the field-filter (`matches_binary` on `ts`) and the
//! merge's alias-aware time-range prune resolve correctly.

use nodedb_types::value::Value;
use nodedb_types::{RowIdentity, Surrogate};

use super::context::StageCtx;
use super::stage_timeseries_ilp::preview_target;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

/// Inputs for [`CoreLoop::stage_timeseries_insert`]. Bundled because the raw
/// parameter list exceeds the project's too-many-arguments bound.
pub(in crate::data::executor) struct StageTimeseriesInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub surrogates: &'a [Surrogate],
    pub format: &'a str,
    /// Compiled row-level-security WRITE predicate carried by the plan,
    /// decided here against the normalized row every payload shape produces.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// Borrowed inputs for the canonical line-protocol staging path. Bundled
/// because the raw parameter list exceeds the project's too-many-arguments
/// bound.
pub(super) struct CanonicalIlpStage<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub surrogates: &'a [Surrogate],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    /// Stage a `TimeseriesOp::Ingest` batch: decode the msgpack row maps and
    /// stage one overlay `Put` per row keyed by its surrogate, body stored
    /// verbatim. Returns the shared `stage_count_response` shape
    /// (`{"affected": N}`).
    pub(in crate::data::executor) fn stage_timeseries_insert(
        &mut self,
        params: StageTimeseriesInsertParams<'_>,
    ) -> Response {
        let StageTimeseriesInsertParams {
            task,
            tid,
            txn_id,
            collection,
            payload,
            surrogates,
            format,
            rls_write_check,
        } = params;

        let stage = CanonicalIlpStage {
            task,
            tid,
            txn_id,
            collection,
            payload,
            surrogates,
            rls_write_check,
        };
        if format == crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT {
            return self.stage_resolved_timeseries(stage);
        }
        // A value a declared DECIMAL, UUID or BOOLEAN column does not take
        // refuses the statement before anything is staged.
        if let Err(error) = self.admit_declared_ts_payload(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
            payload,
            format,
        ) {
            return self.response_error(task, error);
        }
        // The canonical line list always travels with one token per line.
        if format == "ilp-msgpack" {
            return self.stage_canonical_ilp_rows(stage);
        }
        // An ingest with no surrogates has no overlay key for its rows.
        if surrogates.is_empty() {
            return self.stage_unkeyed_timeseries(stage, format);
        }
        if format == "ilp" {
            return self.stage_raw_ilp_rows(stage);
        }

        let rows: Vec<Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(Value::Array(arr)) => arr,
            Ok(v @ Value::Object(_)) => vec![v],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "timeseries insert: payload must be array or object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("timeseries insert: invalid payload: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        // The default timestamp of every untimed row in this batch. The write
        // policy decides against it here, and COMMIT resolve stamps the rows
        // with it, so the stored image is the one decided.
        let now_ms = self.ingest_stamp_ms();

        // Decide the whole batch before the first staged put, so a refusal
        // leaves the overlay untouched and reports no affected count.
        //
        // The decision runs on the payload rather than on the decoded rows
        // above, because the policy governs the row that will be STORED and the
        // ingest normalizes these values on the way there — a numeric-looking
        // string becomes a number, the time column becomes milliseconds under
        // the declared `TIME_KEY`. `admit_msgpack_rows` performs that exact
        // normalization, so this gate and the one COMMIT replay runs decide a
        // byte-identical image and cannot disagree.
        if let Err(error) = crate::data::executor::handlers::timeseries::admit_msgpack_rows(
            rls_write_check,
            crate::data::executor::handlers::timeseries::MsgpackBatch {
                payload,
                measurement: collection
                    .split_once(':')
                    .map(|(_, name)| name)
                    .unwrap_or(collection),
                time_key: self.declared_ts_time_key(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                ),
                default_timestamp_ms: now_ms,
                exact: &self.declared_ts_exact_columns(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                ),
            },
            tid,
            collection,
            &self.crs_context(),
        ) {
            return self.response_error(task, error);
        }
        // A row the preview rejects is not staged: the statement reports it.
        let target = preview_target(&stage);
        let preview = match self.stage_preview(&target, payload, format, now_ms) {
            Ok(preview) => preview,
            Err(error) => return self.response_error(task, error),
        };

        let mut staged = 0usize;
        for (row_idx, row) in rows.iter().enumerate() {
            if !matches!(row, Value::Object(_)) || !preview.accepts(row_idx) {
                continue;
            }

            let surrogate = match surrogates.get(row_idx).copied() {
                Some(s) => s,
                None => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "timeseries insert: missing surrogate for staged row".into(),
                        },
                    );
                }
            };

            // Typed as a committed row reads it.
            let body = match self.staged_ts_body(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                row.clone(),
            ) {
                Ok(b) => b,
                Err(e) => return self.response_error(task, e),
            };

            let ctx = StageCtx::new(
                task,
                tid,
                txn_id,
                collection,
                RowIdentity::for_surrogate(surrogate),
                surrogate,
            );
            if let Err(e) = self.stage_put(&ctx, body) {
                return self.response_error(task, e);
            }
            staged += 1;
        }
        self.note_staged_ingest_now(task, tid, txn_id, collection, surrogates, now_ms);
        self.note_stage_preview(&target, preview.schema);
        self.stage_ts_count_response(task, staged, preview.rejected)
    }
}
