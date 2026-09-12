// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `TimeseriesOp::Ingest`.
//!
//! A timeseries INSERT issued inside a `BEGIN..COMMIT` block is staged here,
//! one overlay `Put` per row, so a later same-transaction RAW timeseries
//! SELECT observes the newly inserted rows (read-your-own-writes) before
//! COMMIT. COMMIT durable replay is unchanged: the buffered
//! `TimeseriesOp::Ingest` plan is still replayed through
//! `execute_timeseries_ingest` inside the COMMIT `TransactionBatch`, which
//! remains the sole durable apply.
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

use std::collections::HashMap;

use nodedb_types::value::Value;
use nodedb_types::{RowIdentity, Surrogate};

use super::context::StageCtx;
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
struct CanonicalIlpStage<'a> {
    task: &'a ExecutionTask,
    tid: u64,
    txn_id: TxnId,
    collection: &'a str,
    payload: &'a [u8],
    surrogates: &'a [Surrogate],
    rls_write_check: &'a nodedb_types::RlsWriteCheck,
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

        if format == "ilp-msgpack" {
            return self.stage_canonical_ilp_rows(CanonicalIlpStage {
                task,
                tid,
                txn_id,
                collection,
                payload,
                surrogates,
                rls_write_check,
            });
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
            payload,
            collection
                .split_once(':')
                .map(|(_, name)| name)
                .unwrap_or(collection),
            self.declared_ts_time_key(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
            ),
            self.ingest_now_ms(),
            tid,
            collection,
        ) {
            return self.response_error(task, error);
        }

        let mut staged = 0usize;
        for (row_idx, row) in rows.iter().enumerate() {
            if !matches!(row, Value::Object(_)) {
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

            let body = match nodedb_types::value_to_msgpack(row) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("timeseries insert: row encode failed: {e}"),
                        },
                    );
                }
            };

            let ctx = StageCtx::new(
                task,
                tid,
                txn_id,
                collection,
                RowIdentity::for_surrogate(surrogate),
                surrogate,
            );
            if let Err(e) = self.stage_put_capped(&ctx, body) {
                return self.response_error(task, e);
            }
            staged += 1;
        }

        self.stage_count_response(task, staged)
    }

    fn stage_canonical_ilp_rows(&mut self, args: CanonicalIlpStage<'_>) -> Response {
        let CanonicalIlpStage {
            task,
            tid,
            txn_id,
            collection,
            payload,
            surrogates,
            rls_write_check,
        } = args;
        let lines: Vec<String> = match zerompk::from_msgpack::<Vec<String>>(payload) {
            Ok(lines) if !lines.is_empty() => lines,
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: "empty canonical ILP payload".into(),
                    },
                );
            }
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("invalid canonical ILP payload: {error}"),
                    },
                );
            }
        };
        let canonical = match zerompk::to_msgpack_vec(&lines) {
            Ok(bytes) => bytes,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("canonical ILP payload re-encode failed: {error}"),
                    },
                );
            }
        };
        if canonical != payload || lines.len() != surrogates.len() {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "canonical ILP row/token mismatch".into(),
                },
            );
        }

        // Parse and route-check the complete batch before either admission or
        // overlay mutation. Joining is safe after the physical-line rejection
        // and retains each original canonical row as exactly one parser line.
        if lines
            .iter()
            .any(|line| line.contains('\n') || line.contains('\r'))
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "canonical ILP payload contains multiple physical lines".into(),
                },
            );
        }
        let grouped_source = lines.join("\n");
        let parsed = match crate::engine::timeseries::ilp::parse_batch(&grouped_source) {
            Ok(parsed) if parsed.lines().len() == lines.len() => parsed,
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: "invalid canonical ILP row".into(),
                    },
                );
            }
        };
        if parsed
            .lines()
            .iter()
            .any(|row| row.measurement.as_ref() != collection)
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "canonical ILP measurement does not match routed collection".into(),
                },
            );
        }
        // The write policy decides the parsed lines, through the very same
        // helper the Data-Plane ingest gate uses, so the statement-time
        // decision and the COMMIT-time one are made on a byte-identical image.
        // It runs before prevalidation and before any overlay mutation, so a
        // refusal leaves nothing behind.
        if let Err(error) = crate::data::executor::handlers::timeseries::admit_ilp_lines(
            rls_write_check,
            parsed.lines(),
            self.declared_ts_time_key(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
            ),
            self.ingest_now_ms(),
            tid,
            collection,
        ) {
            return self.response_error(task, error);
        }

        if let Err(error) = self.prevalidate_deferred_ilp_ingest(
            task,
            crate::types::TenantId::new(tid),
            collection,
            parsed.lines(),
        ) {
            return self.response_error(task, error);
        }

        // A staged row is read back by name, so the line's timestamp must be
        // keyed under the collection's own time column — the same name the
        // base scan and the overlay merge resolve.
        let time_column = self.ts_time_column(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
        );

        // Encode every row before mutating an overlay.
        let mut images = Vec::with_capacity(lines.len());
        for row in parsed.lines() {
            let mut object = HashMap::new();
            for (name, value) in &row.tags {
                object.insert(name.to_string(), Value::String(value.to_string()));
            }
            for (name, value) in &row.fields {
                let value = match value {
                    crate::engine::timeseries::ilp::FieldValue::Float(value) => {
                        Value::Float(*value)
                    }
                    crate::engine::timeseries::ilp::FieldValue::Int(value) => {
                        Value::Integer(*value)
                    }
                    // Overlay values are only scan-visible staging data; preserve
                    // a u64 outside Value's signed range as text. Base apply uses
                    // the untouched raw ILP line and retains the real unsigned value.
                    crate::engine::timeseries::ilp::FieldValue::UInt(value) => {
                        Value::String(value.to_string())
                    }
                    crate::engine::timeseries::ilp::FieldValue::Str(value) => {
                        Value::String(value.to_string())
                    }
                    crate::engine::timeseries::ilp::FieldValue::Bool(value) => Value::Bool(*value),
                };
                object.insert(name.to_string(), value);
            }
            if let Some(timestamp) = row.timestamp_ns {
                object.insert(time_column.clone(), Value::Integer(timestamp / 1_000_000));
            }
            images.push(Value::Object(object));
        }

        let mut encoded_rows = Vec::with_capacity(images.len());
        for image in &images {
            let body = match nodedb_types::value_to_msgpack(image) {
                Ok(body) => body,
                Err(error) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("canonical ILP overlay row encode failed: {error}"),
                        },
                    );
                }
            };
            encoded_rows.push(body);
        }
        // `stage_put_capped` mutates one row at a time. Preserve the complete
        // preimage so a later cap failure cannot expose a partial ILP batch to
        // same-transaction reads or Calvin resolve.
        let prior_marker = self
            .txn_overlays
            .get(&txn_id)
            .map(|overlay| overlay.journal_len());
        for (surrogate, body) in surrogates.iter().copied().zip(encoded_rows) {
            let ctx = StageCtx::new(
                task,
                tid,
                txn_id,
                collection,
                RowIdentity::for_surrogate(surrogate),
                surrogate,
            );
            if let Err(error) = self.stage_put_capped(&ctx, body) {
                self.rollback_canonical_ilp_stage(txn_id, prior_marker);
                return self.response_error(task, error);
            }
        }
        self.stage_count_response(task, lines.len())
    }

    /// Restore the exact overlay state from before canonical ILP row staging.
    /// When this batch created the overlay, remove the now-empty representation
    /// and balance the creation gauge exactly once.
    fn rollback_canonical_ilp_stage(&mut self, txn_id: TxnId, prior_marker: Option<usize>) {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
    use crate::control::metrics::SystemMetrics;
    use crate::data::executor::handlers::timeseries::{TimeseriesApplyMode, TimeseriesIngestExec};
    use crate::data::executor::handlers::transaction::overlay::Staged;
    use crate::data::executor::task::{ExecutionTask, TaskState};
    use crate::types::{ReadConsistency, RequestId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_request_tx, request_rx) = RingBuffer::channel::<BridgeRequest>(8);
        let (response_tx, _response_rx) = RingBuffer::channel::<BridgeResponse>(8);
        let core = CoreLoop::open(
            0,
            request_rx,
            response_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open CoreLoop");
        (core, dir)
    }

    fn coll_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(1), collection.to_owned())
    }

    fn task() -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: RequestId::new(1),
                tenant_id: TenantId::new(1),
                database_id: DatabaseId::DEFAULT,
                vshard_id: VShardId::new(0),
                plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: nodedb_types::QualifiedCollection::new(
                        DatabaseId::DEFAULT,
                        "metrics",
                    ),
                    payload: Vec::new(),
                    format: "ilp-msgpack".into(),
                    wal_lsn: None,
                    surrogates: Vec::new(),
                    provenance: None,
                    rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                    returning: None,
                    rls_filters: Vec::new(),
                }),
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
                priority: Priority::Normal,
                trace_id: TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
            },
            state: TaskState::Running,
            wal_lsn: None,
            resolved_now_ms: None,
        }
    }

    #[test]
    fn canonical_ilp_stage_converts_nanoseconds_to_engine_milliseconds() {
        let (mut core, _dir) = make_core();
        let task = task();
        let payload =
            zerompk::to_msgpack_vec(&vec!["metrics value=1i 1700000000000000001".to_string()])
                .expect("canonical ILP payload");
        let txn_id = TxnId::new(80);

        let response = core.stage_canonical_ilp_rows(super::CanonicalIlpStage {
            task: &task,
            tid: 1,
            txn_id,
            collection: "metrics",
            payload: &payload,
            surrogates: &[Surrogate::new(700)],
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        assert_ne!(response.status, crate::bridge::envelope::Status::Error);
        let body = core
            .txn_overlays
            .get(&txn_id)
            .and_then(|overlay| overlay.get(&coll_key("metrics"), 700))
            .expect("staged canonical ILP row");
        let Staged::Put(body) = body else {
            panic!("canonical ILP row must stage as a put");
        };
        let nodedb_types::Value::Object(row) =
            nodedb_types::value_from_msgpack(body).expect("decode staged row")
        else {
            panic!("staged row must be an object");
        };
        assert_eq!(
            row.get("timestamp"),
            Some(&nodedb_types::Value::Integer(1_700_000_000_000)),
            "overlay timestamp must use the engine's millisecond representation"
        );
    }

    #[test]
    fn canonical_ilp_stage_rejects_dictionary_overflow_without_overlay_residue() {
        let (mut core, _dir) = make_core();
        core.ts_tuning.max_tag_cardinality = 1;
        let task = task();
        let txn_id = TxnId::new(82);
        let payload = zerompk::to_msgpack_vec(&vec![
            "metrics,host=a value=1i".to_string(),
            "metrics,host=b value=2i".to_string(),
        ])
        .expect("canonical ILP payload");

        let response = core.stage_canonical_ilp_rows(super::CanonicalIlpStage {
            task: &task,
            tid: 1,
            txn_id,
            collection: "metrics",
            payload: &payload,
            surrogates: &[Surrogate::new(701), Surrogate::new(702)],
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });

        assert_eq!(response.status, crate::bridge::envelope::Status::Error);
        assert!(!core.txn_overlays.contains_key(&txn_id));
    }

    #[test]
    fn canonical_ilp_prevalidation_uses_live_memtable_capacity_baseline() {
        let (mut core, _dir) = make_core();
        let task = task();
        let seed = b"metrics,host=seed value=1i\n";
        let response = core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid: TenantId::new(1),
            collection: "metrics",
            payload: seed,
            format: "ilp",
            wal_lsn: None,
            provenance: None,
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: &[],
        });
        assert_ne!(response.status, crate::bridge::envelope::Status::Error);

        let key = coll_key("metrics");
        let memtable = core.columnar_memtables.get(&key).expect("seed memtable");
        let live = memtable.memory_bytes();
        let reconstructed =
            crate::engine::timeseries::columnar_memtable::ColumnarMemtable::from_snapshot(
                memtable.export_snapshot(),
                memtable.config(),
            )
            .expect("reconstruct snapshot")
            .memory_bytes();
        assert!(
            live > reconstructed,
            "test requires live vector spare capacity absent from snapshots"
        );
        core.ts_tuning.memtable_budget_bytes = reconstructed + (live - reconstructed) / 2;

        let parsed = crate::engine::timeseries::ilp::parse_batch("metrics,host=seed value=2i")
            .expect("parse candidate");
        let error = core
            .prevalidate_deferred_ilp_ingest(&task, TenantId::new(1), "metrics", parsed.lines())
            .expect_err("live resident bytes must require a pre-mutation flush");
        assert!(matches!(error, ErrorCode::RejectedPrevalidation { .. }));
    }

    #[test]
    fn canonical_ilp_rollback_restores_prior_overlay_and_removes_new_overlay_gauge() {
        let (mut core, _dir) = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        core.metrics = Some(Arc::clone(&metrics));
        let collection = coll_key("metrics");

        let existing = TxnId::new(81);
        {
            let overlay = core.txn_overlay_mut(existing);
            overlay.insert_put(
                collection.clone(),
                1,
                &RowIdentity::from_user_key("1"),
                b"prior".to_vec(),
            );
        }
        let prior_marker = core
            .txn_overlays
            .get(&existing)
            .expect("prior overlay")
            .journal_len();
        {
            let overlay = core.txn_overlay_mut(existing);
            overlay.insert_put(
                collection.clone(),
                2,
                &RowIdentity::from_user_key("2"),
                b"first-new".to_vec(),
            );
            overlay.insert_put(
                collection.clone(),
                3,
                &RowIdentity::from_user_key("3"),
                b"second-new".to_vec(),
            );
        }
        core.rollback_canonical_ilp_stage(existing, Some(prior_marker));
        let overlay = core
            .txn_overlays
            .get(&existing)
            .expect("prior overlay retained");
        assert_eq!(
            overlay.get(&collection, 1),
            Some(&Staged::Put(b"prior".to_vec()))
        );
        assert_eq!(overlay.get(&collection, 2), None);
        assert_eq!(overlay.get(&collection, 3), None);
        assert_eq!(overlay.journal_len(), prior_marker);
        assert_eq!(metrics.active_txn_overlays.load(Ordering::Relaxed), 1);

        let created = TxnId::new(82);
        {
            let overlay = core.txn_overlay_mut(created);
            overlay.insert_put(
                collection.clone(),
                4,
                &RowIdentity::from_user_key("4"),
                b"created-a".to_vec(),
            );
            overlay.insert_put(
                collection,
                5,
                &RowIdentity::from_user_key("5"),
                b"created-b".to_vec(),
            );
        }
        assert_eq!(metrics.active_txn_overlays.load(Ordering::Relaxed), 2);
        core.rollback_canonical_ilp_stage(created, None);
        assert!(!core.txn_overlays.contains_key(&created));
        assert_eq!(metrics.active_txn_overlays.load(Ordering::Relaxed), 1);
    }
}
