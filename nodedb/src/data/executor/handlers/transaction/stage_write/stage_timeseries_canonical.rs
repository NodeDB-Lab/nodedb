// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging of a canonical line-protocol batch: the
//! `ilp-msgpack` list of lines, one surrogate token per line.
//!
//! The whole batch is parsed, route-checked, decided by the write policy and
//! prevalidated before any overlay entry is written. A cap failure part way
//! rewinds the overlay, so same-transaction reads never see a partial batch.

use std::collections::HashMap;

use nodedb_types::RowIdentity;
use nodedb_types::value::Value;

use super::context::StageCtx;
use super::stage_timeseries::CanonicalIlpStage;
use super::stage_timeseries_ilp::PreviewTarget;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    pub(super) fn stage_canonical_ilp_rows(&mut self, args: CanonicalIlpStage<'_>) -> Response {
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
        let now_ms = self.ingest_stamp_ms();
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
            now_ms,
            tid,
            collection,
            &self.crs_context(),
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
        let target = PreviewTarget {
            task,
            tid,
            txn_id,
            collection,
        };
        let preview = match self.stage_preview(&target, payload, "ilp-msgpack", now_ms) {
            Ok(preview) => preview,
            Err(error) => return self.response_error(task, error),
        };

        // A staged row is read back by name, so the line's timestamp must be
        // keyed under the collection's own time column — the same name the
        // base scan and the overlay merge resolve — and typed by that
        // column's kind, so a predicate literal coerced for the column
        // compares against the staged cell exactly as against a base cell.
        let time_column = self.ts_time_column(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
        );
        let time_kind = self.ts_time_key_kind(
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
                let cell = match time_kind.cell_value(timestamp / 1_000_000) {
                    Ok(cell) => cell,
                    Err(error) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "canonical ILP overlay row time cell {}: {error}",
                                    timestamp / 1_000_000
                                ),
                            },
                        );
                    }
                };
                object.insert(time_column.clone(), cell);
            }
            images.push(Value::Object(object));
        }

        let mut encoded_rows = Vec::with_capacity(images.len());
        for image in images {
            // Typed as a committed row reads it.
            let body = match self.staged_ts_body(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                image,
            ) {
                Ok(body) => body,
                Err(error) => return self.response_error(task, error),
            };
            encoded_rows.push(body);
        }
        // `stage_put` mutates one row at a time. Preserve the complete
        // preimage so a later cap failure cannot expose a partial ILP batch to
        // same-transaction reads or Calvin resolve.
        let prior_marker = self
            .txn_overlays
            .get(&txn_id)
            .map(|overlay| overlay.journal_len());
        let accepted = surrogates.iter().copied().zip(encoded_rows).enumerate();
        for (_, (surrogate, body)) in accepted.filter(|(line, _)| preview.accepts(*line)) {
            let ctx = StageCtx::new(
                task,
                tid,
                txn_id,
                collection,
                RowIdentity::for_surrogate(surrogate),
                surrogate,
            );
            if let Err(error) = self.stage_put(&ctx, body) {
                // A rollback error leaves the overlay unusable, and the
                // transaction can only roll back: it outranks the stage error.
                if let Err(rollback) = self.rollback_canonical_ilp_stage(txn_id, prior_marker) {
                    return self.response_error(task, rollback);
                }
                return self.response_error(task, error);
            }
        }
        self.note_staged_ingest_now(task, tid, txn_id, collection, surrogates, now_ms);
        let rejected = preview.rejected;
        self.note_stage_preview(&target, preview.schema);
        self.stage_ts_count_response(task, lines.len().saturating_sub(rejected), rejected)
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
    use crate::types::{ReadConsistency, RequestId, TraceId, TxnId, VShardId};
    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
    use nodedb_types::Surrogate;

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
                commit_hlc: None,
                system_ordinal: None,
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
            .staged_row(txn_id, &coll_key("metrics"), 700)
            .expect("read")
            .expect("staged canonical ILP row");
        let Staged::Put(body) = body else {
            panic!("canonical ILP row must stage as a put");
        };
        let nodedb_types::Value::Object(row) =
            nodedb_types::decode_object(&body).expect("decode staged row")
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
            overlay
                .insert_put(
                    collection.clone(),
                    1,
                    &RowIdentity::from_user_key("1"),
                    b"prior".to_vec(),
                )
                .unwrap();
        }
        let prior_marker = core
            .txn_overlays
            .get(&existing)
            .expect("prior overlay")
            .journal_len();
        {
            let overlay = core.txn_overlay_mut(existing);
            overlay
                .insert_put(
                    collection.clone(),
                    2,
                    &RowIdentity::from_user_key("2"),
                    b"first-new".to_vec(),
                )
                .unwrap();
            overlay
                .insert_put(
                    collection.clone(),
                    3,
                    &RowIdentity::from_user_key("3"),
                    b"second-new".to_vec(),
                )
                .unwrap();
        }
        core.rollback_canonical_ilp_stage(existing, Some(prior_marker))
            .expect("rollback");
        let overlay = core
            .txn_overlays
            .get(&existing)
            .expect("prior overlay retained");
        assert_eq!(
            overlay.get(&collection, 1).unwrap().as_deref(),
            Some(&Staged::Put(b"prior".to_vec()))
        );
        assert_eq!(overlay.get(&collection, 2).unwrap(), None);
        assert_eq!(overlay.get(&collection, 3).unwrap(), None);
        assert_eq!(overlay.journal_len(), prior_marker);
        assert_eq!(metrics.active_txn_overlays.load(Ordering::Relaxed), 1);

        let created = TxnId::new(82);
        {
            let overlay = core.txn_overlay_mut(created);
            overlay
                .insert_put(
                    collection.clone(),
                    4,
                    &RowIdentity::from_user_key("4"),
                    b"created-a".to_vec(),
                )
                .unwrap();
            overlay
                .insert_put(
                    collection,
                    5,
                    &RowIdentity::from_user_key("5"),
                    b"created-b".to_vec(),
                )
                .unwrap();
        }
        assert_eq!(metrics.active_txn_overlays.load(Ordering::Relaxed), 2);
        core.rollback_canonical_ilp_stage(created, None)
            .expect("rollback");
        assert!(!core.txn_overlays.contains_key(&created));
        assert_eq!(metrics.active_txn_overlays.load(Ordering::Relaxed), 1);
    }
}
