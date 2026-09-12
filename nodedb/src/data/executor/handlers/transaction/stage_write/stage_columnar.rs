// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `ColumnarOp::Insert`.
//!
//! A columnar batch INSERT issued inside a `BEGIN..COMMIT` block is staged
//! here, one overlay `Put` per row, so a later same-transaction columnar
//! SELECT observes the newly inserted rows (read-your-own-writes) before
//! COMMIT. COMMIT durable replay is unchanged: the buffered `ColumnarOp::Insert`
//! plan is still replayed through `execute_columnar_insert` inside the
//! COMMIT `TransactionBatch`, which remains the sole durable apply.
//!
//! Row identity: the overlay's identity side-map holds the row's primary-key
//! value when the schema declares one, else the decimal surrogate
//! (`columnar_row_identity`). The overlay slot itself is the surrogate,
//! matching `execute_columnar_scan` (`scan_memtable_rows_with_surrogates`).
//!
//! Row body encoding: each row's schema-ordered `Vec<Value>` is wrapped as a
//! `Value::Array` and encoded via `nodedb_types::value_to_msgpack` — decoded
//! the same way by `merge_overlay_into_columnar_scan`. This is a
//! staging-only representation; it plays no part in the durable segment
//! format written at COMMIT by `execute_columnar_insert`.
//!
//! ON CONFLICT DO UPDATE: the staged body is the MERGED row, not the submitted
//! one. The overlay exists to show what this transaction has written, and after
//! a conflict merge that is the stored row with the assignments applied —
//! exactly what `execute_columnar_insert` persists at COMMIT. Staging the
//! submitted body instead made the overlay and the eventual durable state
//! disagree, so a same-transaction `SELECT` showed a row the COMMIT would never
//! produce. The merge is resolved against this transaction's own overlay first
//! and the engine second, so an earlier statement's staged row is the one it
//! merges against.
//!
//! Row-level security: the write policy decides the batch here, at the
//! statement, not only at COMMIT — otherwise a refused row would be reported as
//! affected and be readable by this transaction until COMMIT failed. A plain
//! insert's rows were already decided at plan time (the plan carries them), so
//! this gate bites for the ON CONFLICT shape. It decides the same merged image
//! that is staged, so the gate and the overlay can never disagree.
//!
//! Field coercion mirrors `execute_columnar_insert` exactly (same
//! `ndb_field_to_value` / bitemporal column population) via the shared
//! `columnar_write::schema` helpers, so a staged row's values match what the
//! durable COMMIT replay will eventually store.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::value::Value;

use super::context::StageCtx;
use super::stage_columnar_dml::columnar_row_identity;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::ndb_field_to_value;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

/// Inputs for [`CoreLoop::stage_columnar_insert`]. Bundled because the raw
/// parameter list exceeds the project's too-many-arguments bound.
pub(in crate::data::executor) struct StageColumnarInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub surrogates: &'a [Surrogate],
    pub schema_bytes: &'a [u8],
    /// `ON CONFLICT (pk) DO UPDATE SET` assignments carried by the plan.
    /// Needed here only to resolve the row image the write policy decides —
    /// the merged row, not the submitted one. The staged body itself is
    /// unaffected.
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    /// Compiled row-level-security WRITE predicate carried by the plan.
    /// `NoPolicyApplies`/`AlreadyDecidedElsewhere` for a plain insert, whose
    /// rows the Control Plane already decided at plan time; a real predicate
    /// for the ON CONFLICT shape, whose merged image only exists once the
    /// stored row has been read.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    /// Stage a `ColumnarOp::Insert` batch: decode the payload, coerce each
    /// row to schema order, and stage one overlay `Put` per row keyed by its
    /// surrogate. Returns the shared `stage_count_response` shape
    /// (`{"affected": N}`) — the first key `extract_affected_count` checks.
    pub(in crate::data::executor) fn stage_columnar_insert(
        &mut self,
        params: StageColumnarInsertParams<'_>,
    ) -> Response {
        let StageColumnarInsertParams {
            task,
            tid,
            txn_id,
            collection,
            payload,
            surrogates,
            schema_bytes,
            on_conflict_updates,
            rls_write_check,
        } = params;

        let ndb_rows: Vec<Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(Value::Array(arr)) => arr,
            Ok(v @ Value::Object(_)) => vec![v],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar insert: payload must be array or object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar insert: invalid payload: {e}"),
                    },
                );
            }
        };

        if ndb_rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        let bitemporal = self.is_bitemporal(task.request.database_id.as_u64(), tid, collection);
        let engine_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        // Registers the engine (schema-only, zero rows) when this is the
        // first insert ever seen for this collection on this core, so a
        // same-transaction SELECT resolves the same schema instead of
        // hitting the scan's "missing engine -> empty result" branch. See
        // `ensure_columnar_engine_schema` doc comment.
        let engine_preexisted = self.columnar_engines.contains_key(&engine_key);
        let schema = self.ensure_columnar_engine_schema(
            &engine_key,
            collection,
            bitemporal,
            &ndb_rows[0],
            schema_bytes,
        );
        // Track engines THIS transaction newly auto-created (never engines
        // that already existed before the txn started) so `MetaOp::DropTxnOverlay`
        // can drop the still-empty ones on rollback without touching engines
        // a prior/concurrent write populated. The staged rows themselves go
        // to the overlay below, never the engine's memtable, so a purely
        // staged-then-rolled-back engine is guaranteed empty at that point.
        if !engine_preexisted {
            self.txn_created_columnar_engines
                .entry(txn_id)
                .or_default()
                .insert(engine_key.clone());
        }

        let sys_now = if bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };
        // Coerce every row, resolve the image it will actually persist, and let
        // the write policy decide all of them BEFORE the first
        // `stage_put_capped`. A rejection must leave the overlay exactly as it
        // was, and must not have reported an affected count for a row the
        // transaction will never be allowed to keep.
        //
        // Resolving before staging means an ON CONFLICT row merges against the
        // state as of the start of THIS statement, so two rows of one `VALUES`
        // list that share a primary key both merge against the same prior row.
        // A prior STATEMENT's staged row is seen, because it is already in the
        // overlay. Splitting it the other way would make a refusal partially
        // durable in the overlay, which is the worse of the two.
        let mut resolved: Vec<(Surrogate, Vec<Value>)> = Vec::with_capacity(ndb_rows.len());

        for (row_idx, row) in ndb_rows.iter().enumerate() {
            let obj = match row {
                Value::Object(m) => m,
                _ => continue,
            };

            let values: Vec<Value> = match schema
                .columns
                .iter()
                .map(|col| match col.name.as_str() {
                    TS_SYSTEM if bitemporal => Ok(Value::Integer(sys_now)),
                    TS_VALID_FROM if bitemporal => Ok(match obj.get(TS_VALID_FROM) {
                        Some(Value::Integer(i)) => Value::Integer(*i),
                        _ => Value::Integer(i64::MIN),
                    }),
                    TS_VALID_UNTIL if bitemporal => Ok(match obj.get(TS_VALID_UNTIL) {
                        Some(Value::Integer(i)) => Value::Integer(*i),
                        _ => Value::Integer(i64::MAX),
                    }),
                    _ => ndb_field_to_value(obj.get(&col.name), &col.column_type),
                })
                .collect::<Result<Vec<Value>, crate::Error>>()
            {
                Ok(v) => v,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar insert coercion: {e}"),
                        },
                    );
                }
            };

            let surrogate = match surrogates.get(row_idx).copied() {
                Some(s) => s,
                None => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "columnar insert: missing surrogate for staged row".into(),
                        },
                    );
                }
            };

            // The row that will exist afterwards: the incoming row for a plain
            // insert, the merged row for the ON CONFLICT branch. This is the
            // body staged as well as the image decided — the overlay is
            // supposed to show what this transaction has written, and after a
            // merge that is the merged row, which is also what COMMIT persists.
            let image = match self.staged_columnar_write_image(
                txn_id,
                &engine_key,
                &schema,
                surrogate,
                values,
                on_conflict_updates,
            ) {
                Ok(image) => image,
                Err(error) => return self.response_error(task, error),
            };

            resolved.push((surrogate, image));
        }

        if let Err(response) = self.stage_admit_columnar_rows(
            task,
            rls_write_check,
            resolved.iter().map(|(_, row)| row.as_slice()),
            &schema,
            tid,
            collection,
        ) {
            return response;
        }

        let mut staged = 0usize;
        for (surrogate, values) in resolved {
            let identity = columnar_row_identity(&schema, &values, surrogate.as_u32());
            let body = match nodedb_types::value_to_msgpack(&Value::Array(values)) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar insert: row encode failed: {e}"),
                        },
                    );
                }
            };

            let ctx = StageCtx::new(task, tid, txn_id, collection, identity, surrogate);
            if let Err(e) = self.stage_put_capped(&ctx, body) {
                return self.response_error(task, e);
            }
            staged += 1;
        }

        self.stage_count_response(task, staged)
    }
}
