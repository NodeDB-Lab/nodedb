// SPDX-License-Identifier: BUSL-1.1

//! Core row-ingest path: per-row value coercion, ON CONFLICT DO UPDATE merge
//! resolution, and the row-level `MutationEngine` insert call.

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::upsert::apply_on_conflict_updates;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::document::UpdateValue;

use super::schema::{ndb_field_to_value, row_values_to_object};

/// Parameters for [`CoreLoop::insert_columnar_rows`].
pub(in crate::data::executor) struct RowIngestParams<'a> {
    pub engine_key: &'a (nodedb_types::DatabaseId, crate::types::TenantId, String),
    pub schema: &'a ColumnarSchema,
    pub bitemporal: bool,
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub surrogates: &'a [Surrogate],
    pub ndb_rows: &'a [nodedb_types::Value],
    /// Compiled row-level-security WRITE predicate carried by the plan. A real
    /// predicate only for the ON CONFLICT DO UPDATE shape, whose merged
    /// post-image the Control Plane could not see; a plain insert's rows were
    /// already decided at plan time.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Whether the caller needs the stored post-image of every row that was
    /// actually written. Only a `RETURNING` clause sets this; the row images
    /// are cloned, so a plain insert must not pay for them.
    pub collect_stored_rows: bool,
}

/// What an ingest run produced: how many rows landed, and — when the caller
/// asked — the exact schema-ordered values that landed for each.
///
/// The two are reported together because they answer the same question and
/// must never disagree: a row that was skipped is neither counted nor
/// returned.
pub(in crate::data::executor) struct RowIngestOutcome {
    pub accepted: u64,
    /// Schema-ordered stored values, one entry per accepted row, in insert
    /// order. Empty unless `collect_stored_rows` was set.
    pub stored_rows: Vec<Vec<Value>>,
}

impl CoreLoop {
    /// Insert each row in `params.ndb_rows` into the columnar engine at
    /// `params.engine_key`, applying intent-specific ON CONFLICT semantics
    /// (upsert-overwrite for `Insert` and `Put`, silent skip for
    /// `InsertIfAbsent`, merge-via-`apply_on_conflict_updates` for `Put`
    /// with non-empty `on_conflict_updates`, `RejectedConstraint` error for
    /// `InsertUnique` on a PK the index or an earlier row of the batch
    /// already carries).
    ///
    /// Every row is resolved and checked before any row is written, so a
    /// refusal applies nothing. Returns the accepted row count (and, on
    /// request, the stored post-images), or `Err(Response)` on the first
    /// error.
    pub(in crate::data::executor) fn insert_columnar_rows(
        &mut self,
        task: &ExecutionTask,
        params: RowIngestParams<'_>,
    ) -> Result<RowIngestOutcome, Response> {
        let resolved = self.resolve_columnar_rows(task, &params)?;
        let RowIngestParams {
            engine_key,
            intent,
            collect_stored_rows,
            ..
        } = params;
        let mut accepted = 0u64;
        let mut stored_rows: Vec<Vec<Value>> = Vec::new();

        for row in resolved {
            let engine = match self.columnar_engines.get_mut(engine_key) {
                Some(e) => e,
                None => {
                    return Err(self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "columnar engine vanished during insert".into(),
                        },
                    ));
                }
            };
            let result = match intent {
                ColumnarInsertIntent::InsertIfAbsent => engine.insert_if_absent(&row.values),
                ColumnarInsertIntent::InsertUnique
                | ColumnarInsertIntent::Insert
                | ColumnarInsertIntent::Put => match row.surrogate {
                    Some(s) => engine.insert_with_surrogate(&row.values, s),
                    None => engine.insert(&row.values),
                },
            };

            match result {
                // An `insert_if_absent` that hit an existing key returns an
                // EMPTY `wal_records` — that is the engine's documented no-op
                // signal, and the only way to tell a skip from a write. Counting
                // it reported an `INSERT 1` for a row that was never stored, and
                // returning it would hand back a row that does not exist.
                Ok(mutation) if mutation.wal_records.is_empty() => {}
                Ok(_) => {
                    accepted += 1;
                    if collect_stored_rows {
                        stored_rows.push(row.values);
                    }
                }
                Err(e) => {
                    return Err(self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar insert failed: {e}"),
                        },
                    ));
                }
            }
        }

        Ok(RowIngestOutcome {
            accepted,
            stored_rows,
        })
    }

    /// Resolve every row of the batch to the values it writes, and run every
    /// check a row can fail, without writing anything.
    ///
    /// An ON CONFLICT DO UPDATE merge reads the prior row from the earlier rows
    /// of this batch first, then from the engine: the same prior a
    /// row-by-row write reads.
    fn resolve_columnar_rows(
        &self,
        task: &ExecutionTask,
        params: &RowIngestParams<'_>,
    ) -> Result<Vec<ResolvedRow>, Response> {
        let RowIngestParams {
            engine_key,
            schema,
            bitemporal,
            intent,
            on_conflict_updates,
            surrogates,
            ndb_rows,
            rls_write_check,
            ..
        } = *params;
        let mut resolved: Vec<ResolvedRow> = Vec::with_capacity(ndb_rows.len());
        let merging = intent == ColumnarInsertIntent::Put && !on_conflict_updates.is_empty();
        let unique = intent == ColumnarInsertIntent::InsertUnique;
        // Primary key → values of the latest earlier row of this batch. Kept
        // only for the intents whose checks read it: a merge reads the prior
        // row, and a unique insert reads the key alone.
        let mut batch_rows: std::collections::HashMap<Vec<u8>, Vec<Value>> =
            std::collections::HashMap::new();

        for (row_idx, row) in ndb_rows.iter().enumerate() {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };

            // Build Value slice in schema order. For bitemporal
            // collections, the three reserved columns are auto-populated
            // when absent from the user payload: `_ts_system` is always
            // clamped to the current wall-clock time (clients cannot
            // forge system time), `_ts_valid_from` / `_ts_valid_until`
            // default to the open interval `[i64::MIN, i64::MAX)` if
            // missing.
            let sys_now = if bitemporal {
                self.bitemporal_now_ms()
            } else {
                0
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
                    return Err(self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar insert coercion: {e}"),
                        },
                    ));
                }
            };

            let pk_bytes = if merging || unique {
                let engine = match self.columnar_engines.get(engine_key) {
                    Some(e) => e,
                    None => {
                        return Err(self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "columnar engine vanished during insert".into(),
                            },
                        ));
                    }
                };
                match engine.encode_pk_from_row(&values) {
                    Ok(b) => b,
                    Err(e) => {
                        return Err(self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("columnar insert: pk encode failed: {e}"),
                            },
                        ));
                    }
                }
            } else {
                Vec::new()
            };

            // Resolve the actual row to write: merged for ON CONFLICT DO
            // UPDATE, plain otherwise.
            let final_values: Vec<Value> = match intent {
                ColumnarInsertIntent::Put if merging => {
                    let prior_row = batch_rows.get(&pk_bytes).cloned().or_else(|| {
                        self.columnar_engines
                            .get(engine_key)
                            .and_then(|e| e.lookup_memtable_row_by_pk(&pk_bytes))
                            .or_else(|| self.read_flushed_row_by_pk(engine_key, &pk_bytes))
                    });
                    match prior_row {
                        None => values,
                        Some(prior) => self.merge_on_conflict(
                            task,
                            schema,
                            &prior,
                            &values,
                            on_conflict_updates,
                        )?,
                    }
                }
                ColumnarInsertIntent::Put
                | ColumnarInsertIntent::Insert
                | ColumnarInsertIntent::InsertIfAbsent
                | ColumnarInsertIntent::InsertUnique => values,
            };

            // The row that will actually exist afterwards is decided here, not
            // at plan time: for an ON CONFLICT DO UPDATE the merged body only
            // exists once the stored row has been read. A rejection fails the
            // whole statement rather than skipping the row, which would report
            // a write that never happened.
            if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_columnar_row(
                rls_write_check,
                &final_values,
                schema,
                task.request.tenant_id.as_u64(),
                engine_key.2.as_str(),
            ) {
                return Err(self.response_error(task, error));
            }

            if unique {
                let taken = batch_rows.contains_key(&pk_bytes)
                    || self
                        .columnar_engines
                        .get(engine_key)
                        .is_some_and(|e| e.pk_index().contains(&pk_bytes));
                if taken {
                    let key_desc = schema
                        .columns
                        .iter()
                        .zip(final_values.iter())
                        .filter(|(col, _)| col.primary_key)
                        .map(|(col, v)| format!("{}={v}", col.name))
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(self.response_error(
                        task,
                        crate::Error::RejectedConstraint {
                            collection: engine_key.2.clone(),
                            constraint: "unique".to_string(),
                            detail: format!(
                                "duplicate key value '{key_desc}' violates primary-key \
                                 uniqueness on '{}'",
                                engine_key.2
                            ),
                        },
                    ));
                }
            }

            if merging {
                batch_rows.insert(pk_bytes, final_values.clone());
            } else if unique {
                batch_rows.insert(pk_bytes, Vec::new());
            }
            resolved.push(ResolvedRow {
                values: final_values,
                surrogate: surrogates.get(row_idx).copied(),
            });
        }
        Ok(resolved)
    }

    /// Merge an incoming row into its prior row by the ON CONFLICT DO UPDATE
    /// assignments.
    fn merge_on_conflict(
        &self,
        task: &ExecutionTask,
        schema: &ColumnarSchema,
        prior: &[Value],
        values: &[Value],
        on_conflict_updates: &[(String, UpdateValue)],
    ) -> Result<Vec<Value>, Response> {
        let existing_val = row_values_to_object(schema, prior);
        let excluded_val = row_values_to_object(schema, values);
        let merged =
            match apply_on_conflict_updates(existing_val, &excluded_val, on_conflict_updates) {
                Ok(v) => v,
                Err(e) => return Err(self.response_error(task, e)),
            };
        let merged_obj = match merged {
            nodedb_types::Value::Object(m) => m,
            _ => {
                return Err(self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "merged ON CONFLICT value was not an object".into(),
                    },
                ));
            }
        };
        schema
            .columns
            .iter()
            .map(|col| ndb_field_to_value(merged_obj.get(&col.name), &col.column_type))
            .collect::<Result<Vec<Value>, crate::Error>>()
            .map_err(|e| {
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar ON CONFLICT coercion: {e}"),
                    },
                )
            })
    }
}

/// One batch row resolved to the values it writes.
struct ResolvedRow {
    values: Vec<Value>,
    surrogate: Option<Surrogate>,
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};
    use nodedb_types::{RlsWriteCheck, Value};

    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::columnar_write::ColumnarInsertParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, TenantId, VShardId};

    const TID: u64 = 1;
    const COLLECTION: &str = "unique_rows";

    fn task() -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TID),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            crate::bridge::envelope::PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                restart_identity: false,
            }),
            None,
        )
    }

    fn row(id: &str) -> Value {
        Value::Object(std::collections::HashMap::from([
            ("id".to_string(), Value::String(id.into())),
            ("v".to_string(), Value::Integer(1)),
        ]))
    }

    fn schema_bytes() -> Vec<u8> {
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid schema");
        zerompk::to_msgpack_vec(&schema).expect("encode schema")
    }

    fn insert(
        core: &mut CoreLoop,
        intent: ColumnarInsertIntent,
        rows: Vec<Value>,
    ) -> crate::bridge::envelope::Response {
        let payload = nodedb_types::value_to_msgpack(&Value::Array(rows)).expect("encode rows");
        let schema = schema_bytes();
        core.execute_columnar_insert(
            &task(),
            ColumnarInsertParams {
                collection: COLLECTION,
                payload: &payload,
                format: "msgpack",
                intent,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &schema,
                provenance: None,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        )
    }

    fn live_rows(core: &CoreLoop) -> usize {
        core.columnar_engines
            .get(&(
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                COLLECTION.to_string(),
            ))
            .map_or(0, |e| e.live_row_count())
    }

    /// The funnel cancels the batch's record on a unique refusal, so the
    /// refusal must leave no row of the batch behind.
    #[test]
    fn a_duplicate_key_late_in_a_unique_batch_writes_no_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let seeded = insert(&mut core, ColumnarInsertIntent::Insert, vec![row("a")]);
        assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);

        let refused = insert(
            &mut core,
            ColumnarInsertIntent::InsertUnique,
            vec![row("b"), row("a")],
        );

        assert!(matches!(
            refused.error_code.as_deref(),
            Some(ErrorCode::RejectedConstraint { .. })
        ));
        assert_eq!(
            live_rows(&core),
            1,
            "the row before the duplicate is not written"
        );
    }

    #[test]
    fn a_key_repeated_inside_a_unique_batch_writes_no_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let refused = insert(
            &mut core,
            ColumnarInsertIntent::InsertUnique,
            vec![row("c"), row("c")],
        );

        assert!(matches!(
            refused.error_code.as_deref(),
            Some(ErrorCode::RejectedConstraint { .. })
        ));
        assert_eq!(live_rows(&core), 0);
    }
}
