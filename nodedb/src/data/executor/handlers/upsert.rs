//! Upsert handler: insert if absent, merge fields if present.
//!
//! Works for schemaless and strict collections. All internal transport
//! uses nodedb_types::Value + zerompk (msgpack). No JSON roundtrips.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Upsert: insert if absent, merge fields if present.
    ///
    /// If a document with `document_id` exists, merges `value` fields into the
    /// existing document (preserving fields not in `value`). If it doesn't exist,
    /// inserts as a new document (identical to PointPut).
    ///
    /// `value` is msgpack-encoded (zerompk). Strict collections decode binary
    /// tuples for existing docs, merge, and re-encode via `apply_point_put`.
    pub(in crate::data::executor) fn execute_upsert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
        on_conflict_updates: &[(String, crate::bridge::physical_plan::UpdateValue)],
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            has_on_conflict = !on_conflict_updates.is_empty(),
            "upsert"
        );

        // Detect strict storage mode for this collection.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let strict_schema = self.doc_configs.get(&config_key).and_then(|config| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Check if document already exists. Bitemporal collections consult
        // the versioned table's current-state view (reverse-scan to newest
        // non-tombstone); non-bitemporal collections use the legacy point
        // lookup.
        let bitemporal = self.is_bitemporal(tid, collection);
        let existing = if bitemporal {
            self.sparse
                .versioned_get_current(tid, collection, document_id)
        } else {
            self.sparse.get(tid, collection, document_id)
        };

        match existing {
            Ok(Some(current_bytes)) => {
                // Decode existing document to nodedb_types::Value.
                let existing_val = if let Some(ref schema) = strict_schema {
                    // Strict: binary tuple → Value via schema.
                    match super::super::strict_format::binary_tuple_to_value(&current_bytes, schema)
                    {
                        Some(v) => v,
                        None => {
                            // Fallback: try msgpack (migration case).
                            match nodedb_types::value_from_msgpack(&current_bytes) {
                                Ok(v) => v,
                                Err(_) => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: "failed to decode document for upsert".into(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                } else {
                    // Schemaless: stored as msgpack.
                    match nodedb_types::value_from_msgpack(&current_bytes) {
                        Ok(v) => v,
                        Err(_) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: "failed to decode document for upsert".into(),
                                },
                            );
                        }
                    }
                };

                // Decode incoming value (msgpack → Value).
                let new_val = match nodedb_types::value_from_msgpack(value) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to decode upsert value from msgpack".into(),
                            },
                        );
                    }
                };

                // Conflict branch: if `ON CONFLICT DO UPDATE SET` assignments
                // are present, evaluate each against the *existing* row and
                // apply only those fields. Otherwise fall back to the plain
                // merge semantics used by `UPSERT INTO` / no-action upserts.
                let merged = if on_conflict_updates.is_empty() {
                    merge_values(existing_val, new_val)
                } else {
                    apply_on_conflict_updates(existing_val, &new_val, on_conflict_updates)
                };

                let sys_from_ms = if bitemporal {
                    self.bitemporal_now_ms()
                } else {
                    0
                };
                // Encode merged value for storage.
                let stored_bytes = if let Some(ref schema) = strict_schema {
                    let result = if bitemporal && schema.bitemporal {
                        super::super::strict_format::value_to_binary_tuple_bitemporal(
                            &merged,
                            schema,
                            sys_from_ms,
                            i64::MIN,
                            i64::MAX,
                        )
                    } else {
                        super::super::strict_format::value_to_binary_tuple(&merged, schema)
                    };
                    match result {
                        Ok(bt) => bt,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("binary tuple encode: {e}"),
                                },
                            );
                        }
                    }
                } else {
                    // Schemaless: encode to msgpack.
                    match nodedb_types::value_to_msgpack(&merged) {
                        Ok(b) => b,
                        Err(_) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: "failed to encode merged upsert value".into(),
                                },
                            );
                        }
                    }
                };

                // Write directly to storage. `current_bytes` is the
                // pre-merge stored row, already read above — thread it to
                // the Event Plane as `old_value` so the emitted WriteOp
                // resolves to Update. Bitemporal collections append a new
                // version instead of overwriting.
                let write_result = if bitemporal {
                    self.sparse
                        .versioned_put(crate::engine::sparse::btree_versioned::VersionedPut {
                            tenant: tid,
                            coll: collection,
                            doc_id: document_id,
                            sys_from_ms,
                            valid_from_ms: i64::MIN,
                            valid_until_ms: i64::MAX,
                            body: &stored_bytes,
                        })
                        .map(|()| None::<Vec<u8>>)
                } else {
                    self.sparse.put(tid, collection, document_id, &stored_bytes)
                };
                match write_result {
                    Ok(_prior) => {
                        self.doc_cache
                            .put(tid, collection, document_id, &stored_bytes);
                        self.emit_put_event(
                            task,
                            tid,
                            collection,
                            document_id,
                            &stored_bytes,
                            Some(&current_bytes),
                        );
                        self.response_ok(task)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Ok(None) => {
                // Insert: document doesn't exist, create new (same as PointPut).
                let txn = match self.sparse.begin_write() {
                    Ok(t) => t,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                };

                // `apply_point_put` returns prior bytes if any; here the
                // existence probe just above found none, and apply_point_put
                // is the only writer on this core — prior must be None. We
                // pass it straight through so the emit resolves to Insert.
                let prior = match self.apply_point_put(&txn, tid, collection, document_id, value) {
                    Ok(p) => p,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                };

                if let Err(e) = txn.commit() {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("commit: {e}"),
                        },
                    );
                }

                self.emit_put_event(task, tid, collection, document_id, value, prior.as_deref());

                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

/// Apply `ON CONFLICT DO UPDATE SET` assignments against the existing row.
///
/// Each assignment's RHS is evaluated via `SqlExpr::eval` — identical to
/// the UPDATE handler's path — so arithmetic (`n = n + 1`), functions
/// (`name = UPPER(name)`), `CASE`, and concatenation all work. Literal
/// assignments bypass the evaluator and decode their msgpack directly.
pub(in crate::data::executor) fn apply_on_conflict_updates(
    existing: nodedb_types::Value,
    excluded: &nodedb_types::Value,
    updates: &[(String, crate::bridge::physical_plan::UpdateValue)],
) -> nodedb_types::Value {
    let mut obj = match existing {
        nodedb_types::Value::Object(map) => map,
        // If the existing row isn't an object (shouldn't happen for
        // document engines) fall back to the assignments as a blank slate.
        _ => std::collections::HashMap::new(),
    };
    // Snapshot the row before any assignment applies, so all assignments
    // see the pre-update state — matches PostgreSQL semantics. `excluded`
    // is the row proposed for INSERT that triggered the conflict — it
    // resolves `EXCLUDED.col` references inside the RHS expressions.
    let snapshot = nodedb_types::Value::Object(obj.clone());
    for (field, update_val) in updates {
        let new_val: nodedb_types::Value = match update_val {
            crate::bridge::physical_plan::UpdateValue::Literal(bytes) => {
                match nodedb_types::value_from_msgpack(bytes) {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            }
            crate::bridge::physical_plan::UpdateValue::Expr(expr) => {
                expr.eval_with_excluded(&snapshot, excluded)
            }
        };
        obj.insert(field.clone(), new_val);
    }
    nodedb_types::Value::Object(obj)
}

/// Merge two `nodedb_types::Value` objects: overlay `new` fields onto `existing`.
fn merge_values(existing: nodedb_types::Value, new: nodedb_types::Value) -> nodedb_types::Value {
    match (existing, new) {
        (nodedb_types::Value::Object(mut existing_map), nodedb_types::Value::Object(new_map)) => {
            for (k, v) in new_map {
                existing_map.insert(k, v);
            }
            nodedb_types::Value::Object(existing_map)
        }
        // If shapes don't match, new value wins entirely.
        (_, new) => new,
    }
}
