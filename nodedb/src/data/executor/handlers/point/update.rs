//! PointUpdate: read-modify-write field-level changes to a single document.
//!
//! Each assignment is either a pre-encoded literal (fast binary merge when
//! possible) or a `SqlExpr` that must be evaluated against the *current* row —
//! the evaluator is `nodedb_query::expr::SqlExpr::eval`, shared with
//! computed-column, window, and typeguard paths.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::UpdateValue;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        updates: &[(String, UpdateValue)],
        returning: bool,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            fields = updates.len(),
            returning,
            "point update"
        );

        let config_key = format!("{tid}:{collection}");
        let is_strict = self.doc_configs.get(&config_key).is_some_and(|c| {
            matches!(
                c.storage_mode,
                crate::bridge::physical_plan::StorageMode::Strict { .. }
            )
        });

        // Reject direct updates to generated columns.
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = super::super::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )
        {
            return self.response_error(task, e);
        }

        // Any non-literal assignment forces the slow decode→eval→re-encode path,
        // because we need the current document to evaluate against.
        let has_expr = updates
            .iter()
            .any(|(_, v)| matches!(v, UpdateValue::Expr(_)));

        match self.sparse.get(tid, collection, document_id) {
            Ok(Some(current_bytes)) => {
                let has_generated = self.doc_configs.get(&config_key).is_some_and(|c| {
                    !c.enforcement.generated_columns.is_empty()
                        && super::super::generated::needs_recomputation(
                            updates,
                            &c.enforcement.generated_columns,
                        )
                });

                // Fast path: non-strict, no generated columns, all literal — merge at binary level.
                let updated_bytes = if !is_strict && !has_generated && !has_expr {
                    let base_mp = super::super::super::doc_format::json_to_msgpack(&current_bytes);
                    let update_pairs: Vec<(&str, &[u8])> = updates
                        .iter()
                        .filter_map(|(field, v)| match v {
                            UpdateValue::Literal(bytes) => Some((field.as_str(), bytes.as_slice())),
                            UpdateValue::Expr(_) => None,
                        })
                        .collect();
                    nodedb_query::msgpack_scan::merge_fields(&base_mp, &update_pairs)
                } else {
                    // Strict, generated, or expression RHS: decode → mutate → re-encode.
                    let mut doc = if is_strict {
                        if let Some(config) = self.doc_configs.get(&config_key)
                            && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                                config.storage_mode
                        {
                            match super::super::super::strict_format::binary_tuple_to_json(
                                &current_bytes,
                                schema,
                            ) {
                                Some(v) => v,
                                None => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: "failed to decode Binary Tuple for update"
                                                .into(),
                                        },
                                    );
                                }
                            }
                        } else {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: "strict config missing during update".into(),
                                },
                            );
                        }
                    } else {
                        match super::super::super::doc_format::decode_document(&current_bytes) {
                            Some(v) => v,
                            None => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: "failed to parse document for update".into(),
                                    },
                                );
                            }
                        }
                    };

                    // Apply field-level updates. Expressions are evaluated
                    // against the current-row snapshot, so a later assignment
                    // observing a column updated earlier in the same statement
                    // still sees the pre-update value — matches PostgreSQL.
                    let eval_doc: nodedb_types::Value = doc.clone().into();
                    if let Some(obj) = doc.as_object_mut() {
                        for (field, update_val) in updates {
                            let val = match update_val {
                                UpdateValue::Literal(bytes) => {
                                    match nodedb_types::json_from_msgpack(bytes) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            return self.response_error(
                                                task,
                                                ErrorCode::Internal {
                                                    detail: format!(
                                                        "update field '{field}': msgpack decode: {e}"
                                                    ),
                                                },
                                            );
                                        }
                                    }
                                }
                                UpdateValue::Expr(expr) => {
                                    let result: nodedb_types::Value = expr.eval(&eval_doc);
                                    // Convert nodedb_types::Value → serde_json::Value so the
                                    // downstream re-encode path (strict or msgpack) can proceed
                                    // through its existing json-based branches unchanged.
                                    let json: serde_json::Value = result.into();
                                    json
                                }
                            };
                            obj.insert(field.clone(), val);
                        }
                    }

                    // Recompute generated columns.
                    if has_generated
                        && let Some(config) = self.doc_configs.get(&config_key)
                        && let Err(e) = super::super::generated::evaluate_generated_columns(
                            &mut doc,
                            &config.enforcement.generated_columns,
                        )
                    {
                        return self.response_error(task, e);
                    }

                    // Re-encode.
                    if is_strict {
                        if let Some(config) = self.doc_configs.get(&config_key)
                            && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                                config.storage_mode
                        {
                            let ndb_val: nodedb_types::Value = doc.clone().into();
                            match super::super::super::strict_format::value_to_binary_tuple(
                                &ndb_val, schema,
                            ) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: format!("strict re-encode: {e}"),
                                        },
                                    );
                                }
                            }
                        } else {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: "strict config missing during re-encode".into(),
                                },
                            );
                        }
                    } else {
                        super::super::super::doc_format::encode_to_msgpack(&doc)
                    }
                };

                match self
                    .sparse
                    .put(tid, collection, document_id, &updated_bytes)
                {
                    Ok(()) => {
                        self.doc_cache
                            .put(tid, collection, document_id, &updated_bytes);

                        // Emit update event to Event Plane.
                        // Convert Binary Tuple → msgpack for strict collections.
                        let new_ev = self.resolve_event_payload(tid, collection, &updated_bytes);
                        let old_ev = self.resolve_event_payload(tid, collection, &current_bytes);
                        self.emit_write_event(
                            task,
                            collection,
                            crate::event::WriteOp::Update,
                            document_id,
                            Some(new_ev.as_deref().unwrap_or(&updated_bytes)),
                            Some(old_ev.as_deref().unwrap_or(&current_bytes)),
                        );

                        if returning {
                            let with_id = nodedb_query::msgpack_scan::inject_str_field(
                                &updated_bytes,
                                "id",
                                document_id,
                            );
                            let mut payload = Vec::with_capacity(with_id.len() + 4);
                            nodedb_query::msgpack_scan::write_array_header(&mut payload, 1);
                            payload.extend_from_slice(&with_id);
                            self.response_with_payload(task, payload)
                        } else {
                            let mut payload = Vec::with_capacity(16);
                            nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
                            nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", 1);
                            self.response_with_payload(task, payload)
                        }
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
                let mut payload = Vec::with_capacity(16);
                nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
                nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", 0);
                self.response_with_payload(task, payload)
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
