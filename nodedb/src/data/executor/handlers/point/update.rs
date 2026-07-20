// SPDX-License-Identifier: BUSL-1.1

//! PointUpdate: read-modify-write field-level changes to a single document.
//!
//! Each assignment is either a pre-encoded literal (fast binary merge when
//! possible) or a `SqlExpr` that must be evaluated against the *current* row —
//! the evaluator is `nodedb_query::expr::SqlExpr::eval`, shared with
//! computed-column, window, and typeguard paths.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::{ReturningSpec, UpdateValue};
use nodedb_types::Surrogate;

/// Parameters for `execute_point_update`.
pub(in crate::data::executor) struct PointUpdateParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        params: PointUpdateParams<'_>,
    ) -> Response {
        let PointUpdateParams {
            tid,
            collection,
            document_id,
            surrogate,
            updates,
            returning,
        } = params;
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            fields = updates.len(),
            has_returning = returning.is_some(),
            "point update"
        );

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let is_strict = self.doc_configs.get(&config_key).is_some_and(|c| {
            matches!(
                c.storage_mode,
                nodedb_physical::physical_plan::StorageMode::Strict { .. }
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

        let bitemporal = self.is_bitemporal(task.request.database_id.as_u64(), tid, collection);
        let sys_from_for_encode = if bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };
        let database_id = task.request.database_id.as_u64();
        let get_result = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, row_key)
        } else {
            self.sparse.get(database_id, tid, collection, row_key)
        };
        match get_result {
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
                    let base_mp = doc_format::json_to_msgpack(&current_bytes);
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
                            && let nodedb_physical::physical_plan::StorageMode::Strict {
                                ref schema,
                            } = config.storage_mode
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
                        match doc_format::decode_document(&current_bytes) {
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
                            && let nodedb_physical::physical_plan::StorageMode::Strict {
                                ref schema,
                            } = config.storage_mode
                        {
                            let ndb_val: nodedb_types::Value = doc.clone().into();
                            let result = if bitemporal && schema.bitemporal {
                                super::super::super::strict_format::value_to_binary_tuple_bitemporal(
                                    &ndb_val,
                                    schema,
                                    sys_from_for_encode,
                                    i64::MIN,
                                    i64::MAX,
                                )
                            } else {
                                super::super::super::strict_format::value_to_binary_tuple(
                                    &ndb_val, schema,
                                )
                            };
                            match result {
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
                        doc_format::encode_to_msgpack(&doc)
                    }
                };

                // The plain `INDEXES` secondary-index paths for this collection.
                // The non-bitemporal write must reconcile these atomically with
                // the primary body so a changed value can't leave a stale index
                // entry pointing at the old value.
                let index_paths = self
                    .doc_configs
                    .get(&config_key)
                    .map(|c| c.index_paths.clone())
                    .unwrap_or_default();

                let write_result = if bitemporal {
                    // Bitemporal collections keep secondary-index entries in the
                    // versioned index only; the update must tombstone values it
                    // dropped and assert current values, atomically with the new
                    // body. Decode old/new docs (storage-mode-aware) so the
                    // reindex sees the real indexed values for strict + schemaless.
                    let index_paths = self
                        .doc_configs
                        .get(&config_key)
                        .map(|c| c.index_paths.clone())
                        .unwrap_or_default();
                    let old_doc = self
                        .doc_configs
                        .get(&config_key)
                        .and_then(|c| self.decode_stored_document(c, &current_bytes));
                    let new_doc = self
                        .doc_configs
                        .get(&config_key)
                        .and_then(|c| self.decode_stored_document(c, &updated_bytes));
                    match new_doc {
                        Some(new_doc) => self
                            .bitemporal_update_reindex(
                                super::update_reindex::BitemporalUpdateReindex {
                                    database_id,
                                    tid,
                                    collection,
                                    doc_id: row_key,
                                    sys_from_ms: sys_from_for_encode,
                                    valid_from_ms: i64::MIN,
                                    valid_until_ms: i64::MAX,
                                    new_body: &updated_bytes,
                                    index_paths: &index_paths,
                                    old_doc: old_doc.as_ref(),
                                    new_doc: &new_doc,
                                    wal_lsn: task.wal_lsn(),
                                },
                            )
                            .map(|()| None::<Vec<u8>>),
                        None => self
                            .sparse
                            .versioned_put(crate::engine::sparse::btree_versioned::VersionedPut {
                                database_id,
                                tenant: tid,
                                coll: collection,
                                doc_id: row_key,
                                sys_from_ms: sys_from_for_encode,
                                valid_from_ms: i64::MIN,
                                valid_until_ms: i64::MAX,
                                body: &updated_bytes,
                            })
                            .map(|()| None::<Vec<u8>>),
                    }
                } else if index_paths.is_empty() {
                    // No secondary index to maintain — nothing to diff, so the
                    // self-committing put is sufficient and avoids a redundant
                    // decode of both document images.
                    self.sparse
                        .put(database_id, tid, collection, row_key, &updated_bytes)
                } else {
                    // Reconcile the plain secondary index atomically with the
                    // primary body. Decode old/new (storage-mode-aware) so the
                    // SET diff drops values the update removed and asserts the
                    // new ones in the same redb transaction — otherwise a later
                    // lookup on the new value misses the row and a lookup on the
                    // old value wrongly returns it. Mirrors the bitemporal branch.
                    let (old_doc, new_doc) = match self.doc_configs.get(&config_key) {
                        Some(cfg) => (
                            self.decode_stored_document(cfg, &current_bytes),
                            self.decode_stored_document(cfg, &updated_bytes),
                        ),
                        None => (None, None),
                    };
                    match (old_doc, new_doc) {
                        (Some(old_doc), Some(new_doc)) => self
                            .nonbitemporal_update_reindex(
                                super::update_reindex::NonbitemporalUpdateReindex {
                                    database_id,
                                    tid,
                                    collection,
                                    doc_id: row_key,
                                    new_body: &updated_bytes,
                                    index_paths: &index_paths,
                                    old_doc: &old_doc,
                                    new_doc: &new_doc,
                                    wal_lsn: task.wal_lsn(),
                                },
                            )
                            .map(|()| None::<Vec<u8>>),
                        _ => {
                            // Unreachable for well-formed data: both images are
                            // documents we just read / re-encoded. If one ever
                            // fails to decode we cannot compute the secondary-index
                            // diff, so we must NOT write the primary alone — that
                            // would silently desync the index (the very bug this
                            // path fixes). Fail loud instead.
                            Err(crate::Error::Storage {
                                engine: "sparse".into(),
                                detail: format!(
                                    "non-bitemporal update: document failed to decode for \
                                     secondary-index diff (collection {collection}, id {row_key})"
                                ),
                            })
                        }
                    }
                };
                match write_result {
                    Ok(_prior) => {
                        self.doc_cache.put(
                            task.request.database_id.as_u64(),
                            tid,
                            collection,
                            row_key,
                            &updated_bytes,
                        );

                        // Maintain the secondary HNSW vector index. The body
                        // rewrite above (sparse.put / bitemporal_update_reindex)
                        // reconciled storage + the secondary btree/FTS/graph
                        // overlays, but never the vector index — re-index the
                        // surrogate's vectors from the new body so KNN search
                        // reflects an embedding change in the same process.
                        // No-op when the collection has no vector index.
                        let has_vectors = self.collection_has_vectors(database_id, tid, collection);
                        self.update_reindex_vector_indexes(
                            super::update_reindex_vector::UpdateVectorReindex {
                                database_id,
                                tid,
                                collection,
                                row_key,
                                surrogate,
                                new_body: &updated_bytes,
                                is_strict,
                                has_vectors,
                            },
                        );

                        // Maintain the sparse inverted index the same way: the
                        // body rewrite never touched it, so re-index the row's
                        // sparse literal from the new body. No-op when the
                        // collection declares no `SparseVector` column.
                        let has_sparse = self.collection_has_sparse(database_id, tid, collection);
                        self.update_reindex_sparse_indexes(
                            super::update_reindex_sparse::UpdateSparseReindex {
                                database_id,
                                tid,
                                collection,
                                row_key,
                                new_body: &updated_bytes,
                                is_strict,
                                has_sparse,
                            },
                        );

                        // Emit update event to Event Plane. `current_bytes`
                        // is the pre-update row already read above; the
                        // helper derives `WriteOp::Update` from the Some
                        // prior + Some new pair and handles strict→msgpack
                        // conversion on both sides.
                        self.emit_put_event(
                            task,
                            tid,
                            collection,
                            row_key,
                            &updated_bytes,
                            Some(&current_bytes),
                        );

                        // Build the response for both the RETURNING and
                        // non-RETURNING branches first, then — only when the
                        // collection carries a secondary vector index — carry the
                        // surrogate + post-image back in the write-set so the
                        // Control Plane can mint a post-apply `Put` redo record.
                        // The autocommit WAL path mints none for a PointUpdate, so
                        // without this a WAL-only restart rebuilds the HNSW from the
                        // pre-update body and resurrects the old embedding.
                        // `updated_bytes` is moved in as its last use.
                        let mut response = if let Some(spec) = returning {
                            // Build the post-update document with id injected.
                            let with_id = nodedb_query::msgpack_scan::inject_str_field(
                                &updated_bytes,
                                "id",
                                document_id,
                            );
                            let doc = match doc_format::decode_document(&with_id) {
                                Some(v) => v,
                                None => serde_json::json!({"id": document_id}),
                            };
                            match returning_rows::build_rows_payload(spec, &[doc]) {
                                Ok(payload) => self.response_with_payload(task, payload),
                                Err(e) => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: format!("RETURNING encode: {e}"),
                                        },
                                    );
                                }
                            }
                        } else {
                            let mut payload = Vec::with_capacity(16);
                            nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
                            nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", 1);
                            self.response_with_payload(task, payload)
                        };
                        if has_vectors {
                            response.write_set = vec![WriteSetEntry {
                                surrogate: surrogate.as_u32(),
                                is_delete: false,
                                value: updated_bytes,
                            }];
                        }
                        response
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
