//! Document write handlers: PointPut, BatchInsert, Upsert, Register, IndexLookup, DropIndex.

use sonic_rs;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_document_batch_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        documents: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = documents.len(), "document batch insert");
        let converted: Vec<(String, Vec<u8>)> = documents
            .iter()
            .map(|(id, val)| {
                (
                    id.clone(),
                    super::super::super::doc_format::canonicalize_document_for_storage(val),
                )
            })
            .collect();
        let refs: Vec<(&str, &[u8])> = converted
            .iter()
            .map(|(id, val)| (id.as_str(), val.as_slice()))
            .collect();
        match self.sparse.batch_put(tid, collection, &refs) {
            Ok(()) => {
                // Auto-index text fields for full-text search (same as PointPut).
                // Also extract secondary indexes for any registered collection config.
                let config_key = (crate::types::TenantId::new(tid), collection.to_string());
                let index_paths: Vec<crate::engine::document::store::IndexPath> = self
                    .doc_configs
                    .get(&config_key)
                    .map(|c| c.index_paths.clone())
                    .unwrap_or_default();
                for (doc_id, val) in documents {
                    if let Some(doc) = super::super::super::doc_format::decode_document(val) {
                        // Full-text inverted index (includes nested block content).
                        let text_content = super::text_extract::extract_indexable_text(&doc);
                        if !text_content.is_empty() {
                            let _ = self.inverted.index_document(
                                crate::types::TenantId::new(tid),
                                collection,
                                doc_id,
                                &text_content,
                            );
                        }

                        // Secondary index extraction.
                        self.apply_secondary_indexes(tid, collection, &doc, doc_id, &index_paths);
                    }
                }

                if let Some(ref m) = self.metrics {
                    m.record_document_insert();
                }
                match super::super::super::response_codec::encode_count("inserted", documents.len())
                {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
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

    /// Register a document collection's secondary index configuration.
    ///
    /// Stores the `CollectionConfig` in `self.doc_configs` so that subsequent
    /// `PointPut` and `DocumentBatchInsert` operations extract and write secondary
    /// index entries automatically.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_register_document_collection(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        indexes: &[crate::bridge::physical_plan::RegisteredIndex],
        crdt_enabled: bool,
        storage_mode: &crate::bridge::physical_plan::StorageMode,
        enforcement: &crate::bridge::physical_plan::EnforcementOptions,
        bitemporal: bool,
    ) -> Response {
        let mode_label = match storage_mode {
            crate::bridge::physical_plan::StorageMode::Schemaless => "schemaless",
            crate::bridge::physical_plan::StorageMode::Strict { .. } => "strict",
        };
        debug!(
            core = self.core_id,
            %collection,
            index_count = indexes.len(),
            crdt_enabled,
            storage_mode = mode_label,
            append_only = enforcement.append_only,
            hash_chain = enforcement.hash_chain,
            balanced = enforcement.balanced.is_some(),
            "register document collection"
        );

        let mut config = crate::engine::document::store::CollectionConfig::new(collection);
        config.crdt_enabled = crdt_enabled;
        config.storage_mode = storage_mode.clone();
        config.enforcement = enforcement.clone();
        config.bitemporal = bitemporal;
        config.index_paths = indexes
            .iter()
            .map(crate::engine::document::store::IndexPath::from_registered)
            .collect();

        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        self.doc_configs.insert(config_key, config);

        self.response_ok(task)
    }

    /// Execute a secondary index lookup: find all doc IDs where `path = value`.
    ///
    /// Delegates to `SparseEngine::range_scan` via a temporary `DocumentEngine`.
    /// Returns a JSON array of document IDs as the response payload.
    pub(in crate::data::executor) fn execute_document_index_lookup(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        path: &str,
        value: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %path,
            %value,
            "document index lookup"
        );

        let doc_engine = crate::engine::document::store::DocumentEngine::new(&self.sparse, tid);
        match doc_engine.index_lookup(collection, path, value) {
            Ok(doc_ids) => {
                let payload = serde_json::json!(doc_ids);
                match sonic_rs::to_vec(&payload) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("index lookup encode: {e}"),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Execute a SELECT rewritten as a secondary-index fetch.
    ///
    /// Resolves doc IDs through `SparseEngine::range_scan` via
    /// `DocumentEngine::index_lookup`, fetches each document's raw
    /// msgpack bytes, applies `offset`/`limit`, and emits rows via
    /// `encode_raw_document_rows` — the same wire format as a document
    /// scan — so the pgwire decoder doesn't need a special case.
    ///
    /// Post-filters and projection are intentionally not applied here:
    /// the planner only rewrites to this op when those are empty
    /// (complex cases fall back to a full scan). Extending this handler
    /// with filter/projection support is additive.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_document_indexed_fetch(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        path: &str,
        value: &str,
        _filters: &[u8],
        _projection: &[String],
        limit: usize,
        offset: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %path,
            %value,
            limit,
            offset,
            "document indexed fetch"
        );

        let doc_engine = crate::engine::document::store::DocumentEngine::new(&self.sparse, tid);
        let doc_ids = match doc_engine.index_lookup(collection, path, value) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("indexed fetch: {e}"),
                    },
                );
            }
        };

        let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
        for doc_id in doc_ids.iter().skip(offset).take(limit) {
            match self.sparse.get(tid, collection, doc_id) {
                Ok(Some(bytes)) => rows.push((doc_id.clone(), bytes)),
                Ok(None) => {
                    // Index entry pointed at a deleted doc — skip, don't
                    // fail. A future compaction will purge the orphan.
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("fetch doc {doc_id}: {e}"),
                        },
                    );
                }
            }
        }

        match super::super::super::response_codec::encode_raw_document_rows(&rows) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: format!("indexed fetch encode: {e}"),
                },
            ),
        }
    }

    /// Backfill an index: scan every document in the collection and
    /// populate sparse-index entries for the given field. Atomic — one
    /// write transaction covers the whole backfill and UNIQUE
    /// violations abort it, leaving the index empty (the caller's
    /// Building→Ready flip is skipped, so readers never see a
    /// partial-index view).
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_backfill_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        path: &str,
        is_array: bool,
        unique: bool,
        case_insensitive: bool,
        predicate: Option<&str>,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %path,
            unique,
            case_insensitive,
            partial = predicate.is_some(),
            "backfill index"
        );
        if let Some(ref m) = self.metrics {
            m.record_document_index_backfill();
        }

        // Parse the partial-index predicate once, up front. An
        // unparsable predicate is a catalog-level bug — the DDL layer
        // already validates the text at CREATE INDEX time, so a
        // failure here means the stored entry drifted from what the
        // grammar accepts. Refuse the backfill rather than silently
        // over-populating a "partial" index.
        let parsed_predicate = match predicate {
            Some(text) => match crate::engine::document::predicate::IndexPredicate::parse(text) {
                Some(p) => Some(p),
                None => {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!(
                                "backfill: partial-index predicate failed to parse: {text}"
                            ),
                        },
                    );
                }
            },
            None => None,
        };

        // Snapshot existing documents outside the write txn. 1,000,000
        // cap matches the Data Plane's other collection-wide scans; rows
        // beyond this are handled by a future chunked backfill (see
        // `scan_documents_chunked`).
        let docs = match self.sparse.scan_documents(tid, collection, 1_000_000) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("backfill scan: {e}"),
                    },
                );
            }
        };

        // Deduplicate-unique-as-we-go: track `(normalized_value → doc_id)`
        // so a dup within the existing set is flagged before we ever
        // touch the index table.
        let mut seen: std::collections::HashMap<String, String> = std::collections::HashMap::new();

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("backfill txn: {e}"),
                    },
                );
            }
        };

        for (doc_id, bytes) in &docs {
            let Some(doc) = super::super::super::doc_format::decode_document(bytes) else {
                continue;
            };
            // Partial-index predicate: skip rows that don't satisfy
            // the `WHERE` clause. `evaluate` treats NULL / non-bool as
            // false (Postgres partial-index semantics), so only rows
            // for which the predicate is explicitly true are indexed.
            if let Some(ref p) = parsed_predicate
                && !p.evaluate_json(&doc)
            {
                continue;
            }
            let values = crate::engine::document::store::extract_index_values(&doc, path, is_array);
            for raw in values {
                let stored = if case_insensitive {
                    raw.to_lowercase()
                } else {
                    raw
                };
                if unique
                    && let Some(prev) = seen.get(&stored)
                    && prev != doc_id
                {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!(
                                "unique index backfill: duplicate value '{stored}' on '{path}' \
                                 (existing '{prev}', new '{doc_id}')"
                            ),
                        },
                    );
                }
                if unique {
                    seen.insert(stored.clone(), doc_id.clone());
                }
                if let Err(e) = self
                    .sparse
                    .index_put_in_txn(&txn, tid, collection, path, &stored, doc_id)
                {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("backfill index_put: {e}"),
                        },
                    );
                }
            }
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: format!("backfill commit: {e}"),
                },
            );
        }

        self.response_ok(task)
    }

    /// Drop all secondary index entries for a field across the entire collection.
    ///
    /// Calls `SparseEngine::delete_index_entries_for_field` directly.
    /// Returns `{"removed": N}` as the response payload.
    pub(in crate::data::executor) fn execute_drop_document_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            "drop document index"
        );

        match self
            .sparse
            .delete_index_entries_for_field(tid, collection, field)
        {
            Ok(removed) => {
                match super::super::super::response_codec::encode_count("removed", removed) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("drop index encode: {e}"),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
