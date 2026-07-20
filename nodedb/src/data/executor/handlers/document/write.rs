// SPDX-License-Identifier: BUSL-1.1

//! Document write handlers: PointPut, BatchInsert, Upsert, Register.
//! Secondary-index lookup / fetch handlers live in `index_fetch`; index
//! backfill / drop handlers live in `index_maintenance`.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_document_batch_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        documents: &[(String, Vec<u8>)],
        surrogates: &[nodedb_types::Surrogate],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = documents.len(), "document batch insert");

        // When per-row surrogates are parallel to the documents, run the batch
        // as ONE atomic, fully cross-engine-indexed insert: each row is applied
        // via `apply_point_put` (document store + FTS + vector + spatial +
        // secondary indexes) inside a single redb transaction, keyed by the
        // row's stable surrogate. A search hit from any cross-engine index then
        // resolves back to the row's identity, and the whole page lands or none
        // of it does (any per-row error rolls the transaction back). The legacy
        // raw `batch_put` path below is kept only for callers that do not supply
        // parallel surrogates (no cross-engine identity available).
        if !documents.is_empty() && surrogates.len() == documents.len() {
            return self.execute_document_batch_insert_indexed(
                task, tid, collection, documents, surrogates,
            );
        }

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
        // FTS indexing requires a valid Surrogate per document. When `surrogates`
        // is parallel to `documents` (same length), each entry can be used. When
        // the field is absent/mismatched (legacy callers), FTS indexing is skipped
        // — surface this loudly so missing search results are diagnosable.
        let fts_enabled = surrogates.len() == documents.len();
        if !fts_enabled && !documents.is_empty() {
            warn!(
                core = self.core_id,
                %collection,
                doc_count = documents.len(),
                surrogate_count = surrogates.len(),
                "document batch insert without parallel surrogates: FTS indexing skipped"
            );
        }
        match self
            .sparse
            .batch_put(task.request.database_id.as_u64(), tid, collection, &refs)
        {
            Ok(()) => {
                // Auto-index text fields for full-text search (same as PointPut).
                // Also extract secondary indexes for any registered collection config.
                let config_key = (
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection.to_string(),
                );
                let index_paths: Vec<crate::engine::document::store::IndexPath> = self
                    .doc_configs
                    .get(&config_key)
                    .map(|c| c.index_paths.clone())
                    .unwrap_or_default();
                for (i, (doc_id, val)) in documents.iter().enumerate() {
                    if let Some(doc) = super::super::super::doc_format::decode_document(val) {
                        // Full-text inverted index (includes nested block content).
                        // Only index when a valid Surrogate is available.
                        if fts_enabled {
                            let surrogate = surrogates[i];
                            // Surrogate::ZERO is the "unassigned" sentinel — the
                            // upstream allocator hasn't assigned a real id, so we
                            // must not write it into the FTS index.
                            if surrogate != nodedb_types::Surrogate::ZERO {
                                let text_content =
                                    super::text_extract::extract_indexable_text(&doc);
                                if !text_content.is_empty() {
                                    let _ = self.inverted.index_document(
                                        task.request.database_id.as_u64(),
                                        crate::types::TenantId::new(tid),
                                        collection,
                                        surrogate,
                                        &text_content,
                                    );
                                }
                            }
                        }

                        // Secondary index extraction (insert-only path: no prior
                        // document, so the diff is pure adds; tuples unused).
                        let _ = self.apply_secondary_indexes(
                            crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                                database_id: task.request.database_id.as_u64(),
                                tid,
                                collection,
                                old_doc: None,
                                new_doc: &doc,
                                doc_id,
                                index_paths: &index_paths,
                            },
                        );
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

    /// Atomic, fully-indexed batch insert (surrogates parallel to documents).
    ///
    /// Applies every row through [`CoreLoop::apply_point_put`] under ONE redb
    /// write transaction so the document store, FTS inverted index, HNSW vector
    /// index, spatial R-tree, and secondary indexes are all maintained and keyed
    /// by each row's stable surrogate. Any per-row error (including a UNIQUE
    /// constraint violation) drops the transaction, leaving the whole page
    /// unchanged. On success the transaction commits once and one Insert write
    /// event is emitted per row.
    fn execute_document_batch_insert_indexed(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        documents: &[(String, Vec<u8>)],
        surrogates: &[nodedb_types::Surrogate],
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, e),
        };

        // Gate post-apply write-set accumulation once for the whole batch so a
        // collection with no vector/sparse field pays nothing. Each row's
        // `apply_point_put` above reconciles storage + the btree/FTS/graph/HNSW/
        // sparse overlays, but `wal_append_document_op` mints no redo for
        // `BatchInsert` (row durability is redb-synchronous). On a WAL-only
        // restart the HNSW and sparse indexes are rebuilt only from redo `Put`
        // records, so a vector- or sparse-indexed batch insert that journals
        // nothing would lose its rows' index entries. Carrying the surrogate +
        // post-image back per row lets the Control Plane mint a durable `Put`
        // redo for each (see `plan_post_apply_redo` / `append_write_set_redo`).
        let has_vectors = self.collection_has_vectors(database_id, tid, collection)
            || self.collection_has_sparse(database_id, tid, collection);

        // Row key for post-commit event emission, captured as each row applies
        // successfully; the value bytes are re-borrowed from `documents` after
        // commit rather than cloned here. On any error we return early
        // (dropping `txn`, which rolls back every row applied so far).
        let mut applied: Vec<String> = Vec::with_capacity(documents.len());
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        // Per-row secondary-index tuples (added ∪ removed ∪ bitemporal),
        // parallel to `applied`. Recorded into the per-index write-value
        // substrate only after `txn.commit()` succeeds below — a row that
        // never commits touched no durable index state.
        let mut row_index_tuples: Vec<Vec<(String, String)>> = Vec::with_capacity(documents.len());
        for (i, (_document_id, value)) in documents.iter().enumerate() {
            let surrogate = surrogates[i];
            let row_key = surrogate_to_doc_id(surrogate);
            let outcome = match self.apply_point_put(
                &txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    document_id: &row_key,
                    surrogate,
                    value,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    wal_lsn: task.wal_lsn(),
                },
            ) {
                Ok(o) => o,
                Err(e) => return self.response_error(task, e),
            };
            if has_vectors {
                write_set.push(WriteSetEntry {
                    surrogate: surrogate.as_u32(),
                    is_delete: false,
                    value: value.clone(),
                });
            }
            if task.wal_lsn().is_some() {
                let mut tuples = outcome.secondary_index_added;
                tuples.extend(outcome.secondary_index_removed);
                tuples.extend(outcome.bitemporal_index_tuples);
                row_index_tuples.push(tuples);
            }
            applied.push(row_key);
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("batch insert commit: {e}"),
                },
            );
        }

        // Record each committed row's touched secondary-index values into the
        // per-index write-value substrate, now that the batch has durably
        // committed.
        if let Some(lsn) = task.wal_lsn() {
            for tuples in &row_index_tuples {
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    tuples,
                    lsn,
                );
            }
        }

        self.checkpoint_coordinator
            .mark_dirty("sparse", documents.len());
        if let Some(ref m) = self.metrics {
            m.record_document_insert();
        }

        for (i, row_key) in applied.iter().enumerate() {
            self.emit_put_event(task, tid, collection, row_key, &documents[i].1, None);
        }

        let mut response =
            match super::super::super::response_codec::encode_count("inserted", documents.len()) {
                Ok(bytes) => self.response_with_payload(task, bytes),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };
        if !write_set.is_empty() {
            response.write_set = write_set;
        }
        response
    }
}

/// Parameters for [`CoreLoop::execute_register_document_collection`].
pub(in crate::data::executor) struct RegisterDocumentCollectionParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub indexes: &'a [nodedb_physical::physical_plan::RegisteredIndex],
    pub crdt_enabled: bool,
    pub storage_mode: &'a nodedb_physical::physical_plan::StorageMode,
    pub enforcement: &'a nodedb_physical::physical_plan::EnforcementOptions,
    pub bitemporal: bool,
    /// Durable CRDT conflict-resolution policy (JSON-serialized
    /// `CollectionPolicy`), persisted on the collection's catalog record.
    /// `Some` rehydrates this core's `PolicyRegistry` so the policy survives
    /// register/reboot instead of falling back to `CollectionPolicy::ephemeral()`.
    pub conflict_policy: Option<&'a str>,
}

impl CoreLoop {
    /// Register a document collection's secondary index configuration.
    ///
    /// Stores the `CollectionConfig` in `self.doc_configs` so that subsequent
    /// `PointPut` and `DocumentBatchInsert` operations extract and write secondary
    /// index entries automatically.
    pub(in crate::data::executor) fn execute_register_document_collection(
        &mut self,
        task: &ExecutionTask,
        params: RegisterDocumentCollectionParams<'_>,
    ) -> Response {
        let RegisterDocumentCollectionParams {
            tid,
            collection,
            indexes,
            crdt_enabled,
            storage_mode,
            enforcement,
            bitemporal,
            conflict_policy,
        } = params;
        let mode_label = match storage_mode {
            nodedb_physical::physical_plan::StorageMode::Schemaless => "document_schemaless",
            nodedb_physical::physical_plan::StorageMode::Strict { .. } => "document_strict",
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
        config.conflict_policy = conflict_policy.map(str::to_string);
        config.index_paths = indexes
            .iter()
            .map(crate::engine::document::store::IndexPath::from_registered)
            .collect();

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.insert(config_key, config);

        // Rehydrate the durable CRDT conflict-resolution policy (if any) into
        // this core's `PolicyRegistry`. Runs on every `Register` — live DDL
        // apply AND boot rehydration replay — so `ALTER COLLECTION ... SET ON
        // CONFLICT ...` survives a restart instead of silently reverting to
        // `CollectionPolicy::ephemeral()`.
        if let Some(policy_json) = conflict_policy {
            match self.get_crdt_engine(task.request.database_id, crate::types::TenantId::new(tid)) {
                Ok(engine) => {
                    if let Err(e) = engine.set_collection_policy(collection, policy_json) {
                        warn!(
                            core = self.core_id,
                            %collection,
                            error = %e,
                            "failed to rehydrate persisted conflict policy on register"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        core = self.core_id,
                        %collection,
                        error = %e,
                        "failed to create CRDT engine for conflict policy rehydration"
                    );
                }
            }
        }

        self.response_ok(task)
    }
}
