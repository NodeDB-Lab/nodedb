// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DocumentOp::UpdateFromJoin`: implements the two-phase
//! `UPDATE target SET ... FROM src WHERE target.col = src.col` execution.
//!
//! Phase 1: scan the source collection to build a lookup map keyed by the
//!          equi-join value (`source[source_join_col]`).
//! Phase 2: scan the target collection; for each row whose join-column value
//!          matches a source row, build a merged document and evaluate the
//!          assignments to produce the post-image (shared classifier in
//!          `update_from_join_collect::collect_update_from_join_rows`).
//! Phase 3: either write each post-image back (`resolve_only == false`) or, on
//!          the COMMIT-time RESOLVE pass (`resolve_only == true`), return the
//!          matched rows as `(doc_id, Option<surrogate>, post_image_body)` for
//!          the expander to rewrite into concrete `PointPut` ops — WITHOUT
//!          writing, re-indexing, accumulating a write-set, or emitting events.

use nodedb_types::Surrogate;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::update_reindex_vector::UpdateVectorReindex;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::response_codec::encode_json;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{ReturningSpec, UpdateValue};

/// One target row matched by the join, with its post-image resolved but not yet
/// written. Produced by [`CoreLoop::collect_update_from_join_rows`] and consumed
/// by BOTH the write pass (below) and the RESOLVE pass — the single shared
/// classifier so the two cannot diverge on which rows match or what post-image
/// each carries.
pub(in crate::data::executor) struct ResolvedUpdateRow {
    /// Target storage key (hex-encoded surrogate on a surrogate-keyed row).
    pub doc_id: String,
    /// The row's registered surrogate, parsed from `doc_id`. `None` for a
    /// legacy non-surrogate-keyed row.
    pub surrogate: Option<Surrogate>,
    /// Post-image body: strict Binary Tuple for a strict target, MessagePack
    /// for a schemaless target.
    pub body: Vec<u8>,
    /// Pre-update stored bytes (same storage-mode encoding as `body`), read
    /// before any field was mutated. Threaded through to the write pass so it
    /// can emit an `Update` `WriteEvent` carrying the row's real `old_value` —
    /// mirrors `execute_point_update` and `execute_bulk_update`, which both
    /// capture the pre-image before re-encoding.
    pub old_body: Vec<u8>,
    /// Post-image decoded to JSON (generated columns applied), reused by the
    /// write pass to build `RETURNING` rows without re-decoding `body`.
    pub doc: serde_json::Value,
}

/// Parameters for `execute_update_from_join`.
pub(in crate::data::executor) struct UpdateFromJoinParams<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub target_filter_bytes: &'a [u8],
    pub returning: Option<&'a ReturningSpec>,
    /// RESOLVE-ONLY read pass (Control-Plane COMMIT expander). When `true`, the
    /// handler runs the identical scan/join/assignment/encode pipeline as the
    /// write path but writes NOTHING — no `sparse.put`, no vector re-index, no
    /// write-set, no events — and returns the matched rows as msgpack
    /// `Vec<(doc_id, Option<surrogate_u32>, post_image_body)>` for the expander
    /// to rewrite into concrete `PointPut` ops. `false` = the normal write path.
    pub resolve_only: bool,
    /// Control-Plane-shipped source rows for cross-core `UPDATE ... FROM`. When
    /// `Some`, the source join-map is built from these pre-scanned
    /// `(source_doc_id, raw_stored_source_bytes)` rows instead of a local read
    /// of the source collection (whose vShard may live on a different core).
    /// `None` selects the legacy local-storage read (co-resident / in-txn
    /// buffered replay).
    pub source_rows: Option<&'a [(String, Vec<u8>)]>,
}

impl CoreLoop {
    /// Execute an `UPDATE target FROM source WHERE target.join_col = source.join_col` operation.
    pub(in crate::data::executor) fn execute_update_from_join(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: UpdateFromJoinParams<'_>,
    ) -> Response {
        let UpdateFromJoinParams {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            updates,
            target_filter_bytes,
            returning,
            resolve_only,
            source_rows,
        } = params;

        debug!(
            core = self.core_id,
            target = %target_collection,
            source = %source_collection,
            resolve_only,
            "update from join"
        );

        // Phase 1: Scan source collection, build join map:
        //   source_join_value (as string) → serde_json::Value (the source document).
        let source_map = match self.build_source_join_map(
            task.request.database_id.as_u64(),
            tid,
            source_collection,
            source_join_col,
            source_rows,
        ) {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Check for strict storage mode on the target.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            target_collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        if source_map.is_empty() {
            // No source rows — nothing matches. The RESOLVE pass returns an
            // empty match set; the write path reports zero affected.
            if resolve_only {
                return self.encode_resolved_update_rows(task, Vec::new());
            }
            let result = serde_json::json!({ "affected": 0u64 });
            return match encode_json(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            };
        }

        // Phase 2: Deserialize target filters.
        let target_filters: Vec<ScanFilter> = if target_filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(target_filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize target_filters: {e}"),
                        },
                    );
                }
            }
        };

        // Phase 3: Scan the target, join each row against the source, evaluate
        // the SET assignments, and encode the post-image — WITHOUT writing. This
        // classification is shared verbatim by both the RESOLVE pass and the
        // write path so the two cannot diverge on match set or post-image.
        let rows = match self.collect_update_from_join_rows(
            super::update_from_join_collect::CollectUpdateRows {
                task,
                tid,
                target_collection,
                source_alias,
                target_join_col,
                updates,
                source_map: &source_map,
                target_filters: &target_filters,
                strict_schema: strict_schema.as_ref(),
                config_key: &config_key,
            },
        ) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // RESOLVE pass: hand the matched rows back for COMMIT-time expansion.
        // No `sparse.put`, no vector re-index, no write-set, no events.
        if resolve_only {
            return self.encode_resolved_update_rows(task, rows);
        }

        // Gate secondary-vector maintenance once for the whole statement so a
        // non-vector target collection pays nothing. When a vector field is
        // present, a joined UPDATE that rewrites an embedding must re-index the
        // row's HNSW vectors, or KNN search keeps scoring the stale embedding.
        let database_id = task.request.database_id.as_u64();
        let has_vectors = self.collection_has_vectors(database_id, tid, target_collection);

        let mut affected = 0u64;
        // One post-apply `Put` redo entry per updated row on a vector collection.
        // Each row's `sparse.put` below reconciled storage + the btree/FTS/graph
        // overlays but minted no WAL redo carrying the new body, so a WAL-only
        // restart would rebuild the HNSW from the pre-update `Put` records and
        // resurrect the stale embeddings. Carrying the surrogate + post-image back
        // lets the Control Plane mint a durable `Put` redo per row. Only populated
        // when the collection has a vector index.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        let mut returned_docs: Vec<serde_json::Value> = if returning.is_some() {
            Vec::with_capacity(rows.len())
        } else {
            Vec::new()
        };

        for row in rows {
            let ResolvedUpdateRow {
                doc_id,
                surrogate: row_surrogate,
                body: updated_bytes,
                old_body,
                mut doc,
            } = row;

            if self
                .sparse
                .put(
                    task.request.database_id.as_u64(),
                    tid,
                    target_collection,
                    &doc_id,
                    &updated_bytes,
                )
                .is_ok()
            {
                self.doc_cache.put(
                    task.request.database_id.as_u64(),
                    tid,
                    target_collection,
                    &doc_id,
                    &updated_bytes,
                );
                // Emit an update event per affected row to the Event Plane, so
                // AFTER-UPDATE triggers and CDC/change-stream consumers see
                // each row `UPDATE ... FROM` touched — mirroring
                // `execute_point_update`/`execute_bulk_update`'s single-row
                // emit. `old_body` is the pre-update stored bytes captured by
                // `collect_update_from_join_rows`; `emit_put_event` derives
                // `WriteOp::Update` from the Some prior + Some new pair and
                // handles strict->msgpack conversion on both sides.
                self.emit_put_event(
                    task,
                    tid,
                    target_collection,
                    &doc_id,
                    &updated_bytes,
                    Some(&old_body),
                );
                // Re-index the row's vectors from the new body (soft-delete the
                // old HNSW node + insert the new one, keyed by the stable
                // surrogate), then carry the surrogate + post-image back for a
                // post-apply `Put` redo (`updated_bytes` is moved as its last
                // use). Both are no-ops unless the collection has a vector
                // field, so a non-vector collection pays nothing.
                if has_vectors && let Some(surrogate) = row_surrogate {
                    self.update_reindex_vector_indexes(UpdateVectorReindex {
                        database_id,
                        tid,
                        collection: target_collection,
                        row_key: &doc_id,
                        surrogate,
                        new_body: &updated_bytes,
                        is_strict: strict_schema.is_some(),
                        has_vectors,
                    });
                    write_set.push(WriteSetEntry {
                        surrogate: surrogate.as_u32(),
                        is_delete: false,
                        value: updated_bytes,
                    });
                }
                affected += 1;
                if returning.is_some() {
                    if let Some(obj) = doc.as_object_mut() {
                        obj.insert("id".to_string(), serde_json::Value::String(doc_id.clone()));
                    }
                    returned_docs.push(doc);
                }
            }
        }

        let mut response = if let Some(spec) = returning {
            match returning_rows::build_rows_payload(spec, &returned_docs) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else {
            let result = serde_json::json!({ "affected": affected });
            match encode_json(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            }
        };
        if !write_set.is_empty() {
            response.write_set = write_set;
        }
        response
    }

    /// Encode the RESOLVE pass payload: a msgpack `Vec<(doc_id,
    /// Option<surrogate_u32>, post_image_body)>` the statement-time expander
    /// decodes and rewrites into concrete `PointPut` ops (see
    /// `control::update_from_join_orchestrator::resolve_and_emit_update_from_join_ops`).
    fn encode_resolved_update_rows(
        &self,
        task: &ExecutionTask,
        rows: Vec<ResolvedUpdateRow>,
    ) -> Response {
        let wire: Vec<(String, Option<u32>, Vec<u8>)> = rows
            .into_iter()
            .map(|r| (r.doc_id, r.surrogate.map(|s| s.as_u32()), r.body))
            .collect();
        match zerompk::to_msgpack_vec(&wire) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("update-from-join resolve encode: {e}"),
                },
            ),
        }
    }
}
