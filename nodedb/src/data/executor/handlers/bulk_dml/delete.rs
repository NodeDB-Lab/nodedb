// SPDX-License-Identifier: BUSL-1.1

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{OllpPredictedEdge, ReturningSpec};

/// OLLP prediction inputs threaded to `execute_bulk_delete`: the predicted
/// matched-doc surrogate set and the predicted implicit-edge set. Both are
/// verified against the actual scan at admission time, returning
/// [`ErrorCode::OllpRetryRequired`] on any divergence (predicate drift or
/// edge-content drift) before any write occurs. Bundled into one struct to keep
/// the handler signature within the argument-count budget.
pub(in crate::data::executor) struct OllpPrediction<'a> {
    pub surrogates: Option<&'a [u32]>,
    pub edges: Option<&'a [OllpPredictedEdge]>,
}

impl CoreLoop {
    /// Bulk delete: scan documents matching filters, delete all matches.
    ///
    /// Cascades to inverted index, secondary indexes, and graph edges.
    /// When `returning` is `None`, returns affected row count as JSON payload: `{"affected": N}`.
    /// When `returning` is `Some(spec)`, returns a `RowsPayload` with the pre-deletion documents.
    pub(in crate::data::executor) fn execute_bulk_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        filter_bytes: &[u8],
        returning: Option<&ReturningSpec>,
        ollp: OllpPrediction<'_>,
    ) -> Response {
        let ollp_predicted_surrogates = ollp.surrogates;
        let ollp_predicted_edges = ollp.edges;
        debug!(core = self.core_id, %collection, has_returning = returning.is_some(), "bulk delete");
        let database_id = task.request.database_id.as_u64();

        // Empty `filter_bytes` means "no WHERE clause" — match every row.
        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize filters: {e}"),
                        },
                    );
                }
            }
        };

        let matching_ids = match self.scan_matching_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &filters,
        ) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // OLLP determinism (multi-replica): the predicted surrogate set carried
        // in the plan is the LEADER's verified write-set and the SINGLE SOURCE
        // OF TRUTH every replica must mutate. The optimistic-lock VERIFICATION
        // (`actual != predicted`) is the guard that the leader's prediction is
        // still valid; it runs ONLY on the data-group leader. A follower whose
        // local redb snapshot lags the leader's prediction window would compute
        // a different `actual` set — so it must NOT independently re-derive a
        // match nor independently raise a mismatch (that poisons the attempt and
        // exhausts retries even on a static dataset). Instead, EVERY replica —
        // leader and follower alike — applies the delete to EXACTLY the
        // predicted set (resolved to doc-ids below), so all replicas mutate
        // identical state. When no predicted set is present (single-shard /
        // non-OLLP path) behavior is unchanged: apply over the local scan.
        let apply_ids: Vec<String> = if let Some(predicted) = ollp_predicted_surrogates {
            // Leader-only verification: compare the local actual matching set to
            // the prediction; on drift return OllpRetryRequired WITHOUT writing.
            // The set comparison is deterministic: both sides are sorted.
            if self.ollp_is_group_leader
                && !super::scan::ollp_surrogates_match(&matching_ids, predicted)
            {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
            // Apply set = the carried predicted surrogates (identical on every
            // replica). On the leader this equals `matching_ids` post-verify; on
            // a follower it is the leader's authoritative set, not a local scan.
            super::scan::ollp_predicted_doc_ids(predicted)
        } else {
            matching_ids
        };

        // OLLP edge-content verification (LEADER-ONLY, same rationale): implicit-
        // edge DELETE derives `EdgeDelete` tasks from the recon scan's
        // `_from`/`_to`/`_type`. If a matched doc's edge fields were concurrently
        // changed (or an edge appeared/disappeared among the matched docs)
        // between recon and now, the wrong edge would be deleted / a new edge
        // would dangle. The surrogate-set check above cannot see this — the
        // surrogate set is unchanged. The leader recomputes the ACTUAL edge set
        // from the matched docs and compares it to the predicted set carried in
        // the plan; on ANY divergence it returns OllpRetryRequired WITHOUT
        // writing. Followers trust the leader's decision. The actual-edge
        // recompute keys off the predicted apply set so leader and follower
        // reconcile the same edges.
        if let Some(predicted) = ollp_predicted_edges
            && self.ollp_is_group_leader
        {
            let actual = self.ollp_actual_edges(database_id, tid, collection, &apply_ids);
            if !super::scan::ollp_edges_match(actual, predicted) {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        // Gate secondary-vector maintenance once for the whole statement so a
        // collection with no vector field pays nothing. When a vector field is
        // present, each delete must also soft-delete the row's HNSW nodes and
        // drop its reverse-map entry — this handler cascades FTS, secondary
        // indexes, and graph edges but never the vector index, so a bulk delete
        // would otherwise leak vector nodes that keep scoring in KNN search.
        let has_vectors = self.collection_has_vectors(database_id, tid, collection);

        // Secondary-index paths for this collection, hoisted once. The delete
        // cascade below (`delete_indexes_for_document`) is a prefix scan that
        // does NOT return the removed `(field, value)` tuples, and the index
        // keys are `:`-delimited with values that may themselves contain `:` —
        // so parsing them back out is unsafe. The removed tuples are instead
        // recomputed from the pre-delete document via `index_tuples_for_doc`.
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

        // Delete each matching document with full cascade.
        let mut affected = 0u64;
        // One post-apply `Delete` redo entry per removed row on a vector
        // collection. The per-row `sparse.delete` above mints no WAL redo of its
        // own, so a WAL-only restart would replay the row's original `INSERT`
        // `Put` record back into the HNSW and resurrect its vector. Carrying the
        // surrogate back lets the Control Plane mint a durable `Delete` redo whose
        // replay soft-deletes the HNSW node through `apply_point_delete`. Only
        // populated when the collection has a vector index.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        let mut returned_docs: Vec<serde_json::Value> = if returning.is_some() {
            Vec::with_capacity(apply_ids.len())
        } else {
            Vec::new()
        };
        for doc_id in &apply_ids {
            // Capture pre-deletion snapshot if RETURNING was requested, or if
            // the collection is indexed (needed to recompute the removed
            // secondary-index tuples below — the delete cascade's prefix scan
            // cannot safely return them).
            let pre_delete_doc: Option<serde_json::Value> =
                if returning.is_some() || !index_paths.is_empty() {
                    self.sparse
                        .get(task.request.database_id.as_u64(), tid, collection, doc_id)
                        .ok()
                        .flatten()
                        .and_then(|bytes| {
                            let with_id =
                                nodedb_query::msgpack_scan::inject_str_field(&bytes, "id", doc_id);
                            doc_format::decode_document(&with_id)
                        })
                } else {
                    None
                };

            let deleted_bytes = self
                .sparse
                .delete(task.request.database_id.as_u64(), tid, collection, doc_id)
                .ok()
                .flatten();
            if let Some(deleted_bytes) = deleted_bytes.as_deref() {
                // Cascade: inverted index. doc_id is the hex-encoded surrogate
                // (the redb storage key). Parse back once for FTS removal and
                // reused below for the write version + write-set entry.
                let row_surrogate = crate::engine::document::store::doc_id_to_surrogate(doc_id);
                match row_surrogate {
                    Some(surrogate) => {
                        if let Err(e) = self.inverted.remove_document(
                            task.request.database_id.as_u64(),
                            crate::types::TenantId::new(tid),
                            collection,
                            surrogate,
                        ) {
                            warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: inverted index removal failed");
                        }
                    }
                    None => {
                        warn!(core = self.core_id, %collection, %doc_id, "bulk delete: doc_id is not a valid surrogate; FTS entry may be orphaned");
                    }
                }
                // Cascade: secondary indexes.
                if let Err(e) = self.sparse.delete_indexes_for_document(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    doc_id,
                ) {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: secondary index cascade failed");
                }
                // Cascade: graph edges.
                let edges_removed = self
                    .csr_partition_mut(database_id, tid)
                    .remove_node_edges(doc_id);
                let cascade_ord = self.hlc.next_ordinal();
                if edges_removed > 0
                    && let Err(e) = self.edge_store.delete_edges_for_node(
                        database_id,
                        nodedb_types::TenantId::new(tid),
                        doc_id,
                        cascade_ord,
                    )
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "bulk delete: edge cascade failed");
                }
                self.mark_node_deleted(database_id, tid, doc_id);
                // Cascade: secondary HNSW vector index. The put path indexed
                // this row's vectors under its surrogate; the delete must
                // soft-delete those nodes and drop the reverse-map entry, or the
                // leaked vector keeps scoring in KNN search in the same process.
                if has_vectors {
                    self.remove_document_vector_indexes(database_id, tid, collection, doc_id);
                }
                self.doc_cache.invalidate(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    doc_id,
                );
                // Record the committed delete's write version against its
                // surrogate + collection.
                if let Some(surrogate) = row_surrogate {
                    self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());
                    // Record the removed secondary-index tuples into the
                    // per-index write-value substrate, recomputed from the
                    // pre-delete document (see `index_paths` comment above).
                    if let (Some(lsn), Some(doc)) = (task.wal_lsn(), pre_delete_doc.as_ref()) {
                        let tuples = self.index_tuples_for_doc(doc, &index_paths);
                        self.note_index_write_values(
                            task.request.database_id,
                            crate::types::TenantId::new(tid),
                            collection,
                            &tuples,
                            lsn,
                        );
                    }
                    // Carry the surrogate back for a post-apply `Delete` redo so
                    // the removed vector node does not resurrect on a WAL-only
                    // restart. Gated on `has_vectors` — a non-vector collection
                    // pays nothing. A delete carries no post-image body.
                    if has_vectors {
                        write_set.push(WriteSetEntry {
                            surrogate: surrogate.as_u32(),
                            is_delete: true,
                            value: Vec::new(),
                        });
                    }
                }
                // Emit a delete event per affected row to the Event Plane, so
                // AFTER-DELETE triggers and CDC/change-stream consumers see
                // each row a bulk DELETE removed — mirroring
                // `execute_point_delete`'s single-row emit. `deleted_bytes` is
                // the prior stored bytes `sparse.delete` returned above (no
                // second read needed); `resolve_event_payload` handles the
                // strict->msgpack conversion for triggers. Emitted per row
                // (not a `WriteOp::BulkDelete` summary) — the Event Plane's
                // WAL-replay bulk variant is aggregate metadata reconstructed
                // only when the live per-row events were lost.
                let old_converted = self.resolve_event_payload(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    deleted_bytes,
                );
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Delete,
                    doc_id,
                    None,
                    Some(old_converted.as_deref().unwrap_or(deleted_bytes)),
                );
                affected += 1;
                if returning.is_some()
                    && let Some(doc) = pre_delete_doc
                {
                    returned_docs.push(doc);
                }
            }
        }

        // Invalidate aggregate cache — a delete changes count(*) for this
        // collection. Only needed when at least one row was actually removed.
        if affected > 0 {
            self.invalidate_aggregate_cache_for_collection(
                task.request.database_id.as_u64(),
                tid,
                collection,
            );
        }

        debug!(core = self.core_id, %collection, affected, "bulk delete complete");

        let mut response = if let Some(spec) = returning {
            match returning_rows::build_rows_payload(spec, &returned_docs) {
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
            let result = serde_json::json!({ "affected": affected });
            match response_codec::encode_json(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };
        if !write_set.is_empty() {
            response.write_set = write_set;
        }
        response
    }

    /// Compute the sorted ACTUAL implicit-edge set for the matched docs.
    ///
    /// For each matched `doc_id`, parse its surrogate (same `len()==8` hex
    /// parse as [`ollp_actual_surrogates`]), fetch the stored doc bytes via the
    /// SAME `sparse.get` path the delete loop uses, decode it, and — only when
    /// it carries BOTH `_from` and `_to` as strings — record an
    /// [`OllpPredictedEdge`] with the raw `_type` as `label`. A matched doc
    /// without both endpoints is not an edge and is skipped; if it gained an
    /// edge after recon it appears here and forces a set mismatch (correct).
    ///
    /// The output is sorted via `OllpPredictedEdge`'s derived `Ord` so it
    /// compares as a plain sorted-slice equality against the Control-Plane-sorted
    /// predicted set. Edge docs are schemaless (`_from`/`_to`), so `decode_document`
    /// (msgpack→JSON) is the field-extraction primitive — no hand-rolled
    /// msgpack. Bytes that don't decode (e.g. a strict Binary Tuple) yield no
    /// edge, matching the schemaless-only scope of implicit edges.
    pub(in crate::data::executor) fn ollp_actual_edges(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        matching_ids: &[String],
    ) -> Vec<OllpPredictedEdge> {
        // `decode_document` returns `serde_json::Value`, whose `get`/`as_str`
        // are inherent methods — no extra trait import needed.
        let mut edges: Vec<OllpPredictedEdge> = Vec::new();
        for doc_id in matching_ids {
            let surrogate = if doc_id.len() == 8 {
                match u32::from_str_radix(doc_id, 16) {
                    Ok(s) => s,
                    Err(_) => continue,
                }
            } else {
                continue;
            };
            let Ok(Some(bytes)) = self.sparse.get(database_id, tid, collection, doc_id) else {
                continue;
            };
            let Some(doc) = doc_format::decode_document(&bytes) else {
                continue;
            };
            let from = doc.get("_from").and_then(|v| v.as_str());
            let to = doc.get("_to").and_then(|v| v.as_str());
            if let (Some(from), Some(to)) = (from, to) {
                let label = doc
                    .get("_type")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                edges.push(OllpPredictedEdge {
                    surrogate,
                    from: from.to_string(),
                    to: to.to_string(),
                    label,
                });
            }
        }
        edges.sort_unstable();
        edges
    }
}
