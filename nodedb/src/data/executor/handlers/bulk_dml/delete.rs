// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{
    OllpPredictedEdge, ResolvedSumTarget, ReturningSpec, StorageMode,
};

use super::delete_cascade::BulkDeleteRowCascade;

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

/// Borrowed arguments for [`CoreLoop::execute_bulk_delete`], grouped so the
/// handler stays within the argument-count limit.
pub(in crate::data::executor) struct BulkDeleteParams<'a> {
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy gating the REMOVAL, decided per row against
    /// its pre-deletion image. A separate slot from `rls_filters`: that one
    /// bounds what may be shown back, this one bounds what may be removed.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// the rows this predicate matches contribute to, resolved on the Control
    /// Plane from its recon scan of the same predicate.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    pub ollp: OllpPrediction<'a>,
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
        params: BulkDeleteParams<'_>,
    ) -> Response {
        let BulkDeleteParams {
            collection,
            filter_bytes,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            ollp,
        } = params;
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

        // Settle the apply set and run every leader-only pre-write
        // verification the plan's predictions call for. Any divergence returns
        // here, before the first row is removed. A delete assigns nothing, so
        // every matched row contributes the join value it currently holds and no
        // other.
        let apply_ids = match self.admit_bulk_predicate_write(
            database_id,
            tid,
            matching_ids,
            &super::BulkAdmission {
                collection,
                predicted_surrogates: ollp_predicted_surrogates,
                predicted_edges: ollp_predicted_edges,
                updates: &[],
                resolved_sum_targets,
            },
        ) {
            Ok(ids) => ids,
            Err(code) => return self.response_error(task, code),
        };

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

        // The stored pre-image is a Binary Tuple on a strict collection and
        // MessagePack otherwise. Hoisted once for the whole statement so the
        // per-row decode below picks the matching decoder — the MessagePack
        // decoder accepts a Binary Tuple without erroring and yields a document
        // with none of the row's real columns.
        let strict_schema = self
            .doc_configs
            .get(&config_key)
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema.clone()),
                StorageMode::Schemaless => None,
            });

        // Gate every matched row on the collection's write policy BEFORE any
        // removal, so a rejected row cannot leave the rows ahead of it already
        // deleted. The pre-deletion image is the only image a delete has. A row
        // that is already absent is admitted: it removes nothing, so there is
        // no image for the policy to restrict.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for doc_id in &apply_ids {
                // `doc_id` is a bare string from a raw-table scan; a shape
                // that fails to parse as a storage key is treated the same
                // as the row-already-gone case right below it.
                let Some(key) = crate::engine::document::store::StorageKey::parse(doc_id) else {
                    continue;
                };
                let stored = match self.sparse.get(database_id, tid, collection, &key) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => continue,
                    Err(e) => return self.response_error(task, e),
                };
                let identity = crate::engine::document::store::identity_of(doc_id);
                if let Err(e) = rls_write_gate::admit_stored_row(
                    rls_write_check,
                    &stored,
                    &identity,
                    strict_schema.as_ref(),
                    tid,
                    collection,
                ) {
                    return self.response_error(task, e);
                }
            }
        }

        // BALANCED, decided over the whole matched set BEFORE the first removal.
        // Each row below commits in its own transaction, so a check that ran
        // after the loop could only report a violation the earlier rows had
        // already made durable. A removal SUBTRACTS its row's amount, so
        // deleting one leg of a balanced journal is refused here — with nothing
        // deleted — while deleting a whole journal nets to zero and proceeds.
        match self.balanced_entries_for_stored_deletes(database_id, tid, collection, &apply_ids) {
            Ok(entries) => {
                if let Err(e) = self.settle_balanced_entries(database_id, tid, collection, entries)
                {
                    return self.response_error(task, e);
                }
            }
            Err(e) => return self.response_error(task, e),
        }

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
            // `doc_id` is a bare string from a raw-table scan several calls
            // removed from `SparseEngine`'s typed scan methods. A shape that
            // fails to parse as a storage key can hold no row in DOCUMENTS
            // either way, so every call below that needs the typed key treats
            // it exactly like the "row already gone" case it already handles.
            let storage_key = crate::engine::document::store::StorageKey::parse(doc_id);

            // Capture pre-deletion snapshot if RETURNING was requested, or if
            // the collection is indexed (needed to recompute the removed
            // secondary-index tuples below — the delete cascade's prefix scan
            // cannot safely return them).
            // `None` means the row was already gone. A row that IS there but
            // will not decode is a different answer: it would silently drop out
            // of RETURNING and, worse, contribute no removed index tuples, so
            // its old secondary-index entries would survive the delete.
            let pre_delete_doc: Option<serde_json::Value> = if returning.is_some()
                || !index_paths.is_empty()
            {
                match storage_key.and_then(|key| {
                    self.sparse
                        .get(task.request.database_id.as_u64(), tid, collection, &key)
                        .ok()
                        .flatten()
                }) {
                    Some(bytes) => {
                        // `doc_id` is the storage key from the scan. `RETURNING`
                        // reports the row's client-visible identity, not the
                        // storage key. A value that fails to parse as a minted
                        // key is a legacy or user key, taken verbatim.
                        let identity = crate::engine::document::store::identity_of(doc_id);
                        match returning_doc::from_stored(&bytes, &identity, strict_schema.as_ref())
                        {
                            Ok(doc) => Some(doc),
                            Err(e) => return self.response_error(task, e),
                        }
                    }
                    None => None,
                }
            } else {
                None
            };

            // The removal and the materialized-sum deltas it owes share ONE
            // transaction, so a debited target row can never outlive a removal
            // that did not commit. The stored bytes the removal hands back are
            // the row's only pre-image and the cheapest one available — the
            // separate `pre_delete_doc` read above exists for RETURNING and the
            // index diff, and is not widened for this.
            let row_txn = match self.sparse.begin_write() {
                Ok(txn) => txn,
                Err(e) => return self.response_error(task, e),
            };
            let deleted_bytes = storage_key.and_then(|key| {
                self.sparse
                    .delete_in_txn(
                        &row_txn,
                        task.request.database_id.as_u64(),
                        tid,
                        collection,
                        &key,
                    )
                    .ok()
                    .flatten()
            });
            // Period lock, the pre-deletion image — a delete has no other.
            // Checked before `write_hook::run` and before commit: dropping
            // `row_txn` un-committed on a refusal reverses the removal.
            if let Some(bytes) = deleted_bytes.as_deref()
                && let Some(config) = self.doc_configs.get(&config_key)
                && let Some(ref pl) = config.enforcement.period_lock
                && let Err(e) = crate::data::executor::enforcement::period_lock::check_period_lock(
                    &self.sparse,
                    database_id,
                    tid,
                    collection,
                    bytes,
                    pl,
                    resolved_sum_targets,
                )
            {
                return self.response_error(task, e);
            }
            let mut target_writes = Vec::new();
            if let Some(bytes) = deleted_bytes.as_deref() {
                match write_hook::run(
                    self,
                    &row_txn,
                    &write_hook::HookCtx {
                        database_id,
                        tid,
                        collection,
                        resolved_targets: resolved_sum_targets,
                        deferred_sum_targets: &[],
                        wal_lsn: task.wal_lsn(),
                    },
                    write_hook::WriteImages::Delete {
                        old: write_hook::ImageBody::Stored(bytes),
                    },
                ) {
                    // The row's BALANCED contribution was settled for the whole
                    // statement above, before any row was removed; taking it
                    // again here would count the same removal twice.
                    Ok(outcome) => target_writes = outcome.target_writes,
                    // Dropping `row_txn` un-committed reverses both the removal
                    // and every target it had already debited.
                    Err(e) => return self.response_error(task, e),
                }
            }
            if let Err(e) = row_txn.commit() {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("bulk delete commit: {e}"),
                    },
                );
            }
            // One durable redo entry per debited target row, naming the TARGET
            // collection: this statement's own redo describes the removed source
            // rows only, so without these a WAL-only restart leaves every total
            // as it stood before the delete.
            write_set.extend(write_hook::target_write_set(&target_writes));
            if let Some(bytes) = deleted_bytes.as_deref() {
                self.bulk_delete_row_cascade(
                    BulkDeleteRowCascade {
                        task,
                        database_id,
                        tid,
                        collection,
                        doc_id: doc_id.as_str(),
                        storage_key,
                        deleted_bytes: bytes,
                        has_vectors,
                        index_paths: &index_paths,
                        pre_delete_doc,
                        returning: returning.is_some(),
                    },
                    &mut write_set,
                    &mut returned_docs,
                );
                affected += 1;
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
            match returning_rows::build_rows_payload(spec, rls_filters, &returned_docs) {
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
            match response_codec::encode_json_as_msgpack(&result) {
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
}
