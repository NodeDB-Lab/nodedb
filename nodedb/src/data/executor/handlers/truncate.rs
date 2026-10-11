// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE and ESTIMATE_COUNT handlers.

use nodedb_physical::physical_plan::{ResolvedSumTarget, StorageMode};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::partial_refusal::refusal_after_rows;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

/// Borrowed arguments for [`CoreLoop::execute_truncate`].
pub(in crate::data::executor) struct TruncateParams<'a> {
    pub collection: &'a str,
    /// Join-key VALUE → target row surrogate for every materialized-sum
    /// target the removed rows contribute to, resolved on the Control Plane.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// The collection's declared `PRIMARY KEY` column, when it has one. Names
    /// each removed row in its redo entry and delete event.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// TRUNCATE: delete all documents in a collection without filter scanning.
    ///
    /// Iterates the DOCUMENTS table prefix and deletes every key. Cascades to
    /// inverted index, secondary indexes, and document cache. The
    /// collection's graph edges are cut by `TruncateEdges`. Returns
    /// `{"truncated": N}` payload.
    ///
    /// Every removed row folds its own `RowImages::Delete` through the
    /// enforcement funnel, from inside this loop. There is deliberately NO bulk
    /// aggregate: TRUNCATE must leave the stored totals exactly where N
    /// individual deletes leave them, and a separate aggregate path is a second
    /// implementation of the same arithmetic — free to drift from the per-row
    /// one that every other delete path uses.
    pub(in crate::data::executor) fn execute_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: TruncateParams<'_>,
    ) -> Response {
        let TruncateParams {
            collection,
            resolved_sum_targets,
            declared_primary_key,
        } = params;
        debug!(core = self.core_id, %collection, "truncate");

        // Collect all document IDs in this collection.
        let all_ids = match self.scan_matching_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &[],
        ) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };

        // Gate secondary-vector maintenance once for the whole statement so a
        // collection with no vector field pays nothing — mirrors
        // `execute_bulk_delete`'s `has_vectors` gate.
        let database_id = task.request.database_id.as_u64();

        // Materialized-sum coverage verification (LEADER-ONLY), identical in
        // contract to the bulk-DML paths: the resolution was derived from a
        // Control-Plane recon scan of this collection taken before execution, and
        // a row inserted since then debits a target the plan holds no surrogate
        // for. TRUNCATE must leave every bound total at exactly what N individual
        // deletes leave it at, so a shortfall returns OllpRetryRequired
        // WITHOUT removing anything rather than emptying the collection and
        // leaving a total that still counts its rows.
        if self.sum_targets_diverged_for_ids(
            &SumTargetCheck {
                database_id,
                tid,
                collection,
                // TRUNCATE assigns nothing: every removed row contributes the
                // join value it currently holds and no other.
                updates: &[],
                resolved: resolved_sum_targets,
            },
            &all_ids,
        ) {
            return self.response_error(task, ErrorCode::OllpRetryRequired);
        }

        let has_vectors = self.collection_has_vectors(database_id, tid, collection);

        // The stored pre-image is a Binary Tuple on a strict collection and
        // MessagePack otherwise. Hoisted once so each removed row's identity is
        // read through the matching decoder.
        let strict_schema = self
            .doc_configs
            .get(&(
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            ))
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema.clone()),
                StorageMode::Schemaless => None,
            });

        // BALANCED, decided over every row about to be removed and BEFORE the
        // first removal — each row below commits in its own transaction, so a
        // check after the loop cannot undo what it found. Emptying a
        // collection whose journals all balance nets to zero and proceeds;
        // emptying one that holds an unbalanced group is refused with nothing
        // removed.
        match self.balanced_entries_for_stored_deletes(database_id, tid, collection, &all_ids) {
            Ok(entries) => {
                if let Err(e) = self.settle_balanced_entries(database_id, tid, collection, entries)
                {
                    return self.response_error(task, e);
                }
            }
            Err(e) => return self.response_error(task, e),
        }

        // Delete each document with full cascade.
        let mut truncated = 0u64;
        // One post-apply `Delete` redo entry per removed row, in removal
        // order, followed by the target rows its fold rewrote.
        // `wal_append_document_op` mints no pre-dispatch record for
        // `DocumentOp::Truncate`, so these entries are the only record of the
        // removals WAL replay and a point-in-time restore apply.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        // Surrogates removed so far. A refusal part-way removes their text
        // from the inverted index. A full TRUNCATE empties it in one purge.
        let mut removed: Vec<nodedb_types::Surrogate> = Vec::new();
        for storage_key in &all_ids {
            // One transaction per removed row, shared with the materialized-sum
            // delta that row owes — identical to `execute_bulk_delete`, so a
            // TRUNCATE and a `DELETE` with no predicate leave the same totals.
            // A refusal after a removal committed keeps the rows removed so
            // far, and carries their entries so they are journalled.
            let row_txn = match self.sparse.begin_write() {
                Ok(txn) => txn,
                Err(e) => {
                    let code = refusal_after_rows(truncated, e);
                    return self.truncate_refusal(task, tid, collection, code, &removed, write_set);
                }
            };
            // A delete error refuses the TRUNCATE: read as an absent row, it
            // would leave the row stored under a success.
            let deleted_bytes =
                match self
                    .sparse
                    .delete_in_txn(&row_txn, database_id, tid, collection, storage_key)
                {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        let code = refusal_after_rows(truncated, e);
                        return self
                            .truncate_refusal(task, tid, collection, code, &removed, write_set);
                    }
                };
            // The row's secondary-index entries leave in the row's own
            // transaction, so the row and its entries go together or not at all.
            if deleted_bytes.is_some()
                && let Err(e) = self.sparse.delete_indexes_for_document_in_txn(
                    &row_txn,
                    database_id,
                    tid,
                    collection,
                    storage_key,
                )
            {
                let code = refusal_after_rows(truncated, e);
                return self.truncate_refusal(task, tid, collection, code, &removed, write_set);
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
                    // again here counts the same removal twice.
                    Ok(outcome) => target_writes = outcome.target_writes,
                    Err(e) => {
                        let code = refusal_after_rows(truncated, e);
                        return self
                            .truncate_refusal(task, tid, collection, code, &removed, write_set);
                    }
                }
            }
            if let Err(e) = row_txn.commit() {
                let code = refusal_after_rows(
                    truncated,
                    crate::Error::Storage {
                        engine: "sparse".into(),
                        detail: format!("truncate commit: {e}"),
                    },
                );
                return self.truncate_refusal(task, tid, collection, code, &removed, write_set);
            }
            if let Some(deleted_bytes) = deleted_bytes.as_deref() {
                let surrogate = storage_key.surrogate();
                // The identity INSERT minted for this row, read from the
                // pre-image the delete returned: the declared primary key
                // when the collection declares one, else the decimal
                // surrogate. The redo entry and the delete event share it.
                let row_identity = stored_row_identity(
                    deleted_bytes,
                    strict_schema.as_ref(),
                    declared_primary_key,
                    *storage_key,
                );
                // Record the removal's version against the row's surrogate
                // and collection, as a point delete does.
                self.note_surrogate_write(task, tid, collection, surrogate.as_u32());
                // The row's text leaves the inverted index with every other
                // row's, in one purge once the loop ends.
                removed.push(surrogate);
                // Cascade: secondary HNSW vector index. The put path indexed
                // this row's vectors under its surrogate; truncate must
                // soft-delete those nodes and drop the reverse-map entry, or
                // the leaked vector keeps scoring in KNN search in the same
                // process (mirrors `execute_bulk_delete`'s vector cascade).
                if has_vectors {
                    self.remove_document_vector_indexes(database_id, tid, collection, *storage_key);
                }
                write_set.push(WriteSetEntry::delete(
                    surrogate.as_u32(),
                    row_identity.clone(),
                ));
                // The collection's edges keep their place here: the
                // TRUNCATE's transaction cuts them with one `TruncateEdges`
                // per vShard, at its ordinal.
                self.doc_cache.invalidate(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    storage_key,
                );
                // Emit a delete event per removed row to the Event Plane, so
                // AFTER-DELETE triggers and CDC/change-stream consumers see
                // each row TRUNCATE removed — mirroring `execute_point_delete`
                // and `execute_bulk_delete`'s single-row emit. `deleted_bytes`
                // is the prior stored bytes `sparse.delete` returned above.
                // Emitted per row rather than a single `WriteOp::BulkDelete`
                // summary: that variant is aggregate metadata the Event
                // Plane's WAL replay reconstructs only when the live per-row
                // events were lost, and per-row events are what ROW-level
                // AFTER-DELETE triggers match on (see
                // `event::trigger::dispatcher::single`).
                self.emit_document_delete_event(
                    task,
                    tid,
                    collection,
                    row_identity,
                    Some(deleted_bytes),
                );
                truncated += 1;
            }
            write_set.extend(write_hook::target_write_set(&target_writes));
        }

        // Every row is removed: empty the collection's inverted index in one
        // purge. Its analyzer, language, and fuzzy configuration stay.
        if let Err(e) = self.inverted.clear_collection(
            database_id,
            crate::types::TenantId::new(tid),
            collection,
        ) {
            let code = refusal_after_rows(truncated, ErrorCode::from(e));
            return self.refusal_with_landed_rows(task, code, write_set);
        }

        // Clear aggregate cache for this collection.
        self.invalidate_aggregate_cache_for_collection(
            task.request.database_id.as_u64(),
            tid,
            collection,
        );

        debug!(core = self.core_id, %collection, truncated, "truncate complete");
        let result = serde_json::json!({ "truncated": truncated });
        // Every row is removed by now, so an encode error answers with the
        // removals' entries as well.
        let mut response = match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        };
        response.write_set = write_set;
        response
    }

    /// The refusal of a TRUNCATE that stopped part-way. The rows removed so
    /// far stay removed, so their text leaves the inverted index in one batch
    /// before the refusal answers. The refusal keeps `code`, so the client
    /// sees the SQLSTATE of what stopped the TRUNCATE.
    ///
    /// A failure of the text removal leaves the inverted index holding text
    /// of removed rows. That cannot be undone here, so the core fail-stops.
    /// Restart replay applies the journalled removals and rebuilds the index.
    fn truncate_refusal(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        code: ErrorCode,
        removed: &[nodedb_types::Surrogate],
        write_set: Vec<WriteSetEntry>,
    ) -> Response {
        if let Err(e) = self.inverted.remove_documents(
            task.request.database_id.as_u64(),
            crate::types::TenantId::new(tid),
            collection,
            removed,
        ) {
            self.fail_stop_core(
                FailStopCause::PostInstallFailed,
                &format!(
                    "TRUNCATE of '{collection}' stopped after {} rows with {code:?}, then \
                     removing those rows' text from the inverted index failed: {e}",
                    removed.len()
                ),
            );
        }
        self.refusal_with_landed_rows(task, code, write_set)
    }

    /// ESTIMATE_COUNT: return approximate row count from HLL cardinality stats.
    pub(in crate::data::executor) fn execute_estimate_count(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Response {
        match self
            .stats_store
            .get(task.request.database_id.as_u64(), tid, collection, field)
        {
            Ok(Some(stats)) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": stats.distinct_count,
                    "row_count": stats.row_count,
                    "null_count": stats.null_count,
                });
                match response_codec::encode_json_as_msgpack(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                }
            }
            Ok(None) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": 0,
                    "row_count": 0,
                    "null_count": 0,
                });
                match response_codec::encode_json_as_msgpack(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                }
            }
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
