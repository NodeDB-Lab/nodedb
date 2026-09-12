// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE and ESTIMATE_COUNT handlers.

use nodedb_physical::physical_plan::{ResolvedSumTarget, StorageMode};
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use crate::data::executor::enforcement::write_hook;
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
    /// inverted index, secondary indexes, graph edges, and document cache.
    /// Returns `{"truncated": N}` payload.
    ///
    /// Every removed row folds its own `RowImages::Delete` through the
    /// enforcement funnel, from inside this loop. There is deliberately NO bulk
    /// aggregate: TRUNCATE must leave the stored totals exactly where N
    /// individual deletes would, and a separate aggregate path would be a second
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
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("scan for truncate: {e}"),
                    },
                );
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
        // deletes would leave it at, so a shortfall returns OllpRetryRequired
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
        // check after the loop could not undo what it found. Emptying a
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
        // One post-apply `Delete` redo entry per removed row on a vector
        // collection. `wal_append_document_op` mints no per-row redo for
        // `DocumentOp::Truncate` (row durability is redb-synchronous), so
        // without this a WAL-only restart would replay each row's original
        // `Put` record and resurrect its HNSW vector — mirrors
        // `execute_bulk_delete`'s `write_set` cascade.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        for storage_key in &all_ids {
            let doc_id = storage_key.to_string();

            // One transaction per removed row, shared with the materialized-sum
            // delta that row owes — identical to `execute_bulk_delete`, so a
            // TRUNCATE and a `DELETE` with no predicate leave the same totals.
            let row_txn = match self.sparse.begin_write() {
                Ok(txn) => txn,
                Err(e) => return self.response_error(task, e),
            };
            let deleted_bytes = self
                .sparse
                .delete_in_txn(&row_txn, database_id, tid, collection, storage_key)
                .ok()
                .flatten();
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
                    Err(e) => return self.response_error(task, e),
                }
            }
            if let Err(e) = row_txn.commit() {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("truncate commit: {e}"),
                    },
                );
            }
            write_set.extend(write_hook::target_write_set(&target_writes));
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
                if let Err(e) = self.inverted.remove_document(
                    database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    surrogate,
                ) {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "truncate: inverted removal failed");
                }
                if let Err(e) = self.sparse.delete_indexes_for_document(
                    database_id,
                    tid,
                    collection,
                    storage_key,
                ) {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "truncate: index cascade failed");
                }
                // Cascade: secondary HNSW vector index. The put path indexed
                // this row's vectors under its surrogate; truncate must
                // soft-delete those nodes and drop the reverse-map entry, or
                // the leaked vector keeps scoring in KNN search in the same
                // process (mirrors `execute_bulk_delete`'s vector cascade).
                if has_vectors {
                    self.remove_document_vector_indexes(database_id, tid, collection, *storage_key);
                    write_set.push(WriteSetEntry {
                        surrogate: surrogate.as_u32(),
                        identity: row_identity.clone(),
                        is_delete: true,
                        value: Vec::new(),
                        collection: None,
                    });
                }
                let edges = self
                    .csr_partition_mut(database_id, tid)
                    .remove_node_edges(&doc_id);
                let cascade_ord = self.hlc.next_ordinal();
                if edges > 0
                    && let Err(e) = self.edge_store.delete_edges_for_node(
                        database_id,
                        nodedb_types::TenantId::new(tid),
                        &doc_id,
                        cascade_ord,
                    )
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "truncate: edge cascade failed");
                }
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
                let old_converted = self.resolve_event_payload(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    deleted_bytes,
                );
                self.emit_document_delete_event(
                    task,
                    collection,
                    row_identity,
                    Some(old_converted.as_deref().unwrap_or(deleted_bytes)),
                );
                truncated += 1;
            }
        }

        // Clear aggregate cache for this collection.
        self.invalidate_aggregate_cache_for_collection(
            task.request.database_id.as_u64(),
            tid,
            collection,
        );

        debug!(core = self.core_id, %collection, truncated, "truncate complete");
        let result = serde_json::json!({ "truncated": truncated });
        let mut response = match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
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
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
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
}
