// SPDX-License-Identifier: BUSL-1.1

//! The `UPDATE ... FROM` write pass: persist each row [`super::update_from_join`]
//! already matched and resolved, one row/transaction at a time, folding each
//! into its materialized-sum target and re-indexing its vectors.

use nodedb_physical::physical_plan::ResolvedSumTarget;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::update_reindex_vector::UpdateVectorReindex;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::task::ExecutionTask;

use super::update_from_join_types::ResolvedUpdateRow;

/// What the write pass produced, handed back to the caller for response
/// encoding.
pub(in crate::data::executor) struct UpdateFromJoinWriteOutcome {
    pub affected: u64,
    /// One post-apply `Put` redo entry per updated row on a vector collection
    /// plus every derived materialized-sum target write. Empty when the
    /// target collection has no vector index and no materialized-sum target.
    pub write_set: Vec<WriteSetEntry>,
    /// Post-image JSON per affected row, populated only when the caller asked
    /// for `RETURNING`.
    pub returned_docs: Vec<serde_json::Value>,
}

/// Everything the write pass needs about the statement, gathered once by the
/// caller so this pass never re-derives it.
pub(in crate::data::executor) struct WriteResolvedRowsCtx<'a> {
    pub tid: u64,
    pub target_collection: &'a str,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    pub has_vectors: bool,
    pub is_strict: bool,
    pub want_returning: bool,
}

impl CoreLoop {
    /// Persist every row in `rows`, one transaction each. Returns `Err(resp)`
    /// with a ready-to-return error `Response` the moment any row's write,
    /// enforcement fold, or vector re-index fails — the caller returns it
    /// immediately rather than continuing the loop.
    pub(in crate::data::executor) fn write_resolved_update_from_join_rows(
        &mut self,
        task: &ExecutionTask,
        ctx: WriteResolvedRowsCtx<'_>,
        rows: Vec<ResolvedUpdateRow>,
    ) -> Result<UpdateFromJoinWriteOutcome, Response> {
        let WriteResolvedRowsCtx {
            tid,
            target_collection,
            resolved_sum_targets,
            has_vectors,
            is_strict,
            want_returning,
        } = ctx;
        let database_id = task.request.database_id.as_u64();
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            target_collection.to_string(),
        );
        let mut affected = 0u64;
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        let mut returned_docs: Vec<serde_json::Value> = if want_returning {
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

            // `put_in_txn` addresses DOCUMENTS rows by `StorageKey` only, and
            // the workspace carries no on-disk-format compatibility burden
            // for a row shape that predates surrogate keying, so a row whose
            // `doc_id` does not parse as one is refused rather than written
            // through a raw string key.
            let storage_key = match crate::engine::document::store::StorageKey::parse(&doc_id) {
                Some(key) => key,
                None => {
                    return Err(self.response_error(
                        task,
                        crate::Error::Storage {
                            engine: "document".into(),
                            detail: format!(
                                "UPDATE ... FROM target row '{doc_id}' in \
                                 '{target_collection}' has no surrogate storage key"
                            ),
                        },
                    ));
                }
            };

            // Period lock, both images — matching `execute_point_update`: a
            // closed period must reject an edit to a row it already holds,
            // and must reject an edit that assigns the period column into it.
            if let Some(config) = self.doc_configs.get(&config_key)
                && let Some(ref pl) = config.enforcement.period_lock
            {
                if let Err(e) = crate::data::executor::enforcement::period_lock::check_period_lock(
                    &self.sparse,
                    database_id,
                    tid,
                    target_collection,
                    &old_body,
                    pl,
                    resolved_sum_targets,
                ) {
                    return Err(self.response_error(task, e));
                }
                if let Err(e) = crate::data::executor::enforcement::period_lock::check_period_lock(
                    &self.sparse,
                    database_id,
                    tid,
                    target_collection,
                    &updated_bytes,
                    pl,
                    resolved_sum_targets,
                ) {
                    return Err(self.response_error(task, e));
                }
            }

            // The row's body and the materialized-sum delta it owes share ONE
            // transaction. `ResolvedUpdateRow` already carries BOTH images —
            // `old_body` as stored and `body` as the post-image — so the fold
            // re-reads nothing; the struct was built to carry them.
            let row_txn = match self.sparse.begin_write() {
                Ok(txn) => txn,
                Err(e) => return Err(self.response_error(task, e)),
            };
            let stored = self.sparse.put_in_txn(
                &row_txn,
                database_id,
                tid,
                target_collection,
                &storage_key,
                &updated_bytes,
            );
            if stored.is_ok() {
                let enforcement = write_hook::run(
                    self,
                    &row_txn,
                    &write_hook::HookCtx {
                        database_id,
                        tid,
                        collection: target_collection,
                        resolved_targets: resolved_sum_targets,
                        deferred_sum_targets: &[],
                        wal_lsn: task.wal_lsn(),
                    },
                    write_hook::WriteImages::Update {
                        old: write_hook::ImageBody::Stored(&old_body),
                        new: write_hook::ImageBody::Stored(&updated_bytes),
                    },
                );
                let target_writes = match enforcement {
                    // Only the target writes are taken: this row's BALANCED
                    // contribution was settled for the whole statement above,
                    // before the first row was rewritten, so taking it again
                    // would count the same update twice.
                    Ok(outcome) => outcome.target_writes,
                    // Dropping `row_txn` un-committed reverses the row and every
                    // target it had already moved.
                    Err(e) => return Err(self.response_error(task, e)),
                };
                if let Err(e) = row_txn.commit() {
                    return Err(self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("update-from-join commit: {e}"),
                        },
                    ));
                }
                // One durable redo entry per moved target row, naming the TARGET
                // collection — this statement's redo describes only the rows of
                // `target_collection` it rewrote.
                write_set.extend(write_hook::target_write_set(&target_writes));
                self.doc_cache.put(
                    database_id,
                    tid,
                    target_collection,
                    &storage_key,
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
                let row_identity = crate::engine::document::store::identity_of(&doc_id);
                // `row_identity` is read again below for `RETURNING`'s `id`
                // field, so the event-emit boundary gets a clone rather than
                // the move.
                self.emit_put_event(
                    task,
                    tid,
                    target_collection,
                    row_identity.clone(),
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
                    if let Err(e) = self.update_reindex_vector_indexes(UpdateVectorReindex {
                        database_id,
                        tid,
                        collection: target_collection,
                        row_key: &doc_id,
                        surrogate,
                        new_body: &updated_bytes,
                        is_strict,
                        has_vectors,
                    }) {
                        return Err(self.response_error(task, e));
                    }
                    write_set.push(WriteSetEntry {
                        surrogate: surrogate.as_u32(),
                        is_delete: false,
                        value: updated_bytes,
                        collection: None,
                    });
                }
                affected += 1;
                if want_returning {
                    // `row_identity` only stands in as `id` for a row that
                    // declares no primary key of its own — overwriting a
                    // declared key would return a value the client never wrote.
                    returning_doc::attach_row_id(&mut doc, &row_identity);
                    returned_docs.push(doc);
                }
            }
        }

        Ok(UpdateFromJoinWriteOutcome {
            affected,
            write_set,
            returned_docs,
        })
    }
}
