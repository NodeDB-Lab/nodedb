// SPDX-License-Identifier: BUSL-1.1

//! Document PointPut helper for transaction sub-plans.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::{self, ChainGuard};
use crate::data::executor::enforcement::funnel::WriteEnforcementOutcome;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::tx_point_put`].
pub(in crate::data::executor::handlers::transaction) struct TxPointPut<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub value: &'a [u8],
    pub user_roles: &'a [String],
    /// Insert-vs-upsert semantics. `None` = PUT/upsert (overwrite is allowed,
    /// no existence probe). `Some(if_absent)` = INSERT semantics: probe for an
    /// existing primary key under the same write txn and, if present, either
    /// silently skip (`if_absent = true`, `INSERT ... ON CONFLICT DO NOTHING`)
    /// or reject with a `unique` constraint violation (`if_absent = false`).
    pub insert_if_absent: Option<bool>,
    /// Join-key VALUE → target row surrogate, resolved on the Control Plane at
    /// plan time for every materialized-sum target this write may touch. The
    /// Data Plane addresses target rows with these and never derives them: the
    /// primary-key → surrogate map is Control-Plane catalog state.
    pub resolved_sum_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
    /// Materialized-sum TARGET collections whose delta the Control Plane
    /// settled at plan time and shipped on its own `ApplyBalanceDelta` task.
    /// This write must not apply them as well.
    ///
    /// It travels this far because the CALVIN apply path runs through here:
    /// `execute_calvin_flush` replays every staged plan via
    /// `execute_transaction_batch`, which routes `PointInsert` into this
    /// helper. A cross-shard statement is the only kind that ever carries a
    /// deferral AND the only kind that commits through Calvin, so dropping it
    /// here dropped it on every write that has one — the source core folded
    /// the balance inline and the sibling task folded it again.
    ///
    /// Empty for a PUT: `PointPut` carries no deferral list, because its
    /// balance is settled from row images and deferred by OMISSION from
    /// `resolved_sum_targets` above, which this struct already forwards.
    pub deferred_sum_targets: &'a [String],
}

impl CoreLoop {
    /// Execute a PointPut within a transaction.
    pub(in crate::data::executor::handlers::transaction) fn tx_point_put(
        &mut self,
        p: TxPointPut<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxPointPut {
            task: dummy_task,
            tid,
            collection,
            document_id,
            surrogate,
            value,
            user_roles,
            insert_if_absent,
            resolved_sum_targets,
            deferred_sum_targets,
        } = p;
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);
        let database_id = dummy_task.request.database_id.as_u64();

        // Pre-read the plain-table value: it decides insert-vs-update for the
        // hash chain, and it is the PRE-IMAGE the enforcement funnel folds — an
        // enforcement that only sees the post-image cannot tell an update from
        // an insert, which is how a running total came to double-count one.
        // The authoritative prior value for the undo entry comes from
        // `apply_point_put`'s outcome, which is bitemporal-aware.
        //
        // Read only when something needs it: a collection that declares neither
        // a chain nor an image-folding constraint must not pay for a read whose
        // result nothing consults.
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets,
            wal_lsn: dummy_task.wal_lsn(),
        };
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        let folds_images = write_hook::folds_images(self, &hook_ctx);
        let prior_bytes = if chain.enabled() || folds_images {
            self.sparse
                .get(database_id, tid, collection, &storage_key)
                .ok()
                .flatten()
        } else {
            None
        };
        let is_insert = prior_bytes.is_none();

        // Hash-chain wraps the document with a `_chain_hash` field on insert;
        // feed that wrapped value into `apply_point_put` so it stores/indexes
        // the chained form.
        let chained: Option<Vec<u8>> = if is_insert {
            chain
                .chain_insert(self, database_id, tid, document_id, value)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("hash chain: {e}"),
                })?
        } else {
            None
        };
        let effective_value: &[u8] = chained.as_deref().unwrap_or(value);

        // Each transaction sub-plan owns its own per-row redb write txn; the
        // batch is stitched together by the undo log, not one big txn.
        let txn = self.sparse.begin_write().map_err(|e| ErrorCode::Internal {
            detail: e.to_string(),
        })?;

        // INSERT semantics: probe for an existing primary key under the SAME
        // write txn we will commit through — linearizable with the write, so no
        // concurrent writer can slip a row in between the probe and the commit.
        // Mirrors autocommit `execute_point_insert`. PUT/upsert (`None`) skips
        // this entirely and keeps overwrite behaviour.
        if let Some(if_absent) = insert_if_absent {
            let exists_result = if self.is_bitemporal(database_id, tid, collection) {
                self.sparse.versioned_exists_current_in_txn(
                    &txn,
                    database_id,
                    tid,
                    collection,
                    &storage_key,
                )
            } else {
                self.sparse
                    .exists_in_txn(&txn, database_id, tid, collection, &storage_key)
            };
            let exists = match exists_result {
                Ok(exists) => exists,
                Err(e) => {
                    // Restore any chain-head pre-image mutated above before bailing.
                    chain.restore(self);
                    return Err(ErrorCode::from(e));
                }
            };
            if exists {
                // No write, no undo push — drop the txn without committing.
                chain.restore(self);
                if if_absent {
                    // `INSERT ... ON CONFLICT DO NOTHING`: silent skip, which
                    // affected NO row. The count is reported, not omitted —
                    // see the count contract at the end of this function.
                    return Ok(self.response_affected(dummy_task, 0));
                }
                return Err(ErrorCode::from(crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "duplicate key value '{document_id}' violates primary-key \
                         uniqueness on '{collection}'"
                    ),
                }));
            }
        }

        // Core write path shared with the autocommit callers: bitemporal-vs-plain
        // primary doc write, FTS/inverted, doc_cache, aggregate-cache
        // invalidation, UNIQUE enforcement, generated columns, stateless PUT
        // enforcement, and the side indexes (secondary/spatial/vector/stats).
        // Every side-effect is captured in the outcome and reversed via the undo
        // log below, so the transactional write is identical to autocommit and
        // fully rollback-safe.
        let outcome = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                storage_key,
                surrogate,
                value: effective_value,
                index_text: true,
                user_roles,
                enforce: true,
                wal_lsn: dummy_task.wal_lsn(),
                resolved_targets: resolved_sum_targets,
            },
        ) {
            Ok(o) => o,
            Err(e) => {
                // `apply_point_put` rejected the write (e.g. UNIQUE violation)
                // after we mutated the chain head and, on the later rejections,
                // after it had already cached the row. Reverse both so the
                // aborted op leaves no trace, then propagate the typed error.
                chain_guard::abort_after_apply(
                    self,
                    &chain,
                    database_id,
                    tid,
                    collection,
                    &storage_key,
                );
                return Err(e.into());
            }
        };

        // Persist the advanced chain head inside the SAME write transaction the
        // chained row lands in. Every abort path above returns before this point
        // and drops `txn` uncommitted, so a rejected insert never leaves a head
        // behind on disk either.
        if let Err(e) = chain.persist_head(self, &txn) {
            chain_guard::abort_after_apply(
                self,
                &chain,
                database_id,
                tid,
                collection,
                &storage_key,
            );
            return Err(ErrorCode::from(e));
        }

        // Write-path enforcement runs one level ABOVE `apply_point_put`, and
        // inside THIS transaction: a materialized-sum target write is itself an
        // `apply_point_put`, so every derived write lands or rolls back with the
        // row that caused it. On failure the chain-head pre-image is restored
        // and `txn` is dropped uncommitted, leaving neither the row nor any
        // target it credited behind.
        //
        // The post-image is the SUBMITTED body, not the chained one:
        // `_chain_hash` is a wrapper the hash chain adds around the row, and no
        // constraint is declared over it.
        let images = match prior_bytes {
            Some(ref old) => WriteImages::Update {
                old: ImageBody::Stored(old),
                new: ImageBody::Submitted(value),
            },
            None => WriteImages::Insert {
                new: ImageBody::Submitted(value),
            },
        };
        let enforcement = match write_hook::run(self, &txn, &hook_ctx, images) {
            Ok(outcome) => outcome,
            Err(e) => {
                chain_guard::abort_after_apply(
                    self,
                    &chain,
                    database_id,
                    tid,
                    collection,
                    &storage_key,
                );
                return Err(ErrorCode::from(e));
            }
        };
        let WriteEnforcementOutcome {
            target_writes,
            balanced_entries,
        } = enforcement;

        // The BALANCED check spans the whole transaction — debits and credits
        // arrive on different rows — so this row's signed contributions are
        // accumulated onto the open batch, which judges them all at its commit
        // boundary. Nothing is checked here: one leg per statement is legal
        // inside an explicit transaction.
        if let Err(e) = self.settle_balanced_entries(database_id, tid, collection, balanced_entries)
        {
            chain_guard::abort_after_apply(
                self,
                &chain,
                database_id,
                tid,
                collection,
                &storage_key,
            );
            return Err(ErrorCode::from(e));
        }

        txn.commit().map_err(|e| ErrorCode::Internal {
            detail: format!("commit: {e}"),
        })?;
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Reverse every derived materialized-sum target write with the SAME set
        // of undo entries the source row uses: the target write is a full
        // document write, so it has index, vector, spatial and stats
        // side-effects of its own to reverse.
        for target in target_writes {
            undo_log.push(UndoEntry::PutDocument {
                collection: target.collection,
                document_id: nodedb_types::StorageKey::for_surrogate(target.surrogate),
                old_value: target.outcome.prior_value,
                bitemporal_sys_from_ms: target.outcome.bitemporal_sys_from_ms,
                bitemporal_index_tuples: target.outcome.bitemporal_index_tuples,
                secondary_index_added: target.outcome.secondary_index_added,
                secondary_index_removed: target.outcome.secondary_index_removed,
                chain_hash_prior: None,
            });
            for delta in target.outcome.vector_inserts {
                undo_log.push(UndoEntry::InsertVector {
                    index_key: delta.index_key,
                    vector_id: delta.vector_id,
                    collection: delta.collection,
                    field: delta.field,
                    doc_id: delta.doc_id,
                });
            }
            for (key, entry_id) in target.outcome.spatial_inserts {
                undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
            }
            for (key, prior) in target.outcome.stats_prior {
                undo_log.push(UndoEntry::StatsRestore { key, prior });
            }
        }

        undo_log.push(UndoEntry::PutDocument {
            collection: collection.to_string(),
            document_id: storage_key,
            old_value: outcome.prior_value,
            bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
            bitemporal_index_tuples: outcome.bitemporal_index_tuples,
            // Plain secondary-index entries this put added/removed; reversed on
            // rollback so the index returns to its pre-tx state.
            secondary_index_added: outcome.secondary_index_added,
            secondary_index_removed: outcome.secondary_index_removed,
            chain_hash_prior: chain.prior(),
        });

        // Reverse any HNSW vector inserts on rollback (one `InsertVector` undo
        // per vector this put added to a per-field index).
        for delta in outcome.vector_inserts {
            undo_log.push(UndoEntry::InsertVector {
                index_key: delta.index_key,
                vector_id: delta.vector_id,
                collection: delta.collection,
                field: delta.field,
                doc_id: delta.doc_id,
            });
        }

        // Reverse any spatial R-tree inserts on rollback (one `SpatialInsert`
        // undo per per-field R-tree entry this put added).
        for (key, entry_id) in outcome.spatial_inserts {
            undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
        }

        // Reverse the column-stats read-modify-write on rollback by restoring
        // each captured pre-image.
        for (key, prior) in outcome.stats_prior {
            undo_log.push(UndoEntry::StatsRestore { key, prior });
        }

        // One row was written, and the count is REPORTED — `PointPut` and
        // `PointInsert` both render an `INSERT <n>` command tag, so their
        // response must carry the count `response_affected` documents as
        // mandatory for every count-bearing plan. The autocommit handlers
        // (`handlers/point/put.rs`, `handlers/point/insert.rs`) already do.
        //
        // This path is not only the explicit-transaction commit, where the tag
        // is `COMMIT` and the count is discarded: `execute_calvin_flush` replays
        // a participant's staged plans through `execute_transaction_batch`,
        // which returns the LAST sub-plan's payload as the whole participant's
        // applied response — and that response is what the scheduler deposits
        // and what the coordinator shapes a cross-shard statement's tag from.
        // Returning a bare `Ok` here left an autocommit cross-shard INSERT with
        // no count to render at all.
        Ok(self.response_affected(dummy_task, 1))
    }
}
