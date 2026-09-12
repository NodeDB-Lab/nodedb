// SPDX-License-Identifier: BUSL-1.1

//! `execute_merge_apply`: verify the resolve→apply prediction, run the
//! UPDATE and INSERT arms inside one shared write transaction, commit, then
//! run the DELETE arms and build the response.

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::merge::MergeParams;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::response_codec::encode_json_as_msgpack;
use crate::data::executor::task::ExecutionTask;

use super::super::abort::MergeAbort;
use super::super::apply_support::{MergePutEvent, gate_merge_arms};
use super::super::delete_arms::{MergeDeleteArms, MergeDeleteTally};
use super::insert_rows::{InsertRowsCtx, InsertRowsTally};
use super::update_rows::{UpdateRowsCtx, UpdateRowsTally};

impl CoreLoop {
    /// APPLY pass: verify the resolve→apply prediction, then atomically apply.
    pub(in crate::data::executor) fn execute_merge_apply(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        let resolved = match params.resolved_inserts {
            Some(r) => r,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "merge apply invoked without resolved inserts".into(),
                    },
                );
            }
        };
        let database_id = task.request.database_id.as_u64();

        let plan = match self.collect_merge_plan(database_id, tid, task.request.txn_id, &params) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };

        // TOCTOU verification: the recomputed NOT-MATCHED insert-key set must
        // still equal the orchestrator's predicted set. Any drift (a target row
        // for a predicted-insert key appeared, or a matched row vanished) means
        // the pre-assigned surrogates no longer describe the merge — return
        // OllpRetryRequired WITHOUT writing so the orchestrator re-resolves.
        let mut actual_keys: Vec<&str> = plan.inserts.iter().map(|i| i.join_key.as_str()).collect();
        actual_keys.sort_unstable();
        let mut predicted_keys: Vec<&str> = resolved.iter().map(|(k, _)| k.as_str()).collect();
        predicted_keys.sort_unstable();
        if actual_keys != predicted_keys {
            return self.response_error(task, ErrorCode::OllpRetryRequired);
        }
        let surrogate_for: HashMap<&str, u32> =
            resolved.iter().map(|(k, s)| (k.as_str(), *s)).collect();

        // Whether the target maintains a secondary vector index. Gated ONCE here
        // (the schemaless half scans `vector_params` unindexed) and threaded into
        // the per-row UPDATE re-index below.
        let has_vectors = self.collection_has_vectors(database_id, tid, params.target_collection);

        // Gate every arm on the target's write policy BEFORE the apply
        // transaction opens, so a rejected row leaves nothing written and
        // nothing to unwind.
        if let Err(e) =
            gate_merge_arms(&plan, params.rls_write_check, tid, params.target_collection)
        {
            return self.response_error(task, e);
        }

        // One post-apply redo entry per indexed row — a `Put` for each
        // UPDATE/INSERT post-image, a `Delete` for each removed row — carried
        // back so the Control Plane mints the durable WAL redo the vector index
        // needs to survive a WAL-only restart. Empty on non-vector targets.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();

        // The whole MERGE is ONE boundary, so its DELETE arms are accounted
        // here, before any phase runs: those arms apply in their own
        // transactions AFTER the phase-A commit, so entries collected as they
        // ran could only report a violation phase A had already made durable.
        // Their pre-images are the plan's captured bodies, which the classifier
        // already holds — nothing is re-read.
        let delete_bodies: Vec<&[u8]> = plan.deletes.iter().map(|d| d.body.as_slice()).collect();
        let mut balanced_entries = self.balanced_entries_for_submitted_deletes(
            database_id,
            tid,
            params.target_collection,
            &delete_bodies,
        );

        // Phase A: matched UPDATE + NOT-MATCHED INSERT share ONE redb write
        // transaction. Any per-row error (including a UNIQUE violation from
        // `apply_point_put`) aborts, dropping the txn and rolling the whole set
        // back — the all-or-nothing guarantee the atomicity test pins.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, e),
        };
        // Captured for post-commit event emission. The clone into `write_set`
        // below is the only owned body copy actually needed, since `plan`
        // doesn't outlive the function but does outlive this loop.
        let mut put_events: Vec<MergePutEvent<'_>> = Vec::new();
        let mut affected = 0u64;
        // Every row key written into `txn`, pushed BEFORE the write so a row that
        // fails mid-apply (its cache entry is populated before the UNIQUE check)
        // is evicted on abort too — see `rollback_merge_cache`.
        let mut applied_keys: Vec<String> = Vec::new();
        // In-memory (HNSW + R-tree) index deltas applied this pass, reversed on
        // any abort path — the redb txn drop only reverses store-backed state.
        let mut undo_log: Vec<UndoEntry> = Vec::new();
        // RETURNING rows for THIS apply attempt: post-images for the UPDATE and
        // INSERT arms, pre-images for the DELETE arms. Built fresh here rather
        // than carried in, because an attempt that ends in `OllpRetryRequired`
        // is fully re-resolved and re-applied by the orchestrator — rows from a
        // failed attempt describe a snapshot that never committed.
        let mut returned_docs: Vec<serde_json::Value> = Vec::new();

        if let Err(response) = self.apply_merge_update_arm(
            &txn,
            &plan.updates,
            UpdateRowsCtx {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                has_vectors,
                returning: params.returning.is_some(),
                resolved_sum_targets: params.resolved_sum_targets,
                declared_primary_key: params.declared_primary_key,
            },
            UpdateRowsTally {
                affected: &mut affected,
                applied_keys: &mut applied_keys,
                undo_log: &mut undo_log,
                put_events: &mut put_events,
                write_set: &mut write_set,
                balanced_entries: &mut balanced_entries,
                returned_docs: &mut returned_docs,
            },
        ) {
            return response;
        }

        if let Err(response) = self.apply_merge_insert_arm(
            &txn,
            &plan.inserts,
            InsertRowsCtx {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                has_vectors,
                returning: params.returning.is_some(),
                resolved_sum_targets: params.resolved_sum_targets,
                surrogate_for: &surrogate_for,
                declared_primary_key: params.declared_primary_key,
            },
            InsertRowsTally {
                affected: &mut affected,
                applied_keys: &mut applied_keys,
                undo_log: &mut undo_log,
                put_events: &mut put_events,
                write_set: &mut write_set,
                balanced_entries: &mut balanced_entries,
                returned_docs: &mut returned_docs,
            },
        ) {
            return response;
        }

        // Every arm of the statement — the UPDATE and INSERT arms folded above
        // and the DELETE arms accounted before phase A — is judged once here,
        // before the phase-A commit, so a MERGE that leaves a journal group
        // unbalanced writes nothing at all.
        if let Err(e) = self.settle_balanced_entries(
            database_id,
            tid,
            params.target_collection,
            balanced_entries,
        ) {
            return self.abort_merge_apply(MergeAbort {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                applied_keys: &applied_keys,
                undo_log,
                err: e.into(),
            });
        }

        if let Err(e) = txn.commit() {
            return self.abort_merge_apply(MergeAbort {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                applied_keys: &applied_keys,
                undo_log,
                err: ErrorCode::Internal {
                    detail: format!("merge apply commit: {e}"),
                },
            });
        }
        self.checkpoint_coordinator
            .mark_dirty("sparse", put_events.len());

        for (identity, body, prior) in put_events {
            self.emit_put_event(
                task,
                tid,
                params.target_collection,
                identity,
                body,
                prior.as_deref(),
            );
        }

        // Phase B: DELETE arms, applied after the put commit because their
        // cascade opens its own transactions.
        if let Err(response) = self.apply_merge_delete_arms(
            MergeDeleteArms {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                deletes: &plan.deletes,
                has_vectors,
                returning: params.returning.is_some(),
                resolved_targets: params.resolved_sum_targets,
                declared_primary_key: params.declared_primary_key,
            },
            MergeDeleteTally {
                affected: &mut affected,
                write_set: &mut write_set,
                returned_docs: &mut returned_docs,
            },
        ) {
            return response;
        }

        let mut response = if let Some(spec) = params.returning {
            match returning_rows::build_rows_payload(spec, params.rls_filters, &returned_docs) {
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
            match encode_json_as_msgpack(&result) {
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
