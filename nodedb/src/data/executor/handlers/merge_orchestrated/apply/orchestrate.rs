// SPDX-License-Identifier: BUSL-1.1

//! `execute_merge_apply`: verify the resolve→apply prediction, run the
//! UPDATE, INSERT, and DELETE arms inside one shared write transaction.

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::merge::MergeParams;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::response_codec::encode_json_as_msgpack;
use crate::data::executor::task::ExecutionTask;

use super::super::abort::MergeAbort;
use super::super::apply_support::MergeEvent;
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

        // One post-apply redo entry per row the MERGE stores — a `Put` for
        // each UPDATE/INSERT post-image, a `Delete` for each removed row — in
        // the order the rows land. The plan carries no pre-dispatch record, so
        // these entries are the rows' only WAL record.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();

        let mut balanced_entries = Vec::new();
        // Every source and derived row shares the statement transaction.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, e),
        };
        // Event images are prepared before commit and retained for publication.
        let mut events: Vec<MergeEvent> = Vec::new();
        let mut affected = 0u64;
        // Every row key written into `txn`, pushed BEFORE the write so a row that
        // fails mid-apply (its cache entry is populated before the put's later
        // steps) is evicted on abort too — see `rollback_merge_cache`.
        let mut applied_keys: Vec<(String, crate::engine::document::store::StorageKey)> =
            Vec::new();
        // In-memory (HNSW + R-tree) index deltas applied this pass, reversed on
        // any abort path — the redb txn drop only reverses store-backed state.
        let mut undo_log: Vec<UndoEntry> = Vec::new();
        // RETURNING rows for THIS apply attempt: post-images for the UPDATE and
        // INSERT arms, pre-images for the DELETE arms. Built fresh here rather
        // than carried in, because an attempt that ends in `OllpRetryRequired`
        // is fully re-resolved and re-applied by the orchestrator — rows from a
        // failed attempt describe a snapshot that never committed.
        let mut returned_docs: Vec<nodedb_types::Value> = Vec::new();

        if let Err(response) = self.apply_merge_update_arm(
            &txn,
            &plan.updates,
            UpdateRowsCtx {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                returning: params.returning.is_some(),
                resolved_sum_targets: params.resolved_sum_targets,
                declared_primary_key: params.declared_primary_key,
                rls_write_check: params.rls_write_check,
            },
            UpdateRowsTally {
                affected: &mut affected,
                applied_keys: &mut applied_keys,
                undo_log: &mut undo_log,
                events: &mut events,
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
                returning: params.returning.is_some(),
                resolved_sum_targets: params.resolved_sum_targets,
                surrogate_for: &surrogate_for,
                declared_primary_key: params.declared_primary_key,
                rls_write_check: params.rls_write_check,
            },
            InsertRowsTally {
                affected: &mut affected,
                applied_keys: &mut applied_keys,
                undo_log: &mut undo_log,
                events: &mut events,
                write_set: &mut write_set,
                balanced_entries: &mut balanced_entries,
                returned_docs: &mut returned_docs,
            },
        ) {
            return response;
        }

        if let Err(response) = self.apply_merge_delete_arms(
            &txn,
            MergeDeleteArms {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                deletes: &plan.deletes,
                returning: params.returning.is_some(),
                resolved_targets: params.resolved_sum_targets,
                declared_primary_key: params.declared_primary_key,
                rls_write_check: params.rls_write_check,
            },
            MergeDeleteTally {
                affected: &mut affected,
                applied_keys: &mut applied_keys,
                undo_log: &mut undo_log,
                events: &mut events,
                write_set: &mut write_set,
                balanced_entries: &mut balanced_entries,
                returned_docs: &mut returned_docs,
            },
        ) {
            return response;
        }

        let prepared = (|| -> crate::Result<_> {
            self.check_written_unit_unique(&txn, database_id, tid, &applied_keys)?;
            self.settle_balanced_entries(
                database_id,
                tid,
                params.target_collection,
                balanced_entries,
            )?;
            let payload = match params.returning {
                Some(spec) => returning_rows::build_rows_payload(
                    spec,
                    params.rls_filters,
                    &returned_docs,
                    &self.crs_context(),
                )?,
                None => encode_json_as_msgpack(&serde_json::json!({ "affected": affected }))?,
            };
            let mut notes = Vec::with_capacity(write_set.len());
            for entry in &write_set {
                let ordinal = match &entry.effect {
                    crate::bridge::envelope::RowEffect::InstalledPut { image, .. } => {
                        image.version.sys_from
                    }
                    crate::bridge::envelope::RowEffect::Delete { system_from } => *system_from,
                    crate::bridge::envelope::RowEffect::CancelForward
                    | crate::bridge::envelope::RowEffect::Edge(_) => {
                        return Err(crate::Error::Storage {
                            engine: "merge".into(),
                            detail: "MERGE capture contains a non-document effect".into(),
                        });
                    }
                };
                let collection = entry
                    .collection
                    .as_deref()
                    .unwrap_or(params.target_collection);
                notes.push((
                    self.prepare_surrogate_write(task, tid, collection, entry.surrogate)?,
                    ordinal,
                ));
            }
            Ok((payload, notes))
        })();
        let (payload, notes) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                return self.abort_merge_apply(MergeAbort {
                    task,
                    database_id,
                    tid,
                    applied_keys: &applied_keys,
                    undo_log,
                    err: error.into(),
                });
            }
        };

        if let Err(e) = txn.commit() {
            self.seal_journalled();
            self.fail_stop_core(
                crate::data::executor::core_loop::fail_stop::FailStopCause::WriteSetUnpersisted,
                &format!("merge commit outcome is unknown: {e}"),
            );
            return self.abort_merge_apply(MergeAbort {
                task,
                database_id,
                tid,
                applied_keys: &applied_keys,
                undo_log,
                err: ErrorCode::Internal {
                    detail: format!("merge apply commit: {e}"),
                },
            });
        }
        for (note, ordinal) in notes {
            self.publish_surrogate_write(note, ordinal);
        }
        self.checkpoint_coordinator
            .mark_dirty("sparse", write_set.len());
        for event in events {
            event.publish(self, task);
        }
        let mut response = self.response_with_payload(task, payload);
        response.write_set = write_set;
        response
    }
}
