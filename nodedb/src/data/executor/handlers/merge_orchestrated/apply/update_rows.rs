// SPDX-License-Identifier: BUSL-1.1

//! The MERGE UPDATE arm (matched + not-matched-by-source), applied inside the
//! statement transaction shared with the INSERT and DELETE arms.

use redb::WriteTransaction;

use crate::bridge::envelope::{Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::WriteSetRow;
use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;

use super::super::abort::MergeAbort;
use super::super::apply_support::{
    MergeEvent, gate_merge_row, record_put_index_undo, retain_target_writes, returning_doc,
};
use super::super::plan::MergeUpdate;

/// Read-only context the UPDATE arm needs from the shared apply pass.
pub(super) struct UpdateRowsCtx<'a> {
    pub(super) task: &'a ExecutionTask,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    /// Whether the statement carries a `RETURNING` projection.
    pub(super) returning: bool,
    pub(super) resolved_sum_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
    /// The target's declared `PRIMARY KEY` column, when it has one. Names
    /// each row in its event and redo entry.
    pub(super) declared_primary_key: Option<&'a str>,
    pub(super) rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// Mutable accumulators the UPDATE arm folds into. Owned by the caller for
/// the whole apply pass and borrowed here rather than cloned.
pub(super) struct UpdateRowsTally<'a> {
    pub(super) affected: &'a mut u64,
    pub(super) applied_keys: &'a mut Vec<(String, crate::engine::document::store::StorageKey)>,
    pub(super) undo_log: &'a mut Vec<UndoEntry>,
    pub(super) events: &'a mut Vec<MergeEvent>,
    pub(super) write_set: &'a mut Vec<WriteSetEntry>,
    pub(super) balanced_entries: &'a mut Vec<BalancedEntry>,
    pub(super) returned_docs: &'a mut Vec<nodedb_types::Value>,
}

impl CoreLoop {
    /// Apply every UPDATE arm (matched + not-matched-by-source) inside the
    /// caller's shared write transaction. `Err(response)` is the terminating
    /// error response the caller must return as-is — the transaction is left
    /// uncommitted for the caller to abort.
    pub(super) fn apply_merge_update_arm(
        &mut self,
        txn: &WriteTransaction,
        updates: &[MergeUpdate],
        ctx: UpdateRowsCtx<'_>,
        tally: UpdateRowsTally<'_>,
    ) -> Result<(), Response> {
        let UpdateRowsCtx {
            task,
            database_id,
            tid,
            collection,
            returning,
            resolved_sum_targets,
            declared_primary_key,
            rls_write_check,
        } = ctx;
        let UpdateRowsTally {
            affected,
            applied_keys,
            undo_log,
            events,
            write_set,
            balanced_entries,
            returned_docs,
        } = tally;
        let identity_column = self.identity_column(database_id, tid, collection);

        for upd in updates {
            let surrogate = upd.key.surrogate();
            applied_keys.push((collection.to_owned(), upd.key));
            // The identity INSERT minted for this row, from its MessagePack
            // body: the declared primary key, else the decimal surrogate.
            let row_identity = RowIdentity::of_stored_row(&upd.body, declared_primary_key, upd.key);
            // `apply_point_put` ends the row's prior vector nodes at the new
            // version's ordinal and records their undo before its inserts', so
            // an abort undeletes the old vector after removing the new one.
            let prepared = match self.prepare_point_put(
                txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    storage_key: upd.key,
                    surrogate,
                    value: &upd.body,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                    wal_lsn: task.wal_lsn(),
                    resolved_targets: resolved_sum_targets,
                    guards: crate::data::executor::enforcement::typeguard::GuardScope::Assigned(
                        &upd.assigned,
                    ),
                },
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    return Err(self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        applied_keys,
                        undo_log: std::mem::take(undo_log),
                        err: error.into(),
                    }));
                }
            };
            let image = prepared.installed();
            let schema = match image.schema.as_ref() {
                nodedb_physical::physical_plan::StorageMode::Schemaless => None,
                nodedb_physical::physical_plan::StorageMode::Strict { schema } => Some(schema),
            };
            if let Err(error) = gate_merge_row(
                self,
                &WriteSetRow {
                    database_id,
                    tid,
                    collection,
                    identity: row_identity.clone(),
                },
                rls_write_check,
                &image.stored,
                schema,
            ) {
                return Err(self.abort_merge_apply(MergeAbort {
                    task,
                    database_id,
                    tid,
                    applied_keys,
                    undo_log: std::mem::take(undo_log),
                    err: error.into(),
                }));
            }
            match self.apply_prepared_point_put(txn, prepared) {
                Ok(mut outcome) => {
                    record_put_index_undo(undo_log, &mut outcome);
                    // The arm's materialized-sum delta is folded inside
                    // the SAME transaction the arm's row lands in, so a
                    // moved total rolls back with the row that moved it.
                    // Both images come from the actual write outcome, so repeated
                    // keys observe earlier writes in this transaction.
                    let event = match self.prepare_installed_put_event(
                        &WriteSetRow {
                            database_id,
                            tid,
                            collection,
                            identity: row_identity.clone(),
                        },
                        &outcome.installed,
                        outcome.prior_value.as_deref(),
                    ) {
                        Ok(event) => event,
                        Err(error) => {
                            return Err(self.abort_merge_apply(MergeAbort {
                                task,
                                database_id,
                                tid,
                                applied_keys: applied_keys.as_slice(),
                                undo_log: std::mem::take(undo_log),
                                err: error.into(),
                            }));
                        }
                    };
                    write_set.push(WriteSetEntry::installed_put(
                        surrogate.as_u32(),
                        row_identity.clone(),
                        outcome.installed.clone(),
                        event.clone(),
                    ));
                    events.push(MergeEvent::Put {
                        collection: collection.to_owned(),
                        identity: row_identity.clone(),
                        event,
                    });
                    match write_hook::run(
                        self,
                        txn,
                        &write_hook::HookCtx {
                            database_id,
                            tid,
                            collection,
                            resolved_targets: resolved_sum_targets,
                            deferred_sum_targets: &[],
                            wal_lsn: task.wal_lsn(),
                            target_unique:
                                crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                        },
                        match outcome.prior_value.as_deref() {
                            Some(old) => write_hook::WriteImages::Update {
                                old: write_hook::ImageBody::Stored(old),
                                new: write_hook::ImageBody::Stored(&outcome.installed.stored),
                            },
                            None => write_hook::WriteImages::Insert {
                                new: write_hook::ImageBody::Stored(&outcome.installed.stored),
                            },
                        },
                    ) {
                        Ok(enforcement) => {
                            retain_target_writes(
                                enforcement.target_writes,
                                undo_log,
                                applied_keys,
                                events,
                                write_set,
                            );
                            balanced_entries.extend(enforcement.balanced_entries);
                        }
                        Err(e) => {
                            return Err(self.abort_merge_apply(MergeAbort {
                                task,
                                database_id,
                                tid,
                                applied_keys: applied_keys.as_slice(),
                                undo_log: std::mem::take(undo_log),
                                err: e.into(),
                            }));
                        }
                    }
                    if returning {
                        match returning_doc(
                            &outcome.installed.canonical,
                            &upd.key,
                            &identity_column,
                        ) {
                            Ok(doc) => returned_docs.push(doc),
                            Err(e) => {
                                return Err(self.abort_merge_apply(MergeAbort {
                                    task,
                                    database_id,
                                    tid,
                                    applied_keys: applied_keys.as_slice(),
                                    undo_log: std::mem::take(undo_log),
                                    err: e.into(),
                                }));
                            }
                        }
                    }

                    *affected += 1;
                }
                Err(e) => {
                    return Err(self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        applied_keys: applied_keys.as_slice(),
                        undo_log: std::mem::take(undo_log),
                        err: e.into(),
                    }));
                }
            }
        }
        Ok(())
    }
}
