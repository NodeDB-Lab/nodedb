// SPDX-License-Identifier: BUSL-1.1

//! The MERGE NOT-MATCHED INSERT arm, applied inside the statement transaction
//! shared with the UPDATE and DELETE arms.

use std::collections::HashMap;

use redb::WriteTransaction;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::WriteSetRow;
use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::enforcement::chain_guard::ChainGuard;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};
use nodedb_types::Surrogate;

use super::super::abort::MergeAbort;
use super::super::apply_support::{
    MergeEvent, gate_merge_row, record_put_index_undo, retain_target_writes, returning_doc,
};
use super::super::plan::MergeInsert;

/// Read-only context the INSERT arm needs from the shared apply pass.
pub(super) struct InsertRowsCtx<'a> {
    pub(super) task: &'a ExecutionTask,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    /// Whether the statement carries a `RETURNING` projection.
    pub(super) returning: bool,
    pub(super) resolved_sum_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
    /// Source join value → Control-Plane-pre-assigned surrogate, verified
    /// against `inserts` by the caller before this arm runs.
    pub(super) surrogate_for: &'a HashMap<&'a str, u32>,
    /// The target's declared `PRIMARY KEY` column, when it has one. Names
    /// each row in its event and redo entry.
    pub(super) declared_primary_key: Option<&'a str>,
    pub(super) rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// Mutable accumulators the INSERT arm folds into. Owned by the caller for
/// the whole apply pass and borrowed here rather than cloned.
pub(super) struct InsertRowsTally<'a> {
    pub(super) affected: &'a mut u64,
    pub(super) applied_keys: &'a mut Vec<(String, crate::engine::document::store::StorageKey)>,
    pub(super) undo_log: &'a mut Vec<UndoEntry>,
    pub(super) events: &'a mut Vec<MergeEvent>,
    pub(super) write_set: &'a mut Vec<WriteSetEntry>,
    pub(super) balanced_entries: &'a mut Vec<BalancedEntry>,
    pub(super) returned_docs: &'a mut Vec<nodedb_types::Value>,
}

impl CoreLoop {
    /// Apply every NOT-MATCHED INSERT arm inside the caller's shared write
    /// transaction. `Err(response)` is the terminating error response the
    /// caller must return as-is — the transaction is left uncommitted for the
    /// caller to abort.
    pub(super) fn apply_merge_insert_arm(
        &mut self,
        txn: &WriteTransaction,
        inserts: &[MergeInsert],
        ctx: InsertRowsCtx<'_>,
        tally: InsertRowsTally<'_>,
    ) -> Result<(), Response> {
        let InsertRowsCtx {
            task,
            database_id,
            tid,
            collection,
            returning,
            resolved_sum_targets,
            surrogate_for,
            declared_primary_key,
            rls_write_check,
        } = ctx;
        let InsertRowsTally {
            affected,
            applied_keys,
            undo_log,
            events,
            write_set,
            balanced_entries,
            returned_docs,
        } = tally;
        let identity_column = self.identity_column(database_id, tid, collection);

        // Every NOT-MATCHED INSERT row of a HASH_CHAIN target is a chain link.
        // The head advances in the shared transaction, and the pre-image goes
        // on the undo log so an aborted MERGE puts it back.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        if let Some(prior) = chain.prior() {
            undo_log.push(UndoEntry::ChainHead {
                collection: collection.to_string(),
                prior,
            });
        }
        for ins in inserts {
            // The verify above proved every insert key has a pre-assigned
            // surrogate; the lookup cannot miss, but a missing entry is treated
            // as drift rather than unwrapped.
            let surrogate = match surrogate_for.get(ins.join_key.as_str()) {
                Some(s) => Surrogate(*s),
                None => {
                    return Err(self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        applied_keys: applied_keys.as_slice(),
                        undo_log: std::mem::take(undo_log),
                        err: ErrorCode::OllpRetryRequired,
                    }));
                }
            };
            let storage_key = StorageKey::for_surrogate(surrogate);
            applied_keys.push((collection.to_owned(), storage_key));
            // The identity INSERT minted for this row, from its MessagePack
            // body: the declared primary key, else the decimal surrogate.
            let row_identity =
                RowIdentity::of_stored_row(&ins.body, declared_primary_key, storage_key);
            if let Err(e) = chain.chain_insert(self, surrogate, &ins.body) {
                chain.restore(self);
                return Err(self.abort_merge_apply(MergeAbort {
                    task,
                    database_id,
                    tid,
                    applied_keys: applied_keys.as_slice(),
                    undo_log: std::mem::take(undo_log),
                    err: e.into(),
                }));
            }
            let prepared = match self.prepare_point_put(
                txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    surrogate,
                    value: &ins.body,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                    wal_lsn: task.wal_lsn(),
                    resolved_targets: resolved_sum_targets,
                    guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
                },
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    chain.restore(self);
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
                chain.restore(self);
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
                    // The head persists with each row, so the next row's link
                    // reads the head this transaction already holds.
                    if let Err(e) = chain
                        .settle_installed(self, surrogate, &mut outcome.installed)
                        .and_then(|()| chain.persist_head(self, txn))
                    {
                        chain.restore(self);
                        return Err(self.abort_merge_apply(MergeAbort {
                            task,
                            database_id,
                            tid,
                            applied_keys: applied_keys.as_slice(),
                            undo_log: std::mem::take(undo_log),
                            err: e.into(),
                        }));
                    }
                    // A NOT-MATCHED INSERT arm credits its target with the whole
                    // new row — post-image only, which is exactly what
                    // `RowImages::Insert` expresses.
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
                            &storage_key,
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
                    chain.restore(self);
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
        if let Err(e) = chain.persist_head(self, txn) {
            chain.restore(self);
            return Err(self.abort_merge_apply(MergeAbort {
                task,
                database_id,
                tid,
                applied_keys: applied_keys.as_slice(),
                undo_log: std::mem::take(undo_log),
                err: e.into(),
            }));
        }
        Ok(())
    }
}
