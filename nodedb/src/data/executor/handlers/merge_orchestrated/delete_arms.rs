// SPDX-License-Identifier: BUSL-1.1

//! MERGE deletions participate in the statement's source transaction and undo.

use redb::WriteTransaction;

use crate::bridge::envelope::{Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::removed_row_image;
use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};

use super::abort::MergeAbort;
use super::apply_support::{MergeEvent, gate_merge_row, retain_target_writes};
use super::plan::MergeDelete;

pub(super) struct MergeDeleteArms<'a> {
    pub(super) task: &'a ExecutionTask,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    pub(super) deletes: &'a [MergeDelete],
    pub(super) returning: bool,
    pub(super) resolved_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
    pub(super) declared_primary_key: Option<&'a str>,
    pub(super) rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

pub(super) struct MergeDeleteTally<'a> {
    pub(super) affected: &'a mut u64,
    pub(super) applied_keys: &'a mut Vec<(String, StorageKey)>,
    pub(super) undo_log: &'a mut Vec<UndoEntry>,
    pub(super) events: &'a mut Vec<MergeEvent>,
    pub(super) write_set: &'a mut Vec<WriteSetEntry>,
    pub(super) balanced_entries: &'a mut Vec<BalancedEntry>,
    pub(super) returned_docs: &'a mut Vec<nodedb_types::Value>,
}

impl CoreLoop {
    /// Apply actual deletions without committing or publishing statement effects.
    pub(super) fn apply_merge_delete_arms(
        &mut self,
        txn: &WriteTransaction,
        arms: MergeDeleteArms<'_>,
        tally: MergeDeleteTally<'_>,
    ) -> Result<(), Response> {
        let MergeDeleteArms {
            task,
            database_id,
            tid,
            collection,
            deletes,
            returning,
            resolved_targets,
            declared_primary_key,
            rls_write_check,
        } = arms;
        let MergeDeleteTally {
            affected,
            applied_keys,
            undo_log,
            events,
            write_set,
            balanced_entries,
            returned_docs,
        } = tally;
        let identity_column = self.identity_column(database_id, tid, collection);
        for del in deletes {
            let surrogate = del.key.surrogate();
            let identity = RowIdentity::of_stored_row(&del.body, declared_primary_key, del.key);
            let source_position = applied_keys.len();
            applied_keys.push((collection.to_owned(), del.key));
            let mut outcome = match self.apply_point_delete(
                txn,
                PointDeleteParams {
                    database_id,
                    tid,
                    collection,
                    document_id: identity.as_str(),
                    surrogate,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    resolved_targets,
                },
            ) {
                Ok(outcome) => outcome,
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
            let Some(prior) = outcome.prior_value.as_deref() else {
                self.doc_cache
                    .invalidate(database_id, tid, collection, &del.key);
                let undone = self.undo_memory_effects(
                    database_id,
                    tid,
                    std::mem::take(&mut outcome.memory_undo),
                );
                applied_keys.truncate(source_position);
                if let Err(error) = undone {
                    return Err(self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        applied_keys,
                        undo_log: std::mem::take(undo_log),
                        err: error.into(),
                    }));
                }
                continue;
            };
            undo_log.append(&mut outcome.memory_undo);
            let prepared = (|| -> crate::Result<_> {
                let schema = self.strict_schema_for(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                );
                gate_merge_row(
                    self,
                    &crate::data::executor::core_loop::redo_image::WriteSetRow {
                        database_id,
                        tid,
                        collection,
                        identity: identity.clone(),
                    },
                    rls_write_check,
                    prior,
                    schema.as_ref(),
                )?;
                let ordinal = outcome
                    .version_sys_from
                    .ok_or_else(|| crate::Error::Storage {
                        engine: "sparse".into(),
                        detail: format!(
                            "MERGE removed row {} without a tombstone ordinal",
                            identity.as_str()
                        ),
                    })?;
                let old_value = self
                    .stored_event_image(database_id, tid, collection, identity.as_str(), prior)
                    .map_err(|_| {
                        crate::data::executor::strict_format::undecodable_strict_row(
                            collection,
                            identity.as_str(),
                        )
                    })?;
                let row = if returning {
                    let schema = self.strict_schema_for(
                        task.request.database_id,
                        crate::types::TenantId::new(tid),
                        collection,
                    );
                    Some(crate::data::executor::handlers::returning_doc::from_stored(
                        prior,
                        &identity,
                        schema.as_ref(),
                        &identity_column,
                    )?)
                } else {
                    None
                };
                Ok((ordinal, old_value, row))
            })();
            let (ordinal, old_value, returned) = match prepared {
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
            write_set.push(removed_row_image(
                surrogate.as_u32(),
                identity.clone(),
                ordinal,
            ));
            events.push(MergeEvent::Delete {
                collection: collection.to_owned(),
                identity,
                old_value,
            });
            let enforcement = match write_hook::run(
                self,
                txn,
                &write_hook::HookCtx {
                    database_id,
                    tid,
                    collection,
                    resolved_targets,
                    deferred_sum_targets: &[],
                    wal_lsn: task.wal_lsn(),
                    target_unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                },
                write_hook::WriteImages::Delete {
                    old: write_hook::ImageBody::Stored(prior),
                },
            ) {
                Ok(enforcement) => enforcement,
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
            retain_target_writes(
                enforcement.target_writes,
                undo_log,
                applied_keys,
                events,
                write_set,
            );
            balanced_entries.extend(enforcement.balanced_entries);
            if let Some(row) = returned {
                returned_docs.push(row);
            }
            *affected += 1;
        }
        Ok(())
    }
}
