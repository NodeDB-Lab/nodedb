// SPDX-License-Identifier: BUSL-1.1

//! What a committed record's install owes once its arms wrote all of it:
//! the write set capture, the applied note, the settle, the write notes
//! and the events. The whole-record install and the batched install end
//! here alike, so both leave the same state after their arms.
//!
//! The notes, the settle and the collection notes run before the first
//! event is sent. The events then stream from the spools. A spool that
//! does not read back stops the stream there: the events before it reached
//! the Event Plane, or under a root journal the journal's held events, and
//! the core fail-stops. Under a root journal the held events leave only
//! once the task succeeds.

use std::collections::{BTreeSet, HashSet};

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_note::WriteStamp;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::handlers::spill::run::BlockReader;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::install_counts::TsInstallCounts;
use crate::types::{Lsn, TenantId};
use crate::wal::RedoStreamId;

use super::super::events::{CommittedWrites, EventRows};
use super::super::state::{RedoApplyScope, TsInstalled};
use super::rollback::BatchedRollback;
use super::state::FoldedThrough;
use super::undo::SpillableUndo;

/// The collections whose rows the arms noted by key. The whole-record
/// install borrows their names from the record, the batched install holds
/// the set its validate built.
pub(in crate::data::executor::handlers::transaction::redo_apply) trait KeyedCollections {
    fn noted_by_key(&self, collection: &str) -> bool;
}

impl KeyedCollections for BTreeSet<String> {
    fn noted_by_key(&self, collection: &str) -> bool {
        self.contains(collection)
    }
}

impl KeyedCollections for HashSet<&str> {
    fn noted_by_key(&self, collection: &str) -> bool {
        self.contains(collection)
    }
}

/// What the end of an install reads besides its scope.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct InstallFinish<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub lsn: Lsn,
    /// The stamp every write of the record took.
    pub stamp: WriteStamp,
    /// The ordinal every KV write of the record stamped. A rollback stamps
    /// it too.
    pub apply_ordinal: Option<i64>,
    pub permit: Option<RootJournalPermit>,
    /// Every collection the record writes.
    pub collections: &'a [String],
    /// The collections whose rows the arms noted by key.
    pub keyed: &'a dyn KeyedCollections,
    /// Reads what the scope's spools spilled.
    pub reader: &'a BlockReader,
    /// What a batched install ends with. `None` for a whole-record install.
    pub batched: Option<BatchedFinish>,
}

/// What the end of a batched install reads besides the scope.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct BatchedFinish {
    /// The install's undo log. A whole-record install keeps its own in the
    /// scope.
    pub undo: SpillableUndo,
    /// The stream the record's marker names.
    pub stream: RedoStreamId,
    /// The fold ledger each durable document batch left.
    pub folded: FoldedThrough,
}

impl CoreLoop {
    /// End the install of the record whose arms filled `scope`. When the
    /// record stored resolved timeseries batches, the response payload
    /// carries their [`TsInstallCounts`] and `ts_installs` receives what
    /// each stored.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn finish_redo_install(
        &mut self,
        finish: InstallFinish<'_>,
        mut scope: RedoApplyScope,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let InstallFinish {
            task,
            tid,
            lsn,
            stamp,
            apply_ordinal,
            permit,
            collections,
            keyed,
            reader,
            mut batched,
        } = finish;
        // A batched install under a root journal wrote the fold ledger and
        // target rows of the record. Its write set is the target rows it
        // stored, and the commit that stores the write set removes them.
        let ledgered: Option<u64> = scope.ledger.take().map(|ledger| ledger.lsn);
        // The commit that ends a batched install removes its cursor with
        // its fold ledger and target rows.
        let drop_installs: Vec<u64> = batched.as_ref().map(|_| lsn.as_u64()).into_iter().collect();
        // A crash test kills the process here: the install folded its sums
        // into deferred store commits, and its write set is not stored. Each
        // collection the record writes has its own point, taken in name
        // order.
        #[cfg(feature = "failpoints")]
        {
            let mut names: Vec<&String> = collections.iter().collect();
            names.sort();
            for collection in names {
                crate::fail_point!(
                    self.fail_scope,
                    &format!("redo_apply::after_install_pass::{collection}")
                );
            }
        }
        if batched.is_some() {
            // A crash test kills the process here: every batch is durable,
            // and the write set is not stored.
            crate::fail_point!(self.fail_scope, "redo_apply::batched::before_final_capture");
        }
        let write_set = match permit {
            Some(permit) => {
                let prepared = self
                    .install_write_set(&mut scope, ledgered)
                    .and_then(|write_set| {
                        self.prepare_redo_capture(task, permit, &write_set)
                            .map(|capture| (capture, write_set))
                    });
                let (capture, write_set) = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let kv_scope = self.kv_engine.open_write_scope(apply_ordinal);
                        let detail = error.to_string();
                        let database_id = task.request.database_id.as_u64();
                        let refusal = match batched.take() {
                            Some(BatchedFinish {
                                undo,
                                stream,
                                folded,
                            }) => self.roll_back_batched(
                                BatchedRollback {
                                    database_id,
                                    tid,
                                    undo,
                                    lsn: lsn.as_u64(),
                                    stream,
                                    label_mark_before: scope.label_mark_before,
                                    folded,
                                },
                                ErrorCode::from(error),
                            ),
                            None => self.roll_back_redo(
                                database_id,
                                tid,
                                std::mem::take(&mut scope.undo),
                                ErrorCode::from(error),
                                scope.label_mark_before,
                            ),
                        };
                        self.kv_engine.close_write_scope(kv_scope);
                        let error = crate::Error::Storage {
                            engine: "journal".into(),
                            detail,
                        };
                        let code = refusal.into_code();
                        self.fail_stop_on_rollback_code(&code);
                        self.fail_journalled(&error);
                        return self.response_error(task, code);
                    }
                };
                if let Err(error) = self.persist_redo_capture(capture, &write_set, &drop_installs) {
                    let mut response = self.response_error(task, error);
                    response.write_set = write_set;
                    return response;
                }
                write_set
            }
            // With no root journal the record stores no write set. A batched
            // install folds only under a root journal, so it wrote no fold
            // ledger row.
            None => {
                let write_set = std::mem::take(&mut scope.target_set);
                // Every commit of the install is durable already. The
                // cursor goes before the record counts as applied.
                if batched.is_some()
                    && let Err(error) = self.end_install_durable(lsn.as_u64())
                {
                    self.fail_install_progress(lsn.as_u64(), &error);
                    let mut response = self.response_error(task, error);
                    response.write_set = write_set;
                    return response;
                }
                write_set
            }
        };
        // The install applied the record: every flush the settle runs, and
        // every later checkpoint, names it. No artifact written before this
        // point names it whole: its stamp prefix is an outcome floor the
        // record was above, since the record's dispatcher window held the
        // floor below it until this apply answers, and its applied ranges
        // name only records applied before. A batched install's flush
        // between batches names the ops it holds as a partial record. So
        // restart replay applies the rest of it, and nothing written earlier
        // needs publishing again.
        self.floors.applied_prefix.note_applied(lsn);
        // The settle cannot be rolled back once it started, so a failure
        // leaves live state restart replay does not rebuild: the core
        // fail-stops. The funnel keeps the record for restart replay.
        if let Some(batched) = batched.as_mut() {
            self.finalize_vector_truncates(batched.undo.ram_entries_mut());
        }
        if let Err(error) = self.settle_redo_install(task, &mut scope) {
            return self.fail_after_install(task, lsn, error, write_set);
        }
        #[cfg(test)]
        {
            self.redo_apply.last_held = super::super::state::HeldReport {
                peak_hot_bytes: scope.held.peak_hot_bytes(),
                spilled_entries: scope.held.spilled_entries(),
            };
        }
        let held = std::mem::take(&mut scope.held);
        // Write notes and the watermark move only once the record is
        // settled.
        if let Err(error) = self.publish_collapsed_notes(held.notes) {
            return self.fail_after_install(task, lsn, error, write_set);
        }
        let tenant = TenantId::new(tid);
        // The arms noted every document and KV row by key, which folded
        // those collections into the newest-ordinal cache. Every other
        // collection's note forgets its cached newest ordinal.
        for collection in collections {
            if keyed.noted_by_key(collection) {
                self.publish_applied(stamp);
            } else if let Err(error) =
                self.note_settled_collection(task.request.database_id, tenant, collection, stamp)
            {
                return self.fail_after_install(task, lsn, error, write_set);
            }
        }
        // Edge writes count only once the record settled: a rolled-back
        // install applied none.
        self.publish_edge_writes(scope.edge_writes);
        // Events leave only once the record is settled.
        let rows = EventRows {
            rows: &held.row_sources,
            reader,
        };
        let sent = self
            .send_held_events(task, held.pending_events, &rows, lsn)
            .and_then(|()| {
                self.emit_committed_redo_events(
                    task,
                    CommittedWrites {
                        doc_writes: held.doc_writes,
                        kv_priors: held.kv_priors,
                        labels: held.labels,
                    },
                    &rows,
                )
            })
            .and_then(|()| self.emit_committed_publishes(task, held.publishes));
        // A spool that does not read back stops the events after it. The
        // core fail-stops, as after any failure past the settle.
        if let Err(error) = sent {
            return self.fail_after_install(task, lsn, error, write_set);
        }

        let mut response = self.response_ok(task);
        response.write_set = write_set;
        // The writer reports each resolved batch's apply count, not its
        // resolve's: the install rejected the rows that conflict with the
        // schema at this record's position.
        let counts = TsInstallCounts::new(
            scope
                .ts_installs
                .iter()
                .map(|installed| installed.count.clone())
                .collect(),
        );
        if !counts.is_empty() {
            match counts.to_bytes() {
                Ok(bytes) => response.payload = crate::bridge::envelope::Payload::from_vec(bytes),
                Err(error) => response.error_code = Some(Box::new(ErrorCode::from(error))),
            }
        }
        ts_installs.append(&mut scope.ts_installs);
        response
    }
}

impl CoreLoop {
    /// The write set the install answers and stores: under a fold ledger
    /// the fold target rows its batches stored at `ledgered`, in write
    /// order, else the ones the scope holds.
    fn install_write_set(
        &self,
        scope: &mut RedoApplyScope,
        ledgered: Option<u64>,
    ) -> crate::Result<Vec<WriteSetEntry>> {
        match ledgered {
            Some(lsn) => self.stored_install_targets(lsn),
            None => Ok(std::mem::take(&mut scope.target_set)),
        }
    }
}
