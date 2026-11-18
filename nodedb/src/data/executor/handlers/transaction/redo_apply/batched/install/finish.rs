// SPDX-License-Identifier: BUSL-1.1

//! What a committed record's install owes once its arms wrote all of it:
//! the write set capture, the applied note, the settle, the write notes
//! and the events. The whole-record install and the batched install end
//! here alike, so both leave the same state after their arms.
//!
//! The capture is prepared first. A whole record whose capture cannot be
//! prepared rolls back, then the journal fails. A batched install rolls
//! nothing back: the journal fails at once, and the next boot completes
//! the record forward from its durable batches.
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

use super::super::super::events::{CommittedWrites, EventRows};
use super::super::super::passes::PassRefusal;
use super::super::super::state::{RedoApplyScope, TsInstalled};
use super::super::rollback::undo::SpillableUndo;

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
    /// Every collection the record writes.
    pub collections: &'a [String],
    /// The collections whose rows the arms noted by key.
    pub keyed: &'a dyn KeyedCollections,
    /// Reads what the scope's spools spilled.
    pub reader: &'a BlockReader,
}

/// What the end of a batched install reads besides the scope.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct BatchedFinish {
    /// The install's undo log. A whole-record install keeps its own in the
    /// scope.
    pub undo: SpillableUndo,
}

/// The write set an install stores, prepared before its settle.
pub(in crate::data::executor::handlers::transaction::redo_apply) enum PreparedCapture {
    /// Under a root journal: the capture, and the write set it holds.
    Journalled {
        capture: (u64, Vec<u8>),
        write_set: Vec<WriteSetEntry>,
    },
    /// No root journal: the record stores no write set.
    Unjournalled,
}

impl CoreLoop {
    /// End the whole-record install whose arms filled `scope`, under the
    /// root journal `permit` names, if any. A capture that cannot be
    /// prepared rolls the record back first.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn finish_whole_install(
        &mut self,
        finish: InstallFinish<'_>,
        permit: Option<RootJournalPermit>,
        mut scope: RedoApplyScope,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let capture = match self.prepare_install_capture(&finish, false, permit, &mut scope) {
            Ok(capture) => capture,
            Err(error) => {
                let detail = error.to_string();
                let kv_scope = self.kv_engine.open_write_scope(finish.apply_ordinal);
                let refusal = self.roll_back_redo(
                    finish.task.request.database_id.as_u64(),
                    finish.tid,
                    std::mem::take(&mut scope.undo),
                    ErrorCode::from(error),
                    scope.label_mark_before,
                );
                self.kv_engine.close_write_scope(kv_scope);
                return self.answer_capture_refusal(finish.task, refusal, detail);
            }
        };
        self.finish_redo_install(finish, capture, None, scope, ts_installs)
    }

    /// Prepare the write set capture of the install `finish` names, under
    /// the root journal `permit` names. `batched` names a batched install.
    /// `Err` when the capture cannot be prepared.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn prepare_install_capture(
        &self,
        finish: &InstallFinish<'_>,
        batched: bool,
        permit: Option<RootJournalPermit>,
        scope: &mut RedoApplyScope,
    ) -> crate::Result<PreparedCapture> {
        // A batched install under a root journal wrote the fold ledger and
        // target rows of the record. Its write set is the target rows it
        // stored, and the commit that stores the write set removes them.
        let ledgered: Option<u64> = scope.ledger.take().map(|ledger| ledger.lsn);
        // A crash test kills the process here: the install folded its sums
        // into deferred store commits, and its write set is not stored. Each
        // collection the record writes has its own point, taken in name
        // order.
        #[cfg(feature = "failpoints")]
        {
            let mut names: Vec<&String> = finish.collections.iter().collect();
            names.sort();
            for collection in names {
                crate::fail_point!(
                    self.fail_scope,
                    &format!("redo_apply::after_install_pass::{collection}")
                );
            }
        }
        if batched {
            // A crash test kills the process here: every batch is durable,
            // and the write set is not stored.
            crate::fail_point!(self.fail_scope, "redo_apply::batched::before_final_capture");
        }
        let Some(permit) = permit else {
            return Ok(PreparedCapture::Unjournalled);
        };
        // A test fails the prepare here: the install's effects are in the
        // stores, and no capture holds them.
        crate::fail_point_err!(
            self.fail_scope,
            "redo_apply::capture_prepare_fails",
            |detail: String| crate::Error::Storage {
                engine: "journal".into(),
                detail,
            }
        );
        let write_set = self.install_write_set(scope, ledgered)?;
        let capture = self.prepare_redo_capture(finish.task, permit, &write_set)?;
        Ok(PreparedCapture::Journalled { capture, write_set })
    }

    /// Answer `task`, whose batched install at `lsn` could not prepare its
    /// capture: `error`. Every batch is durable under the install's cursor,
    /// so nothing rolls back. The journal fails, so no later commit of the
    /// task persists, and the core fail-stops before any other task runs.
    /// The next boot completes the record forward.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn refuse_batched_capture(
        &mut self,
        task: &ExecutionTask,
        lsn: Lsn,
        error: &crate::Error,
    ) -> Response {
        let detail = format!(
            "the batched install of the committed redo at lsn {} could not prepare its write \
             set capture: {error}; its durable batches complete at the next boot",
            lsn.as_u64()
        );
        self.fail_journalled(&crate::Error::Storage {
            engine: "journal".into(),
            detail: detail.clone(),
        });
        self.response_error(task, ErrorCode::Internal { detail })
    }

    /// Answer `task`, whose whole-record install's capture failed to
    /// prepare with `detail`, once `refusal` ended its rollback. The journal
    /// fails, and the core fail-stops.
    fn answer_capture_refusal(
        &mut self,
        task: &ExecutionTask,
        refusal: PassRefusal,
        detail: String,
    ) -> Response {
        let error = crate::Error::Storage {
            engine: "journal".into(),
            detail,
        };
        let code = refusal.into_code();
        self.fail_stop_on_rollback_code(&code);
        self.fail_journalled(&error);
        self.response_error(task, code)
    }

    /// End the install of the record whose arms filled `scope`, with the
    /// write set `capture` prepared. `batched` is what a batched install
    /// ends with. When the record stored resolved timeseries batches, the
    /// response payload carries their [`TsInstallCounts`] and `ts_installs`
    /// receives what each stored.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn finish_redo_install(
        &mut self,
        finish: InstallFinish<'_>,
        capture: PreparedCapture,
        mut batched: Option<BatchedFinish>,
        mut scope: RedoApplyScope,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let InstallFinish {
            task,
            tid,
            lsn,
            stamp,
            apply_ordinal: _,
            collections,
            keyed,
            reader,
        } = finish;
        // The commit that ends a batched install removes its cursor with
        // its fold ledger and target rows.
        let drop_installs: Vec<u64> = batched.as_ref().map(|_| lsn.as_u64()).into_iter().collect();
        let write_set = match capture {
            PreparedCapture::Journalled { capture, write_set } => {
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
            PreparedCapture::Unjournalled => {
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
            self.redo_apply.last_held = super::super::super::state::HeldReport {
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
