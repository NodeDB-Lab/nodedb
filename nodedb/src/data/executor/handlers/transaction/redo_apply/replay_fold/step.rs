// SPDX-License-Identifier: BUSL-1.1

//! Restart replay of a committed record whose install never became durable.
//!
//! Boot settle hands the core the LSNs of such records (see
//! `bootstrap::write_group_settle::verdict`). The document arm folds each
//! one it owns, through the committed path the install runs
//! (`document::committed_document_put`), with the sum targets the record's
//! WAL copy carries.
//!
//! The record's store commits run deferred, as a whole-record install's
//! did. Once its last document write ran, one durable commit stores the
//! record's write set and persists every deferred commit before it. That write set is the
//! record's durable marker:
//!
//! - A crash before it reverts every write of the record. The next boot
//!   finds no write set and no part, judges the record never durable again,
//!   and folds it once from the same state.
//! - A crash after it leaves the folds and the write set. The next boot
//!   judges the record complete, journals its parts from the write set and
//!   replays the record plainly, folding nothing.

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::core_loop::write_set_journal::InstallRows;
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::wal::{NeverDurableRedos, ReplayedCaptures};

use super::capture::fold_capture;
use super::state::OpenFold;

impl CoreLoop {
    /// Hand the core the records boot settle judged never durable, with the
    /// fold ledger its batched installs left durable. Runs before replay.
    ///
    /// A ledger that cannot be read fail-stops the core: replay without it
    /// folds an op twice.
    pub(crate) fn set_never_durable_redos(
        &mut self,
        never_durable: &NeverDurableRedos,
    ) -> crate::Result<()> {
        let ledger = match self.sparse.load_fold_ledgers() {
            Ok(ledger) => ledger.into_iter().collect(),
            Err(error) => {
                self.fail_stop_core(
                    FailStopCause::StoreUnreadable,
                    &format!("the redo fold ledger cannot be read: {error}"),
                );
                return Err(error);
            }
        };
        self.redo_apply
            .replay_folds
            .set_never_durable(never_durable.clone(), ledger);
        Ok(())
    }

    /// The write sets replay stored for the records it folded. Replay
    /// ended, so the core forgets every fold.
    pub(crate) fn take_replay_captures(&mut self) -> ReplayedCaptures {
        self.redo_apply.replay_folds.finish()
    }

    /// Keep the sum targets of the never-durable records among `targets`,
    /// the targets each committed record of the replayed stream carries.
    pub(crate) fn keep_replay_fold_targets(
        &mut self,
        targets: Vec<(u64, Vec<nodedb_physical::physical_plan::RedoSumTargets>)>,
    ) {
        if self.redo_apply.scope.is_none() {
            self.redo_apply.replay_folds.keep_targets(targets);
        }
    }

    /// Whether the document arm folds op `op` of the record at `lsn`: under
    /// a committed-redo apply, or in restart replay of a never-durable
    /// record whose fold ledger lies at or below `op`.
    pub(in crate::data::executor) fn redo_folds_op(&self, lsn: u64, op: Option<u64>) -> bool {
        self.redo_apply.scope.is_some() || self.redo_apply.replay_folds.folds_op(lsn, op)
    }

    /// The sum targets a restart-replay write of the record at `lsn` to
    /// `collection` folds into.
    pub(in crate::data::executor) fn replay_fold_targets(
        &self,
        lsn: u64,
        collection: &str,
    ) -> Vec<ResolvedSumTarget> {
        self.redo_apply.replay_folds.targets_for(lsn, collection)
    }

    /// Keep the target rows a restart-replay fold wrote, for the record's
    /// write set.
    pub(in crate::data::executor) fn record_replay_fold_targets(
        &mut self,
        targets: Vec<TargetWrite>,
    ) {
        if let Some(open) = self.redo_apply.replay_folds.open.as_mut() {
            open.target_writes.extend(targets);
        }
    }

    /// Move the document arm to `record`. A record past the one replay
    /// folds ends that fold. A never-durable record this core owns opens
    /// its fold. A committed-redo apply folds through its own scope, so
    /// this does nothing under one.
    pub(in crate::data::executor) fn step_replay_fold(
        &mut self,
        record: &WalRecord,
        num_cores: usize,
    ) {
        if self.redo_apply.scope.is_some() || self.redo_apply.replay_folds.is_idle() {
            return;
        }
        let lsn = record.header.lsn;
        if self
            .redo_apply
            .replay_folds
            .open_lsn()
            .is_some_and(|open| open != lsn)
        {
            self.close_replay_fold();
        }
        let owned = (record.header.vshard_id as usize) % num_cores.max(1) == self.core_id;
        let opens = self.redo_apply.replay_folds.open_lsn().is_none()
            && owned
            && !self.replay_halted()
            && RecordType::from_raw(record.logical_record_type())
                == Some(RecordType::TransactionRedo)
            && self.redo_apply.replay_folds.is_never_durable(lsn);
        if opens {
            self.open_replay_fold(record);
        }
    }

    /// End the fold the document arm left open when its pass ends.
    pub(in crate::data::executor) fn end_replay_fold(&mut self) {
        if self.redo_apply.replay_folds.open.is_some() {
            self.close_replay_fold();
        }
    }

    /// Defer the sparse store's durability and stamp the record's ordinal,
    /// as the install's task did.
    fn open_replay_fold(&mut self, record: &WalRecord) {
        let ordinal = record.system_ordinal();
        if let Some(ordinal) = ordinal {
            self.observe_bitemporal_ordinal(ordinal);
        }
        let outer_ordinal = self.apply_scope.replicated_ordinal;
        self.apply_scope.replicated_ordinal = ordinal.or(outer_ordinal);
        self.sparse.db().defer();
        let header = &record.header;
        self.redo_apply.replay_folds.open = Some(OpenFold {
            lsn: header.lsn,
            tenant_id: header.tenant_id,
            vshard_id: header.vshard_id,
            database_id: header.database_id,
            apply_key: header.apply_key,
            event_source: header.event_source,
            commit_hlc: header.commit_hlc,
            payload: record.payload.clone(),
            target_writes: Vec::new(),
            outer_ordinal,
        });
    }

    /// Store the folded record's write set in one durable commit, which
    /// persists every deferred commit of the record with it. The write set
    /// holds the target row writes a batched install of the record stored,
    /// then the ones replay folded. The
    /// same commit removes those rows and the record's fold ledger. The
    /// record's install cursor stays until every arm replayed it. A halted
    /// replay seals the store instead, so no write of the record becomes
    /// durable without its write set.
    fn close_replay_fold(&mut self) {
        let Some(open) = self.redo_apply.replay_folds.open.take() else {
            return;
        };
        self.apply_scope.replicated_ordinal = open.outer_ordinal;
        if self.replay_halted() {
            self.sparse.db().seal();
            return;
        }
        // A crash test stops boot here: the record's writes and folds ran
        // deferred, and its write set is not stored.
        crate::fail_point!(self.fail_scope, "replay_fold::before_write_set");
        let targets = self.redo_apply.replay_folds.record_targets(open.lsn);
        let stored = self
            .stored_install_targets(open.lsn)
            .and_then(|installed| fold_capture(&open, targets, installed))
            .and_then(|capture| {
                let bytes = capture.to_bytes()?;
                self.store_encoded_write_sets(
                    &[(capture.origin, bytes)],
                    InstallRows::Folded(&[open.lsn]),
                )?;
                Ok(capture)
            });
        match stored {
            Ok(capture) => {
                self.sparse.db().resolve_journal();
                self.sparse.db().resume();
                self.redo_apply.replay_folds.captures.push(capture);
            }
            Err(error) => {
                self.sparse.db().seal();
                self.replay_record_unapplied(
                    "document",
                    "replay_fold_write_set",
                    open.lsn,
                    &format!("storing the write set of the never-durable record failed: {error}"),
                );
            }
        }
    }
}
