// SPDX-License-Identifier: BUSL-1.1

//! Core state the committed-redo apply shares with the document redo arm.
//!
//! WAL replay and the committed-redo apply drive the same per-engine replay
//! arms. The apply opens a [`RedoApplyScope`] for each pass over one record:
//! while it is open, the document arm folds materialized sums, links hash
//! chains, and reports every row it wrote back through the scope. With no
//! scope open the arms are plain restart replay.
//!
//! The apply runs the arms twice. The [`RedoApplyPass::Validate`] pass
//! decodes, routes and checks every sub-record and writes nothing. The
//! [`RedoApplyPass::Install`] pass writes, and records the undo entries that
//! reverse each write, so a failure part way through rolls the record back.
//!
//! Restart replay opens no scope. It folds only a record whose install
//! never became durable (see `replay_fold`).

use std::collections::HashMap;

use nodedb_physical::physical_plan::{RedoSumTargets, ResolvedSumTarget};
use nodedb_types::RowIdentity;

use crate::bridge::envelope::{ErrorCode, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_note::{KeyRepr, WriteStamp};
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::event::WriteOp;
use crate::types::document_event::DocumentPutEvent;
use crate::wal::redo::replay_arm::RedoArm;

/// Committed-redo apply state owned by one core.
pub(in crate::data::executor) struct RedoApplyState {
    /// Number of Data Plane cores on this node. The replay arms route a record
    /// to core `vshard_id % num_cores`, so the apply hands them this count.
    pub(in crate::data::executor) num_cores: usize,
    /// Bytes of whole framed records one batch of a stream marker's validate
    /// and install holds.
    pub(in crate::data::executor) stream_batch_bytes: usize,
    /// `Some` only while one committed redo record applies on this core.
    pub(in crate::data::executor) scope: Option<RedoApplyScope>,
    /// Restart replay's folds of the records whose install never became
    /// durable.
    pub(in crate::data::executor) replay_folds: super::replay_fold::ReplayFolds,
    /// The batched install cursors this core's store held at boot. Restart
    /// replay completes each one's record, then boot drops them. Empty
    /// outside boot replay.
    pub(in crate::data::executor) install_cursors: crate::wal::redo::install_cursor::InstallCursors,
    /// The stream install that gave the core back between its slices, with
    /// its task. `tick()` runs one slice of it per tick.
    pub(in crate::data::executor) parked: Option<Box<super::batched::ParkedInstall>>,
    /// The install that parked in the task running now. `run_task` takes
    /// it once the task ran.
    pub(in crate::data::executor) parking: Option<Box<super::batched::PendingPark>>,
    /// The task running now may park its stream install: `run_task` runs
    /// it, and no other install is parked.
    pub(in crate::data::executor) yield_grant: bool,
    /// What the last finished install held, for the tests that bound it.
    #[cfg(test)]
    pub(in crate::data::executor) last_held: HeldReport,
    /// What the last batched rollback reversed, for the tests that bound it.
    #[cfg(test)]
    pub(in crate::data::executor) last_rollback: UndoReport,
    /// The most resident bytes one timeseries memtable of a batched install
    /// held after a batch, before its flush, for the tests that bound it.
    #[cfg(test)]
    pub(in crate::data::executor) peak_ts_memtable_bytes: usize,
}

/// The entries each lane of one batched rollback held.
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy)]
pub(in crate::data::executor) struct UndoReport {
    pub spill_lane: u64,
    /// RAM the spill lane held when the rollback began.
    pub spill_lane_hot_bytes: usize,
    pub ram_lane: usize,
}

/// The hot bytes and the spilled entries of one finished install's held
/// state.
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy)]
pub(in crate::data::executor) struct HeldReport {
    pub peak_hot_bytes: usize,
    pub spilled_entries: u64,
}

impl RedoApplyState {
    /// A single-core default. Every multi-core runtime sets the real count
    /// through `CoreLoop::set_num_cores` before the core serves requests.
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            num_cores: 1,
            stream_batch_bytes: nodedb_types::config::tuning::CalvinTuning::default()
                .redo_stream_batch_bytes,
            scope: None,
            replay_folds: Default::default(),
            install_cursors: Default::default(),
            parked: None,
            parking: None,
            yield_grant: false,
            #[cfg(test)]
            last_held: HeldReport::default(),
            #[cfg(test)]
            last_rollback: UndoReport::default(),
            #[cfg(test)]
            peak_ts_memtable_bytes: 0,
        }
    }
}

/// One document row the committed-redo apply wrote.
pub(in crate::data::executor) struct AppliedDocWrite {
    pub collection: String,
    /// The row's client identity.
    pub identity: RowIdentity,
    pub op: WriteOp,
    /// The deleted row's stored preimage. Puts carry their public preimage in `event`.
    pub old_value: Option<Vec<u8>>,
    /// The prepared public put event pair. `None` for a delete.
    pub event: Option<DocumentPutEvent>,
    /// Whether the write emits a change event. A clone copy emits none.
    pub emits_event: bool,
}

/// Which pass over a committed redo record drives the arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum RedoApplyPass {
    /// Every arm decodes, routes and checks each sub-record it owns, claims
    /// it, and writes nothing.
    Validate,
    /// Every arm writes each sub-record it owns and records the undo entries
    /// that reverse the write.
    Install,
}

/// Scratch for one pass over a committed redo record.
pub(in crate::data::executor) struct RedoApplyScope {
    pub(in crate::data::executor) pass: RedoApplyPass,
    /// Sub-records the arms claimed in the validate pass. Every sub-record
    /// belongs to exactly one arm, so the count equals the record's.
    pub(in crate::data::executor) claimed: usize,
    /// The sub-records each arm claimed in the validate pass.
    pub(super) arm_claims: super::batched::ActiveArms,
    /// The entries that reverse every write of the install pass, in write
    /// order. A batched install records into `spillable` instead.
    pub(in crate::data::executor) undo: Vec<UndoEntry>,
    /// `Some` for a batched install: its undo log, which spills.
    pub(super) spillable: Option<super::batched::SpillableUndo>,
    /// Materialized-sum resolution keyed by SOURCE collection.
    sum_targets: HashMap<String, RedoSumTargets>,
    /// The write set entry of each materialized-sum target row the folds
    /// wrote, in write order: one per target write.
    pub(in crate::data::executor) target_set: Vec<WriteSetEntry>,
    /// The RAM `target_set` holds. It cannot spill, so the spill check
    /// counts it with the spools.
    pub(super) target_set_bytes: usize,
    /// First error any arm hit on a sub-record. No later arm runs over the
    /// record once it is set.
    pub(in crate::data::executor) error: Option<ErrorCode>,
    /// The replay step that runs now.
    arm: RedoArm,
    /// The replay step that recorded `error`.
    error_arm: RedoArm,
    /// The index of the batch the arms run over: 0 for a whole record, the
    /// stream batch for a batched pass.
    pub(in crate::data::executor) batch: u32,
    /// The record op index of the first op of the batch the arms run over:
    /// 0 for a whole record. An arm's op index is relative to the batch.
    pub(in crate::data::executor) op_base: u64,
    /// The batch that recorded `error`.
    error_batch: u32,
    /// Every array the record wrote cells to.
    pub(in crate::data::executor) arrays_written: Vec<nodedb_array::types::ArrayId>,
    /// Columnar collections the install wrote, flushed once it succeeded.
    pub(in crate::data::executor) columnar_written: Vec<CollectionKey>,
    /// Timeseries collections the install ingested into, settled once it
    /// succeeded.
    pub(in crate::data::executor) timeseries_written: Vec<CollectionKey>,
    /// What each resolved timeseries batch the install stored, in install
    /// order. The apply answers their counts.
    pub(in crate::data::executor) ts_installs: Vec<TsInstalled>,
    /// Graph edge writes the install applied, counted once it settled.
    pub(in crate::data::executor) edge_writes:
        crate::data::executor::core_loop::graph_metrics::EdgeWriteTally,
    /// What the install holds until the record settled: the document rows
    /// it wrote, the events its handlers raised, its write notes, and what
    /// the validate kept for its events.
    pub(super) held: super::batched::InstallSpools,
    /// Who committed the record: a restore's document puts carry their
    /// source chain link, which the install relinks instead of refusing.
    pub(in crate::data::executor) origin: nodedb_physical::physical_plan::RedoOrigin,
    /// `Some` while a batched install runs one batch: each source row's
    /// transaction then records the fold ledger and its fold target rows.
    pub(in crate::data::executor) ledger: Option<super::fold_ledger::FoldLedger>,
    /// The graph node-label LSN mark before the install's first label
    /// version commit raised it. A rollback lowers the mark back to it, so
    /// restart replay versions the record again when it applies it.
    pub(in crate::data::executor) label_mark_before: Option<u64>,
}

/// What one resolved timeseries batch a committed redo install stored.
pub(in crate::data::executor) struct TsInstalled {
    pub count: crate::engine::timeseries::install_counts::TsInstallCount,
}

/// One write note an install pass holds back until the record settles.
pub(in crate::data::executor) struct DeferredNote {
    pub db: crate::types::DatabaseId,
    pub tenant: crate::types::TenantId,
    pub collection: String,
    pub stamp: WriteStamp,
    pub kind: DeferredNoteKind,
}

/// What a held-back write note records.
pub(in crate::data::executor) enum DeferredNoteKind {
    /// A write to one key (`Some`), or to the collection only (`None`).
    Row(Option<KeyRepr>),
    /// A write that removed every row of the collection.
    Wipe,
}

/// `(database, tenant, collection)`.
pub(in crate::data::executor) type CollectionKey =
    (crate::types::DatabaseId, crate::types::TenantId, String);

impl RedoApplyScope {
    /// The RAM the scope holds between batches: the hot part of its spools
    /// and its fold targets' write set entries.
    pub(super) fn resident_bytes(&self) -> usize {
        self.held.hot_bytes().saturating_add(self.target_set_bytes)
    }

    pub(in crate::data::executor) fn new(
        pass: RedoApplyPass,
        sum_targets: Vec<RedoSumTargets>,
    ) -> Self {
        Self {
            pass,
            claimed: 0,
            arm_claims: Default::default(),
            undo: Vec::new(),
            spillable: None,
            sum_targets: sum_targets
                .into_iter()
                .map(|targets| (targets.collection.clone(), targets))
                .collect(),
            target_set: Vec::new(),
            target_set_bytes: 0,
            error: None,
            arm: RedoArm::Reconstitute,
            error_arm: RedoArm::Reconstitute,
            batch: 0,
            op_base: 0,
            error_batch: 0,
            arrays_written: Vec::new(),
            columnar_written: Vec::new(),
            timeseries_written: Vec::new(),
            ts_installs: Vec::new(),
            edge_writes: Default::default(),
            held: Default::default(),
            origin: nodedb_physical::physical_plan::RedoOrigin::Commit,
            ledger: None,
            label_mark_before: None,
        }
    }

    /// The scope of a record `origin` committed.
    pub(in crate::data::executor) fn with_origin(
        mut self,
        origin: nodedb_physical::physical_plan::RedoOrigin,
    ) -> Self {
        self.origin = origin;
        self
    }

    /// Whether a batched install drives the arms: it holds the record one
    /// batch at a time, and runs the record's timeseries and array flushes
    /// between batches itself.
    pub(in crate::data::executor) fn is_batched(&self) -> bool {
        self.spillable.is_some()
    }

    /// Whether the batched install's log keeps a snapshot of the timeseries
    /// collection `key` that restores it whole on rollback.
    pub(in crate::data::executor) fn holds_ts_snapshot(&self, key: &CollectionKey) -> bool {
        self.spillable
            .as_ref()
            .is_some_and(|undo| undo.holds_ts_snapshot(key))
    }

    /// Record the entries that reverse one write, in write order: into the
    /// batched install's log when there is one, else into `undo`.
    pub(in crate::data::executor) fn record_undo(
        &mut self,
        entries: impl IntoIterator<Item = UndoEntry>,
    ) {
        match self.spillable.as_mut() {
            Some(spillable) => spillable.record(entries),
            None => self.undo.extend(entries),
        }
    }

    /// Hold back one write note until the record settles. The validate pass
    /// writes nothing, so it holds nothing.
    pub(in crate::data::executor) fn defer_write(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: crate::types::TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        stamp: WriteStamp,
    ) {
        if self.pass == RedoApplyPass::Install {
            self.held.notes.hold(DeferredNote {
                db,
                tenant,
                collection: collection.to_string(),
                stamp,
                kind: DeferredNoteKind::Row(key),
            });
        }
    }

    /// Hold back one whole-collection removal until the record settles. The
    /// validate pass writes nothing, so it holds nothing.
    pub(in crate::data::executor) fn defer_collection_wipe(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: crate::types::TenantId,
        collection: &str,
        stamp: WriteStamp,
    ) {
        if self.pass == RedoApplyPass::Install {
            self.held.notes.hold(DeferredNote {
                db,
                tenant,
                collection: collection.to_string(),
                stamp,
                kind: DeferredNoteKind::Wipe,
            });
        }
    }

    /// Hold `event` until the record settles. The validate pass writes
    /// nothing, so it raises none.
    pub(in crate::data::executor) fn hold_event(&mut self, event: crate::event::WriteEvent) {
        if self.pass == RedoApplyPass::Install {
            self.held.pending_events.push(event);
        }
    }

    /// Hold one document row the install wrote.
    pub(super) fn hold_doc_write(&mut self, write: AppliedDocWrite) {
        self.held.doc_writes.push(write.into());
    }

    /// Hold the target rows one fold wrote: their write set entries, and
    /// their document rows for the events. Under a fold ledger the source
    /// row's transaction stored the entries, and the write set is read back
    /// from the store.
    pub(super) fn hold_targets(&mut self, targets: &[TargetWrite]) {
        let stored = self.ledger.is_some();
        if !stored {
            self.target_set
                .extend(crate::data::executor::enforcement::write_hook::target_write_set(targets));
        }
        for target in targets {
            if !stored {
                self.target_set_bytes += target_entry_weight(target);
            }
            self.hold_doc_write(target_doc_write(target));
        }
    }

    /// The sum targets writes to `collection` fold on this core. A
    /// cross-shard target with no entry travels on its own task.
    pub(in crate::data::executor) fn sum_targets_for(
        &self,
        collection: &str,
    ) -> Vec<ResolvedSumTarget> {
        self.sum_targets
            .get(collection)
            .map(|targets| targets.resolved.clone())
            .unwrap_or_default()
    }

    /// Keep the first error, tagged with the step that recorded it. A later
    /// one is a consequence of it.
    pub(in crate::data::executor) fn record_error(&mut self, error: impl Into<ErrorCode>) {
        if self.error.is_none() {
            self.error = Some(error.into());
            self.error_arm = self.arm;
            self.error_batch = self.batch;
        }
    }

    /// Claim one sub-record for the arm that runs now.
    pub(in crate::data::executor) fn claim_sub_record(&mut self) {
        self.claimed += 1;
        if let Err(error) = self.arm_claims.claim(self.arm, 1) {
            self.record_error(error);
        }
    }

    /// Take the first error with the step that recorded it. The scope then
    /// keeps the next error a later pass over it hits.
    pub(in crate::data::executor) fn take_arm_error(&mut self) -> Option<ArmError> {
        self.error.take().map(|code| ArmError {
            arm: self.error_arm,
            batch: self.error_batch,
            code,
        })
    }
}

/// An error one replay step of a pass recorded, with the step and the
/// batch it ran over.
///
/// The steps run in [`RedoArm`] order. So among the errors of a record's
/// batches, the smallest `(arm, batch)` pair is the error a whole-record
/// pass records first.
pub(in crate::data::executor) struct ArmError {
    pub arm: RedoArm,
    pub batch: u32,
    pub code: ErrorCode,
}

impl CoreLoop {
    /// Name the replay step that runs next. An open committed-redo scope
    /// tags each error it records with it. Restart replay holds no scope.
    pub(crate) fn enter_redo_arm(&mut self, arm: RedoArm) {
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.arm = arm;
        }
    }
}

/// The RAM the write set entry of `target` holds.
fn target_entry_weight(target: &TargetWrite) -> usize {
    let installed = &target.outcome.installed;
    std::mem::size_of::<WriteSetEntry>()
        + installed.canonical.len()
        + installed.stored.len()
        + target.identity.as_str().len()
        + target.collection.len()
        + target.event.new_value.len()
        + target.event.old_value.as_ref().map_or(0, Vec::len)
}

/// The applied-write record of one materialized-sum target row.
fn target_doc_write(target: &TargetWrite) -> AppliedDocWrite {
    let outcome = &target.outcome;
    AppliedDocWrite {
        collection: target.collection.clone(),
        identity: target.identity.clone(),
        op: if outcome.prior_value.is_some() {
            WriteOp::Update
        } else {
            WriteOp::Insert
        },
        old_value: None,
        event: Some(target.event.clone()),
        emits_event: true,
    }
}
