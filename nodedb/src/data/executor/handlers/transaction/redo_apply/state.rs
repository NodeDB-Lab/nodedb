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

use std::collections::HashMap;

use nodedb_physical::physical_plan::{RedoSumTargets, ResolvedSumTarget};
use nodedb_types::RowIdentity;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::event::WriteOp;
use crate::types::Lsn;

/// Committed-redo apply state owned by one core.
pub(in crate::data::executor) struct RedoApplyState {
    /// Number of Data Plane cores on this node. The replay arms route a record
    /// to core `vshard_id % num_cores`, so the apply hands them this count.
    pub(in crate::data::executor) num_cores: usize,
    /// `Some` only while one committed redo record applies on this core.
    pub(in crate::data::executor) scope: Option<RedoApplyScope>,
    /// During restart replay: the materialized-sum targets each Calvin redo
    /// record's stamp carries, keyed by the record's LSN. The document redo
    /// arm folds a row at such an LSN into its targets, as the live install
    /// did. Empty outside restart replay.
    pub(in crate::data::executor) replay_folds: HashMap<u64, Vec<RedoSumTargets>>,
}

impl RedoApplyState {
    /// The sum targets restart replay folds a write to `collection` at
    /// `record_lsn` into, when the record carries any.
    pub(in crate::data::executor) fn replay_folds_for(
        &self,
        record_lsn: u64,
        collection: &str,
    ) -> Option<(Vec<ResolvedSumTarget>, Vec<String>)> {
        self.replay_folds
            .get(&record_lsn)?
            .iter()
            .find(|targets| targets.collection == collection)
            .map(|targets| (targets.resolved.clone(), targets.deferred.clone()))
    }

    /// A single-core default. Every multi-core runtime sets the real count
    /// through `CoreLoop::set_num_cores` before the core serves requests.
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            num_cores: 1,
            scope: None,
            replay_folds: HashMap::new(),
        }
    }
}

impl crate::data::executor::core_loop::CoreLoop {
    /// Arm restart replay with the sum targets each Calvin record carries.
    /// Returns whether this is restart replay: a committed-redo apply folds
    /// from its open scope instead, and leaves `folds` unused.
    pub(crate) fn begin_replay_folds(&mut self, folds: HashMap<u64, Vec<RedoSumTargets>>) -> bool {
        let restart = self.redo_apply.scope.is_none();
        if restart {
            self.redo_apply.replay_folds = folds;
        }
        restart
    }

    /// Drop the restart-replay sum targets once the document redo arm ran.
    pub(crate) fn end_replay_folds(&mut self) {
        self.redo_apply.replay_folds.clear();
    }
}

/// One document row the committed-redo apply wrote.
pub(in crate::data::executor) struct AppliedDocWrite {
    pub collection: String,
    /// The row's client identity.
    pub identity: RowIdentity,
    pub op: WriteOp,
    /// The row as stored before this write, when the write read it.
    pub old_value: Option<Vec<u8>>,
    /// The MessagePack body this write installed. `None` for a delete.
    pub new_body: Option<Vec<u8>>,
    /// Every `(field, value)` index entry the write added, removed, or
    /// versioned.
    pub index_tuples: Vec<(String, String)>,
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
    /// The entries that reverse every write of the install pass, in write
    /// order.
    pub(in crate::data::executor) undo: Vec<UndoEntry>,
    /// Materialized-sum resolution keyed by SOURCE collection.
    sum_targets: HashMap<String, RedoSumTargets>,
    /// Document rows written, source rows and fold targets alike, in order.
    pub(in crate::data::executor) doc_writes: Vec<AppliedDocWrite>,
    /// Materialized-sum target rows the folds wrote.
    pub(in crate::data::executor) target_writes: Vec<TargetWrite>,
    /// First error any arm hit on a sub-record. The apply reports it once
    /// every arm has run.
    pub(in crate::data::executor) error: Option<ErrorCode>,
    /// Every array the record wrote cells to.
    pub(in crate::data::executor) arrays_written: Vec<nodedb_array::types::ArrayId>,
    /// Columnar collections the install wrote, flushed once it succeeded.
    pub(in crate::data::executor) columnar_written: Vec<CollectionKey>,
    /// Timeseries collections the install ingested into, settled once it
    /// succeeded.
    pub(in crate::data::executor) timeseries_written: Vec<CollectionKey>,
    /// Events the install's writes raised, sent once it succeeded.
    pub(in crate::data::executor) pending_events: Vec<crate::event::WriteEvent>,
    /// Write versions the install's writes produced, published once the
    /// record settled.
    pub(in crate::data::executor) write_versions: Vec<DeferredWriteVersion>,
}

/// One write version an install pass holds back until the record settles.
pub(in crate::data::executor) struct DeferredWriteVersion {
    pub db: crate::types::DatabaseId,
    pub tenant: crate::types::TenantId,
    pub collection: String,
    pub key: Option<KeyRepr>,
    pub lsn: Lsn,
}

/// `(database, tenant, collection)`.
pub(in crate::data::executor) type CollectionKey =
    (crate::types::DatabaseId, crate::types::TenantId, String);

impl RedoApplyScope {
    pub(in crate::data::executor) fn new(
        pass: RedoApplyPass,
        sum_targets: Vec<RedoSumTargets>,
    ) -> Self {
        Self {
            pass,
            claimed: 0,
            undo: Vec::new(),
            sum_targets: sum_targets
                .into_iter()
                .map(|targets| (targets.collection.clone(), targets))
                .collect(),
            doc_writes: Vec::new(),
            target_writes: Vec::new(),
            error: None,
            arrays_written: Vec::new(),
            columnar_written: Vec::new(),
            timeseries_written: Vec::new(),
            pending_events: Vec::new(),
            write_versions: Vec::new(),
        }
    }

    /// Hold back one write version until the record settles. The validate
    /// pass writes nothing, so it holds nothing.
    pub(in crate::data::executor) fn defer_write_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: crate::types::TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        lsn: Lsn,
    ) {
        if self.pass == RedoApplyPass::Install {
            self.write_versions.push(DeferredWriteVersion {
                db,
                tenant,
                collection: collection.to_string(),
                key,
                lsn,
            });
        }
    }

    /// The resolved and deferred sum targets for writes to `collection`.
    pub(in crate::data::executor) fn sum_targets_for(
        &self,
        collection: &str,
    ) -> (Vec<ResolvedSumTarget>, Vec<String>) {
        match self.sum_targets.get(collection) {
            Some(targets) => (targets.resolved.clone(), targets.deferred.clone()),
            None => (Vec::new(), Vec::new()),
        }
    }

    /// Keep the first error; a later one is a consequence of it.
    pub(in crate::data::executor) fn record_error(&mut self, error: impl Into<ErrorCode>) {
        if self.error.is_none() {
            self.error = Some(error.into());
        }
    }
}

/// The applied-write record of one materialized-sum target row.
pub(in crate::data::executor) fn target_doc_write(target: &TargetWrite) -> AppliedDocWrite {
    let outcome = &target.outcome;
    let mut index_tuples = Vec::with_capacity(
        outcome.secondary_index_added.len()
            + outcome.secondary_index_removed.len()
            + outcome.bitemporal_index_tuples.len(),
    );
    index_tuples.extend_from_slice(&outcome.secondary_index_added);
    index_tuples.extend_from_slice(&outcome.secondary_index_removed);
    index_tuples.extend_from_slice(&outcome.bitemporal_index_tuples);
    AppliedDocWrite {
        collection: target.collection.clone(),
        identity: target.identity.clone(),
        op: if outcome.prior_value.is_some() {
            WriteOp::Update
        } else {
            WriteOp::Insert
        },
        old_value: outcome.prior_value.clone(),
        new_body: Some(target.body.clone()),
        index_tuples,
    }
}
