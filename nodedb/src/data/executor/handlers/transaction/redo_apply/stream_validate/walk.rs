// SPDX-License-Identifier: BUSL-1.1

//! What a streamed validate takes, keeps across its batches, and hands back.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{RedoOrigin, RedoSumTargets, ResolvedSumTarget};
use nodedb_types::calvin::VersionedReadEntry;

use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::redo::frame::FrameOrder;
use crate::wal::{RedoRecord, RedoStreamRef, RedoSubRecord};

use super::super::passes::ValidateBatches;
use super::post_state::PostState;
use super::verdict::Verdict;

/// The bounds a streamed validate works within.
#[derive(Debug, Clone, Copy)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamLimits {
    /// Bytes of whole framed records one batch holds.
    pub batch_bytes: usize,
    /// Hot bytes of the post-state past which it spills.
    pub spill_threshold: usize,
}

/// One stream marker's validate.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamValidateInput<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    /// The marker record. Its Calvin stamp and cross-shard key are the
    /// content's.
    pub marker: &'a RedoRecord,
    pub reference: &'a RedoStreamRef,
    pub origin: RedoOrigin,
    /// The snapshot ordinal of a single-shard snapshot commit. `None`
    /// skips the serialization check.
    pub snapshot: Option<i64>,
    /// The instant retention is judged at, in ms since the Unix epoch.
    pub judged_at_ms: i64,
    /// The marker's LSN: every batch the arms check takes it.
    pub lsn: Lsn,
    pub limits: StreamLimits,
}

/// A stream whose content passed every check before the install. The
/// post-state's spill directory goes when this drops.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamValidated {
    _post: PostState,
}

impl StreamValidated {
    pub(super) fn new(post: PostState) -> Self {
        Self { _post: post }
    }
}

/// The fields of the content header the checks read.
pub(super) struct StreamHeader {
    pub(super) version: u16,
    pub(super) user_roles: Vec<String>,
    pub(super) sum_targets: Vec<RedoSumTargets>,
}

/// What one collection's checks read once.
pub(super) struct CollectionTrack {
    pub(super) bitemporal: bool,
    pub(super) resolved: Vec<ResolvedSumTarget>,
}

/// Reads of the read set held until they are checked together.
pub(super) const READ_CHUNK: usize = 1024;

/// The state one streamed validate keeps across its batches.
pub(super) struct StreamWalk {
    pub(super) order: FrameOrder,
    pub(super) header: Option<StreamHeader>,
    pub(super) verdict: Verdict,
    pub(super) post: PostState,
    /// Every collection a document op wrote with a registered config, in
    /// name order.
    pub(super) collections: BTreeMap<String, CollectionTrack>,
    /// The arms' validate pass. `None` before the header and after the
    /// pass stopped on a panic or a lost scope.
    pub(super) batches: Option<ValidateBatches>,
    /// The ops of the batch the walk is in, for the arms.
    pub(super) batch_ops: Vec<RedoSubRecord>,
    /// The index of the batch the walk is in.
    pub(super) batch: u64,
    /// Op frames seen so far.
    pub(super) ops: u64,
    /// Reads not checked yet.
    pub(super) reads: Vec<VersionedReadEntry>,
}

impl StreamWalk {
    pub(super) fn new(post: PostState) -> Self {
        Self {
            order: FrameOrder::default(),
            header: None,
            verdict: Verdict::default(),
            post,
            collections: BTreeMap::new(),
            batches: None,
            batch_ops: Vec::new(),
            batch: 0,
            ops: 0,
            reads: Vec::new(),
        }
    }
}
