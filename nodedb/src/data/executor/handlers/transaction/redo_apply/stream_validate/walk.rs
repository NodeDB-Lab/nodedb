// SPDX-License-Identifier: BUSL-1.1

//! What a streamed validate takes, keeps across its batches, and hands back.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::{RedoOrigin, RedoPins, RedoSumTargets, ResolvedSumTarget};
use nodedb_types::calvin::VersionedReadEntry;

use crate::data::executor::array_install_flush::ArrayStreamLoads;
use crate::data::executor::core_loop::install_fence::FenceKey;
use crate::data::executor::handlers::timeseries::TsStreamLoads;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::redo::frame::FrameOrder;
use crate::wal::{RedoRecord, RedoStreamRef, RedoSubRecord};

use super::super::batched::{ActiveArms, InstallSpace, InstallSpools};
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

/// A stream whose content passed every check before the install, and what
/// the install reads from it.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamValidated {
    pub(in crate::data::executor::handlers::transaction::redo_apply) header: StreamHeader,
    /// The ops each replay arm claimed.
    pub(in crate::data::executor::handlers::transaction::redo_apply) active_arms: ActiveArms,
    /// Every collection a document op or a KV key image names: the arms
    /// note those rows by key.
    pub(in crate::data::executor::handlers::transaction::redo_apply) keyed: BTreeSet<String>,
    /// What the install's events read: the value each KV key the content
    /// writes held before the install, the node-label deltas, the row
    /// sources, and the messages stamped with the reference's publish
    /// position.
    pub(in crate::data::executor::handlers::transaction::redo_apply) held: InstallSpools,
    /// Where `held` spilled. The install keeps spilling into it.
    pub(in crate::data::executor::handlers::transaction::redo_apply) space: InstallSpace,
    /// What the content writes into each timeseries collection: the install
    /// flushes by it once, before its timeseries batches, and flushes the
    /// pressed ones of them after each batch.
    pub(in crate::data::executor::handlers::transaction::redo_apply) ts_loads: TsStreamLoads,
    /// The cells the content writes into each array: the install flushes
    /// by it once, before its array batches, and flushes the full ones of
    /// them after each batch.
    pub(in crate::data::executor::handlers::transaction::redo_apply) array_loads: ArrayStreamLoads,
    pub(in crate::data::executor::handlers::transaction::redo_apply) spill: ValidateSpill,
}

/// The post-state's spill files. They go when this drops, once the install
/// ran over the validated content.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct ValidateSpill {
    _post: PostState,
}

impl ValidateSpill {
    pub(super) fn new(post: PostState) -> Self {
        Self { _post: post }
    }
}

/// The fields of the content header the checks and the install read.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamHeader {
    pub(in crate::data::executor::handlers::transaction::redo_apply) version: u16,
    pub(in crate::data::executor::handlers::transaction::redo_apply) user_roles: Vec<String>,
    pub(in crate::data::executor::handlers::transaction::redo_apply) sum_targets:
        Vec<RedoSumTargets>,
    /// The config content the proposer pinned for the stream's content.
    pub(in crate::data::executor::handlers::transaction::redo_apply) pins: RedoPins,
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
    /// Reads not checked yet. No read stays here past the slice that
    /// walked it.
    pub(super) reads: Vec<VersionedReadEntry>,
    /// The collections of the reads checked so far. A yielding install
    /// fences each before its slice ends.
    pub(super) read_keys: BTreeSet<FenceKey>,
    /// The instant the KV key images read at, in ms since the Unix epoch.
    pub(super) kv_now_ms: u64,
    pub(super) keyed: BTreeSet<String>,
    /// What the install reads after its arms, spilled into `space`.
    pub(super) held: InstallSpools,
    pub(super) space: InstallSpace,
    pub(super) ts_loads: TsStreamLoads,
    pub(super) array_loads: ArrayStreamLoads,
}

impl StreamWalk {
    pub(super) fn new(post: PostState, space: InstallSpace, kv_now_ms: u64) -> Self {
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
            read_keys: BTreeSet::new(),
            kv_now_ms,
            keyed: BTreeSet::new(),
            held: InstallSpools::default(),
            space,
            ts_loads: TsStreamLoads::default(),
            array_loads: ArrayStreamLoads::default(),
        }
    }
}
