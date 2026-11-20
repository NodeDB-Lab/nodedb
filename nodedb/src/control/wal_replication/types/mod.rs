// SPDX-License-Identifier: BUSL-1.1

//! Distributed WAL write path — propose writes through Raft, apply after commit.
//!
//! Write flow:
//! 1. Handler serializes write as [`ReplicatedWrite`]
//! 2. Handler proposes to Raft via [`RaftLoop::propose`]
//! 3. Handler registers a waiter in [`ProposeTracker`] keyed by (group_id, log_index)
//! 4. Raft replicates to quorum and commits
//! 5. [`DistributedApplier`] receives committed entries, queues for async execution
//! 6. Background task dispatches each write to the local Data Plane
//! 7. If a waiter exists (leader path), sends the response; otherwise only applies (follower)
//!
//! Split into:
//! - [`aliases`]: Raft propose/compact callback type aliases.
//! - [`wire_shapes`]: small wire-format types embedded inside [`ReplicatedWrite`].
//! - [`replicated_write`]: the [`ReplicatedWrite`] enum itself.
//! - [`replicated_entry`]: [`ReplicatedEntry`] (routing envelope) + (de)serialization.
//! - [`transaction_redo_wire`]: wire types of the committed-transaction redo entry.
//! - [`redo_chunk_wire`]: a redo body, inline or chunked across entries.
//! - [`redo_content_frames`]: the framed redo content a chunk stream carries.
//! - [`system_span`]: the run of system-time ordinals a write stamps.
//! - [`calvin_frozen_reply`]: the frozen `RETURNING` replies of a Calvin slice.
//! - [`pinned_writes`]: the config pins a replicated write requires.

mod aliases;
mod calvin_frozen_reply;
mod pinned_writes;
mod redo_chunk_wire;
mod redo_content_frames;
mod replicated_entry;
mod replicated_write;
mod retention_op;
mod system_span;
mod transaction_redo_wire;
mod wire_shapes;

pub use aliases::{
    AppliedOutput, AppliedWait, AsyncRaftProposer, AsyncRaftSubmit, ProposedAt, ProposedWrite,
    RaftAppliedIndexSink, RaftCompactor, RaftProposer,
};
pub use calvin_frozen_reply::{
    CalvinFrozenReply, FrozenPlanReply, FrozenRejection, FrozenSequenceEffect, PlanSequenceEffects,
};
pub(crate) use pinned_writes::UncoveredPins;
pub use redo_chunk_wire::{RedoBody, RedoContent};
pub use redo_content_frames::RedoContentView;
pub use replicated_entry::{CollectionIncarnation, ReplicatedEntry};
pub use replicated_write::ReplicatedWrite;
pub use retention_op::RetentionOp;
pub(crate) use system_span::columnar_rows;
pub use transaction_redo_wire::{CalvinRedoMeta, ReplicatedEventSource, ReplicatedIdentity};
pub use wire_shapes::{
    BalanceDeltaFields, ColumnarResolvedRow, ConstraintChangeOp, DocumentResolvedMutationWire,
    KvResolvedMutationWire, MirrorBarrier, MirrorCalvinFloor, ReplicatedBatchEdge,
    ReplicatedSumTarget,
};
