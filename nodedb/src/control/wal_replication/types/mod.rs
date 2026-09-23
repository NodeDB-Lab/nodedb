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
//! 7. If a waiter exists (leader path), sends the response; otherwise just applies (follower)
//!
//! Split into:
//! - [`aliases`]: Raft propose/compact callback type aliases + serde defaults.
//! - [`wire_shapes`]: small wire-format types embedded inside [`ReplicatedWrite`].
//! - [`replicated_write`]: the [`ReplicatedWrite`] enum itself.
//! - [`replicated_entry`]: [`ReplicatedEntry`] (routing envelope) + (de)serialization.
//! - [`transaction_redo_wire`]: wire types of the committed-transaction redo entry.

mod aliases;
mod replicated_entry;
mod replicated_write;
mod transaction_redo_wire;
mod wire_shapes;

pub use aliases::{AsyncRaftProposer, RaftAppliedIndexSink, RaftCompactor, RaftProposer};
pub use replicated_entry::ReplicatedEntry;
pub use replicated_write::ReplicatedWrite;
pub use transaction_redo_wire::{ReplicatedEventSource, ReplicatedIdentity};
pub use wire_shapes::{
    BalanceDeltaFields, ColumnarResolvedRow, ConstraintChangeOp, DocumentResolvedMutationWire,
    KvResolvedMutationWire, ReplicatedBatchEdge, ReplicatedSumTarget,
};
