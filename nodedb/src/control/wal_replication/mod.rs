// SPDX-License-Identifier: BUSL-1.1

//! Distributed WAL write path — propose writes through Raft, apply after commit.
//!
//! [`types`] holds the wire types; [`replicable_write`] the decided-write type
//! [`encode`] consumes; [`decode`] converts wire bytes back to `PhysicalPlan`;
//! [`transaction_redo`] carries committed transactions through the data-group
//! log.

pub mod decode;
mod decode_sync_engines;
pub mod encode;
pub mod propose;
pub mod replicable_write;
pub mod transaction_redo;
pub mod types;

pub use decode::{decode_replicated_entry, from_replicated_entry};
pub use encode::to_replicated_entry;
pub(crate) use propose::{
    propose_replicated_entry, stamp_collection_incarnations, stamp_metadata_floor,
    statement_propose_deadline,
};
pub use replicable_write::ReplicableWrite;
pub use types::{
    AppliedOutput, AppliedWait, AsyncRaftProposer, AsyncRaftSubmit, CollectionIncarnation,
    ConstraintChangeOp, ProposedAt, ProposedWrite, RaftAppliedIndexSink, RaftCompactor,
    RaftProposer, ReplicatedEntry, ReplicatedEventSource, ReplicatedIdentity, ReplicatedSumTarget,
    ReplicatedWrite,
};

pub use crate::control::distributed_applier::{
    AppliedWrite, ApplyBatch, DistributedApplier, ProposeResult, ProposeTracker,
    create_distributed_applier, run_apply_loop,
};
