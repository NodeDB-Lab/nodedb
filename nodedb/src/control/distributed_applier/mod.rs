// SPDX-License-Identifier: BUSL-1.1

//! Distributed apply machinery — tracks pending Raft proposals and applies
//! committed entries to the local Data Plane.
//!
//! See [`crate::control::wal_replication`] for the full write flow description.

pub mod applied_index;
pub mod applier;
pub mod apply_loop;
pub mod apply_window;
pub mod proposal_ledger;
pub mod propose_tracker;

pub use applied_index::{AppliedPrefix, save_applied_index};
pub use applier::{ApplyBatch, DistributedApplier, create_distributed_applier};
pub use apply_loop::run_apply_loop;
pub use apply_window::{APPLY_WINDOW_PER_GROUP, ApplyWindow};
pub use proposal_ledger::{AppliedOutcome, PROPOSAL_LEDGER_CAPACITY, PriorApply, ProposalLedger};
pub use propose_tracker::{AppliedWrite, ApplyingEntry, ProposeResult, ProposeTracker};
