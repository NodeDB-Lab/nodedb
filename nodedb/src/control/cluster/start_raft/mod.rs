// SPDX-License-Identifier: BUSL-1.1

//! Start the Raft event loop, RPC server, and both appliers.
//!
//! Split by phase — [`start_raft`] runs each in order:
//! - [`group_setup`]: bootstrap the sequencer Raft group; build the propose
//!   tracker, distributed applier, Calvin state, metadata applier, plan/array
//!   executors, and vshard handler.
//! - [`hooks`]: build the snapshot quarantine hook, per-group snapshot
//!   builder/applier (incl. boot completion of interrupted installs), and the shuffle/
//!   surrogate/Calvin routing hooks.
//! - [`loop_build`]: construct `RaftLoop`, consume `pending_subsystems`,
//!   build the Calvin sequencer service, spawn vShard schedulers, and start
//!   cluster subsystems.
//! - [`proposer_wiring`]: install the sync/async Raft proposer + compactor
//!   closures and spawn the apply loop.
//! - [`sequencer_route`]: run a proposal the leader's write gate refused
//!   through the Calvin sequencer.
//! - [`observability`]: publish observability handles and spawn the tick
//!   loop, sequencer service, RPC server, and health monitor.

mod core;
mod group_setup;
mod hooks;
mod loop_build;
mod observability;
mod propose_error;
mod proposer_wiring;
mod sequencer_route;

pub use core::start_raft;
