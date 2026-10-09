// SPDX-License-Identifier: BUSL-1.1

//! Raft event loop — drives MultiRaft ticks and dispatches messages over the transport.
//!
//! Split across files:
//! - [`loop_core`]: `RaftLoop` struct, constructors, builders, public API
//!   (`propose`, `propose_conf_change`, `group_statuses`), and the main
//!   `run` shutdown-driven loop.
//! - [`tick`]: one iteration of the tick pipeline — drive MultiRaft,
//!   dispatch outbound AE/RV, apply committed entries, promote
//!   caught-up learners.
//! - [`handle_rpc`]: inbound RPC routing (`impl RaftRpcHandler`). The
//!   `JoinRequest` arm delegates to [`join`].
//! - [`join`]: async server-side `JoinRequest` orchestration — register
//!   peer, propose `AddLearner` on every group, wait for commit,
//!   broadcast topology, persist catalog, build the wire response.

pub mod apply_gate;
mod auth_lease;
pub mod auth_lease_hook;
mod builder;
mod group_unmount;
pub mod handle_rpc;
pub mod hooks;
mod hooks_routed;
pub mod in_flight_snapshots;
pub mod join;
mod leader_balance;
mod leadership_transfer;
mod lease_gc;
pub mod loop_core;
mod membership_convergence;
mod placement_reconcile;
pub mod proposals;
pub mod propose_gate;
mod read_index;
mod routing_persist;
mod run;
mod snapshot_membership;
pub mod tick;
mod tick_state;

pub use apply_gate::{ApplyPermit, GroupApplyGates, InstallPermit};
pub use auth_lease_hook::AuthLeaseService;
pub use hooks::{
    AssignRemoteSurrogate, BuiltGroupSnapshot, CalvinSubmit, CalvinSubmitInbox,
    MetadataSnapshotCapture, ReleaseReservation, ReserveRead, ShuffleAggregator, ShuffleConsumer,
    ShuffleProducer, ShuffleReceiver, SnapshotApplier, SnapshotBuilder, SnapshotQuarantineHook,
};
pub use in_flight_snapshots::{InFlightSnapshotGuard, InFlightSnapshots};
pub use loop_core::{CommitApplier, RaftLoop, VShardEnvelopeHandler};
pub use propose_gate::{DataProposeGate, ProposeHold};
