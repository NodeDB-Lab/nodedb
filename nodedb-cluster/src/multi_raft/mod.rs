// SPDX-License-Identifier: BUSL-1.1

//! Multi-Raft coordinator: owns every Raft group hosted on this node,
//! routes inbound RPCs, and surfaces aggregated `Ready` output to the
//! tick loop.
//!
//! Split across files:
//! - [`core`]: struct, constructors, group lifecycle (`add_group`,
//!   `add_group_as_learner`), tick pipeline, routing accessors.
//! - [`status`]: observability snapshots (`group_statuses`,
//!   `group_membership`).
//! - [`proposals`]: leadership checks, proposals and log access.
//! - [`committed_read`]: a bounded read of a group's committed log.
//! - [`rpc_dispatch`]: inbound RPC routing to the correct group and the
//!   corresponding response handlers.
//! - [`conf_change`]: `propose_conf_change` / `apply_conf_change` with
//!   proper voter / learner / promotion semantics.
//! - [`conf_change_gate`]: the leader's check that a change keeps a voter
//!   that holds the group's state.
//! - [`membership`]: learner catch-up / promotion helpers
//!   (`commit_index_for`, `ready_learners`, etc.) driven by the tick loop.
//! - [`membership_sync`]: set a group's voters and learners from routing
//!   after a snapshot install.
//! - [`leader_board`]: each mounted group's leadership, read without the
//!   `MultiRaft` lock.
//! - [`read_index`]: leadership confirmation for linearizable reads.
//! - [`ordinal_floor`]: each group's system-time floor, read and raised by
//!   the floor exchange.
//! - [`sequencer_cut_floor`]: the cut floors of the markers this node
//!   appends to its sequencer log, which every epoch it mints stays above.

pub mod committed_read;
pub mod conf_change;
pub mod conf_change_gate;
pub mod core;
pub mod leader_board;
pub mod membership;
pub mod membership_sync;
pub mod mount;
pub mod ordinal_floor;
pub mod peer_contact;
pub mod proposals;
pub mod read_index;
pub mod rpc_dispatch;
pub mod sequencer_cut_floor;
pub mod status;

pub use committed_read::CommittedRead;
pub use core::{MultiRaft, MultiRaftReady, SnapshotRequirement};
pub use leader_board::LeaderBoard;
pub use mount::{GroupMountSpec, OpenedGroup};
pub use ordinal_floor::{FloorAnswer, FloorRound};
pub use peer_contact::PeerAckSample;
pub use proposals::{AdmissionTerm, GatedPropose};
pub use status::{GroupMembership, GroupStatus};
