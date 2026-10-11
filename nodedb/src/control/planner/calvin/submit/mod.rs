// SPDX-License-Identifier: BUSL-1.1

//! Calvin submit-and-await primitive and sequencer-leader routing (Cv1).
//!
//! NodeDB's Calvin cross-shard write path only completes when the transaction is
//! submitted on the SEQUENCER-GROUP leader:
//!
//! - the sequencer SERVICE assigns transactions (`note_assigned`) ONLY on the
//!   `SEQUENCER_GROUP_ID` leader — a non-leader's sequencer service drains and
//!   DISCARDS its inbox;
//! - the replicated `CompletionAck` is applied on ALL sequencer-group members,
//!   so every member's `CalvinCompletionRegistry` receives `note_completion_ack`.
//!
//! The consequence: a submit-and-await is correct ONLY on the leader, whose
//! local registry receives BOTH the assignment and the completion ack. A submit
//! on a non-leader is silently lost and the caller times out at the ASSIGNMENT
//! phase.
//!
//! [`submit_and_await_calvin`] is the local primitive — it MUST run on the
//! sequencer leader. [`submit_calvin_routed`] is the entry point every
//! coordinator calls: it resolves the sequencer leader and either runs the
//! submit-and-await locally (this node IS the leader) or forwards the `TxClass`
//! to the leader via a one-shot RPC (`SubmitCalvinTxn`), mirroring the routed
//! surrogate-exchange path exactly.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's / leader's Control Plane (Tokio). The QUIC
//! `send_rpc` call is Control-Plane I/O, allowed here. The actual transaction
//! execution happens on the Data Plane via the sequencer service / per-vshard
//! schedulers; this module never does storage I/O or io_uring directly.

pub mod assign;
pub mod budget;
pub mod edge_slices;
pub mod local;
pub mod parts;
pub mod routed;
pub mod stream;
pub mod unique_claims;

pub(crate) use assign::submit_local_assign;
pub use assign::{RoutedAssignment, submit_calvin_routed_assign};
pub use local::{submit_and_await_calvin, submit_and_await_calvin_with_timeout};
pub use routed::{submit_calvin_routed, submit_calvin_routed_write};
