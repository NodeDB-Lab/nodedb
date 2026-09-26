// SPDX-License-Identifier: BUSL-1.1

//! Spawn and shutdown lifecycle for [`TestClusterNode`].
//!
//! A `TestClusterNode` owns one full NodeDB server instance configured
//! for cluster mode. Spawn via [`TestClusterNode::spawn`], passing the
//! node id and the seed list (empty for the bootstrap node; otherwise
//! a list of already-running peer addresses). On return the node has:
//!
//! - Pre-bound QUIC transport (so the listen address is known before
//!   peers need it).
//! - `SharedState` wired with credentials, metadata_cache, and the
//!   cluster handles (topology / routing / transport / applied-index
//!   watcher).
//! - Data Plane core running on a spawn_blocking task.
//! - Response poller running on a tokio task.
//! - Event Plane spawned.
//! - Raft loop + QUIC RPC server started via `start_raft` (installs
//!   the production `MetadataCommitApplier` with `Weak<SharedState>`
//!   so committed `CollectionDdl::Create` entries trigger Data Plane
//!   registers on every node).
//! - pgwire listener bound on an ephemeral port.
//! - `tokio_postgres::Client` connected to that listener.
//!
//! Shutdown flips every shutdown channel, aborts background tasks,
//! and drops the TempDir.
//!
//! Struct definition in [`types`]; thin `spawn*` convenience wrappers in
//! [`spawn_variants`]; the full spawn body in [`spawn_full`]; query
//! execution + shutdown + `Drop` teardown in [`teardown`]; the in-place
//! restart in [`restart`].

mod client_slot;
mod restart;
mod spawn_full;
mod spawn_variants;
mod teardown;
mod types;

pub use types::{HARNESS_SUPERUSER, TestClusterNode};
