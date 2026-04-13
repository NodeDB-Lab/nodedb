//! Cluster mode startup and integration.
//!
//! Bridges `nodedb-cluster` (Raft, transport, routing, metadata group)
//! into the main server. Split into one concern per file:
//!
//! - [`init`] — cluster startup (transport, catalog, bootstrap/join/restart).
//! - [`start_raft`] — Raft event loop + RPC server + applier wiring.
//! - [`handle`] — the `ClusterHandle` passed between init and start_raft.
//! - [`spsc_applier`] — committed data-group entries → SPSC bridge.
//! - [`metadata_applier`] — committed metadata-group entries →
//!   `MetadataCache` + `AppliedIndexWatcher` + optional redb writeback.
//! - [`applied_index_watcher`] — sync wait primitive used by
//!   [`crate::control::metadata_proposer`].

pub mod applied_index_watcher;
pub mod handle;
pub mod init;
pub mod metadata_applier;
pub mod spsc_applier;
pub mod start_raft;

pub use applied_index_watcher::AppliedIndexWatcher;
pub use handle::ClusterHandle;
pub use init::init_cluster;
pub use metadata_applier::MetadataCommitApplier;
pub use spsc_applier::SpscCommitApplier;
pub use start_raft::start_raft;
