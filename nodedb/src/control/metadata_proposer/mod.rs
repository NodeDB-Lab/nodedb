// SPDX-License-Identifier: BUSL-1.1

//! `propose-and-wait-for-local-apply` helpers for replicated catalog DDL.
//!
//! The sole entry point DDL handlers use to write a
//! [`crate::control::catalog_entry::CatalogEntry`] through the metadata raft
//! group (group 0): `propose_catalog_entry_async` and
//! `propose_catalog_batch_async`. Both run on any runtime flavor. Every wait
//! is awaited: the preparation lock and lease, the descriptor drain, the
//! metadata commit's apply, and the authorization barrier.
//!
//! A preparation lease dropped unreleased, as when its proposer's future is
//! cancelled, hands its release to the background lease releaser. A release
//! that never applies ends when the metadata leader reclaims the lease: at
//! once when its owner left, once its owner is dead past the grace, and after
//! the lease time when its owner is alive but stuck.
//!
//! Every node runs a metadata raft group, a one-node cluster included.
//! `start_raft` installs its handle before any listener opens. A state with
//! no handle refuses the propose with a typed error. No caller writes the
//! catalog itself.
//!
//! Semantics:
//!
//! 1. If this node is the metadata-group leader, proposes the entry, waits
//!    until its local applied watermark reaches the assigned log index, and
//!    returns the log index. The wait ends at the statement deadline with
//!    `Error::DeadlineExceeded`: the entry was proposed, so its outcome is
//!    unknown.
//! 2. If this node is NOT the leader, forwards the entry to the leader.
//!    With no leader to take it, returns `Error::MetadataLeaderUnavailable`:
//!    nothing was proposed. A forward with no answer returns
//!    `Error::DeadlineExceeded`: the outcome is unknown.

pub(crate) mod audit_intent;
pub mod catalog;
pub mod catalog_batch;
pub mod ddl_owner;
pub mod ddl_prepare;
pub(crate) mod ddl_reclaim;
pub mod handle;
pub mod replicated_entries;
pub mod timeouts;
pub(crate) mod wait;

pub use catalog::propose_catalog_entry_async;
pub use catalog_batch::{BatchOutcome, CatalogBatch, propose_catalog_batch_async};
pub use ddl_owner::DdlPrepareOwner;
pub(crate) use ddl_prepare::{
    DdlPrepareLease, acquire_ddl_prepare_lease_async, lock_ddl_preparation_async,
    release_ddl_prepare_token,
};
pub use handle::{MetadataRaftHandle, ProposeFuture, RaftLoopProposerHandle};
pub use replicated_entries::{
    propose_cursor_commit, propose_cursor_commit_audited, propose_database_id_reserve,
    propose_restore_point, propose_surrogate_hwm, propose_surrogate_reserve,
    propose_sync_peer_bind, propose_sync_producer_fence, propose_sync_producer_register,
};
pub use timeouts::DEFAULT_DRAIN_TIMEOUT;
