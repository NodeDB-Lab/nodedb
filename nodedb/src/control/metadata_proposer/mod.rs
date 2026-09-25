// SPDX-License-Identifier: BUSL-1.1

//! Synchronous `propose-and-wait-for-local-apply` helper for
//! replicated catalog DDL.
//!
//! The sole entry point pgwire DDL handlers use to write a
//! [`crate::control::catalog_entry::CatalogEntry`] through the metadata raft group (group 0). It is
//! deliberately sync — pgwire DDL handlers are not async, and
//! `tokio::task::block_in_place`-style wrapping keeps the blocking
//! wait from starving the tokio runtime.
//!
//! Semantics:
//!
//! 1. If no cluster is configured (`shared.metadata_raft` not
//!    installed), returns `ProposeOutcome::LocalOnly`. The caller's
//!    single-node direct-write path stays authoritative.
//! 2. If this node is the metadata-group leader, proposes the
//!    entry, blocks until its local applied watermark reaches the
//!    assigned log index (5s default timeout), and returns the
//!    log index on success.
//! 3. If this node is NOT the leader, returns
//!    `Error::Config { detail: "metadata propose: not leader ..." }`.
//!    Gateway-side redirection will make this transparent.

pub mod catalog;
pub mod ddl_prepare;
pub mod handle;
pub mod replicated_entries;
pub mod timeouts;

pub use catalog::{propose_catalog_entry, propose_catalog_entry_with_timeout};
pub(crate) use ddl_prepare::{DdlPrepareGuard, acquire_ddl_prepare_lease};
pub use handle::{MetadataRaftHandle, RaftLoopProposerHandle};
pub use replicated_entries::{
    propose_surrogate_hwm, propose_surrogate_reserve, propose_sync_peer_bind,
    propose_sync_producer_fence, propose_sync_producer_register,
};
pub use timeouts::{DEFAULT_DRAIN_TIMEOUT, DEFAULT_PROPOSE_TIMEOUT};
