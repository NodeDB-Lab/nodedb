// SPDX-License-Identifier: BUSL-1.1

//! This node's state of the replicated descriptor preparation lease.

use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use crate::control::metadata_proposer::DdlPrepareOwner;

/// This node's state of the replicated descriptor preparation lease.
pub struct MetadataDdlState {
    /// Serializes this node's attempts to acquire the replicated descriptor
    /// preparation lease, and a local DDL through its post-apply. An async
    /// proposer holds it across the post-apply, so it is a tokio mutex.
    pub lock: tokio::sync::Mutex<()>,
    /// Replicated preparation owner: its token, its node, and the local
    /// monotonic apply time. The token and node are persisted in
    /// `SystemCatalog` and seeded at boot.
    pub owner: Mutex<Option<DdlPrepareOwner>>,
    /// Most recent fenced DDL token applied while its owner remained current.
    /// Starts at 0 on boot.
    pub applied_token: AtomicU64,
    /// Per-node uniqueness component for descriptor-preparation lease tokens.
    /// Starts at 1.
    pub token_seq: AtomicU64,
}

impl MetadataDdlState {
    pub fn new() -> Self {
        Self {
            lock: tokio::sync::Mutex::new(()),
            owner: Mutex::new(None),
            applied_token: AtomicU64::new(0),
            token_seq: AtomicU64::new(1),
        }
    }
}

impl Default for MetadataDdlState {
    fn default() -> Self {
        Self::new()
    }
}
