// SPDX-License-Identifier: BUSL-1.1

//! The Raft proposal handles `start_raft` installs together.

use std::sync::Arc;

/// Atomically installed Raft proposal handles.
pub(super) struct AsyncRaftProposerPair {
    pub(super) sequenced: Arc<crate::control::wal_replication::AsyncRaftProposer>,
    pub(super) raw: Arc<crate::control::wal_replication::AsyncRaftProposer>,
    /// The propose phase both proposers are built from.
    pub(super) submit: Arc<crate::control::wal_replication::AsyncRaftSubmit>,
}
