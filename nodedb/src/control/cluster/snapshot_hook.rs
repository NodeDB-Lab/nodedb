// SPDX-License-Identifier: BUSL-1.1

//! `RaftSnapshotQuarantineHook` — bridges cluster snapshot events to the
//! in-process `QuarantineRegistry`.

use std::sync::Arc;

use crate::storage::quarantine::SegmentKey;

/// `nodedb`-side implementation of [`nodedb_cluster::SnapshotQuarantineHook`].
///
/// A snapshot chunk is keyed by its group id and `last_included_index`.
pub(super) struct RaftSnapshotQuarantineHook {
    pub(super) registry: Arc<crate::storage::quarantine::QuarantineRegistry>,
}

fn chunk_key(group_id: u64, last_included_index: u64) -> SegmentKey {
    SegmentKey::Raft {
        group_id,
        last_included_index,
    }
}

impl nodedb_cluster::SnapshotQuarantineHook for RaftSnapshotQuarantineHook {
    fn is_quarantined(&self, group_id: u64, last_included_index: u64) -> bool {
        self.registry
            .is_quarantined(&chunk_key(group_id, last_included_index))
    }

    fn record_success(&self, group_id: u64, last_included_index: u64) {
        self.registry
            .record_success(&chunk_key(group_id, last_included_index));
    }

    fn record_failure(&self, group_id: u64, last_included_index: u64, error: &str) -> bool {
        // `record_failure` returns `Err(SegmentQuarantined)` on the second strike.
        self.registry
            .record_failure(chunk_key(group_id, last_included_index), error)
            .is_err()
    }
}
