// SPDX-License-Identifier: BUSL-1.1

//! Apply a group's committed entries: detect + apply conf-changes, dispatch
//! to the metadata or data applier, advance the applied watermark, flip the
//! boot-time readiness watch, and bump the cluster epoch on metadata-group
//! leadership acquisition.

use tracing::warn;

use crate::conf_change::ConfChange;
use crate::forward::PlanExecutor;

use super::super::loop_core::{CommitApplier, RaftLoop};

/// The first committed index that is not greater than its predecessor, when
/// the batch carries one.
///
/// Committed entries are contiguous and strictly ascending by construction, so
/// a non-increasing pair is a producer regression. The applier's delivery guard
/// stays the boundary that absorbs a repeat; this makes the regression
/// observable instead of silent.
fn first_non_increasing_committed_index(entries: &[nodedb_raft::LogEntry]) -> Option<u64> {
    entries
        .windows(2)
        .find(|pair| pair[1].index <= pair[0].index)
        .map(|pair| pair[1].index)
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Apply one group's committed entries from this tick's `Ready` output.
    /// Called only when `!group_ready.committed_entries.is_empty()`.
    pub(super) fn apply_group_commits(&self, group_id: u64, group_ready: &nodedb_raft::Ready) {
        if let Some(index) = first_non_increasing_committed_index(&group_ready.committed_entries) {
            warn!(
                group_id,
                index,
                "committed batch is not strictly increasing; the applier guard absorbs a repeat"
            );
        }
        for entry in &group_ready.committed_entries {
            if let Some(cc) = ConfChange::from_entry_data(&entry.data) {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                if let Err(e) = mr.apply_conf_change(group_id, &cc) {
                    warn!(group_id, error = %e, "failed to apply conf change");
                }
            }
        }

        let last_applied = if group_id == crate::metadata_group::METADATA_GROUP_ID {
            // Metadata group (0): dispatch to the metadata applier.
            // Raft no-op entries and conf-changes are already
            // handled above; data entries carry a serialized
            // `MetadataEntry` and are decoded by the applier.
            let pairs: Vec<(u64, Vec<u8>)> = group_ready
                .committed_entries
                .iter()
                .filter(|e| ConfChange::from_entry_data(&e.data).is_none())
                .map(|e| (e.index, e.data.clone()))
                .collect();
            // The applied cluster epoch advances here, in the cluster crate,
            // rather than inside whichever applier the host installed: the
            // epoch is what this node stamps on its own frames, so it must
            // move on every node that applies the entry, not only on nodes
            // whose host applier happens to know about it.
            self.adopt_committed_cluster_epochs(&pairs);
            self.metadata_applier.apply(&pairs)
        } else {
            self.applier
                .apply_committed(group_id, &group_ready.committed_entries)
        };
        if last_applied > 0 {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(e) = mr.advance_applied(group_id, last_applied) {
                warn!(group_id, error = %e, "failed to advance applied index");
            } else if group_id == crate::metadata_group::METADATA_GROUP_ID {
                // Metadata group: the metadata applier
                // applied entries synchronously to redb
                // before returning, so the apply
                // watermark is data-visible at this
                // point. Bump the watcher.
                //
                // Data groups are NOT bumped here — for
                // them `applier.apply_committed` only
                // enqueues entries onto the
                // `DistributedApplier` channel; the
                // actual data lands in storage when
                // `run_apply_loop` finishes the
                // SPSC round-trip to the Data Plane.
                // The host crate bumps the watcher
                // there, so the watermark always means
                // "data visible on this node up to
                // index N" regardless of which group.
                //
                // Snapshot-install path also bumps
                // (in `super::handle_rpc`) — covers
                // jump-on-snapshot for both group
                // kinds.
                self.group_watchers.bump(group_id, last_applied);
            }
        }

        // Boot-time readiness: the first time the metadata
        // group (0) applies any entry on this node — which
        // is the leader-election no-op or a replayed entry
        // — flip the ready watch. The host crate's
        // `start_raft` returns the receiver; `main.rs`
        // awaits it before binding client-facing
        // listeners. Idempotent: subsequent ticks are a
        // no-op once the latch is set.
        if group_id == crate::metadata_group::METADATA_GROUP_ID && !*self.ready_watch.borrow() {
            let _ = self.ready_watch.send(true);
        }

        // On acquiring metadata-group leadership, PROPOSE a new cluster
        // generation rather than bumping a local counter. Going through the
        // log is what makes the epoch an agreed fact: every node advances by
        // applying the same committed entry, so no node has to infer the
        // generation from stamps it overheard on the wire. This node's own
        // applied epoch moves in the applier like everyone else's, not here.
        if group_id == crate::metadata_group::METADATA_GROUP_ID {
            let is_leader = self
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .group_role_is_leader(group_id);
            let was_leader = self
                .prev_metadata_leader
                .swap(is_leader, std::sync::atomic::Ordering::AcqRel);
            if is_leader && !was_leader {
                self.propose_cluster_epoch_bump();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raft::LogEntry;

    fn entry(index: u64) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            data: Vec::new(),
        }
    }

    #[test]
    fn detects_a_non_increasing_committed_index() {
        assert_eq!(first_non_increasing_committed_index(&[]), None);
        assert_eq!(first_non_increasing_committed_index(&[entry(1)]), None);
        assert_eq!(
            first_non_increasing_committed_index(&[entry(1), entry(2)]),
            None
        );
        // A repeat inside the batch: the first index not greater than its
        // predecessor is reported.
        assert_eq!(
            first_non_increasing_committed_index(&[entry(2), entry(2), entry(3)]),
            Some(2)
        );
        assert_eq!(
            first_non_increasing_committed_index(&[entry(1), entry(2), entry(1)]),
            Some(1)
        );
    }
}
