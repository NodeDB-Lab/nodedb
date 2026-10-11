// SPDX-License-Identifier: BUSL-1.1

//! Apply a group's committed entries: detect + apply conf-changes, hand the
//! metadata group's entries to the metadata lane or a data group's to the
//! data applier, advance the applied watermark, and bump the cluster epoch
//! on metadata-group leadership acquisition.

use std::sync::Mutex;

use nodedb_raft::LogEntry;
use tracing::{debug, error, warn};

use crate::conf_change::ConfChange;
use crate::forward::PlanExecutor;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::apply_gate::ApplyPermit;

use super::super::loop_core::{CommitApplier, RaftLoop};

/// The first committed index that is not greater than its predecessor, when
/// the batch carries one.
///
/// Committed entries are contiguous and strictly ascending by construction, so
/// a non-increasing pair is a producer regression. The applier's delivery guard
/// stays the boundary that absorbs a repeat; this makes the regression
/// observable instead of silent.
fn first_non_increasing_committed_index(entries: &[LogEntry]) -> Option<u64> {
    entries
        .windows(2)
        .find(|pair| pair[1].index <= pair[0].index)
        .map(|pair| pair[1].index)
}

/// Admit `batch` for apply: take the group's apply gate, then return the
/// entries above the group's `last_applied`.
///
/// The batch leaves `Ready` before the apply runs. A snapshot adopted since
/// raises `last_applied`, and the entries it covers must not apply on top of
/// it. The permit keeps a new install from starting until the caller has
/// handed the entries to the applier. `None` when an install holds or awaits
/// the gate: the batch then goes back to `Ready` for the next tick. An
/// unmounted group applies the batch as taken.
fn admit_batch<'a>(
    multi_raft: &Mutex<MultiRaft>,
    group_id: u64,
    batch: &'a [LogEntry],
) -> Option<(ApplyPermit, &'a [LogEntry])> {
    let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
    let Some(permit) = mr.apply_gates().try_apply(group_id) else {
        if let Err(e) = mr.requeue_committed(group_id, batch.to_vec()) {
            warn!(group_id, error = %e, "failed to requeue a batch deferred by a snapshot install");
        }
        return None;
    };
    let entries = match mr.last_applied(group_id) {
        Some(applied) => {
            let start = batch
                .iter()
                .position(|entry| entry.index > applied)
                .unwrap_or(batch.len());
            &batch[start..]
        }
        None => batch,
    };
    Some((permit, entries))
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Log a committed range the group's log no longer holds.
    ///
    /// Apply for the group halts: the missing entries exist only in a
    /// snapshot. The node rejects `AppendEntries` at its applied index until
    /// the leader installs one.
    pub(super) fn surface_committed_read_error(&self, group_id: u64, err: &nodedb_raft::RaftError) {
        let last_applied = self
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_applied(group_id);
        error!(
            group_id,
            ?last_applied,
            error = %err,
            "committed entries are below the retained log; apply is halted until a snapshot covers the gap"
        );
    }

    /// Apply the conf changes among `entries` to this node's Raft and
    /// routing view. Returns whether the batch goes on to the applier now.
    ///
    /// A data group's durable applied floor passes a conf change once a
    /// later entry applies, and a restart never delivers that entry again.
    /// So no entry of the batch reaches the applier before the routing table
    /// the change produced is durable. A restart then mounts the group with
    /// the membership the change left.
    ///
    /// The tick never waits on the save. It applies the changes in memory
    /// once, asks the routing persister for a save, and puts the batch back
    /// in `Ready`. Each later tick finds the batch again and lets it go on
    /// once the save is durable. The persister retries a failed save, and
    /// the batch waits meanwhile.
    ///
    /// The metadata group's changes apply here too. The metadata lane saves
    /// the routing table before its applied floor moves.
    fn apply_conf_changes(&self, group_id: u64, entries: &[LogEntry]) -> bool {
        let is_data_group = group_id != crate::metadata_group::METADATA_GROUP_ID;
        let persister = self.routing_persister.as_ref().filter(|_| is_data_group);
        let waiting = persister.and_then(|_| self.tick_state.conf_save(group_id));
        let applied_through = waiting.map_or(0, |(through, _)| through);
        let mut last_change = None;
        for entry in entries {
            let Some(cc) = ConfChange::from_entry_data(&entry.data) else {
                continue;
            };
            last_change = Some(entry.index);
            if entry.index <= applied_through {
                continue;
            }
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(e) = mr.apply_conf_change(group_id, &cc) {
                warn!(group_id, error = %e, "failed to apply conf change");
            }
        }
        let (Some(persister), Some(through)) = (persister, last_change) else {
            return true;
        };
        let seq = match waiting {
            Some((waited_through, seq)) if waited_through >= through => seq,
            _ => {
                let seq = persister.request();
                self.tick_state.set_conf_save(group_id, through, seq);
                seq
            }
        };
        if persister.is_durable(seq) {
            self.tick_state.clear_conf_save(group_id);
            return true;
        }
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = mr.requeue_committed(group_id, entries.to_vec()) {
            warn!(group_id, error = %e, "failed to requeue a batch whose conf change is not saved");
        }
        false
    }

    /// Apply one group's committed entries from this tick's `Ready` output.
    /// Called only when `!group_ready.committed_entries.is_empty()`.
    ///
    /// Never waits for an apply. A data group's applier only enqueues. The
    /// metadata group's entries go to the metadata lane (see
    /// [`super::metadata_lane`]), which applies them in order off the tick
    /// and reports the applied index back to Raft.
    pub(super) fn apply_group_commits(&self, group_id: u64, group_ready: &nodedb_raft::Ready) {
        let batch = &group_ready.committed_entries;
        if let Some(index) = first_non_increasing_committed_index(batch) {
            warn!(
                group_id,
                index,
                "committed batch is not strictly increasing; the applier guard absorbs a repeat"
            );
        }
        // Held until this function returns: from the floor check through the
        // applier call and the watermark advance.
        let Some((_permit, entries)) = admit_batch(&self.multi_raft, group_id, batch) else {
            debug!(
                group_id,
                "snapshot install in progress; committed batch deferred"
            );
            return;
        };
        if entries.len() < batch.len() {
            debug!(
                group_id,
                skipped = batch.len() - entries.len(),
                "skipped committed entries an installed snapshot already covers"
            );
        }
        if !self.apply_conf_changes(group_id, entries) {
            return;
        }

        let last_applied = if entries.is_empty() {
            0
        } else if group_id == crate::metadata_group::METADATA_GROUP_ID {
            // Metadata group (0): the lane applies the entries, with cluster
            // epochs adopted in log order and the durable applied floor
            // saved before the watcher moves. Entries the lane cannot take
            // go back to `Ready` for the next tick.
            let back = self.tick_state.send_to_metadata_lane(entries);
            if !back.is_empty() {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                if let Err(e) = mr.requeue_committed(group_id, back) {
                    warn!(group_id, error = %e, "failed to requeue metadata entries");
                }
            }
            0
        } else {
            self.applier.apply_committed(group_id, entries)
        };
        if last_applied > 0 {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(e) = mr.advance_applied(group_id, last_applied) {
                warn!(group_id, error = %e, "failed to advance applied index");
            } else if group_id == crate::calvin::SEQUENCER_GROUP_ID {
                // Sequencer group: the host applies each entry to the
                // sequencer state machine inline, before returning. The
                // watcher is the sequencer's applied index that
                // authorization lease coverage reports.
                //
                // Data groups are not bumped here. `apply_committed` only
                // hands their entries to the host apply loop, which bumps
                // the watcher once the data is visible on this node. The
                // metadata lane bumps the metadata group's watcher. A
                // snapshot install bumps every group whose state it
                // restored (see `super::super::handle_rpc`).
                self.group_watchers.bump(group_id, last_applied);
            }
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
    use std::time::{Duration, Instant};

    use super::*;
    use crate::routing::RoutingTable;

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

    fn indices(entries: &[LogEntry]) -> Vec<u64> {
        entries.iter().map(|e| e.index).collect()
    }

    /// A single-voter group 1 with three committed entries taken from
    /// `Ready` and not yet applied.
    fn group_with_taken_batch(dir: &tempfile::TempDir) -> (MultiRaft, Vec<LogEntry>) {
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(1, vec![]).expect("mount group 1");
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        // Elect, then deliver the no-op once the disk holds it.
        mr.tick().expect("tick");
        mr.wait_all_durable_blocking();
        for (gid, ready) in mr.tick().expect("tick").groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).expect("advance");
            }
        }
        for _ in 0..3 {
            mr.propose(0, b"write".to_vec())
                .expect("single voter commits");
        }
        mr.wait_all_durable_blocking();
        let batch: Vec<LogEntry> = mr
            .tick()
            .expect("tick")
            .groups
            .into_iter()
            .find(|(gid, _)| *gid == 1)
            .map(|(_, ready)| ready.committed_entries)
            .unwrap_or_default();
        assert_eq!(batch.len(), 3, "batch: {:?}", indices(&batch));
        (mr, batch)
    }

    /// A batch taken before a snapshot install skips the entries the
    /// snapshot covers and applies only the rest.
    #[test]
    fn batch_taken_before_install_skips_snapshot_covered_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut mr, batch) = group_with_taken_batch(&dir);

        // The snapshot lands between taking the batch and applying it.
        mr.adopt_snapshot_boundary(1, batch[1].index, batch[1].term)
            .expect("adopt snapshot");
        let mr = Mutex::new(mr);

        let (_permit, applying) = admit_batch(&mr, 1, &batch).expect("gate open");
        assert_eq!(indices(applying), vec![batch[2].index]);
    }

    /// While an install holds the gate, the tick applies nothing and the
    /// batch returns to `Ready`. After the release it applies the entries
    /// the install did not cover.
    #[tokio::test]
    async fn batch_is_deferred_while_an_install_holds_the_gate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mr, batch) = group_with_taken_batch(&dir);
        let gates = mr.apply_gates();
        let mr = Mutex::new(mr);

        let mut install = gates.install(1).await;
        assert!(admit_batch(&mr, 1, &batch).is_none());
        {
            let mut guard = mr.lock().unwrap_or_else(|p| p.into_inner());
            guard
                .adopt_snapshot_boundary(1, batch[0].index, batch[0].term)
                .expect("adopt snapshot");
        }
        install.adopted(batch[0].index);
        drop(install);

        let requeued: Vec<LogEntry> = mr
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tick()
            .expect("tick")
            .groups
            .into_iter()
            .find(|(gid, _)| *gid == 1)
            .map(|(_, ready)| ready.committed_entries)
            .unwrap_or_default();
        assert_eq!(indices(&requeued), indices(&batch[1..]));

        let (_permit, applying) = admit_batch(&mr, 1, &requeued).expect("gate open");
        assert_eq!(indices(applying), indices(&batch[1..]));
    }
}
