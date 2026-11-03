// SPDX-License-Identifier: BUSL-1.1

//! Single tick of the Raft event loop.
//!
//! Ordering (each phase uses a short-lived `MultiRaft` lock acquisition —
//! the lock is never held across an `.await`):
//!
//! 1. **Tick Raft groups**: drive election timeouts / heartbeats and pull
//!    `MultiRaftReady` output (messages, vote requests, committed
//!    entries, snapshot-needed peers).
//! 2. **Dispatch AppendEntries**: one background task per peer, batching
//!    all messages targeting the same peer (see [`super::dispatch_outbound`]).
//! 3. **Dispatch RequestVote**: same batching strategy.
//! 4. **Apply committed entries**: feed them to the user-supplied
//!    `CommitApplier`, or hand group 0's entries to the metadata lane, which
//!    applies them off the tick (see [`super::metadata_lane`]). The tick
//!    never waits for an apply. Conf-change entries are detected and applied to
//!    `MultiRaft` before the user applier sees them (see
//!    [`super::apply_committed`]).
//! 5. **Install snapshots**: send `InstallSnapshot` RPCs to peers that
//!    have fallen behind the leader's snapshot boundary (see
//!    [`super::snapshot_dispatch`]).
//! 6. **Converge entering learners**: for each group this node leads,
//!    propose `AddLearner` for placement nodes not yet present as
//!    voters or learners. Re-proposals while a conf-change is pending
//!    are harmlessly rejected by Raft and retried next tick.
//! 7. **Promote caught-up learners**: for every group where this node is
//!    leader, query learners whose `match_index >= commit_index` and
//!    propose `PromoteLearner` for each. Idempotent by design — after
//!    the first promotion the peer has moved from `learners` to
//!    `members` and won't be returned again.
//! 8. **Remove leaving voters**: for each group this node leads, propose
//!    `RemoveNode` for committed voters no longer in the placement set —
//!    capped so the group never drops below RF committed voters, one at a
//!    time, and never removing the group leader.
//! 9. **Remove leaving learners**: for each group this node leads that has an
//!    authored placement set, propose `RemoveLearner` for non-voting learners
//!    not in the placement. Inert while placement is `None` (bootstrap window).
//! 10. **Unmount left groups**: drop the replica of a data group this node
//!     was removed from and is not placed in (see [`super::super::group_unmount`]).
//! 11. **Throttled passes**: placement reconcile, leader balance (see
//!     [`super::super::leader_balance`]), the unhosted-group leader probe
//!     (see [`super::leader_probe`]), orphan snapshot GC, and lease GC.

use tracing::{debug, error};

use crate::forward::PlanExecutor;

use super::super::loop_core::{CommitApplier, RaftLoop};

/// Ticks between placement-reconcile passes (100 ticks × ~10ms ≈ 1s).
///
/// `SetPlacement` is a normal metadata entry, not a conf-change, so Raft does
/// not dedup per-tick re-proposals before they commit. Running reconcile ~1s
/// apart is long enough that a proposed `SetPlacement` commits+applies before
/// the next diff, so an unchanged target never re-proposes.
const PLACEMENT_RECONCILE_TICK_INTERVAL: u64 = 100;

/// Orphaned-partial-snapshot GC runs far less often than placement reconcile:
/// once every ~60s (6000 ticks at the 10ms default tick interval). Stale
/// `.partial` files only appear from crashed/interrupted snapshot transfers,
/// so a minute of latency before reclaiming their disk space is ample, and a
/// directory scan every tick would be wasteful. Like placement reconcile this
/// is throttled by tick count, not wall-clock, so a faster test tick simply
/// sweeps proportionally more often (harmless).
const ORPHAN_PARTIAL_GC_TICK_INTERVAL: u64 = 6000;

/// Lease GC for nodes that left the topology: every 200 ticks (~2s at the
/// 10ms default tick). Cheaper than placement reconcile (single map scan),
/// and the Leave apply hook usually beats it to the release — this sweep
/// only catches cases the hook missed.
const LEASE_GC_TICK_INTERVAL: u64 = 200;

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Drive every group without advancing its clock: count the entries the
    /// disks made durable, dispatch the messages that produces, and apply the
    /// entries that commit. Runs when a disk finishes a batch, so a commit
    /// does not wait for the next tick.
    pub(in crate::raft_loop) async fn do_drive(&self) {
        let ready = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.drive() {
                Ok(ready) => ready,
                Err(e) => {
                    error!(
                        error = %e,
                        "raft drive failed to stage hard state; \
                         skipping message/vote dispatch"
                    );
                    return;
                }
            }
        };
        self.handle_ready(ready);
    }

    /// Route one collection of group output: the leader hints, the outbound
    /// messages, the committed entries and the snapshot requests.
    fn handle_ready(&self, ready: crate::multi_raft::MultiRaftReady) {
        // Every leader change this node's Raft saw since the last collection,
        // including its own election and a leader learned from an
        // AppendEntries or heartbeat, reaches the routing table's leader hint.
        self.sync_leader_hints();

        if ready.is_empty() {
            return;
        }
        self.dispatch_outbound_messages(&ready.groups);

        // Apply committed entries and conf-changes, then dispatch any needed
        // install-snapshot RPCs, per group.
        for (group_id, group_ready) in ready.groups {
            if let Some(err) = &group_ready.committed_read_error {
                self.surface_committed_read_error(group_id, err);
            }
            if !group_ready.committed_entries.is_empty() {
                self.apply_group_commits(group_id, &group_ready);
            }
            if !group_ready.snapshots_needed.is_empty() {
                self.dispatch_group_snapshots(group_id, group_ready.snapshots_needed);
            }
        }
    }

    /// Execute a single tick: drive Raft, dispatch outbound messages,
    /// apply commits, promote caught-up learners.
    pub(in crate::raft_loop) async fn do_tick(&self) {
        // Tick under lock and extract Ready. `tick` durably persists any
        // HardState staged this tick (election term bump + self-vote) before
        // returning the vote requests it carries. A persist failure is a
        // split-brain hazard: dispatching vote requests for a term that was
        // not made durable is exactly the double-vote bug. Handle it loud and
        // skip this tick's dispatch entirely — the next tick retries.
        let ready = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.tick() {
                Ok(ready) => ready,
                Err(e) => {
                    error!(
                        error = %e,
                        "raft tick failed to stage hard state; \
                         skipping message/vote dispatch for this tick"
                    );
                    return;
                }
            }
        };

        // Dispatch outgoing messages and persist log/HardState first (even if
        // ready looks "empty" we still want to run the learner-promotion step
        // each tick so a just-caught-up learner is promoted promptly).
        self.handle_ready(ready);

        // Mount a local replica for any group this node should host (per the
        // replicated placement set) but does not yet — the recovery path for a
        // group whose join-time AddLearner(self) was deferred. Runs before the
        // convergence phases so a freshly-mounted group is visible to the rest
        // of the pipeline on the same tick. At most one mount per tick.
        self.mount_entering_groups();

        // Add placement nodes as learners so `promote_ready_learners` can
        // pick them up once they catch up. Re-proposals while a conf-change
        // is already pending are harmlessly rejected by Raft. Runs every tick.
        self.converge_entering_learners();

        // Promote caught-up learners. Runs every tick so a
        // just-caught-up learner is promoted within one tick interval of
        // catching up. Idempotent: once promoted, the peer is in
        // `members` and `ready_learners` no longer returns it.
        self.promote_ready_learners();

        // Step aside before the leaving-voter removal below: if this node is the
        // leader yet placement says it must leave, transfer leadership to an
        // in-placement voter so a later tick can remove this node cleanly. Runs
        // after promotion (so a freshly-promoted voter can be a target) and
        // before `converge_leaving_voters` (whose leader-skip defers self-removal
        // until this transfer takes effect).
        self.transfer_leadership_for_leaving_voters();

        // Remove committed voters no longer in a group's placement set, once
        // their replacement has been promoted (RF floor on committed voters).
        // At most one removal per group per pass; never removes the leader.
        // Re-proposals while a conf-change is pending are harmlessly rejected.
        self.converge_leaving_voters();

        // Remove non-voting learners not in the group's placement set. Runs
        // after converge_leaving_voters so voter and learner convergence steps
        // share the same tick ordering (voters first, learners after). Inert
        // while placement is None (bootstrap window — guard is inside the fn).
        self.converge_leaving_learners();

        // Drop the replica of every data group this node has left, once its
        // own removal applied. A node outside a group's placement then hosts
        // no replica of it.
        self.unmount_left_groups();

        // Placement reconcile is throttled well above the tick rate: SetPlacement
        // is a normal metadata entry (not a conf-change), so Raft would not dedup
        // per-tick re-proposals before they commit. Running ~1s apart lets each
        // proposal apply before the next diff.
        let tick = self
            .tick_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if tick.is_multiple_of(PLACEMENT_RECONCILE_TICK_INTERVAL) {
            self.reconcile_placement();
        }
        // Move each data group's leadership to its preferred leader. The
        // bootstrap node leads every group otherwise.
        let balance_interval = super::super::leader_balance::LEADER_BALANCE_TICK_INTERVAL;
        if tick.is_multiple_of(balance_interval) {
            self.balance_leadership(tick / balance_interval);
        }
        // Find the leader of an unhosted data group whose hint names none.
        let probe_interval = super::leader_probe::LEADER_PROBE_TICK_INTERVAL;
        if tick.is_multiple_of(probe_interval) {
            self.probe_unhosted_leaders(tick / probe_interval);
        }
        if tick.is_multiple_of(ORPHAN_PARTIAL_GC_TICK_INTERVAL)
            && let Some(ref dir) = self.data_dir
        {
            match crate::install_snapshot::gc::sweep_orphans(dir, self.orphan_partial_max_age_secs)
            {
                Ok((removed, errs)) => {
                    if removed > 0 {
                        tracing::info!(
                            removed,
                            "periodic gc: removed orphaned partial snapshot files"
                        );
                    }
                    for e in errs {
                        tracing::warn!(error = %e, "periodic gc: partial snapshot GC error");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "periodic gc: failed to sweep partial snapshot directory");
                }
            }
        }
        if tick.is_multiple_of(LEASE_GC_TICK_INTERVAL) {
            self.gc_stale_node_leases();
        }
    }

    /// For every group where this node is leader, propose
    /// `PromoteLearner` for every learner whose `match_index` has
    /// reached the current `commit_index` and whose node-id appears in
    /// the group's placement set (when one is configured).
    ///
    /// This is the automated second phase of the two-step Raft single-
    /// server add. The first phase (`AddLearner`) is proposed by the
    /// join flow; the second phase is proposed here, asynchronously,
    /// once the learner has caught up.
    fn promote_ready_learners(&self) {
        // Snapshot the work list under one lock acquisition.
        let promotions: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            group_ids
                .into_iter()
                .flat_map(|gid| {
                    // Placement set for this group (None = promote all — unchanged behavior).
                    let placement: Option<Vec<u64>> = mr
                        .routing()
                        .read()
                        .unwrap_or_else(|p| p.into_inner())
                        .group_info(gid)
                        .and_then(|info| info.placement.clone());
                    mr.ready_learners(gid)
                        .into_iter()
                        .filter(move |&learner| {
                            should_promote_learner(placement.as_deref(), learner)
                        })
                        .map(move |learner| (gid, learner))
                })
                .collect()
        };

        // Propose each promotion in its own lock acquisition. If any fails
        // (e.g., `NotLeader` after a step-down between phases), log and
        // move on — the next tick will retry any still-pending learner.
        for (group_id, learner_id) in promotions {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let change = crate::conf_change::ConfChange {
                change_type: crate::conf_change::ConfChangeType::PromoteLearner,
                node_id: learner_id,
            };
            match mr.propose_conf_change(group_id, &change) {
                Ok((_gid, idx)) => {
                    debug!(
                        group_id,
                        learner_id,
                        log_index = idx,
                        "proposed learner promotion"
                    );
                }
                Err(e) => {
                    debug!(
                        group_id,
                        learner_id,
                        error = %e,
                        "learner promotion proposal deferred"
                    );
                }
            }
        }
    }
}

/// Decide whether a caught-up learner may be promoted to voter.
///
/// `None` placement means no explicit placement set for the group —
/// all caught-up learners are promoted (unchanged behavior).
/// `Some(set)` means only learners whose node-id appears in the
/// intended voter set are promoted; others remain learners until the
/// membership reconciler acts on them.
fn should_promote_learner(placement: Option<&[u64]>, learner_id: u64) -> bool {
    match placement {
        None => true,
        Some(set) => set.contains(&learner_id),
    }
}

#[cfg(test)]
mod tests {
    use super::should_promote_learner;

    #[test]
    fn no_placement_always_promotes() {
        assert!(should_promote_learner(None, 1));
        assert!(should_promote_learner(None, 42));
    }

    #[test]
    fn placement_contains_learner_promotes() {
        assert!(should_promote_learner(Some(&[1, 2, 3]), 2));
    }

    #[test]
    fn placement_missing_learner_holds() {
        assert!(!should_promote_learner(Some(&[1, 2, 3]), 99));
    }

    #[test]
    fn empty_placement_promotes_nobody() {
        assert!(!should_promote_learner(Some(&[]), 1));
        assert!(!should_promote_learner(Some(&[]), 42));
    }
}
