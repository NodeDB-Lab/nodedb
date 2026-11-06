// SPDX-License-Identifier: BUSL-1.1

//! Data-group Raft recovery gate.
//!
//! Writes to replicable collections are made durable as `ReplicatedEntry`
//! records in their data group's Raft log, and the engine state they produce
//! (KV hash index, columnar/timeseries/spatial in-memory state, graph node
//! labels, …) is rebuilt only by re-applying that log into the Data Plane.
//!
//! Raft's `commit_index` is volatile: after a restart every locally hosted
//! group must win its own randomized election before it commits a no-op and
//! carries its retained log forward to the applier. Those elections run
//! independently of the metadata group's, so waiting only on metadata
//! readiness lets the gateway open while a data group has replayed nothing — an
//! acknowledged write then reads back as if it never happened, with no error to
//! explain it.
//!
//! This gate closes that window: startup is held until every locally hosted
//! data group has elected a leader, committed through the end of its retained
//! log, and applied that log on this node. It fails closed: a wait that makes
//! no progress for the configured bound aborts startup rather than opening a
//! gateway that would serve incomplete state. A replay that keeps applying
//! entries keeps the wait open, however long the backlog is.
//!
//! A group whose replica here awaits a snapshot install does not hold startup.
//! Its data diverged from the log, so it applies nothing until a snapshot
//! installs, and its reads are refused until then.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_raft::ReplicaState;
use nodedb_types::config::tuning::DataGroupRecoveryTimeout;
use tracing::{debug, info, warn};

use crate::bootstrap::stall_clock::StallClock;
use crate::control::state::SharedState;

/// How often the Raft status snapshot is re-read while waiting. Elections take
/// seconds, so a coarse poll costs nothing and avoids a busy loop.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// True when `group_id` names a data group whose log carries user writes that
/// must be replayed into the Data Plane before queries are served.
///
/// The metadata group is covered by its own readiness wait earlier in startup,
/// and the Calvin sequencer group carries transaction ordering rather than
/// engine state, so neither participates in this gate.
fn is_data_group(group_id: u64) -> bool {
    group_id != nodedb_cluster::METADATA_GROUP_ID
        && group_id != nodedb_cluster::calvin::SEQUENCER_GROUP_ID
}

/// Why a data group is not yet recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupWait {
    /// No leader yet — the post-restart election is still running, so the group
    /// has not begun carrying its log forward.
    NoLeader,
    /// A leader exists but has not yet committed through the end of the
    /// retained log.
    LogUncommitted {
        commit_index: u64,
        last_log_index: u64,
    },
    /// The committed log is known, but this node has not applied all of it.
    ReplayLagging {
        commit_index: u64,
        host_applied: u64,
    },
}

/// Where one data group stands against the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupRecovery {
    /// Applied on this node through its committed log.
    Replayed,
    /// Its replica here awaits a snapshot install. It applies nothing until
    /// one installs and serves no reads meanwhile, so it does not hold
    /// startup.
    AwaitingSnapshot,
    /// Still holding startup.
    Pending(GroupWait),
}

/// One group's progress, from its Raft status and this node's watermark.
#[derive(Debug, Clone, Copy)]
struct GroupProgress {
    leader_id: u64,
    commit_index: u64,
    last_log_index: u64,
    /// The highest index whose apply concluded on this node: the watermark
    /// reads wait on.
    host_applied: u64,
    replica_state: ReplicaState,
}

/// Recovery verdict for one group.
///
/// The progress signal is the host watermark, not Raft's `last_applied`. Raft
/// advances `last_applied` when it hands an entry to the apply loop, before
/// the entry's apply concludes. Reads wait on the host watermark, so a gate on
/// `last_applied` opens while replay still runs. The election no-op reaches
/// the apply loop and concludes like any entry, so the host watermark reaches
/// `commit_index`.
///
/// `commit_index` alone is not a sufficient target either. It is volatile and
/// starts at 0 after a restart, and Raft publishes `leader_id` before the new
/// leader commits the no-op that carries the retained log forward. Treating
/// `commit_index == 0` as "caught up" would report recovered inside that window
/// and open the gateway against an unreplayed log — the exact race this gate
/// exists to close. `last_log_index` is recovered from the durable on-disk log,
/// so requiring the leader to commit through it first pins the target before
/// progress is compared.
///
/// Once `commit_index >= last_log_index` the target is static while the gateway
/// is closed — no new entries can be proposed — so this converges rather than
/// chasing a moving head.
fn group_recovery(progress: GroupProgress) -> GroupRecovery {
    // Checked first: such a replica never leads, so a group it holds alone
    // never elects.
    if progress.replica_state == ReplicaState::AwaitingSnapshot {
        return GroupRecovery::AwaitingSnapshot;
    }
    if progress.leader_id == 0 {
        return GroupRecovery::Pending(GroupWait::NoLeader);
    }
    // Empty log: nothing was ever written to this group, so there is nothing to
    // replay once it has a leader.
    if progress.last_log_index == 0 {
        return GroupRecovery::Replayed;
    }
    if progress.commit_index < progress.last_log_index {
        return GroupRecovery::Pending(GroupWait::LogUncommitted {
            commit_index: progress.commit_index,
            last_log_index: progress.last_log_index,
        });
    }
    if progress.host_applied >= progress.commit_index {
        return GroupRecovery::Replayed;
    }
    GroupRecovery::Pending(GroupWait::ReplayLagging {
        commit_index: progress.commit_index,
        host_applied: progress.host_applied,
    })
}

/// One group still holding startup, with the detail needed for diagnostics.
#[derive(Debug, Clone, Copy)]
struct PendingGroup {
    group_id: u64,
    wait: GroupWait,
    progress: GroupProgress,
}

impl PendingGroup {
    /// The measures the stall clock reads: whether the group has a leader,
    /// how far its log is committed, and how far this node applied it.
    fn measures(&self) -> [u64; 3] {
        [
            u64::from(self.progress.leader_id != 0),
            self.progress.commit_index,
            self.progress.host_applied,
        ]
    }
}

impl std::fmt::Display for PendingGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.wait {
            GroupWait::NoLeader => write!(f, "group {} has no leader", self.group_id),
            GroupWait::LogUncommitted {
                commit_index,
                last_log_index,
            } => write!(
                f,
                "group {} committed {commit_index} of {last_log_index} retained entries",
                self.group_id
            ),
            GroupWait::ReplayLagging {
                commit_index,
                host_applied,
            } => write!(
                f,
                "group {} applied {host_applied} of {commit_index} committed entries on this node",
                self.group_id
            ),
        }
    }
}

/// The data groups still holding startup, and the groups awaiting a snapshot.
#[derive(Debug, Default)]
struct GateView {
    pending: Vec<PendingGroup>,
    awaiting_snapshot: Vec<u64>,
}

/// Sort every data group in `statuses` against the gate. `host_applied`
/// reads this node's watermark of a group.
fn gate_view(
    statuses: Vec<nodedb_cluster::GroupStatus>,
    host_applied: impl Fn(u64) -> u64,
) -> GateView {
    let mut view = GateView::default();
    for status in statuses.into_iter().filter(|s| is_data_group(s.group_id)) {
        let progress = GroupProgress {
            leader_id: status.leader_id,
            commit_index: status.commit_index,
            last_log_index: status.last_log_index,
            host_applied: host_applied(status.group_id),
            replica_state: status.replica_state,
        };
        match group_recovery(progress) {
            GroupRecovery::Replayed => {}
            GroupRecovery::AwaitingSnapshot => view.awaiting_snapshot.push(status.group_id),
            GroupRecovery::Pending(wait) => view.pending.push(PendingGroup {
                group_id: status.group_id,
                wait,
                progress,
            }),
        }
    }
    view
}

/// Hold startup until every locally hosted data group has replayed its retained
/// Raft log on this node. A group awaiting a snapshot install does not hold it.
/// On success it sets `SharedState::data_groups_replayed`.
///
/// `stall_timeout` bounds how long the wait can go without progress. Progress
/// is a pending group electing a leader, committing more of its log, applying
/// more of it on this node, or finishing. A group that cannot elect or cannot
/// apply fails the wait. A group that keeps applying never does.
///
/// Refuses when `start_raft` installed no Raft status source.
pub async fn await_data_group_recovery(
    shared: &Arc<SharedState>,
    stall_timeout: DataGroupRecoveryTimeout,
) -> anyhow::Result<()> {
    let Some(status_fn) = shared.raft_status_fn.get() else {
        anyhow::bail!("data group recovery: no Raft status source: start_raft has not run");
    };
    let status_fn = Arc::clone(status_fn);
    let mut clock = StallClock::<3>::new(stall_timeout.0, Instant::now());

    loop {
        let view = gate_view(status_fn(), |group_id| {
            shared.applied_index_watcher(group_id).current()
        });
        if view.pending.is_empty() {
            for group_id in &view.awaiting_snapshot {
                warn!(
                    group_id,
                    "data raft group awaits a snapshot install on this node; it serves no reads \
                     until one installs"
                );
            }
            info!("all local data raft groups replayed — opening client gateway");
            shared
                .data_groups_replayed
                .store(true, std::sync::atomic::Ordering::Release);
            return Ok(());
        }

        let now = Instant::now();
        clock.observe(
            now,
            view.pending
                .iter()
                .map(|pending| (pending.group_id, pending.measures())),
        );
        if clock.stalled(now) {
            let detail = view
                .pending
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(anyhow::anyhow!(
                "data raft group recovery made no progress for {:?}: {detail}",
                clock.bound()
            ));
        }

        debug!(
            pending = view.pending.len(),
            "waiting for data raft group replay"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Run [`await_data_group_recovery`] in a loop on `shared`'s shutdown
/// watch until it succeeds, without holding the caller. A host that brings
/// nodes up one at a time uses it: a group here elects only once its peers
/// run. Each failed attempt is logged, and the next one starts at once.
pub fn spawn_data_group_recovery(
    shared: &Arc<SharedState>,
    stall_timeout: DataGroupRecoveryTimeout,
) {
    let task_shared = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "data_group_recovery",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_cancelled() => break,
                    recovered = await_data_group_recovery(&task_shared, stall_timeout) => match recovered {
                        Ok(()) => break,
                        Err(error) => warn!(%error, "data raft group recovery did not finish; retrying"),
                    },
                }
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(
        leader_id: u64,
        commit_index: u64,
        last_log_index: u64,
        host_applied: u64,
    ) -> GroupProgress {
        GroupProgress {
            leader_id,
            commit_index,
            last_log_index,
            host_applied,
            replica_state: ReplicaState::Whole,
        }
    }

    fn status(
        group_id: u64,
        raft_applied: u64,
        replica_state: ReplicaState,
    ) -> nodedb_cluster::GroupStatus {
        nodedb_cluster::GroupStatus {
            group_id,
            role: "Leader".into(),
            leader_id: 1,
            term: 2,
            commit_index: 12,
            last_applied: raft_applied,
            last_log_index: 12,
            snapshot_index: 0,
            retained_bytes: 0,
            replica_state,
            member_count: 1,
            learner_count: 0,
            vshard_count: 1,
        }
    }

    #[test]
    fn metadata_and_sequencer_groups_are_not_data_groups() {
        assert!(!is_data_group(nodedb_cluster::METADATA_GROUP_ID));
        assert!(!is_data_group(nodedb_cluster::calvin::SEQUENCER_GROUP_ID));
    }

    #[test]
    fn ordinary_group_ids_are_data_groups() {
        assert!(is_data_group(1));
        assert!(is_data_group(4_294_967_295));
    }

    #[test]
    fn leaderless_group_waits_for_election() {
        assert_eq!(
            group_recovery(progress(0, 0, 0, 0)),
            GroupRecovery::Pending(GroupWait::NoLeader)
        );
        assert_eq!(
            group_recovery(progress(0, 9, 9, 9)),
            GroupRecovery::Pending(GroupWait::NoLeader)
        );
    }

    #[test]
    fn empty_log_is_recovered_once_leader_exists() {
        assert_eq!(
            group_recovery(progress(1, 0, 0, 0)),
            GroupRecovery::Replayed
        );
    }

    /// The window this gate exists to close: a restart resets the volatile
    /// `commit_index` to 0, and Raft publishes `leader_id` before the new leader
    /// commits the no-op that carries the retained log forward. A group with a
    /// durable log must NOT be reported recovered here.
    #[test]
    fn fresh_leader_with_uncommitted_retained_log_waits() {
        assert_eq!(
            group_recovery(progress(1, 0, 12, 0)),
            GroupRecovery::Pending(GroupWait::LogUncommitted {
                commit_index: 0,
                last_log_index: 12,
            })
        );
    }

    #[test]
    fn partially_committed_retained_log_waits() {
        assert_eq!(
            group_recovery(progress(1, 5, 12, 5)),
            GroupRecovery::Pending(GroupWait::LogUncommitted {
                commit_index: 5,
                last_log_index: 12,
            })
        );
    }

    #[test]
    fn committed_but_unapplied_log_waits() {
        assert_eq!(
            group_recovery(progress(1, 12, 12, 4)),
            GroupRecovery::Pending(GroupWait::ReplayLagging {
                commit_index: 12,
                host_applied: 4,
            })
        );
    }

    /// Raft hands every entry to the apply loop, so its `last_applied` reaches
    /// `commit_index` while replay still runs. The host watermark below it
    /// holds the gate.
    #[test]
    fn a_handed_off_log_waits_for_the_host_watermark() {
        let view = gate_view(vec![status(4, 12, ReplicaState::Whole)], |_| 7);
        assert!(view.awaiting_snapshot.is_empty());
        assert_eq!(view.pending.len(), 1);
        assert_eq!(
            view.pending[0].wait,
            GroupWait::ReplayLagging {
                commit_index: 12,
                host_applied: 7,
            }
        );
    }

    /// The host watermark at `commit_index` opens the gate, the election no-op
    /// included: it concludes on the apply loop like any entry.
    #[test]
    fn a_host_watermark_at_the_commit_index_is_recovered() {
        let view = gate_view(vec![status(4, 12, ReplicaState::Whole)], |_| 12);
        assert!(view.pending.is_empty());
        assert!(view.awaiting_snapshot.is_empty());
        assert_eq!(
            group_recovery(progress(1, 1, 1, 1)),
            GroupRecovery::Replayed
        );
        assert_eq!(
            group_recovery(progress(1, 12, 12, 13)),
            GroupRecovery::Replayed
        );
    }

    /// A replica that awaits a snapshot install applies nothing until one
    /// installs. It does not hold startup, even with no leader, and is
    /// reported apart.
    #[test]
    fn a_group_awaiting_a_snapshot_does_not_hold_startup() {
        let awaiting = GroupProgress {
            replica_state: ReplicaState::AwaitingSnapshot,
            ..progress(0, 0, 12, 3)
        };
        assert_eq!(group_recovery(awaiting), GroupRecovery::AwaitingSnapshot);

        let view = gate_view(
            vec![
                status(4, 12, ReplicaState::AwaitingSnapshot),
                status(5, 12, ReplicaState::Whole),
            ],
            |group_id| if group_id == 4 { 3 } else { 12 },
        );
        assert!(view.pending.is_empty());
        assert_eq!(view.awaiting_snapshot, vec![4]);
    }

    /// A replica that only wants a snapshot keeps its data whole and can
    /// lead, so it holds startup like any other group until it replays.
    #[test]
    fn a_group_wanting_a_snapshot_still_replays() {
        let wanting = GroupProgress {
            replica_state: ReplicaState::WantsSnapshot,
            ..progress(1, 12, 12, 4)
        };
        assert_eq!(
            group_recovery(wanting),
            GroupRecovery::Pending(GroupWait::ReplayLagging {
                commit_index: 12,
                host_applied: 4,
            })
        );
    }
}
