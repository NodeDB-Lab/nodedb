// SPDX-License-Identifier: BUSL-1.1

//! Data-group Raft recovery gate.
//!
//! Writes to replicable collections are made durable as `ReplicatedEntry`
//! records in their data group's Raft log, and the engine state they produce
//! (KV hash index, columnar/timeseries/spatial in-memory state, graph node
//! labels, …) is rebuilt only by re-applying that log into the Data Plane.
//!
//! Raft's `commit_index` / `last_applied` are volatile: after a restart every
//! locally hosted group must win its own randomized election before it commits
//! a no-op and carries its retained log forward to the applier. Those elections
//! run independently of the metadata group's, so waiting only on metadata
//! readiness lets the gateway open while a data group has replayed nothing — an
//! acknowledged write then reads back as if it never happened, with no error to
//! explain it.
//!
//! This gate closes that window: startup is held until every locally hosted
//! data group has elected a leader, committed through the end of its retained
//! log, and applied that log. It fails closed — a timeout aborts startup rather
//! than opening a gateway that would serve incomplete state.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info};

use crate::control::state::SharedState;

/// How often the Raft status snapshot is re-read while waiting. Elections take
/// seconds, so a coarse poll costs nothing and avoids a busy loop.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Upper bound on the whole wait. Generous relative to a randomized election
/// timeout plus replay of a retained log, but finite: a group that cannot elect
/// or cannot apply is a failure, not a reason to hang forever.
pub const DATA_GROUP_RECOVERY_TIMEOUT: Duration = Duration::from_secs(600);

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

/// Why a data group is not yet recovered, or `None` when it is.
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
    /// The committed log is known, but has not been fully applied yet.
    ReplayLagging {
        commit_index: u64,
        last_applied: u64,
    },
}

/// Recovery predicate for one group, from a single `GroupStatus` snapshot.
///
/// `last_applied` is Raft's own applied watermark, and is the only usable
/// progress signal here. The Data-Plane-side apply watermark cannot be the
/// target: a leader's election no-op is committed but is never delivered to the
/// distributed applier, so a Data-Plane watermark stops one short of
/// `commit_index` and never converges — and every post-restart log ends in
/// exactly such a no-op.
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
fn group_wait(
    leader_id: u64,
    commit_index: u64,
    last_log_index: u64,
    last_applied: u64,
) -> Option<GroupWait> {
    if leader_id == 0 {
        return Some(GroupWait::NoLeader);
    }
    // Empty log: nothing was ever written to this group, so there is nothing to
    // replay once it has a leader.
    if last_log_index == 0 {
        return None;
    }
    if commit_index < last_log_index {
        return Some(GroupWait::LogUncommitted {
            commit_index,
            last_log_index,
        });
    }
    if last_applied >= commit_index {
        return None;
    }
    Some(GroupWait::ReplayLagging {
        commit_index,
        last_applied,
    })
}

/// One group still holding startup, with the detail needed for diagnostics.
#[derive(Debug, Clone, Copy)]
struct PendingGroup {
    group_id: u64,
    wait: GroupWait,
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
                last_applied,
            } => write!(
                f,
                "group {} applied {last_applied} of {commit_index} committed entries",
                self.group_id
            ),
        }
    }
}

/// Collect the data groups that are not yet recovered.
fn pending_groups(statuses: Vec<nodedb_cluster::GroupStatus>) -> Vec<PendingGroup> {
    statuses
        .into_iter()
        .filter(|s| is_data_group(s.group_id))
        .filter_map(|s| {
            group_wait(
                s.leader_id,
                s.commit_index,
                s.last_log_index,
                s.last_applied,
            )
            .map(|wait| PendingGroup {
                group_id: s.group_id,
                wait,
            })
        })
        .collect()
}

/// Hold startup until every locally hosted data group has replayed its retained
/// Raft log.
///
/// A node with no Raft status source (a deployment with no cluster handle
/// installed) hosts no data groups and returns immediately.
pub async fn await_data_group_recovery(shared: &Arc<SharedState>) -> anyhow::Result<()> {
    let Some(status_fn) = shared.raft_status_fn.get() else {
        return Ok(());
    };
    let status_fn = Arc::clone(status_fn);
    let deadline = Instant::now() + DATA_GROUP_RECOVERY_TIMEOUT;

    loop {
        let pending = pending_groups(status_fn());
        if pending.is_empty() {
            info!("all local data raft groups replayed — opening client gateway");
            return Ok(());
        }

        if Instant::now() >= deadline {
            let detail = pending
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(anyhow::anyhow!(
                "data raft group recovery timeout after {DATA_GROUP_RECOVERY_TIMEOUT:?}: {detail}"
            ));
        }

        debug!(
            pending = pending.len(),
            "waiting for data raft group replay"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(group_wait(0, 0, 0, 0), Some(GroupWait::NoLeader));
        assert_eq!(group_wait(0, 9, 9, 9), Some(GroupWait::NoLeader));
    }

    #[test]
    fn empty_log_is_recovered_once_leader_exists() {
        assert_eq!(group_wait(1, 0, 0, 0), None);
    }

    /// The window this gate exists to close: a restart resets the volatile
    /// `commit_index` to 0, and Raft publishes `leader_id` before the new leader
    /// commits the no-op that carries the retained log forward. A group with a
    /// durable log must NOT be reported recovered here.
    #[test]
    fn fresh_leader_with_uncommitted_retained_log_waits() {
        assert_eq!(
            group_wait(1, 0, 12, 0),
            Some(GroupWait::LogUncommitted {
                commit_index: 0,
                last_log_index: 12,
            })
        );
    }

    #[test]
    fn partially_committed_retained_log_waits() {
        assert_eq!(
            group_wait(1, 5, 12, 5),
            Some(GroupWait::LogUncommitted {
                commit_index: 5,
                last_log_index: 12,
            })
        );
    }

    #[test]
    fn committed_but_unapplied_log_waits() {
        assert_eq!(
            group_wait(1, 12, 12, 4),
            Some(GroupWait::ReplayLagging {
                commit_index: 12,
                last_applied: 4,
            })
        );
    }

    /// A freshly elected leader whose only committed entry is its own election
    /// no-op is recovered once Raft has applied it. The no-op never reaches the
    /// Data Plane, which is why Raft's `last_applied` — not a Data-Plane
    /// watermark — is the progress signal here.
    #[test]
    fn election_noop_only_log_converges() {
        assert_eq!(group_wait(1, 1, 1, 1), None);
    }

    #[test]
    fn caught_up_group_is_recovered() {
        assert_eq!(group_wait(1, 12, 12, 12), None);
        assert_eq!(group_wait(1, 12, 12, 13), None);
    }
}
