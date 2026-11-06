// SPDX-License-Identifier: BUSL-1.1

//! Observability snapshots of the Raft groups hosted on this node.

use crate::multi_raft::core::MultiRaft;

/// Snapshot of a single Raft group's state for observability.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GroupStatus {
    pub group_id: u64,
    /// Role as a human-readable string ("Leader", "Follower", "Candidate", "Learner").
    pub role: String,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    /// Highest log index covered by the latest compacted snapshot.
    /// Advances when the group's log is compacted past the start (gated
    /// by `RaftConfig::log_compaction_threshold`). A non-zero value
    /// means entries at or below it are no longer in the log and a
    /// lagging peer below this index can only be caught up via
    /// `InstallSnapshot`, never `AppendEntries`.
    pub snapshot_index: u64,
    /// The entry bytes this replica's log holds in memory.
    pub retained_bytes: u64,
    /// What this replica needs before it takes log entries. A replica that
    /// awaits a snapshot applies nothing until one installs.
    pub replica_state: nodedb_raft::ReplicaState,
    pub member_count: usize,
    pub learner_count: usize,
    pub vshard_count: usize,
}

/// Membership snapshot for a hosted Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMembership {
    pub group_id: u64,
    pub leader_id: u64,
    /// Voting members, including this node when it is a voter.
    pub voters: Vec<u64>,
    /// Non-voting learners, including this node when it is a learner.
    pub learners: Vec<u64>,
}

impl MultiRaft {
    /// Snapshot the actual Raft membership rather than the vShard routing view.
    pub fn group_membership(&self, group_id: u64) -> Option<GroupMembership> {
        let node = self.groups.get(&group_id)?;
        let mut voters = node.voters().to_vec();
        let mut learners = node.learners().to_vec();
        match node.role() {
            nodedb_raft::NodeRole::Learner => learners.push(self.node_id),
            nodedb_raft::NodeRole::Observer => {}
            _ => voters.push(self.node_id),
        }
        voters.sort_unstable();
        voters.dedup();
        learners.sort_unstable();
        learners.dedup();
        Some(GroupMembership {
            group_id,
            leader_id: node.leader_id(),
            voters,
            learners,
        })
    }

    /// Every hosted group's live leader as this node's Raft knows it, with
    /// the term it knows it at: `(group_id, leader_id, term)`.
    ///
    /// `leader_id` is this node when it leads, or the leader whose contact
    /// is fresher than `election_timeout_min`. It is `0` otherwise, as during
    /// an election or once a crashed leader's contact went stale.
    pub fn observed_leaders(&self) -> Vec<(u64, u64, u64)> {
        let now = std::time::Instant::now();
        self.groups
            .iter()
            .map(|(&group_id, node)| (group_id, node.live_leader(now), node.current_term()))
            .collect()
    }

    /// Snapshot of all Raft group states for observability.
    ///
    /// The caller holds the `MultiRaft` lock, so this takes the routing read
    /// guard second. That follows the crate lock order: `MultiRaft`, then routing.
    /// One guard covers the whole loop so a queued routing writer cannot
    /// interleave between groups.
    pub fn group_statuses(&self) -> Vec<GroupStatus> {
        let routing = self.routing.read().unwrap_or_else(|p| p.into_inner());
        let mut statuses = Vec::with_capacity(self.groups.len());
        for (&group_id, node) in &self.groups {
            let vshard_count = routing.vshards_for_group(group_id).len();
            let self_is_voter = !matches!(
                node.role(),
                nodedb_raft::NodeRole::Learner | nodedb_raft::NodeRole::Observer
            );

            statuses.push(GroupStatus {
                group_id,
                role: format!("{:?}", node.role()),
                leader_id: node.leader_id(),
                term: node.current_term(),
                commit_index: node.commit_index(),
                last_applied: node.last_applied(),
                last_log_index: node.last_log_index(),
                snapshot_index: node.log_snapshot_index(),
                retained_bytes: node.log_retained_bytes(),
                replica_state: node.replica_state(),
                member_count: node.voters().len() + usize::from(self_is_voter),
                learner_count: node.learners().len()
                    + usize::from(node.role() == nodedb_raft::NodeRole::Learner),
                vshard_count,
            });
        }
        statuses.sort_by_key(|s| s.group_id);
        statuses
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::routing::RoutingTable;

    const DATA_GROUPS: u64 = 4;
    const CALLERS: usize = 4;
    const ROUNDS: usize = 500;
    const DEADLINE: Duration = Duration::from_secs(30);

    /// Status callers that follow the lock order finish under a contending routing writer.
    ///
    /// Each caller takes the `MultiRaft` lock for `group_statuses`, drops it, then reads routing.
    /// A writer thread queues on the routing lock without pause.
    /// std's Linux `RwLock` blocks a new reader while a writer waits.
    /// A caller that holds a routing read guard across `group_statuses` deadlocks here.
    /// Its nested routing read waits behind the writer.
    /// The writer waits behind its outer guard.
    /// It also holds the `MultiRaft` lock, so every other caller stalls and the deadline fails.
    #[test]
    fn status_callers_finish_under_a_contending_routing_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut multi_raft = MultiRaft::new(
            1,
            RoutingTable::uniform(DATA_GROUPS, &[1], 1),
            dir.path().to_path_buf(),
        );
        for group_id in 0..=DATA_GROUPS {
            multi_raft.add_group(group_id, vec![]).expect("add group");
        }
        let routing = multi_raft.routing();
        let multi_raft = Arc::new(Mutex::new(multi_raft));
        let stop = Arc::new(AtomicBool::new(false));

        let writer = {
            let routing = Arc::clone(&routing);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    drop(routing.write().unwrap_or_else(|p| p.into_inner()));
                    thread::yield_now();
                }
            })
        };

        let (done_tx, done_rx) = mpsc::channel();
        for _ in 0..CALLERS {
            let multi_raft = Arc::clone(&multi_raft);
            let routing = Arc::clone(&routing);
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let mut consistent = true;
                for _ in 0..ROUNDS {
                    let statuses = multi_raft
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .group_statuses();
                    let table = routing.read().unwrap_or_else(|p| p.into_inner());
                    consistent &= statuses.len() as u64 == DATA_GROUPS + 1
                        && statuses.iter().all(|status| {
                            status.vshard_count == table.vshards_for_group(status.group_id).len()
                        });
                }
                // The receiver can be gone after a failed deadline. Nothing then reads this.
                let _ = done_tx.send(consistent);
            });
        }
        drop(done_tx);

        for _ in 0..CALLERS {
            let consistent = done_rx
                .recv_timeout(DEADLINE)
                .expect("a status caller deadlocked on the routing lock");
            assert!(consistent, "a status disagreed with the routing table");
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().expect("routing writer");
    }
}
