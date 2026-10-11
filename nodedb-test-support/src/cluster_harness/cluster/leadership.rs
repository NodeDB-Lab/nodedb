// SPDX-License-Identifier: BUSL-1.1

//! Data-group leadership moves and waits on a [`TestCluster`].
//!
//! A move asks the group's leader to hand its leadership to a target, the
//! path the leader balance takes. The leader balance keeps running, so it
//! can move a group back to its preferred leader a few seconds later.

use std::time::{Duration, Instant};

use super::TestCluster;
use crate::cluster_harness::wait::wait_for_report;

/// Which node a wait expects to lead a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupLeader {
    /// Any live node.
    Any,
    /// The node with this id.
    Node(u64),
}

/// How long one transfer request gets before the leader is asked again.
const TRANSFER_ROUND: Duration = Duration::from_secs(3);

/// How long a leadership move gets in all.
const TRANSFER_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two leadership reads.
const LEADER_POLL: Duration = Duration::from_millis(20);

impl TestCluster {
    /// The node that leads `group_id`: it leads in its own Raft state, and
    /// every live node hosting the group names it leader. `None` while no
    /// node leads or the nodes disagree.
    pub fn group_leader(&self, group_id: u64) -> Option<u64> {
        self.leader_report(group_id).ok()
    }

    /// Wait until `leader` leads `group_id` as [`Self::group_leader`] reads
    /// it, and return the leader's id. Panics at `timeout` with each node's
    /// view of the group.
    pub async fn wait_for_group_leader(
        &self,
        group_id: u64,
        leader: GroupLeader,
        timeout: Duration,
    ) -> u64 {
        let expected = match leader {
            GroupLeader::Any => "any node".to_owned(),
            GroupLeader::Node(node_id) => format!("node {node_id}"),
        };
        let mut found = 0;
        wait_for_report(
            &format!("{expected} leads group {group_id}"),
            timeout,
            LEADER_POLL,
            || {
                let current = self.leader_report(group_id)?;
                match leader {
                    GroupLeader::Node(node_id) if node_id != current => {
                        Err(format!("node {current} leads"))
                    }
                    GroupLeader::Any | GroupLeader::Node(_) => {
                        found = current;
                        Ok(())
                    }
                }
            },
        )
        .await;
        found
    }

    /// Move the leadership of `group_id` to node `target`, and wait until
    /// [`Self::group_leader`] names it. Each round asks the group's current
    /// leader again: a transfer the leader refused, or one that lapsed,
    /// starts nothing. Panics when the move does not finish within 30 s,
    /// with the last refusal and each node's view.
    pub async fn transfer_leadership(&self, group_id: u64, target: u64) {
        let deadline = Instant::now() + TRANSFER_DEADLINE;
        let mut last_refusal = String::from("no transfer was refused");
        loop {
            if self.group_leader(group_id) == Some(target) {
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "group {group_id} did not move to node {target} within \
                     {TRANSFER_DEADLINE:?}; last refusal: {last_refusal}; views: {}",
                    self.leader_views(group_id)
                );
            }
            let leader = self.nodes.iter().find(|node| node.leads_group(group_id));
            if let Some(leader) = leader
                && leader.node_id != target
                && let Err(refusal) = leader.transfer_group_leadership(group_id, target)
            {
                last_refusal = refusal;
            }
            let round_end = Instant::now() + TRANSFER_ROUND;
            while Instant::now() < round_end {
                if self.group_leader(group_id) == Some(target) {
                    return;
                }
                tokio::time::sleep(LEADER_POLL).await;
            }
        }
    }

    /// Wait until each data group is led by its preferred leader. The
    /// leader balance then moves no group by itself, so the leaders stay
    /// put until a test moves them. Panics at `timeout`.
    pub async fn wait_for_preferred_leaders(&self, timeout: Duration) {
        wait_for_report(
            "every data group is led by its preferred leader",
            timeout,
            LEADER_POLL,
            || {
                let preferred = {
                    let routing = self.nodes[0]
                        .shared
                        .cluster_routing
                        .as_ref()
                        .ok_or_else(|| "node has no cluster routing".to_owned())?
                        .read()
                        .unwrap_or_else(|p| p.into_inner());
                    nodedb_cluster::rebalancer::preferred_leaders(&routing)
                };
                for (group_id, node_id) in preferred {
                    let current = self.leader_report(group_id)?;
                    if current != node_id {
                        return Err(format!(
                            "node {current} leads group {group_id}, node {node_id} is preferred"
                        ));
                    }
                }
                Ok(())
            },
        )
        .await;
    }

    /// The agreed leader of `group_id`, or why there is none.
    fn leader_report(&self, group_id: u64) -> Result<u64, String> {
        let mut named = None;
        for node in &self.nodes {
            let Some(leader) = node
                .all_group_leaders()
                .into_iter()
                .find(|(group, _)| *group == group_id)
                .map(|(_, leader)| leader)
            else {
                continue;
            };
            match named {
                None => named = Some(leader),
                Some(first) if first != leader => {
                    return Err(format!(
                        "the nodes disagree on group {group_id}'s leader: {}",
                        self.leader_views(group_id)
                    ));
                }
                Some(_) => {}
            }
        }
        let leader = named
            .filter(|leader| *leader != 0)
            .ok_or_else(|| format!("no node leads group {group_id}"))?;
        let leads = self
            .nodes
            .iter()
            .any(|node| node.node_id == leader && node.leads_group(group_id));
        if leads {
            Ok(leader)
        } else {
            Err(format!(
                "node {leader} is named leader of group {group_id} but does not lead it"
            ))
        }
    }

    /// Each live node's view of `group_id`'s leader, for a failure report.
    fn leader_views(&self, group_id: u64) -> String {
        self.nodes
            .iter()
            .map(|node| node.group_status_line(group_id))
            .collect::<Vec<_>>()
            .join("; ")
    }
}
