// SPDX-License-Identifier: BUSL-1.1

//! A read cut a data group's leader closed binds every later leader.
//!
//! A leader closes a cut one minute past every node's wall clock, as a node
//! whose clock leads closes it. The group's ordinal floor rises to the cut on
//! a quorum. Leadership then moves to a replica whose clock trails the cut.
//! Its first write still lands above the cut: the new leader gathers the floor
//! from a quorum before its write gate stamps the write.

use std::time::Duration;

use super::calvin_multishard_fixture::{Fixture, keyed_ddl};
use super::calvin_replica_content::run_retrying;
use crate::common::cluster_harness::shared_steps::{group_members, group_of};
use crate::common::cluster_harness::{TestClusterNode, wait_for_async};

/// How long the group gets to settle a leader and close the cut.
const CONVERGENCE: Duration = Duration::from_secs(30);

/// How far the cut leads the wall clock, in ordinals (nanoseconds).
const CUT_LEAD_NS: i64 = 60_000_000_000;

fn node(fx: &Fixture, node_id: u64) -> &TestClusterNode {
    fx.cluster
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .unwrap_or_else(|| panic!("node {node_id} is in the cluster"))
}

/// Close `cut` on `group`'s current leader. Returns the leader's id.
async fn close_cut_on_leader(fx: &Fixture, group: u64, cut: i64) -> u64 {
    let deadline = tokio::time::Instant::now() + CONVERGENCE;
    loop {
        if let Some(leader) = fx.cluster.nodes.iter().find(|node| node.leads_group(group)) {
            let gate = leader
                .shared
                .raft_read_gate
                .get()
                .unwrap_or_else(|| panic!("node {} serves raft", leader.node_id));
            if gate
                .cut_read_index(group, Duration::from_secs(5), cut)
                .await
                .is_ok()
            {
                return leader.node_id;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader of group {group} closed the cut within {CONVERGENCE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_data_group_leader_stamps_its_first_write_above_a_closed_cut() {
    let collection = "ordinal_floor_cut";
    let fx = Fixture::spawn(&[keyed_ddl(collection)]).await;
    fx.wait_group_mounted(collection).await;
    fx.cluster.wait_for_preferred_leaders(CONVERGENCE).await;
    let group = group_of(&fx.cluster.nodes[0], collection);

    let cut = nodedb_types::wall_now_ns_i64() + CUT_LEAD_NS;
    let old_leader = close_cut_on_leader(&fx, group, cut).await;

    let next_leader = group_members(&fx.cluster.nodes[0], group)
        .into_iter()
        .find(|member| *member != old_leader)
        .unwrap_or_else(|| panic!("group {group} has a replica besides {old_leader}"));
    fx.cluster.transfer_leadership(group, next_leader).await;

    let next = node(&fx, next_leader);
    let clock = next.shared.wal.ordinal_clock();
    assert!(
        clock.next_ordinal() < cut,
        "the new leader's clock trails the cut until its gate folds the floor"
    );

    let holders = fx
        .cluster
        .nodes
        .iter()
        .filter(|node| {
            node.shared.multi_raft.get().is_some_and(|multi_raft| {
                multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .group_ordinal_floor(group)
                    .is_some_and(|floor| floor >= cut)
            })
        })
        .count();
    assert!(holders >= 2, "{holders} replicas hold the cut's floor");

    run_retrying(
        &next.client,
        "the first write after the leader change",
        &format!("INSERT INTO {collection} (id, v) VALUES ('after', 'cut')"),
    )
    .await;

    wait_for_async(
        "the write's stamp reached the new leader's clock",
        CONVERGENCE,
        Duration::from_millis(20),
        || {
            let clock = next.shared.wal.ordinal_clock();
            async move { clock.next_ordinal() > cut }
        },
    )
    .await;
}
