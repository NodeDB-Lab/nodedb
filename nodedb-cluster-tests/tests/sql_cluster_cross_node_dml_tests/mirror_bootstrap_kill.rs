// SPDX-License-Identifier: BUSL-1.1

//! A mirror group's leader dies mid-cut under a writer: the cut committed in its
//! groups and did not raise. The mirror converges with no row missing and none
//! twice. An append-only collection shows a duplicate, which a primary key hides.
//!
//! The test runs two clusters joined the way an operator joins them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nodedb::control::mirror::applier::CUT_RAISE_FAIL_POINT;
use nodedb_types::CollectionKey;
use nodedb_types::fail_point::{FailAction, FailGuard};
use nodedb_types::id::VShardId;

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, link_pki, mirror_settings,
    read_mirror, serve_source, use_database,
};
use crate::common::cluster_harness::shared_steps::{kill_node, leader_of};
use crate::common::cluster_harness::{TestCluster, wait_for};

const DATABASE: &str = "kill_source";
const COPY: &str = "kill_copy";
const FIRST_TS: usize = 1_000;

/// The index of the mirror node that leads the group holding `collection`.
fn leader_index(mirror: &TestCluster, vshard: VShardId) -> usize {
    let probe = &mirror.nodes[0];
    let group = probe
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(vshard.as_u32())
        .expect("the vShard maps to a group");
    let leader = leader_of(probe, group);
    mirror
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .expect("a live node leads the group")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_leader_killed_mid_cut_converges_with_no_row_lost_or_doubled() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    use_database(&source, DATABASE).await;
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create the keyed collection");
    source
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION ticks (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create the append-only collection");
    let mut mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));

    let stop = AtomicBool::new(false);
    let writer = async {
        let mut written = 0usize;
        while !stop.load(Ordering::Acquire) {
            source.nodes[0]
                .exec(&format!(
                    "INSERT INTO docs (id, v) VALUES ('w{written:05}', 1)"
                ))
                .await
                .expect("the source takes every keyed write");
            source.nodes[0]
                .exec(&format!(
                    "INSERT INTO ticks (ts, host, value) VALUES ({}, 'a', 1.0)",
                    FIRST_TS + written
                ))
                .await
                .expect("the source takes every append");
            written += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        written
    };
    let killer = async {
        mirror
            .exec_ddl_on_any_leader(&format!(
                "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{DATABASE}"
            ))
            .await
            .expect("MIRROR DATABASE");
        let database = await_following(&mirror, COPY).await;

        let gates = tempfile::tempdir().expect("gate directory");
        let release = gates.path().join("release");
        let parked = gates.path().join("release.parked");
        let gate = FailGuard::install(
            CUT_RAISE_FAIL_POINT,
            FailAction::WaitForFile(release.clone()),
        );
        wait_for(
            "a cut commits in its groups and parks before its raise",
            Duration::from_secs(60),
            Duration::from_millis(20),
            || parked.exists(),
        )
        .await;
        let vshard = CollectionKey::from_bare(database, "docs").vshard();
        let victim = leader_index(&mirror, vshard);
        kill_node(&mut mirror, victim).await;
        std::fs::write(&release, b"go").expect("release the gate");
        drop(gate);

        tokio::time::sleep(Duration::from_secs(3)).await;
        stop.store(true, Ordering::Release);
        read_mirror(&mirror, COPY).await;
    };
    let (written, ()) = tokio::join!(writer, killer);
    assert!(written > 0, "the writer ran through the kill");

    let keyed: Vec<String> = (0..written).map(|row| format!("w{row:05}")).collect();
    await_mirror_reads(&mirror, "SELECT id FROM docs", &keyed).await;
    let appended: Vec<String> = (0..written)
        .map(|row| format!("{}|a", FIRST_TS + row))
        .collect();
    await_mirror_reads(&mirror, "SELECT ts, host FROM ticks", &appended).await;

    mirror.shutdown().await;
    source.shutdown().await;
}
