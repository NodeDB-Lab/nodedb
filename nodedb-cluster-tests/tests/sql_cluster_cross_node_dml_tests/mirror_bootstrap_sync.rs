// SPDX-License-Identifier: BUSL-1.1

//! `MIRROR DATABASE ... MODE = sync`: a write on the source answers once the
//! mirror holds it, and a stopped mirror answers a typed timeout. The write stays
//! committed on the source.
//!
//! The test runs two clusters joined the way an operator joins them.

use std::time::Duration;

use nodedb_types::error::sqlstate;

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, link_pki, mirror_settings,
    read_mirror, rows, serve_source, use_database,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const DATABASE: &str = "sync_source";
const COPY: &str = "sync_copy";

fn now_ordinal() -> i64 {
    nodedb_types::wall_now_ns_i64()
}

/// The newest confirmation of the source's one link, on the node that waited.
fn confirmed_ordinal(node: &TestClusterNode) -> i64 {
    node.shared
        .mirror
        .pins
        .all()
        .iter()
        .map(|pin| pin.acked_ordinal)
        .max()
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_sync_write_waits_for_its_mirror_and_a_stopped_mirror_times_out() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    use_database(&source, DATABASE).await;
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{DATABASE} MODE = sync"
        ))
        .await
        .expect("MIRROR DATABASE");
    let database = await_following(&mirror, COPY).await;
    read_mirror(&mirror, COPY).await;
    wait_for(
        "every source node holds writes for the link's first cut",
        Duration::from_secs(60),
        Duration::from_millis(50),
        || {
            source
                .nodes
                .iter()
                .all(|node| node.shared.mirror.pins.holds_sync(database.as_u64()))
        },
    )
    .await;

    // An autocommit write answers only with a cut above it confirmed.
    let before = now_ordinal();
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('a', 1)")
        .await
        .expect("a sync write with a live mirror");
    assert!(
        confirmed_ordinal(&source.nodes[0]) >= before,
        "the write answered before the mirror confirmed it"
    );
    await_mirror_reads(&mirror, "SELECT id FROM docs", &["a".to_string()]).await;

    // A transaction's write waits at its COMMIT.
    let client = &source.nodes[1].client;
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("INSERT INTO docs (id, v) VALUES ('b', 2)")
        .await
        .expect("insert in a block");
    let before = now_ordinal();
    client.simple_query("COMMIT").await.expect("COMMIT");
    assert!(
        confirmed_ordinal(&source.nodes[1]) >= before,
        "the COMMIT answered before the mirror confirmed it"
    );
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), "b".to_string()],
    )
    .await;

    mirror.shutdown().await;

    let refused = source.nodes[0]
        .client
        .simple_query("INSERT INTO docs (id, v) VALUES ('c', 3)")
        .await
        .expect_err("a stopped sync mirror confirms nothing");
    let refused = refused.as_db_error().expect("a typed error");
    assert_eq!(refused.code().code(), sqlstate::MIRROR_SYNC_TIMEOUT);
    assert!(
        refused.message().contains("committed"),
        "the error says the write committed: {}",
        refused.message()
    );

    // The row is on the source, so no client retries it as a failed write.
    assert!(
        rows(&source.nodes[0], "SELECT id FROM docs")
            .await
            .contains(&"c".to_string()),
        "the write stays committed"
    );

    source.shutdown().await;
}
