// SPDX-License-Identifier: BUSL-1.1

//! `ALTER DATABASE ... PROMOTE`: a mirror ends at its last ready cut, and every
//! node takes writes with no row of that cut lost.
//!
//! Each test runs two clusters joined the way an operator joins them.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nodedb::bootstrap::background_loops::mirror::sample_mirror_lag;
use nodedb::control::mirror::applier::seal_mirror;
use nodedb_types::{DatabaseId, MirrorStatus};

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, link_pki, mirror_settings,
    read_mirror, rows, serve_source, use_database,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const COPY: &str = "promote_copy";
const WRITES_PER_NODE: usize = 3;

async fn seeded_source(database: &str) -> TestCluster {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create database");
    use_database(&source, database).await;
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");
    source
}

fn status_on(node: &TestClusterNode, database: DatabaseId) -> Option<MirrorStatus> {
    node.shared
        .credentials
        .catalog()
        .get_database(database)
        .expect("catalog")
        .and_then(|descriptor| descriptor.mirror_origin)
        .map(|origin| origin.status)
}

async fn await_promoted(mirror: &TestCluster, database: DatabaseId) {
    wait_for(
        "every mirror node shows the database promoted",
        Duration::from_secs(120),
        Duration::from_millis(100),
        || {
            mirror.nodes.iter().all(|node| {
                matches!(status_on(node, database), Some(MirrorStatus::Promoted))
                    && !node.shared.mirror.fence.is_read_only(database)
            })
        },
    )
    .await;
}

/// `w{row:05}` for each row `0..count`.
fn prefix_ids(count: usize) -> BTreeSet<String> {
    (0..count).map(|row| format!("w{row:05}")).collect()
}

/// The promoted rows hold no gap: one writer writes one group's log in order, so
/// a final cut holds a prefix of the rows. Answers the prefix length.
async fn assert_prefix_on_every_node(mirror: &TestCluster) -> usize {
    let first: BTreeSet<String> = rows(&mirror.nodes[0], "SELECT id FROM docs")
        .await
        .into_iter()
        .collect();
    assert_eq!(first, prefix_ids(first.len()), "a final cut holds a prefix");
    let expected: Vec<String> = first.iter().cloned().collect();
    await_mirror_reads(mirror, "SELECT id FROM docs", &expected).await;
    first.len()
}

/// Every node writes, and every node reads every write.
async fn assert_every_node_writes(mirror: &TestCluster, held: usize) {
    let mut expected = prefix_ids(held);
    for (at, node) in mirror.nodes.iter().enumerate() {
        for row in 0..WRITES_PER_NODE {
            let id = format!("x{at}{row}");
            node.exec(&format!("INSERT INTO docs (id, v) VALUES ('{id}', 9)"))
                .await
                .unwrap_or_else(|e| panic!("node {at} writes the promoted database: {e}"));
            expected.insert(id);
        }
    }
    let expected: Vec<String> = expected.into_iter().collect();
    await_mirror_reads(mirror, "SELECT id FROM docs", &expected).await;
}

/// Writes keep arriving at the source through PROMOTE. The mirror ends at one
/// cut: it holds a prefix, later source rows stay out, and every node writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_promote_under_a_writer_keeps_every_row_of_its_final_cut() {
    const DATABASE: &str = "promote_writer";
    let source = seeded_source(DATABASE).await;
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
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
                .expect("the source takes writes through the promotion");
            written += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        written
    };
    let promote = async {
        mirror
            .exec_ddl_on_any_leader(&format!(
                "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{DATABASE}"
            ))
            .await
            .expect("MIRROR DATABASE");
        let database = await_following(&mirror, COPY).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        mirror
            .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} PROMOTE"))
            .await
            .expect("PROMOTE");
        tokio::time::sleep(Duration::from_secs(2)).await;
        stop.store(true, Ordering::Release);
        database
    };
    let (written, database) = tokio::join!(writer, promote);
    await_promoted(&mirror, database).await;

    read_mirror(&mirror, COPY).await;
    let held = assert_prefix_on_every_node(&mirror).await;
    assert!(held > 0, "the mirror held rows when it was promoted");
    assert!(
        held < written,
        "the source wrote past the final cut: {held} of {written}"
    );
    assert_every_node_writes(&mirror, held).await;

    // A later source row never arrives, and a repeat changes nothing.
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('y0', 1)")
        .await
        .expect("insert");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !rows(&mirror.nodes[0], "SELECT id FROM docs")
            .await
            .contains(&"y0".to_string()),
        "a promoted database follows nothing"
    );
    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} PROMOTE"))
        .await
        .expect("a repeated PROMOTE succeeds");

    mirror.shutdown().await;
    source.shutdown().await;
}

/// A crash right after the seal leaves `Promoting` in the catalog. A restart
/// elects a metadata leader, which seals every group and opens writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_new_leader_finishes_a_sealed_promote_after_a_crash() {
    const DATABASE: &str = "promote_crash";
    let source = seeded_source(DATABASE).await;
    for row in 0..10 {
        source.nodes[0]
            .exec(&format!("INSERT INTO docs (id, v) VALUES ('w{row:05}', 1)"))
            .await
            .expect("insert");
    }
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    let settings = mirror_settings(&pki, &seeds);
    boot_mirror_loops(&mirror, &settings);
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{DATABASE}"
        ))
        .await
        .expect("MIRROR DATABASE");
    let database = await_following(&mirror, COPY).await;
    read_mirror(&mirror, COPY).await;
    let held: Vec<String> = prefix_ids(10).into_iter().collect();
    await_mirror_reads(&mirror, "SELECT id FROM docs", &held).await;

    let mut final_cut = None;
    for node in &mirror.nodes {
        if let Ok(cut) = seal_mirror(&node.shared, database).await {
            final_cut = Some(cut);
            break;
        }
    }
    assert!(
        final_cut.is_some_and(|cut| cut > 0),
        "the seal lands at a ready cut"
    );

    let mirror = mirror.restart_all().await.expect("restart the mirror");
    boot_mirror_loops(&mirror, &settings);
    await_promoted(&mirror, database).await;

    read_mirror(&mirror, COPY).await;
    await_mirror_reads(&mirror, "SELECT id FROM docs", &held).await;
    assert_every_node_writes(&mirror, held.len()).await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// The reason a mirror exists: its source died. The lag monitor reports it, and
/// PROMOTE ends the mirror at what it holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_whose_source_died_reports_its_lag_and_promotes() {
    const DATABASE: &str = "promote_source_dead";
    let source = seeded_source(DATABASE).await;
    for row in 0..5 {
        source.nodes[0]
            .exec(&format!("INSERT INTO docs (id, v) VALUES ('w{row:05}', 1)"))
            .await
            .expect("insert");
    }
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{DATABASE}"
        ))
        .await
        .expect("MIRROR DATABASE");
    let database = await_following(&mirror, COPY).await;
    read_mirror(&mirror, COPY).await;
    let held: Vec<String> = prefix_ids(5).into_iter().collect();
    await_mirror_reads(&mirror, "SELECT id FROM docs", &held).await;

    source.shutdown().await;

    // The harness runs no monitor. A real node samples on a timer.
    crate::common::cluster_harness::wait_for_async(
        "the lag monitor moves every node past Following",
        Duration::from_secs(120),
        Duration::from_millis(500),
        || async {
            for node in &mirror.nodes {
                sample_mirror_lag(&node.shared).await;
            }
            mirror.nodes.iter().all(|node| {
                matches!(
                    status_on(node, database),
                    Some(MirrorStatus::Degraded | MirrorStatus::Disconnected)
                )
            })
        },
    )
    .await;

    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} PROMOTE"))
        .await
        .expect("PROMOTE with the source gone");
    await_promoted(&mirror, database).await;
    await_mirror_reads(&mirror, "SELECT id FROM docs", &held).await;
    assert_every_node_writes(&mirror, held.len()).await;

    mirror.shutdown().await;
}
