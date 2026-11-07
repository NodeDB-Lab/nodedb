// SPDX-License-Identifier: BUSL-1.1

//! The source's log compacts past a stopped mirror. The mirror's link removed its
//! pin long ago, so nothing holds the log. Its next fetch answers `LogTruncated`,
//! the mirror fails with the source's real bound, and a rebuild recovers it.
//!
//! The test runs two clusters joined the way an operator joins them.

use std::time::Duration;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_types::{DatabaseId, MirrorFailure, MirrorGroupPosition, MirrorStatus};

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, link_pki, mirror_settings,
    read_mirror, serve_source, use_database,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

const DATABASE: &str = "compact_source";
const COPY: &str = "compact_copy";
/// Entries a group's log holds before it compacts.
const COMPACT_AT: u64 = 16;

/// The first index `node`'s replica of `group` holds, or `None` when it hosts no
/// replica.
fn first_available(node: &TestClusterNode, group: u64, from_index: u64) -> Option<u64> {
    node.shared
        .multi_raft
        .get()
        .expect("the Raft loop runs")
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .read_committed_within(group, from_index, 1)
        .ok()
        .map(|read| read.first_available)
}

/// Whether every replica of `position`'s group compacted past it.
fn compacted_past(source: &TestCluster, position: &MirrorGroupPosition) -> bool {
    let from_index = position.log_index + 1;
    let replicas: Vec<u64> = source
        .nodes
        .iter()
        .filter_map(|node| first_available(node, position.group_id, from_index))
        .collect();
    !replicas.is_empty() && replicas.iter().all(|first| *first > from_index)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_log_compacted_past_a_mirror_fails_it_and_a_rebuild_recovers() {
    let source = TestCluster::spawn_three_with_compaction_threshold(COMPACT_AT)
        .await
        .expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    use_database(&source, DATABASE).await;
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('first', 1)")
        .await
        .expect("insert");
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
    await_mirror_reads(&mirror, "SELECT id FROM docs", &["first".to_string()]).await;

    let positions: Vec<MirrorGroupPosition> = mirror.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_mirror_cut_state(database)
        .expect("catalog")
        .expect("the cut state")
        .positions
        .into_iter()
        .filter(|position| position.group_id != METADATA_GROUP_ID)
        .collect();
    let stopped = mirror.stop_all().await.expect("stop the mirror");

    // A pin past its age leaves the source's catalog after twice its age. The
    // test removes the row, and waits for no age.
    let pin = source.nodes[0]
        .shared
        .mirror
        .pins
        .all()
        .into_iter()
        .find(|pin| pin.database == database.as_u64())
        .expect("the link's pin");
    propose_catalog_entry_async(
        &source.nodes[0].shared,
        &CatalogEntry::DeleteMirrorPin {
            link_id: pin.link_id,
        },
    )
    .await
    .expect("remove the pin");
    wait_for(
        "every source node drops the pin",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            source
                .nodes
                .iter()
                .all(|node| node.shared.mirror.pins.get(pin.link_id).is_none())
        },
    )
    .await;

    let written = std::cell::Cell::new(0usize);
    wait_for_async(
        "writes compact a group's log past the mirror",
        Duration::from_secs(180),
        Duration::from_millis(5),
        || async {
            let row = written.get();
            written.set(row + 1);
            source.nodes[0]
                .exec(&format!("INSERT INTO docs (id, v) VALUES ('r{row:05}', 1)"))
                .await
                .expect("insert");
            positions
                .iter()
                .any(|position| compacted_past(&source, position))
        },
    )
    .await;

    let mirror = stopped.start_all().await.expect("restart the mirror");
    boot_mirror_loops(&mirror, &settings);
    wait_for(
        "every mirror node shows the log truncated at a real bound",
        Duration::from_secs(120),
        Duration::from_millis(100),
        || {
            mirror.nodes.iter().all(|node| {
                matches!(
                    status_on(node, database),
                    Some(MirrorStatus::Failed {
                        failure: MirrorFailure::LogTruncated { first_available, .. },
                    }) if first_available > 0
                )
            })
        },
    )
    .await;
    let refused = mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} MIRROR RESUME"))
        .await
        .expect_err("RESUME has no log to resume from");
    assert!(refused.contains("MIRROR REBOOTSTRAP"), "{refused}");

    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} MIRROR REBOOTSTRAP"))
        .await
        .expect("MIRROR REBOOTSTRAP");
    await_following(&mirror, COPY).await;
    read_mirror(&mirror, COPY).await;

    source.nodes[1]
        .exec("INSERT INTO docs (id, v) VALUES ('last', 1)")
        .await
        .expect("insert");
    let mut expected: Vec<String> = (0..written.get()).map(|row| format!("r{row:05}")).collect();
    expected.push("first".to_string());
    expected.push("last".to_string());
    expected.sort();
    await_mirror_reads(&mirror, "SELECT id FROM docs", &expected).await;

    mirror.shutdown().await;
    source.shutdown().await;
}
