// SPDX-License-Identifier: BUSL-1.1

//! `CONVERT COLLECTION` and `PURGE TENANT` reach a mirror, through its tail and
//! through its first capture.
//!
//! The two clusters hold a different number of data groups, so a purge's vShard
//! list spans more than one mirror group.

use std::time::Duration;

use nodedb_types::{CollectionType, DatabaseId, DocumentMode};

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, link_pki, mirror_settings,
    read_mirror, rows, serve_source, use_database,
};
use crate::common::cluster_harness::{TestCluster, wait_for};

const DATABASE: &str = "mirror_boot_pc";
const COPY: &str = "mirror_boot_pc_copy";
/// The tenant of the harness superuser.
const TENANT: u64 = 1;
const ROWS: usize = 5;
const REPLICAS: usize = 3;
const COMPACTION_THRESHOLD: u64 = 4_096;
const CONVERT: &str = "CONVERT COLLECTION pc_docs TO document_strict \
                       (id TEXT PRIMARY KEY, name TEXT)";

async fn source_cluster() -> TestCluster {
    let source = TestCluster::spawn_three_with_groups_compaction_threshold_and_rf(
        2,
        COMPACTION_THRESHOLD,
        REPLICAS,
    )
    .await
    .expect("source cluster");
    // A cluster that already holds the row refuses this, and PURGE TENANT only
    // needs the row to exist.
    let _ = source
        .exec_ddl_on_any_leader(&format!("CREATE TENANT harness_tenant ID {TENANT}"))
        .await;
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    use_database(&source, DATABASE).await;
    for collection in ["pc_docs", "pc_keep"] {
        source
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION {collection} WITH (engine='document_schemaless')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {collection}: {e}"));
        for row in 0..ROWS {
            source.nodes[0]
                .exec(&format!(
                    "INSERT INTO {collection} (id, name) VALUES ('r{row}', 'name{row}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("insert into {collection}: {e}"));
        }
    }
    source
}

async fn mirror_cluster() -> TestCluster {
    TestCluster::spawn_three_with_groups_compaction_threshold_and_rf(
        3,
        COMPACTION_THRESHOLD,
        REPLICAS,
    )
    .await
    .expect("mirror cluster")
}

/// Every mirror node's catalog holds `pc_docs` as a strict document.
async fn await_strict_catalog(mirror: &TestCluster, database: DatabaseId) {
    wait_for(
        "every mirror node holds the converted catalog row",
        Duration::from_secs(60),
        Duration::from_millis(100),
        || {
            mirror.nodes.iter().all(|node| {
                node.shared
                    .credentials
                    .catalog()
                    .get_collection(database, TENANT, "pc_docs")
                    .expect("catalog")
                    .is_some_and(|row| {
                        matches!(
                            row.collection_type,
                            CollectionType::Document(DocumentMode::Strict(_))
                        )
                    })
            })
        },
    )
    .await;
}

/// The mirror answers a scan and a point read of `pc_docs` as the source does.
async fn assert_docs_match(source: &TestCluster, mirror: &TestCluster) {
    for sql in [
        "SELECT id, name FROM pc_docs",
        "SELECT name FROM pc_docs WHERE id = 'r1'",
    ] {
        let expected = rows(&source.nodes[0], sql).await;
        assert!(!expected.is_empty(), "the source answers {sql}");
        await_mirror_reads(mirror, sql, &expected).await;
    }
}

/// Each statement arrives from the tail, with the mirror's groups split
/// differently from the source's, and the link keeps following past both.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_convert_and_a_purge_reach_the_mirror_from_its_tail() {
    let source = source_cluster().await;
    let mirror = mirror_cluster().await;
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
    assert_docs_match(&source, &mirror).await;

    source.nodes[0].exec(CONVERT).await.expect("convert");
    source.nodes[0]
        .exec("INSERT INTO pc_docs (id, name) VALUES ('after', 'convert')")
        .await
        .expect("insert after the conversion");
    await_strict_catalog(&mirror, database).await;
    await_mirror_reads(
        &mirror,
        "SELECT name FROM pc_docs WHERE id = 'after'",
        &["convert".to_string()],
    )
    .await;
    assert_docs_match(&source, &mirror).await;

    source.nodes[0]
        .exec(&format!("PURGE TENANT {TENANT} CONFIRM"))
        .await
        .expect("purge the tenant");
    for collection in ["pc_docs", "pc_keep"] {
        await_mirror_reads(&mirror, &format!("SELECT id FROM {collection}"), &[]).await;
    }

    // Writes after the purge land on both clusters, and no purged row returns.
    source.nodes[0]
        .exec("INSERT INTO pc_keep (id, name) VALUES ('later', 'purge')")
        .await
        .expect("insert after the purge");
    await_mirror_reads(&mirror, "SELECT id FROM pc_keep", &["later".to_string()]).await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// A collection converted before the capture holds its rows in the target
/// encoding. Every mirror replica reads them, takes later writes in that
/// encoding, and keeps doing so after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_bootstrap_carries_a_conversion() {
    let source = source_cluster().await;
    source.nodes[0].exec(CONVERT).await.expect("convert");
    source
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let mirror = mirror_cluster().await;
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
    await_strict_catalog(&mirror, database).await;
    read_mirror(&mirror, COPY).await;
    assert_docs_match(&source, &mirror).await;

    source.nodes[0]
        .exec("INSERT INTO pc_docs (id, name) VALUES ('after', 'capture')")
        .await
        .expect("insert after the capture");
    await_mirror_reads(
        &mirror,
        "SELECT name FROM pc_docs WHERE id = 'after'",
        &["capture".to_string()],
    )
    .await;
    assert_docs_match(&source, &mirror).await;

    let mirror = mirror.restart_all().await.expect("restart the mirror");
    boot_mirror_loops(&mirror, &settings);
    read_mirror(&mirror, COPY).await;
    assert_docs_match(&source, &mirror).await;

    mirror.shutdown().await;
    source.shutdown().await;
}
