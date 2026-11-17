// SPDX-License-Identifier: BUSL-1.1

//! Writes made while a failpoint holds a REINDEX rebuild: full-text and CSR
//! rebuilds keep them, and a plain REINDEX leaves the core serving.

use std::time::{Duration, Instant};

use nodedb::fail_point::{FailAction, FailGuard};
use nodedb_test_support::pgwire_harness::TestServer;

use super::reindex_concurrent_writes::{
    events, insert_edge, out_of_root, search_index, text_ids, wait_for_cutovers,
};

/// Arm `failpoint` to hold its rebuild thread until the returned path
/// exists.
fn hold(failpoint: &str) -> (FailGuard, tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let release = dir.path().join("release");
    let guard = FailGuard::install(failpoint, FailAction::WaitForFile(release.clone()));
    (guard, dir, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fts_rebuild_keeps_writes_made_during_the_rebuild() {
    let log = events();
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION rx_fts WITH (engine='document_schemaless')")
        .await
        .unwrap();
    search_index(&server, "rx_fts").await;
    for i in 1..=4 {
        server
            .exec(&format!(
                "INSERT INTO rx_fts {{ id: 'd{i}', body: 'alpha seed' }}"
            ))
            .await
            .unwrap();
    }

    // CREATE SEARCH INDEX ran its own rebuild and cut it over.
    let started_before = log.count("rebuild_started", "fts", "rx_fts");
    let cut_before = log.count("atomic_cutover", "fts", "rx_fts");
    let (_guard, _dir, release) = hold("reindex::fts_build_hold");
    server
        .exec("REINDEX INDEX fts CONCURRENTLY rx_fts")
        .await
        .unwrap();
    assert!(log.count("rebuild_started", "fts", "rx_fts") > started_before);

    // The rebuild's snapshot is pinned; these writes land after it.
    for i in 5..=7 {
        server
            .exec(&format!(
                "INSERT INTO rx_fts {{ id: 'd{i}', body: 'alpha fresh' }}"
            ))
            .await
            .unwrap();
    }
    server
        .exec("UPDATE rx_fts SET body = 'beta moved' WHERE id = 'd1'")
        .await
        .unwrap();
    server
        .exec("DELETE FROM rx_fts WHERE id = 'd2'")
        .await
        .unwrap();
    assert_eq!(
        log.count("atomic_cutover", "fts", "rx_fts"),
        cut_before,
        "the held rebuild must not cut over before it is released"
    );

    std::fs::write(&release, b"").unwrap();
    wait_for_cutovers(&log, "fts", "rx_fts").await;

    assert_eq!(
        text_ids(&server, "rx_fts", "alpha").await,
        ["d3", "d4", "d5", "d6", "d7"],
        "rows inserted during the rebuild are found once, the updated row \
         lost the term, and the deleted row is gone"
    );
    assert_eq!(text_ids(&server, "rx_fts", "beta").await, ["d1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn csr_rebuild_keeps_writes_made_during_the_rebuild() {
    let log = events();
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION rx_csr").await.unwrap();
    for dst in ["keep0", "gone1", "keep2"] {
        insert_edge(&server, "rx_csr", dst).await;
    }

    let (_guard, _dir, release) = hold("reindex::csr_build_hold");
    server
        .exec("REINDEX INDEX csr CONCURRENTLY rx_csr")
        .await
        .unwrap();
    assert!(log.count("rebuild_started", "csr", "rx_csr") > 0);

    // The partition snapshot is taken; these writes land after it.
    insert_edge(&server, "rx_csr", "new3").await;
    insert_edge(&server, "rx_csr", "new4").await;
    server
        .exec("GRAPH DELETE EDGE IN 'rx_csr' FROM 'root' TO 'gone1' TYPE 'l'")
        .await
        .unwrap();
    assert_eq!(
        log.count("atomic_cutover", "csr", "rx_csr"),
        0,
        "the held rebuild must not cut over before it is released"
    );

    std::fs::write(&release, b"").unwrap();
    wait_for_cutovers(&log, "csr", "rx_csr").await;

    let blob = out_of_root(&server, "rx_csr").await;
    for kept in ["keep0", "keep2", "new3", "new4"] {
        assert!(
            blob.contains(kept),
            "traversal after the cutover must reach {kept}; got: {blob}"
        );
    }
    assert!(
        !blob.contains("gone1"),
        "an edge deleted during the rebuild must stay deleted; got: {blob}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_reindex_leaves_the_core_serving_while_its_rebuild_is_held() {
    let log = events();
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION rx_held WITH (engine='document_schemaless')")
        .await
        .unwrap();
    search_index(&server, "rx_held").await;
    server
        .exec("INSERT INTO rx_held { id: 'h1', body: 'alpha held' }")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION rx_other WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rx_other { id: 'o1', body: 'other row' }")
        .await
        .unwrap();

    // CREATE SEARCH INDEX ran its own rebuild and cut it over.
    let started_before = log.count("rebuild_started", "fts", "rx_held");
    let cut_before = log.count("atomic_cutover", "fts", "rx_held");
    let (_guard, _dir, release) = hold("reindex::fts_build_hold");

    // The plain REINDEX runs on its own connection: it answers only
    // after the cutover, and the cutover waits for the release.
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let reindex = tokio::spawn(async move {
        client
            .simple_query("REINDEX INDEX fts rx_held")
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    while log.count("rebuild_started", "fts", "rx_held") == started_before {
        assert!(Instant::now() < deadline, "the plain REINDEX never started");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The core that holds the rebuild answers other requests.
    let rows = tokio::time::timeout(
        Duration::from_secs(10),
        server.query_text("SELECT id FROM rx_other"),
    )
    .await
    .expect("a query on another collection must answer while the rebuild is held")
    .unwrap();
    assert_eq!(rows, ["o1"]);
    assert!(
        !reindex.is_finished(),
        "plain REINDEX must not answer before its cutover"
    );
    assert_eq!(log.count("atomic_cutover", "fts", "rx_held"), cut_before);

    std::fs::write(&release, b"").unwrap();
    tokio::time::timeout(Duration::from_secs(60), reindex)
        .await
        .expect("plain REINDEX must answer after the release")
        .unwrap()
        .expect("plain REINDEX must succeed");
    assert_eq!(
        log.count("atomic_cutover", "fts", "rx_held"),
        log.count("rebuild_started", "fts", "rx_held"),
        "plain REINDEX answers only after its cutover"
    );
    assert_eq!(text_ids(&server, "rx_held", "alpha").await, ["h1"]);
}
