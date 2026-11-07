// SPDX-License-Identifier: BUSL-1.1

//! `MIRROR DATABASE` end to end across two clusters: the refusals, a bootstrap
//! under a live writer, and a restart that resumes with no new capture.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb_types::MirrorStatus;

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, database_on, link_pki,
    mirror_settings, read_mirror, serve_source, use_database,
};
use crate::common::cluster_harness::{TestCluster, wait_for};

const DATABASE: &str = "mirror_boot_db";
const NOT_ALLOWED: &str = "mirror_boot_hidden";
const CLONED: &str = "mirror_boot_clone";
const TAKEN: &str = "mirror_boot_taken";
const MISSING: &str = "mirror_boot_missing";

/// Each refused statement leaves no descriptor, and its error says why.
async fn assert_refused(mirror: &TestCluster, local: &str, sql: &str, why: &str) {
    let error = mirror.nodes[0]
        .exec(sql)
        .await
        .expect_err("the statement is refused");
    assert!(error.contains(why), "{sql}: got {error}, expected {why}");
    assert!(
        database_on(&mirror.nodes[0], local).is_none(),
        "{sql} left a descriptor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mirror_database_refuses_what_it_cannot_follow() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    for database in [DATABASE, NOT_ALLOWED, TAKEN] {
        source
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
            .await
            .expect("create database");
    }
    source
        .exec_ddl_on_any_leader(&format!("CLONE DATABASE {CLONED} FROM {DATABASE}"))
        .await
        .expect("clone database");

    let pki = link_pki();
    let seeds = serve_source(
        &source,
        &pki,
        &[DATABASE, "default", CLONED, TAKEN, MISSING],
    );
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));

    assert_refused(
        &mirror,
        "r1",
        "MIRROR DATABASE r1 FROM elsewhere.anything",
        "names no source",
    )
    .await;
    assert_refused(
        &mirror,
        "r2",
        &format!("MIRROR DATABASE r2 FROM dr.{DATABASE}"),
        "self-mirroring",
    )
    .await;
    assert_refused(
        &mirror,
        "r3",
        &format!("MIRROR DATABASE r3 FROM {SOURCE_NAME}.default"),
        "default database",
    )
    .await;
    assert_refused(
        &mirror,
        "r4",
        &format!("MIRROR DATABASE r4 FROM {SOURCE_NAME}.{NOT_ALLOWED}"),
        "allowlist",
    )
    .await;
    assert_refused(
        &mirror,
        "r5",
        &format!("MIRROR DATABASE r5 FROM {SOURCE_NAME}.{MISSING}"),
        "does not exist",
    )
    .await;
    assert_refused(
        &mirror,
        "r6",
        &format!("MIRROR DATABASE r6 FROM {SOURCE_NAME}.{CLONED}"),
        "unmaterialized clone",
    )
    .await;

    // An id this cluster already uses: a local database squats the source's id.
    let taken_id = database_on(&source.nodes[0], TAKEN).expect("the source database");
    let mut squatter = source.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_database(taken_id)
        .expect("catalog")
        .expect("descriptor");
    squatter.name = "squatter".to_string();
    propose_catalog_entry_async(
        &mirror.nodes[0].shared,
        &CatalogEntry::PutDatabase(Box::new(squatter)),
    )
    .await
    .expect("seed the squatter");
    assert_refused(
        &mirror,
        "r7",
        &format!("MIRROR DATABASE r7 FROM {SOURCE_NAME}.{TAKEN}"),
        "uses on this cluster",
    )
    .await;
    // The hello pinned the source's log. The refusal releases the pin.
    wait_for(
        "the refused statement leaves no pin on the source",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            !source.nodes.iter().any(|node| {
                node.shared
                    .credentials
                    .catalog()
                    .list_mirror_pins()
                    .expect("pins")
                    .iter()
                    .any(|pin| pin.database == taken_id.as_u64())
            })
        },
    )
    .await;

    // A second mirror of one source database.
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE first_copy FROM {SOURCE_NAME}.{DATABASE}"
        ))
        .await
        .expect("the first mirror");
    assert_refused(
        &mirror,
        "r8",
        &format!("MIRROR DATABASE r8 FROM {SOURCE_NAME}.{DATABASE}"),
        "one mirror per source database",
    )
    .await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// A writer runs through the capture. Each row lands once: a row below the cut
/// comes in the capture, and a row above it from the tail. A restart then
/// resumes from the replicated positions and takes no new capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstraps_under_a_writer_and_resumes_after_a_restart() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    use_database(&source, DATABASE).await;
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");
    for row in 0..20 {
        source.nodes[0]
            .exec(&format!(
                "INSERT INTO docs (id, v) VALUES ('old{row:04}', 1)"
            ))
            .await
            .expect("insert");
    }

    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    let settings = mirror_settings(&pki, &seeds);
    boot_mirror_loops(&mirror, &settings);

    let stop = AtomicBool::new(false);
    let writer = async {
        let mut written = Vec::new();
        while !stop.load(Ordering::Acquire) {
            let id = format!("new{:04}", written.len());
            source.nodes[0]
                .exec(&format!("INSERT INTO docs (id, v) VALUES ('{id}', 2)"))
                .await
                .expect("insert during the bootstrap");
            written.push(id);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        written
    };
    let bootstrap = async {
        mirror
            .exec_ddl_on_any_leader(&format!(
                "MIRROR DATABASE copy_db FROM {SOURCE_NAME}.{DATABASE}"
            ))
            .await
            .expect("MIRROR DATABASE");
        let database = await_following(&mirror, "copy_db").await;
        // Some rows land after the status moved, from the tail alone.
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop.store(true, Ordering::Release);
        database
    };
    let (written, database) = tokio::join!(writer, bootstrap);
    assert!(!written.is_empty(), "the writer ran through the bootstrap");

    read_mirror(&mirror, "copy_db").await;
    let mut expected: Vec<String> = (0..20).map(|row| format!("old{row:04}")).collect();
    expected.extend(written);
    expected.sort();
    await_mirror_reads(&mirror, "SELECT id FROM docs", &expected).await;

    let shared = &mirror.nodes[0].shared;
    let origin = shared
        .credentials
        .catalog()
        .get_database(database)
        .expect("catalog")
        .and_then(|descriptor| descriptor.mirror_origin)
        .expect("the mirror's origin");
    assert_eq!(origin.source_database, database, "the mirror keeps the id");
    assert_eq!(
        Some(database),
        database_on(&source.nodes[0], DATABASE),
        "the mirror's id is the source's"
    );
    assert!(matches!(origin.status, MirrorStatus::Following));
    mirror.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('local', 1)")
        .await
        .expect_err("a mirror refuses a client write");
    let before = shared
        .credentials
        .catalog()
        .get_mirror_cut_state(database)
        .expect("catalog")
        .expect("the cut state");
    assert_ne!(before.ready_cut, 0, "the bootstrap installed as a real cut");

    // Every node comes back from its own disk, with no capture to fetch.
    let mirror = mirror.restart_all().await.expect("restart the mirror");
    boot_mirror_loops(&mirror, &settings);
    read_mirror(&mirror, "copy_db").await;
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('after_restart', 3)")
        .await
        .expect("insert");
    expected.push("after_restart".to_string());
    expected.sort();
    await_mirror_reads(&mirror, "SELECT id FROM docs", &expected).await;
    wait_for(
        "the cut state moved past where the restart found it",
        Duration::from_secs(60),
        Duration::from_millis(100),
        || {
            mirror.nodes[0]
                .shared
                .credentials
                .catalog()
                .get_mirror_cut_state(database)
                .expect("catalog")
                .is_some_and(|state| state.ready_cut > before.ready_cut)
        },
    )
    .await;

    mirror.shutdown().await;
    source.shutdown().await;
}
