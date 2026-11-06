// SPDX-License-Identifier: BUSL-1.1

//! A mirror's link stops on a failure an operator acts on: `ALTER DATABASE ...
//! MIRROR RESUME` and `MIRROR REBOOTSTRAP`.
//!
//! Each test runs two clusters joined the way an operator joins them.

use std::time::Duration;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb::control::mirror::applier::REISSUE_FAIL_POINT;
use nodedb::control::mirror::source::cut_loop::CUT_BARRIER_FAIL_POINT;
use nodedb_types::fail_point::{FailAction, FailGuard};
use nodedb_types::{
    DatabaseId, MirrorFailure, MirrorPin, MirrorPinState, MirrorStatus, MirrorStatusStep,
};

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, database_on, link_pki,
    mirror_settings, read_mirror, rows, serve_source, use_database,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const COPY: &str = "recover_copy";

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
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('a', 1)")
        .await
        .expect("insert");
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

async fn await_failed(
    mirror: &TestCluster,
    name: &str,
    failed: impl Fn(&MirrorFailure) -> bool,
) -> DatabaseId {
    wait_for(
        &format!("every mirror node shows '{name}' failed"),
        Duration::from_secs(120),
        Duration::from_millis(100),
        || {
            mirror.nodes.iter().all(|node| {
                database_on(node, name)
                    .and_then(|database| status_on(node, database))
                    .is_some_and(
                        |status| matches!(status, MirrorStatus::Failed { failure } if failed(&failure)),
                    )
            })
        },
    )
    .await;
    database_on(&mirror.nodes[0], name).expect("the mirror exists")
}

async fn assert_statement_refused(mirror: &TestCluster, sql: &str, why: &str) {
    let refused = mirror
        .exec_ddl_on_any_leader(sql)
        .await
        .expect_err("the statement must refuse");
    assert!(refused.contains(why), "{sql}: {refused}");
}

/// A bootstrap stops after it re-issued rows. The rebuild leaves nothing of the
/// attempt: a collection the source dropped since then is gone with its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_rebuild_leaves_no_state_of_the_failed_bootstrap() {
    const DATABASE: &str = "recover_rebuild";
    let source = seeded_source(DATABASE).await;
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION stale (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create the collection the source later drops");
    source.nodes[0]
        .exec("INSERT INTO stale (id, v) VALUES ('s', 1)")
        .await
        .expect("insert");
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));

    let stop = FailGuard::fail(REISSUE_FAIL_POINT, "the test stops this bootstrap");
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{DATABASE}"
        ))
        .await
        .expect("MIRROR DATABASE");
    let database = await_failed(&mirror, COPY, |failure| {
        matches!(failure, MirrorFailure::BootstrapFailed { .. })
    })
    .await;
    for node in &mirror.nodes {
        assert!(
            node.shared
                .credentials
                .catalog()
                .get_collection(database, 1, "stale")
                .expect("catalog")
                .is_some(),
            "the failed attempt installed its catalog"
        );
    }
    // The failed status keeps the database readable and writes refused.
    assert!(
        mirror
            .nodes
            .iter()
            .all(|node| node.shared.mirror.fence.is_read_only(database))
    );
    assert_statement_refused(
        &mirror,
        &format!("ALTER DATABASE {COPY} MIRROR RESUME"),
        "MIRROR REBOOTSTRAP",
    )
    .await;

    drop(stop);
    source
        .exec_ddl_on_any_leader("DROP COLLECTION stale")
        .await
        .expect("drop");
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('b', 2)")
        .await
        .expect("insert");
    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} MIRROR REBOOTSTRAP"))
        .await
        .expect("MIRROR REBOOTSTRAP");
    assert_eq!(await_following(&mirror, COPY).await, database);
    read_mirror(&mirror, COPY).await;
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), "b".to_string()],
    )
    .await;
    for node in &mirror.nodes {
        let held = node
            .shared
            .credentials
            .catalog()
            .get_collection(database, 1, "stale")
            .expect("catalog");
        assert!(
            held.is_none_or(|row| !row.is_active),
            "the rebuild keeps no object of the failed attempt"
        );
    }

    // The rebuilt mirror keeps following.
    source.nodes[1]
        .exec("INSERT INTO docs (id, v) VALUES ('c', 3)")
        .await
        .expect("insert");
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), "b".to_string(), "c".to_string()],
    )
    .await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// The source expires the link's pin, so the log the mirror needs can go. Only a
/// rebuild clears the failure, and the rebuilt mirror follows again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_past_its_retained_log_rebuilds() {
    const DATABASE: &str = "recover_truncated";
    let source = seeded_source(DATABASE).await;
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
    await_mirror_reads(&mirror, "SELECT id FROM docs", &["a".to_string()]).await;

    let pin = source.nodes[0]
        .shared
        .credentials
        .catalog()
        .list_mirror_pins()
        .expect("pins")
        .into_iter()
        .find(|pin| pin.database == database.as_u64())
        .expect("the link's pin");
    propose_catalog_entry_async(
        &source.nodes[0].shared,
        &CatalogEntry::PutMirrorPin(Box::new(MirrorPin {
            state: MirrorPinState::Expired,
            ..pin
        })),
    )
    .await
    .expect("expire the pin");
    await_failed(&mirror, COPY, |failure| {
        matches!(failure, MirrorFailure::LogTruncated { .. })
    })
    .await;
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('b', 2)")
        .await
        .expect("insert");
    assert_statement_refused(
        &mirror,
        &format!("ALTER DATABASE {COPY} MIRROR RESUME"),
        "MIRROR REBOOTSTRAP",
    )
    .await;
    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} MIRROR REBOOTSTRAP"))
        .await
        .expect("MIRROR REBOOTSTRAP");
    await_following(&mirror, COPY).await;
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), "b".to_string()],
    )
    .await;
    source.nodes[1]
        .exec("INSERT INTO docs (id, v) VALUES ('c', 3)")
        .await
        .expect("insert");
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), "b".to_string(), "c".to_string()],
    )
    .await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// A cut's mark reads its metadata index ahead of its barriers. A value issued
/// between the two sits in the cut, with its reservation past the mark. The source
/// then dies, so no later cut covers it, and the promoted mirror still issues above
/// the value.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_final_cut_covers_the_values_it_holds() {
    const DATABASE: &str = "recover_final_value";
    let source = seeded_source(DATABASE).await;
    source
        .exec_ddl_on_any_leader("CREATE SEQUENCE ids")
        .await
        .expect("create sequence");
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

    let gates = tempfile::tempdir().expect("gate directory");
    let release = gates.path().join("release");
    let parked = gates.path().join("release.parked");
    let first = FailGuard::install(
        CUT_BARRIER_FAIL_POINT,
        FailAction::WaitForFile(release.clone()),
    );
    wait_for(
        "a source cut parks between its mark and its barriers",
        Duration::from_secs(30),
        Duration::from_millis(20),
        || parked.exists(),
    )
    .await;
    let value: i64 = rows(&source.nodes[1], "SELECT nextval('ids')")
        .await
        .first()
        .and_then(|row| row.trim().parse().ok())
        .expect("nextval answers one number");
    let id = format!("v{value}");
    source.nodes[1]
        .exec(&format!("INSERT INTO docs (id, v) VALUES ('{id}', 1)"))
        .await
        .expect("insert");

    // Every later cut parks for good, so this one is the mirror's last.
    drop(first);
    let _later_cuts = FailGuard::install(
        CUT_BARRIER_FAIL_POINT,
        FailAction::WaitForFile(gates.path().join("never")),
    );
    std::fs::write(&release, b"go").expect("release the cut");
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), id.clone()],
    )
    .await;
    source.shutdown().await;

    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} PROMOTE"))
        .await
        .expect("PROMOTE");
    wait_for(
        "every node shows the mirror promoted",
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
    for node in &mirror.nodes {
        let next: i64 = rows(node, "SELECT nextval('ids')")
            .await
            .first()
            .and_then(|row| row.trim().parse().ok())
            .expect("nextval answers one number");
        assert!(next > value, "{next} repeats the source's range of {value}");
    }

    mirror.shutdown().await;
}

/// A resumable failure resumes from the row's positions, with no row lost. A
/// following mirror takes neither statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_failure_resumes_and_follows_again() {
    const DATABASE: &str = "recover_resume";
    let source = seeded_source(DATABASE).await;
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
    await_mirror_reads(&mirror, "SELECT id FROM docs", &["a".to_string()]).await;
    assert_statement_refused(
        &mirror,
        &format!("ALTER DATABASE {COPY} MIRROR RESUME"),
        "has not failed",
    )
    .await;
    assert_statement_refused(
        &mirror,
        &format!("ALTER DATABASE {COPY} MIRROR REBOOTSTRAP"),
        "has not failed",
    )
    .await;

    propose_catalog_entry_async(
        &mirror.nodes[0].shared,
        &CatalogEntry::StepMirrorStatus {
            database_id: database.as_u64(),
            step: MirrorStatusStep::Fail {
                failure: MirrorFailure::UnsupportedSourceOp {
                    detail: "the test stops this link".to_string(),
                },
            },
        },
    )
    .await
    .expect("stop the link");
    await_failed(&mirror, COPY, |failure| {
        matches!(failure, MirrorFailure::UnsupportedSourceOp { .. })
    })
    .await;
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('b', 2)")
        .await
        .expect("insert");
    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} MIRROR RESUME"))
        .await
        .expect("MIRROR RESUME");
    await_following(&mirror, COPY).await;
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), "b".to_string()],
    )
    .await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// Two statements for one source race to the log. The first wins, and the second
/// gets the typed refusal with no row of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_race_for_one_source_has_one_winner() {
    const DATABASE: &str = "recover_race";
    let source = seeded_source(DATABASE).await;
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));
    let first = format!("MIRROR DATABASE race_one FROM {SOURCE_NAME}.{DATABASE}");
    let second = format!("MIRROR DATABASE race_two FROM {SOURCE_NAME}.{DATABASE}");
    let (one, two) = tokio::join!(
        mirror.exec_ddl_on_any_leader(&first),
        mirror.exec_ddl_on_any_leader(&second),
    );
    assert!(
        one.is_ok() != two.is_ok(),
        "exactly one statement wins: {one:?} {two:?}"
    );
    let (winner, loser, refusal) = match (one, two) {
        (Ok(_), Err(refusal)) => ("race_one", "race_two", refusal),
        (Err(refusal), Ok(_)) => ("race_two", "race_one", refusal),
        other => panic!("exactly one statement wins: {other:?}"),
    };
    assert!(refusal.contains("one mirror per source"), "{refusal}");
    await_following(&mirror, winner).await;
    assert!(
        mirror
            .nodes
            .iter()
            .all(|node| database_on(node, loser).is_none()),
        "the loser wrote no row"
    );

    mirror.shutdown().await;
    source.shutdown().await;
}

/// The hello names another timeline than the first capture's, so the log this
/// mirror holds positions in no longer exists. Only a rebuild clears it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_of_another_timeline_stops_the_link() {
    const DATABASE: &str = "recover_timeline";
    let source = seeded_source(DATABASE).await;
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
    let mut state = mirror.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_mirror_cut_state(database)
        .expect("catalog")
        .expect("the cut state");
    state.source_timeline = Some(u64::MAX);
    propose_catalog_entry_async(
        &mirror.nodes[0].shared,
        &CatalogEntry::PutMirrorCutState {
            database_id: database.as_u64(),
            state: Box::new(state),
        },
    )
    .await
    .expect("seed another timeline");
    let mirror = mirror.restart_all().await.expect("restart the mirror");
    boot_mirror_loops(&mirror, &settings);
    await_failed(&mirror, COPY, |failure| {
        matches!(failure, MirrorFailure::SourceTimelineChanged)
    })
    .await;
    assert_statement_refused(
        &mirror,
        &format!("ALTER DATABASE {COPY} MIRROR RESUME"),
        "MIRROR REBOOTSTRAP",
    )
    .await;
    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} MIRROR REBOOTSTRAP"))
        .await
        .expect("MIRROR REBOOTSTRAP");
    await_following(&mirror, COPY).await;

    mirror.shutdown().await;
    source.shutdown().await;
}
