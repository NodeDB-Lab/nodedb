// SPDX-License-Identifier: BUSL-1.1

//! `PROMOTE` and `MIRROR RESUME` refuse a state they cannot serve, and name the
//! way out. A success answer here leaves the status unchanged.

use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, MirrorFailure, MirrorStatus};

use super::helpers::{assert_sqlstate, inject_cut_state_for_id, mark_database_mirror};

const NOT_IN_STATE: &str = "55000";
const REBOOTSTRAP: &str = "REBOOTSTRAP";

/// A mirror that failed before its first cut, on a failure `RESUME` takes.
async fn failed_before_first_cut() -> (TestServer, String) {
    let (server, name) = TestServer::with_database("mirror_refusal").await;
    mark_database_mirror(
        &server,
        &name,
        MirrorStatus::Failed {
            failure: MirrorFailure::UnsupportedSourceOp {
                detail: "the test stops this bootstrap".to_string(),
            },
        },
    )
    .await;
    (server, name)
}

fn database_id(server: &TestServer, name: &str) -> DatabaseId {
    server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .expect("catalog lookup")
        .expect("database exists")
}

fn status_of(server: &TestServer, database: DatabaseId) -> MirrorStatus {
    server
        .shared
        .credentials
        .catalog()
        .get_database(database)
        .expect("catalog read")
        .expect("descriptor exists")
        .mirror_origin
        .expect("mirror origin")
        .status
}

async fn assert_refusal_names_rebootstrap(server: &TestServer, sql: &str) {
    assert_sqlstate(&server.client, sql, NOT_IN_STATE).await;
    server.expect_error(sql, REBOOTSTRAP).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_refuses_a_database_that_never_was_a_mirror() {
    let (server, name) = TestServer::with_database("plain_promote").await;
    assert_sqlstate(
        &server.client,
        &format!("ALTER DATABASE {name} PROMOTE"),
        "0A000",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_of_a_mirror_failed_before_its_first_cut_names_rebootstrap() {
    let (server, name) = failed_before_first_cut().await;
    assert_refusal_names_rebootstrap(&server, &format!("ALTER DATABASE {name} PROMOTE")).await;
    assert!(matches!(
        status_of(&server, database_id(&server, &name)),
        MirrorStatus::Failed { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_of_a_mirror_failed_before_its_first_cut_names_rebootstrap() {
    let (server, name) = failed_before_first_cut().await;
    assert_refusal_names_rebootstrap(&server, &format!("ALTER DATABASE {name} MIRROR RESUME"))
        .await;
    assert!(matches!(
        status_of(&server, database_id(&server, &name)),
        MirrorStatus::Failed { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_of_a_mirror_that_holds_a_cut_follows_again() {
    let (server, name) = failed_before_first_cut().await;
    let database = database_id(&server, &name);
    inject_cut_state_for_id(server.shared.credentials.catalog(), database, 0, 5);
    server
        .exec(&format!("ALTER DATABASE {name} MIRROR RESUME"))
        .await
        .expect("RESUME");
    assert_eq!(status_of(&server, database), MirrorStatus::Following);
}
