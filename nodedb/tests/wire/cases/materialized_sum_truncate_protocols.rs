// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE of the source of a cross-shard materialized sum over the pgwire
//! and native protocols, with the source changed between settlement and the
//! TRUNCATE's transaction.
//!
//! Settlement ships the balance delta of the removed rows from the source
//! images it reads. Only the OCC read versions stamped on those images guard
//! the delta against a source row that moves afterwards. Every protocol must
//! carry them into the TRUNCATE's transaction.
//!
//! The `recon::before_submit` gate parks the TRUNCATE after settlement. The
//! test commits the source change while it is parked, then releases it. The
//! balance must equal the source sum whether the TRUNCATE aborts or settles
//! again. Each test runs on a collection without edges.

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request_within};
use nodedb_types::protocol::opcodes::OpCode;
use nodedb_types::protocol::text_fields::TextFields;

use super::materialized_sum_cross_shard_expanders::{FIXTURE_CORES, balance, sum_fixture_on};
use crate::harness::TestServer;

/// The concurrent change: `e2` joins `acc`, which goes from 15 to 22.
const MOVE_SOURCE: &str = "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e2', 'acc', '7')";
/// How long a native request waits for its parked answer.
const PARKED_BOUND: Duration = Duration::from_secs(60);

/// A server whose recon writes park at `recon::before_submit` until
/// `release` exists, with the sum fixture created.
async fn parked_fixture(release: &Path) -> TestServer {
    let spec = format!("recon::before_submit=wait_file({})", release.display());
    let server = TestServer::start_multicores_with_failpoints(FIXTURE_CORES, &spec).await;
    sum_fixture_on(server).await
}

/// Wait until the gate reports a write parked.
async fn wait_parked(release: &Path) {
    let mut marker = release.as_os_str().to_owned();
    marker.push(".parked");
    let marker = PathBuf::from(marker);
    for _ in 0..600 {
        if marker.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the TRUNCATE never reached recon::before_submit");
}

/// The sum of the `acc` postings, read from the source rows.
async fn source_sum(server: &TestServer) -> i64 {
    server
        .query_text("SELECT amount FROM ptv_post WHERE account_id = 'acc'")
        .await
        .expect("read the source postings")
        .iter()
        .map(|amount| amount.parse::<i64>().expect("a numeric amount"))
        .sum()
}

/// Move the source while `parked` waits at the gate, release it, and check
/// that the balance equals the source sum.
async fn move_source_then_release<F>(
    server: &TestServer,
    release: &Path,
    parked: tokio::task::JoinHandle<F>,
) {
    wait_parked(release).await;
    tokio::time::timeout(Duration::from_secs(30), server.exec(MOVE_SOURCE))
        .await
        .expect("the concurrent source change did not return while the TRUNCATE was parked")
        .expect("the concurrent source change");
    assert_eq!(
        balance(server, "acc").await,
        "22",
        "the concurrent change settles its own delta"
    );
    std::fs::write(release, b"release").expect("release the parked TRUNCATE");
    // Either end is fine: the check is the balance, not the answer.
    parked.await.expect("the parked TRUNCATE task");

    let sum = source_sum(server).await;
    assert_eq!(
        balance(server, "acc").await,
        sum.to_string(),
        "the balance must equal the source sum: the TRUNCATE either aborts \
         or settles again, and never applies the delta it computed from the old images"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_truncate_keeps_the_sum_when_a_settled_source_row_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-truncate");
    let server = parked_fixture(&release).await;

    let (writer, _writer_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the parked writer");
    let parked = tokio::spawn(async move {
        writer
            .simple_query("TRUNCATE ptv_post")
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    });
    move_source_then_release(&server, &release, parked).await;
}

/// The same TRUNCATE over the native protocol's SQL opcode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_truncate_keeps_the_sum_when_a_settled_source_row_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-truncate");
    let server = parked_fixture(&release).await;

    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    // `send_sql` gives up after 5 s. A parked request needs a longer bound.
    let parked = tokio::spawn(async move {
        send_request_within(
            &mut stream,
            2,
            OpCode::Sql,
            TextFields {
                sql: Some("TRUNCATE ptv_post".into()),
                ..Default::default()
            },
            Some(PARKED_BOUND),
        )
        .await
        .status
    });
    move_source_then_release(&server, &release, parked).await;
}

/// The same TRUNCATE through `OpCode::DocumentTruncate`, which runs in
/// `handle_direct_op`. `build_truncate` reads only the collection name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_document_truncate_keeps_the_sum_when_a_settled_source_row_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-truncate");
    let server = parked_fixture(&release).await;

    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let parked = tokio::spawn(async move {
        send_request_within(
            &mut stream,
            2,
            OpCode::DocumentTruncate,
            TextFields {
                collection: Some("ptv_post".to_string()),
                ..Default::default()
            },
            Some(PARKED_BOUND),
        )
        .await
        .status
    });
    move_source_then_release(&server, &release, parked).await;
}
