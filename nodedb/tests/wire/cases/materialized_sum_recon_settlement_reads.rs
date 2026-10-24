// SPDX-License-Identifier: BUSL-1.1

//! A predicate write of the source of a cross-shard materialized sum, with
//! the source changed between settlement and the recon transaction.
//!
//! Settlement folds the balance delta of the write from the source images it
//! reads, and ships the delta to the target's vShard. A source row that moves
//! after that leaves the delta stale: the recon scan sees the same row
//! identity, so its prediction does not drift. Only the OCC read versions
//! settlement stamped on those images guard the delta, so the recon
//! transaction must carry them.
//!
//! The `recon::before_submit` gate parks the write after settlement and
//! reconnaissance. The test commits the source change while the write is
//! parked, then releases it. The balance must equal the source sum whether the
//! write aborts or recomputes.

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::materialized_sum_cross_shard_expanders::{FIXTURE_CORES, balance, sum_fixture_on};
use crate::harness::TestServer;

/// Sets both postings of `acc` (10 and 5) to 1. Settlement ships -13.
const PREDICATE_UPDATE: &str = "UPDATE ptv_post SET amount = '1' WHERE account_id = 'acc'";
/// The concurrent change: `e0` goes from 10 to 20, and `acc` from 15 to 25.
const MOVE_SOURCE: &str = "UPDATE ptv_post SET amount = '20' WHERE id = 'e0'";

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
    panic!("the predicate write never reached recon::before_submit");
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
/// that the balance equals the source sum. Returns how the write ended.
async fn move_source_then_release<F>(
    server: &TestServer,
    release: &Path,
    parked: tokio::task::JoinHandle<F>,
) -> F {
    wait_parked(release).await;
    server
        .exec(MOVE_SOURCE)
        .await
        .expect("the concurrent source change");
    assert_eq!(
        balance(server, "acc").await,
        "25",
        "the concurrent change settles its own delta"
    );
    std::fs::write(release, b"release").expect("release the parked write");
    let ended = parked.await.expect("the parked write task");

    let sum = source_sum(server).await;
    assert_eq!(
        balance(server, "acc").await,
        sum.to_string(),
        "the balance must equal the source sum: the parked write either aborts \
         or settles again, and never applies the delta it computed from the old images"
    );
    ended
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_recon_write_keeps_the_sum_when_a_settled_source_row_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-recon");
    let server = parked_fixture(&release).await;

    let http_port = server.http_port;
    let parked = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/v1/query"))
            .json(&serde_json::json!({ "sql": PREDICATE_UPDATE }))
            .send()
            .await
            .map(|response| response.status())
            .map_err(|e| e.to_string())
    });
    move_source_then_release(&server, &release, parked)
        .await
        .expect("the HTTP answer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_recon_write_keeps_the_sum_when_a_settled_source_row_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-recon");
    let server = parked_fixture(&release).await;

    let (writer, _writer_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the parked writer");
    let parked = tokio::spawn(async move {
        writer
            .simple_query(PREDICATE_UPDATE)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    });
    // Either end is fine: the check is the balance, not the answer.
    let _ = move_source_then_release(&server, &release, parked).await;
}

/// The same predicate write as `PREDICATE_UPDATE`, over the native protocol's
/// SQL opcode. It reaches the shared `route_atomic_statement` path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_predicate_write_keeps_the_sum_after_a_concurrent_change() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-recon");
    let server = parked_fixture(&release).await;

    let mut stream =
        nodedb_test_support::native_harness::open_trust_session(server.native_port, "nodedb").await;
    let parked = tokio::spawn(async move {
        nodedb_test_support::native_harness::send_sql(&mut stream, 2, PREDICATE_UPDATE)
            .await
            .status
    });
    // Either end is fine: the check is the balance, not the answer.
    let _ = move_source_then_release(&server, &release, parked).await;
}

/// The same predicate write through `OpCode::DocumentBulkUpdate`, which runs in
/// `handle_direct_op` and passes its own `sum_target_reads` to the recon gates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_bulk_update_keeps_the_sum_after_a_concurrent_change() {
    use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
    use nodedb_test_support::native_harness::{open_trust_session, send_request};
    use nodedb_types::Value;
    use nodedb_types::protocol::opcodes::OpCode;
    use nodedb_types::protocol::text_fields::TextFields;

    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-recon");
    let server = parked_fixture(&release).await;

    let filters = zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: "account_id".to_string(),
        op: FilterOp::Eq,
        value: Value::String("acc".to_string()),
        clauses: Vec::new(),
        expr: None,
    }])
    .expect("encode the filter");
    let amount = nodedb_types::value_to_msgpack(&Value::String("1".to_string()))
        .expect("encode the new amount");
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let parked = tokio::spawn(async move {
        send_request(
            &mut stream,
            2,
            OpCode::DocumentBulkUpdate,
            TextFields {
                collection: Some("ptv_post".to_string()),
                filters: Some(filters),
                updates: Some(vec![("amount".to_string(), amount)]),
                ..Default::default()
            },
        )
        .await
        .status
    });
    let _ = move_source_then_release(&server, &release, parked).await;
}
