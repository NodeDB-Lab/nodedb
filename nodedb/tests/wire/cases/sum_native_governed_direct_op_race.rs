// SPDX-License-Identifier: BUSL-1.1

//! A governed native predicate write on the cross-shard sum source, with the
//! source changed between settlement and the write's Calvin transaction.
//!
//! Settlement folds the balance delta from the source images it reads. Only
//! the OCC read versions of those images guard the delta, so the transaction
//! must carry them. The write must commit or abort with SQLSTATE 40001. The
//! balance must equal the source sum either way.
//!
//! Only a predicate write has a gate on the native route: `recon::before_submit`.
//! A point write has none, so its cell is a data assertion in the parent module.

use std::path::{Path, PathBuf};
use std::time::Duration;

use nodedb_types::protocol::{NativeResponse, OpCode};

use super::super::materialized_sum_cross_shard_expanders::{
    FIXTURE_CORES, balance, sum_fixture_on,
};
use super::{POSTINGS, Writer, bulk_update_fields, govern, source_sum};
use crate::harness::TestServer;

/// The gate a recon write parks at before it submits its Calvin transaction.
const RECON_GATE: &str = "recon::before_submit";
/// How long a parked request waits for its answer.
const PARKED_BOUND: Duration = Duration::from_secs(60);
/// The SQLSTATE of a serialization conflict.
const SERIALIZATION_FAILURE: &str = "40001";
/// The concurrent change: `e0` goes from 10 to 20, and `acc` from 15 to 25.
const MOVE_SOURCE: &str = "UPDATE ptv_post SET amount = '20' WHERE id = 'e0'";

/// `release` with `.parked` appended: the marker a gate writes on arrival.
fn parked_marker(release: &Path) -> PathBuf {
    let mut marker = release.as_os_str().to_owned();
    marker.push(".parked");
    PathBuf::from(marker)
}

/// A server whose recon writes park at `recon::before_submit` until
/// `release` exists, with the sum fixture created. The gate is armed on return.
async fn parked_fixture(release: &Path) -> TestServer {
    std::fs::write(release, b"open").expect("open the gate for the fixture");
    let spec = format!("{RECON_GATE}=wait_file({})", release.display());
    let server = TestServer::start_multicores_with_failpoints(FIXTURE_CORES, &spec).await;
    let server = sum_fixture_on(server).await;
    if let Err(e) = std::fs::remove_file(parked_marker(release))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        panic!("clear the marker an open pass left: {e}");
    }
    std::fs::remove_file(release).expect("arm the gate");
    server
}

/// Wait until the `what` write reports parked at `recon::before_submit`.
async fn wait_parked(release: &Path, what: &str) {
    let marker = parked_marker(release);
    for _ in 0..600 {
        if marker.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the {what} never reached {RECON_GATE}");
}

/// Assert that a native `response` either committed or aborted with a
/// serialization conflict. `what` names the write.
fn assert_commit_or_conflict(response: &NativeResponse, what: &str) {
    if let Some(error) = &response.error {
        assert_eq!(
            error.code, SERIALIZATION_FAILURE,
            "{what} must commit or abort with a serialization conflict, got {}: {}",
            error.code, error.message
        );
    }
}

/// Run `sql` as the superuser. A statement that parks fails the test.
async fn exec_bounded(server: &TestServer, sql: &str) {
    tokio::time::timeout(Duration::from_secs(30), server.exec(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql} did not return while the write was parked"))
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// Release the parked write, then assert how it ended and that the balance
/// equals the source sum. `what` names the write.
async fn release_then_check(
    server: &TestServer,
    release: &Path,
    parked: tokio::task::JoinHandle<NativeResponse>,
    what: &str,
) {
    std::fs::write(release, b"release").expect("release the parked write");
    let ended = parked.await.expect("the parked write task");
    assert_commit_or_conflict(&ended, what);
    let sum = source_sum(server).await;
    assert_eq!(
        balance(server, "acc").await,
        sum.to_string(),
        "{what}: the balance must equal the source sum. The write either aborts or \
         settles again, and never applies the delta it folded from the old image"
    );
}

/// A predicate write parks at `recon::before_submit` after settlement and
/// reconnaissance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_bulk_update_keeps_the_sum_when_a_settled_source_row_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-recon");
    let server = parked_fixture(&release).await;
    govern(&server, POSTINGS).await;
    let mut writer = Writer::open(&server).await;

    let parked = tokio::spawn(async move {
        writer
            .op_within(
                OpCode::DocumentBulkUpdate,
                bulk_update_fields("account_id", "acc", "1"),
                PARKED_BOUND,
            )
            .await
    });
    wait_parked(&release, "governed bulk update").await;
    exec_bounded(&server, MOVE_SOURCE).await;
    assert_eq!(
        balance(&server, "acc").await,
        "25",
        "the concurrent change settles its own delta"
    );
    release_then_check(&server, &release, parked, "the governed bulk update").await;
}
