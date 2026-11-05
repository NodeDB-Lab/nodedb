// SPDX-License-Identifier: BUSL-1.1

//! A predicate write of the source of a cross-shard materialized sum, with
//! the source changed between settlement and the recon transaction.
//!
//! Settlement folds the balance delta of the write from the source images it
//! reads, and ships the delta to the target's vShard. A source row that moves
//! after that leaves the delta stale: the recon scan sees the same row
//! identity, so its prediction does not drift. Only the settlement reads,
//! validated at their cut, guard the delta. The recon transaction must carry
//! them and their snapshot ordinal.
//!
//! The `recon::before_submit` gate parks the write after settlement and
//! reconnaissance. The test commits the source change while the write is
//! parked, then releases it.

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
            .map_err(|e| e.code().map(|code| code.code().to_owned()))
    });

    wait_parked(&release).await;
    server
        .exec(MOVE_SOURCE)
        .await
        .expect("the concurrent source change");
    assert_eq!(
        balance(&server, "acc").await,
        "25",
        "the concurrent change settles its own delta"
    );
    std::fs::write(&release, b"release").expect("release the parked write");

    assert_eq!(
        parked.await.expect("the parked write task"),
        Err(Some("40001".to_owned())),
        "a stale settlement read aborts the write"
    );
    assert_eq!(
        balance(&server, "acc").await,
        source_sum(&server).await.to_string(),
        "the balance must equal the source sum"
    );
}
