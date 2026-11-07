// SPDX-License-Identifier: BUSL-1.1

//! A write of a cross-shard sum source whose net delta is zero.
//!
//! Settlement ships no balance task for it, so the statement stays on the
//! source's vShard. It still folded the balance from source images and removed
//! the balance from the source write. Its reads guard that fold, so it commits
//! through the scheduler. A source row that moves after settlement aborts it.

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};

/// A posting of `acc2` that the writes below leave at 10.
const SEED: &str = "INSERT INTO ptv_post (id, account_id, amount) VALUES ('z0', 'acc2', '10')";
/// A point write of one row to its stored amount.
const POINT: &str = "UPDATE ptv_post SET amount = '10' WHERE id = 'z0'";
/// A predicate write of the same row to its stored amount.
const PREDICATE: &str = "UPDATE ptv_post SET amount = '10' WHERE account_id = 'acc2'";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_net_write_commits_over_pgwire_and_keeps_the_balance() {
    let server = sum_fixture().await;
    server.exec(SEED).await.expect("seed the posting");

    server.exec(POINT).await.expect("the point write");
    assert_eq!(balance(&server, "acc2").await, "10");

    server.exec(PREDICATE).await.expect("the predicate write");
    assert_eq!(balance(&server, "acc2").await, "10");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_net_write_commits_over_http_and_keeps_the_balance() {
    let server = sum_fixture().await;
    server.exec(SEED).await.expect("seed the posting");

    server.http_exec(POINT).await;
    assert_eq!(balance(&server, "acc2").await, "10");

    server.http_exec(PREDICATE).await;
    assert_eq!(balance(&server, "acc2").await, "10");
}

#[cfg(feature = "failpoints")]
mod parked {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::super::materialized_sum_cross_shard_expanders::{
        FIXTURE_CORES, balance, sum_fixture_on,
    };
    use super::{PREDICATE, SEED};
    use crate::harness::TestServer;

    /// The concurrent change: `z0` goes from 10 to 20, and `acc2` from 10 to 20.
    const MOVE_SOURCE: &str = "UPDATE ptv_post SET amount = '20' WHERE id = 'z0'";

    async fn parked_fixture(release: &Path) -> TestServer {
        let spec = format!("recon::before_submit=wait_file({})", release.display());
        let server = TestServer::start_multicores_with_failpoints(FIXTURE_CORES, &spec).await;
        let server = sum_fixture_on(server).await;
        server.exec(SEED).await.expect("seed the posting");
        server
    }

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
        panic!("the zero-net write never reached recon::before_submit");
    }

    /// Move the source while the write parks, release it, and check the balance
    /// against the source row.
    async fn move_source_then_release(server: &TestServer, release: &Path) {
        wait_parked(release).await;
        server
            .exec(MOVE_SOURCE)
            .await
            .expect("the concurrent source change");
        assert_eq!(balance(server, "acc2").await, "20", "its own delta landed");
        std::fs::write(release, b"release").expect("release the parked write");
    }

    async fn assert_balance_equals_source(server: &TestServer) {
        let amounts = server
            .query_text("SELECT amount FROM ptv_post WHERE account_id = 'acc2'")
            .await
            .expect("read the source posting");
        assert_eq!(amounts, ["20"], "the aborted write changed no row");
        assert_eq!(balance(server, "acc2").await, "20");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pgwire_zero_net_predicate_write_aborts_when_a_settled_source_row_moved() {
        let gate_dir = tempfile::tempdir().expect("gate tempdir");
        let release = gate_dir.path().join("release-zero-net");
        let server = parked_fixture(&release).await;

        let (writer, _writer_handle) = server
            .connect_as("nodedb", "nodedb")
            .await
            .expect("connect the parked writer");
        let parked = tokio::spawn(async move {
            writer
                .simple_query(PREDICATE)
                .await
                .map(|_| ())
                .map_err(|e| e.code().map(|code| code.code().to_owned()))
        });
        move_source_then_release(&server, &release).await;

        assert_eq!(
            parked.await.expect("the parked write task"),
            Err(Some("40001".to_owned())),
            "a stale settlement read aborts the write"
        );
        assert_balance_equals_source(&server).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_zero_net_predicate_write_aborts_when_a_settled_source_row_moved() {
        let gate_dir = tempfile::tempdir().expect("gate tempdir");
        let release = gate_dir.path().join("release-zero-net");
        let server = parked_fixture(&release).await;

        let http_port = server.http_port;
        let parked = tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("http://127.0.0.1:{http_port}/v1/query"))
                .json(&serde_json::json!({ "sql": PREDICATE }))
                .send()
                .await
                .map(|response| response.status())
                .map_err(|e| e.to_string())
        });
        move_source_then_release(&server, &release).await;

        let status = parked
            .await
            .expect("the parked write task")
            .expect("the HTTP answer");
        // HTTP renders the `40001` abort as status 409 with no SQLSTATE in the
        // body.
        assert_eq!(status.as_u16(), 409, "the write aborts");
        assert_balance_equals_source(&server).await;
    }
}
