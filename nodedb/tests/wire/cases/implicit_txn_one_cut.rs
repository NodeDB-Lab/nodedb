// SPDX-License-Identifier: BUSL-1.1

//! An implicit transaction reads one cut, taken at its first statement, and
//! its COMMIT checks every read, the leading ones too.
//!
//! The sequential runner opens a private transaction. Its reads join the read
//! set, so COMMIT checks them at the transaction's snapshot. A read with no
//! snapshot fails that check with XX000.
//!
//! pgwire opens its implicit block at the first statement of a run that holds
//! a statement that is not a read. So a leading read joins the block, and a
//! change of the row it read answers 40001 at COMMIT.
//!
//! Levers:
//! - Drop the snapshot bind in `run_statement` (`sequential.rs`). The two
//!   sequential cells answer XX000.
//! - Make `run_opens_implicit_block` ask only the head statement. The pgwire
//!   race answers success, and the feed row holds 21.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use super::materialized_sum_cross_shard_expanders::sum_fixture;
use super::wire_support::post;

const READ_E0: &str = "SELECT amount FROM ptv_post WHERE id = 'e0'";
/// A router-claimed statement that sends the request to the sequential
/// runner.
const SHOW: &str = "SHOW COLLECTIONS";
const WRITE_E0: &str = "UPDATE ptv_post SET amount = '11' WHERE id = 'e0'";

/// A native sequential request that reads, then writes, commits with no
/// concurrent change. Its read holds the transaction's cut, so COMMIT can
/// check it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_native_sequential_read_then_write_commits() {
    let server = sum_fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let sql = format!("{READ_E0}; {SHOW}; {WRITE_E0}");
    let response = send_sql(&mut stream, 2, &sql).await;
    assert!(
        response.error.is_none(),
        "the request must commit, got {:?}",
        response.error
    );
    let stored = server.query_text(READ_E0).await.expect("read e0");
    assert_eq!(stored, vec!["11".to_string()], "the write must commit");
}

/// An HTTP sequential request that reads, then writes, commits with no
/// concurrent change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_http_sequential_read_then_write_commits() {
    let server = sum_fixture().await;
    let sql = format!("{READ_E0}; {SHOW}; {WRITE_E0}");
    let (status, raw) = post(&server, "/v1/query", &sql).await;
    assert_eq!(status, 200, "the request must commit: {raw}");
    let stored = server.query_text(READ_E0).await.expect("read e0");
    assert_eq!(stored, vec!["11".to_string()], "the write must commit");
}

#[cfg(feature = "failpoints")]
mod raced {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::super::materialized_sum_cross_shard_expanders::{
        FIXTURE_CORES, feed, sum_fixture_on,
    };
    use super::READ_E0;
    use crate::harness::TestServer;

    /// The gate a pgwire statement parks at after it plans, so after its cut.
    const ROUTE_GATE: &str = "stmt::before_route";
    /// A write that stages a row, so COMMIT checks the block's reads. A block
    /// with no write commits on its snapshot.
    const WRITE_FEED: &str = "UPDATE ptv_feed SET amount = '21' WHERE id = 'e0'";
    const READ_FEED: &str = "SELECT amount FROM ptv_feed WHERE id = 'e0'";
    /// Moves `e0` from 10 to 20 through the feed row. It runs in an implicit
    /// transaction, which returns before [`ROUTE_GATE`].
    const MOVE_E0_FROM_FEED: &str = "UPDATE ptv_post SET amount = f.amount \
                                     FROM ptv_feed f WHERE ptv_post.id = f.id";
    /// The SQLSTATE of a serialization error.
    const SERIALIZATION_FAILURE: &str = "40001";

    fn parked_marker(release: &Path) -> PathBuf {
        let mut marker = release.as_os_str().to_owned();
        marker.push(".parked");
        PathBuf::from(marker)
    }

    /// Close the open gate: remove the marker a pass left, then the release
    /// file. The next statement to reach the gate parks.
    fn arm_gate(release: &Path) {
        if let Err(e) = std::fs::remove_file(parked_marker(release))
            && e.kind() != std::io::ErrorKind::NotFound
        {
            panic!("clear the marker an open pass left: {e}");
        }
        std::fs::remove_file(release).expect("arm the gate");
    }

    async fn wait_parked(release: &Path) {
        let marker = parked_marker(release);
        for _ in 0..600 {
            if marker.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the leading read never reached {ROUTE_GATE}");
    }

    /// A read of `e0` that parks after its cut, then a write that stages,
    /// over pgwire, with `e0` changed while the read is parked. The block
    /// holds the read, so COMMIT answers 40001 and the write rolls back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_pgwire_leading_read_is_checked_when_the_implicit_block_commits() {
        let gate_dir = tempfile::tempdir().expect("gate tempdir");
        let release = gate_dir.path().join("release");
        std::fs::write(&release, b"open").expect("open the gate for the fixture");
        let spec = format!("{ROUTE_GATE}=wait_file({})", release.display());
        let server = TestServer::start_multicores_with_failpoints(FIXTURE_CORES, &spec).await;
        let server = sum_fixture_on(server).await;
        feed(&server, "e0", "acc", "20").await;
        let (client, connection) = server
            .connect_as("nodedb", "nodedb")
            .await
            .expect("connect the request session");
        arm_gate(&release);
        let parked = tokio::spawn(async move {
            let _connection = connection;
            let sql = format!("{READ_E0}; {WRITE_FEED}");
            client.simple_query(&sql).await.map(drop)
        });
        wait_parked(&release).await;
        tokio::time::timeout(Duration::from_secs(30), server.exec(MOVE_E0_FROM_FEED))
            .await
            .expect("the move returned while the read was parked")
            .expect("move e0");
        std::fs::write(&release, b"release").expect("release the parked read");
        let end = parked.await.expect("the parked request task");
        let code = end
            .as_ref()
            .err()
            .and_then(|e| e.code().map(|code| code.code().to_owned()));
        assert_eq!(
            code.as_deref(),
            Some(SERIALIZATION_FAILURE),
            "COMMIT must check the leading read of a row that moved, got {end:?}"
        );
        let stored = server
            .query_text(READ_FEED)
            .await
            .expect("read the feed row");
        assert_eq!(stored, vec!["20".to_string()], "the write must roll back");
    }
}
