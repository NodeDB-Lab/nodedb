// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE of an edge-bearing source of a cross-shard materialized sum over
//! HTTP, with the source changed between settlement and the TRUNCATE's
//! transaction.
//!
//! Settlement ships the balance delta of the removed rows from the source
//! images it reads. A source row that moves after that leaves the delta stale.
//! Only the settlement reads, validated at their cut, guard it.
//!
//! The `recon::before_submit` gate parks the TRUNCATE after settlement. The
//! test commits the source change while it is parked, then releases it.

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::materialized_sum_cross_shard_expanders::{FIXTURE_CORES, balance, sum_fixture_on};
use crate::harness::TestServer;

/// The concurrent change: `e2` joins `acc`, which goes from 15 to 22.
const MOVE_SOURCE: &str = "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e2', 'acc', '7')";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_bearing_truncate_keeps_the_sum_when_a_settled_source_row_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-truncate");
    let spec = format!("recon::before_submit=wait_file({})", release.display());
    let server = TestServer::start_multicores_with_failpoints(FIXTURE_CORES, &spec).await;
    let server = sum_fixture_on(server).await;
    server
        .exec("GRAPH INSERT EDGE IN 'ptv_post' FROM 'e0' TO 'e1' TYPE 'l'")
        .await
        .expect("write one edge into the source");

    let http_port = server.http_port;
    let parked = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/v1/query"))
            .json(&serde_json::json!({ "sql": "TRUNCATE ptv_post" }))
            .send()
            .await
            .map(|response| response.status())
            .map_err(|e| e.to_string())
    });
    wait_parked(&release).await;
    tokio::time::timeout(Duration::from_secs(30), server.exec(MOVE_SOURCE))
        .await
        .expect("the concurrent source change did not return while the TRUNCATE was parked")
        .expect("the concurrent source change");
    assert_eq!(
        balance(&server, "acc").await,
        "22",
        "the concurrent change settles its own delta"
    );
    std::fs::write(&release, b"release").expect("release the parked TRUNCATE");
    let status = parked
        .await
        .expect("the parked TRUNCATE task")
        .expect("the HTTP answer");
    assert!(
        !status.is_success(),
        "a stale settlement read aborts the TRUNCATE: {status}"
    );

    assert_eq!(
        balance(&server, "acc").await,
        source_sum(&server).await.to_string(),
        "the balance must equal the source sum"
    );
}
