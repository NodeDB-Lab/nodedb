// SPDX-License-Identifier: BUSL-1.1

//! A CRDT document delete in one request with a write to the source of a
//! cross-shard materialized sum, with the source changed between settlement
//! and the transaction.
//!
//! Settlement ships the balance delta of the source write from the images it
//! reads. The CRDT delete's transaction carries that delta, so it must carry
//! the read versions that guard it. The `recon::before_submit` gate parks the
//! request after settlement. The test commits a source change while it is
//! parked, then releases it. The balance must equal the source sum whether the
//! request aborts or settles again.

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::materialized_sum_cross_shard_expanders::{FIXTURE_CORES, balance, sum_fixture_on};
use crate::harness::TestServer;

const CRDT: &str = "ptv_crdt_nodes";
/// The request's source write. Settlement ships -9 for `e0` going 10 to 1.
const REQUEST_WRITE: &str = "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'";
/// The concurrent change: `e0` goes from 10 to 20, and `acc` from 15 to 25.
const MOVE_SOURCE: &str = "UPDATE ptv_post SET amount = '20' WHERE id = 'e0'";

async fn wait_parked(release: &Path) {
    let mut marker = release.as_os_str().to_owned();
    marker.push(".parked");
    let marker = PathBuf::from(marker);
    for _ in 0..200 {
        if marker.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the request never reached recon::before_submit");
}

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
async fn crdt_delete_beside_a_sum_source_write_keeps_the_sum_when_the_source_moved() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-crdt");
    let spec = format!("recon::before_submit=wait_file({})", release.display());
    let server = TestServer::start_multicores_with_failpoints(FIXTURE_CORES, &spec).await;
    let server = sum_fixture_on(server).await;
    server
        .exec(&format!(
            "CREATE TABLE {CRDT} (id TEXT PRIMARY KEY, name TEXT) WITH (crdt='true')"
        ))
        .await
        .expect("create the crdt collection");
    server
        .exec(&format!("INSERT INTO {CRDT} (id, name) VALUES ('a', 'A')"))
        .await
        .expect("insert the document");
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{CRDT}' FROM 'a' TO 'b' TYPE 'knows'"
        ))
        .await
        .expect("insert the edge");

    let http_port = server.http_port;
    let sql = format!("DELETE FROM {CRDT} WHERE id = 'a'; {REQUEST_WRITE}");
    let parked = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/v1/query"))
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .await
            .map(|response| response.status())
            .map_err(|e| e.to_string())
    });
    wait_parked(&release).await;
    tokio::time::timeout(Duration::from_secs(30), server.exec(MOVE_SOURCE))
        .await
        .expect("the concurrent source change returned while the request was parked")
        .expect("the concurrent source change");
    assert_eq!(
        balance(&server, "acc").await,
        "25",
        "the concurrent change settles its own delta"
    );
    std::fs::write(&release, b"release").expect("release the parked request");
    parked
        .await
        .expect("the parked request task")
        .expect("the HTTP answer");

    let sum = source_sum(&server).await;
    assert_eq!(
        balance(&server, "acc").await,
        sum.to_string(),
        "the balance must equal the source sum: the request either aborts \
         or settles again, and never applies the delta it computed from the old images"
    );
}
