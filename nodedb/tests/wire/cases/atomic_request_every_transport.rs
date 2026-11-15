// SPDX-License-Identifier: BUSL-1.1

//! A request of several statements with a write is atomic on HTTP, NDJSON
//! `/v1/query/stream`, WebSocket RPC `/v1/ws` and native SQL.
//!
//! The last statement of each request is a read that exceeds
//! `max_query_result_bytes`, so it errors after the write before it ran. The
//! request answers that error and applies no write.
//!
//! Two shapes run on every transport:
//!
//! - Split: the write is a cross-shard sum source. The sum move flags the
//!   request for the implicit transaction.
//! - Plain: the write is a single-vShard insert. Only the request of several
//!   statements takes it into the implicit transaction.

use super::atomic_transport::Transport;
use super::materialized_sum_cross_shard_expanders::{
    POSTINGS, balance, source_sum, sum_fixture_on,
};
use crate::harness::TestServer;

/// Bytes the server lets one query result carry.
const LIMIT: u64 = 4096;
/// The collection whose full read exceeds [`LIMIT`].
const BIG: &str = "ar_big";
/// The collection of the plain shape.
const PLAIN: &str = "ar_plain";
/// The balance of `acc` after the sum fixture seeds its postings.
const SEED_BALANCE: &str = "15";

/// A short tag of `transport` for row ids.
fn tag(transport: Transport) -> &'static str {
    match transport {
        Transport::Http => "h",
        Transport::Ndjson => "n",
        Transport::Ws => "w",
        Transport::Native => "x",
    }
}

/// A two-core server with the sum fixture, [`BIG`] and [`PLAIN`].
async fn fixture() -> TestServer {
    let server =
        sum_fixture_on(TestServer::start_multicores_with_result_limit(2, LIMIT).await).await;
    for ddl in [
        format!(
            "CREATE COLLECTION {BIG} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
        ),
        format!(
            "CREATE COLLECTION {PLAIN} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
        ),
    ] {
        server
            .exec(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    let rows: Vec<String> = (0..40)
        .map(|n| format!("('r{n}', '{}')", "x".repeat(200)))
        .collect();
    server
        .exec(&format!(
            "INSERT INTO {BIG} (id, v) VALUES {}",
            rows.join(", ")
        ))
        .await
        .expect("seed the big collection");
    server
}

/// Check that a read of [`BIG`] alone errors on `transport`. A request that
/// ends in that read can then only fail on the read.
async fn assert_big_read_errors(server: &TestServer, transport: Transport) {
    let sent = transport
        .send(server, &format!("SELECT * FROM {BIG}"))
        .await;
    assert!(
        sent.errored,
        "{transport:?}: a full read of {BIG} must exceed the {LIMIT} byte limit: {}",
        sent.text
    );
}

/// Check that `collection` holds no row `id`.
async fn assert_absent(server: &TestServer, collection: &str, id: &str) {
    let found = server
        .query_text(&format!("SELECT id FROM {collection} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"));
    assert!(
        found.is_empty(),
        "an errored request must not apply {collection}.{id}, found {found:?}"
    );
}

/// An insert into the sum source, then a read that errors.
async fn split_request(transport: Transport) {
    let server = fixture().await;
    assert_big_read_errors(&server, transport).await;
    let id = format!("{}1", tag(transport));
    let sql = format!(
        "INSERT INTO {POSTINGS} (id, account_id, amount) VALUES ('{id}', 'acc', '3'); \
         SELECT * FROM {BIG}"
    );
    let sent = transport.send(&server, &sql).await;
    assert!(sent.errored, "the trailing read must error: {sql}");
    assert_absent(&server, POSTINGS, &id).await;
    assert_eq!(
        balance(&server, "acc").await,
        SEED_BALANCE,
        "an errored request leaves the balance"
    );
    assert_eq!(
        source_sum(&server).await.to_string(),
        SEED_BALANCE,
        "the source rows still sum to the balance"
    );
}

/// An insert into a plain collection, then a read that errors.
async fn plain_request(transport: Transport) {
    let server = fixture().await;
    assert_big_read_errors(&server, transport).await;
    let id = format!("{}1", tag(transport));
    let sql = format!("INSERT INTO {PLAIN} (id, v) VALUES ('{id}', 'x'); SELECT * FROM {BIG}");
    let sent = transport.send(&server, &sql).await;
    assert!(sent.errored, "the trailing read must error: {sql}");
    assert_absent(&server, PLAIN, &id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_split_request_with_an_erroring_read_applies_no_write() {
    split_request(Transport::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_split_request_with_an_erroring_read_applies_no_write() {
    split_request(Transport::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_split_request_with_an_erroring_read_applies_no_write() {
    split_request(Transport::Ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_split_request_with_an_erroring_read_applies_no_write() {
    split_request(Transport::Native).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_plain_request_with_an_erroring_read_applies_no_write() {
    plain_request(Transport::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_plain_request_with_an_erroring_read_applies_no_write() {
    plain_request(Transport::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_plain_request_with_an_erroring_read_applies_no_write() {
    plain_request(Transport::Ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_plain_request_with_an_erroring_read_applies_no_write() {
    plain_request(Transport::Native).await;
}
