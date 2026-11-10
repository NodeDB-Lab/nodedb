// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` writes on two vShards that commit through Calvin.
//!
//! - Inside a transaction block each statement answers its rows when it
//!   runs. The COMMIT answers its tag: it carries no `RETURNING` clause, so
//!   the writes commit, and a timeseries ingest beside them keeps its count.
//! - One request's writes share its `RETURNING` limits. Each participant
//!   refuses past its share before it votes, so a request whose rows pass
//!   the limit stores nothing.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::id::DatabaseId;

use crate::harness::TestServer;

const ONE: &str = "ctr_one";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A collection name with `prefix` on another vShard than `ONE`.
fn other(prefix: &str) -> String {
    (0..)
        .map(|n| format!("{prefix}_{n}"))
        .find(|name| vshard_of(name) != vshard_of(ONE))
        .expect("a name on another vShard")
}

/// The `v` values of `collection`, by id.
async fn values(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT v FROM {collection} ORDER BY id"))
        .await
        .expect("read the collection")
}

/// Create the strict collections `names`, each with rows `r1` and `r2`.
async fn seeded(server: &TestServer, names: [&str; 2]) {
    for collection in names {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .expect("create the collection");
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, v) VALUES ('r1', 'a'), ('r2', 'b')"
            ))
            .await
            .expect("seed the collection");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_returning_writes_on_two_vshards_commit_in_one_block() {
    let server = TestServer::start_multicores(2).await;
    let two = other("ctr_two");
    let series = other("ctr_ts");
    seeded(&server, [ONE, two.as_str()]).await;
    server
        .exec(&format!(
            "CREATE COLLECTION {series} COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("create the timeseries collection");

    server.exec("BEGIN").await.expect("BEGIN");
    assert_eq!(
        server
            .query_text(&format!(
                "UPDATE {ONE} SET v = 'x' WHERE id = 'r1' RETURNING v"
            ))
            .await
            .expect("the first RETURNING write"),
        ["x"]
    );
    assert_eq!(
        server
            .query_text(&format!(
                "UPDATE {two} SET v = 'y' WHERE id = 'r2' RETURNING id"
            ))
            .await
            .expect("the second RETURNING write"),
        ["r2"]
    );
    server
        .exec(&format!(
            "INSERT INTO {series} (id, ts, v) VALUES ('s1', 1000, 1), ('s2', 2000, 2)"
        ))
        .await
        .expect("the ingest");
    server
        .exec("COMMIT")
        .await
        .expect("COMMIT of two RETURNING writes on two vShards");

    assert_eq!(values(&server, ONE).await, ["x", "b"]);
    assert_eq!(values(&server, &two).await, ["a", "y"]);
    assert_eq!(
        server
            .query_text(&format!("SELECT id FROM {series}"))
            .await
            .expect("read the series")
            .len(),
        2,
        "the ingest committed every line"
    );
}

/// Two writes of one request share a limit of one `RETURNING` row: each
/// participant's share is none, so each refuses its row before it votes.
/// The request fails with `54000` and stores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_rows_past_the_request_limit_store_nothing() {
    let server = TestServer::start_multicores_with_max_returning_rows(2, 1).await;
    let two = other("ctr_lim");
    seeded(&server, [ONE, two.as_str()]).await;

    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let sql = format!(
        "UPDATE {ONE} SET v = 'x' WHERE id = 'r1' RETURNING id; \
         UPDATE {two} SET v = 'x' WHERE id = 'r1' RETURNING id"
    );
    let response = send_sql(&mut stream, 2, &sql).await;

    let error = response
        .error
        .unwrap_or_else(|| panic!("the request passes the limit: {sql}"));
    assert_eq!(error.code, "54000", "{}", error.message);
    assert!(
        error.message.contains("RETURNING rows 1 exceeds limit 0"),
        "{}",
        error.message
    );
    assert_eq!(values(&server, ONE).await, ["a", "b"]);
    assert_eq!(values(&server, &two).await, ["a", "b"]);
}
