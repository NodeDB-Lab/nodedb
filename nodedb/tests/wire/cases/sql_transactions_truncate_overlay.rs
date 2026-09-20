// SPDX-License-Identifier: BUSL-1.1

//! `TRUNCATE` inside an explicit transaction is staged at statement time: it
//! answers the bare `TRUNCATE` tag, and the same transaction's scan, point
//! get, index lookup, vector search and text search see the collection
//! empty until it inserts again. Another connection keeps seeing the rows
//! until COMMIT. Sibling `sql_transactions_truncate_overlay_lifecycle`
//! covers COMMIT ordering, ROLLBACK, `ROLLBACK TO SAVEPOINT` and
//! `RESTART IDENTITY`.

use crate::harness::TestServer;
use crate::harness::raw_pgwire::{RawPgConn, command_tags};

pub(super) async fn create(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING NOT NULL PRIMARY KEY, region STRING) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

pub(super) fn insert_sql(name: &str, id: &str, region: &str) -> String {
    format!("INSERT INTO {name} (id, region) VALUES ('{id}', '{region}')")
}

pub(super) async fn insert(server: &TestServer, name: &str, id: &str, region: &str) {
    server
        .exec(&insert_sql(name, id, region))
        .await
        .unwrap_or_else(|e| panic!("insert {id} into {name}: {e}"));
}

/// Seed `a` / `b` in `us` and `c` in `eu`.
pub(super) async fn seed(server: &TestServer, name: &str) {
    for (id, region) in [("a", "us"), ("b", "us"), ("c", "eu")] {
        insert(server, name, id, region).await;
    }
}

/// Ids of every row a full scan returns, sorted.
pub(super) async fn scan_ids(server: &TestServer, name: &str) -> Vec<String> {
    let mut ids: Vec<String> = server
        .query_rows(&format!("SELECT id FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("scan {name}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    ids.sort();
    ids
}

/// `region` of the point read `WHERE id = '<id>'`.
pub(super) async fn point(server: &TestServer, name: &str, id: &str) -> Vec<String> {
    server
        .query_rows(&format!("SELECT region FROM {name} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("point read {id} from {name}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
}

/// Ids the indexed lookup `WHERE region = '<region>'` returns, sorted.
pub(super) async fn region_ids(server: &TestServer, name: &str, region: &str) -> Vec<String> {
    let mut ids: Vec<String> = server
        .query_rows(&format!("SELECT id FROM {name} WHERE region = '{region}'"))
        .await
        .unwrap_or_else(|e| panic!("index lookup {region} on {name}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    ids.sort();
    ids
}

pub(super) fn ids(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

/// Every `CommandComplete` tag string `sql` answers with, in wire order, on
/// a fresh trust-mode connection. A multi-statement `sql` keeps its
/// transaction on that one connection.
async fn raw_command_tags(port: u16, sql: &str) -> Vec<String> {
    let mut conn = RawPgConn::connect(port, "nodedb", "default").await;
    let messages = conn.simple_query(sql).await;
    command_tags(&messages)
}

/// BEGIN; TRUNCATE answers the bare `TRUNCATE` tag, no row count, exactly as
/// autocommit does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_reports_bare_truncate_tag() {
    let server = TestServer::start().await;
    create(&server, "trunc_txn_tag").await;
    seed(&server, "trunc_txn_tag").await;

    let tags = raw_command_tags(server.pg_port, "BEGIN; TRUNCATE trunc_txn_tag; ROLLBACK").await;
    assert_eq!(
        tags,
        vec![
            "BEGIN".to_string(),
            "TRUNCATE".to_string(),
            "ROLLBACK".to_string(),
        ]
    );
    assert_eq!(
        scan_ids(&server, "trunc_txn_tag").await,
        ids(&["a", "b", "c"]),
        "a rolled-back TRUNCATE leaves every row"
    );
}

/// After an in-transaction TRUNCATE the same transaction's scan, point get
/// and secondary-index lookup all see the collection empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_hides_rows_from_scan_point_get_and_index_lookup() {
    let server = TestServer::start().await;
    create(&server, "trunc_txn_hide").await;
    server
        .exec("CREATE INDEX ON trunc_txn_hide(region)")
        .await
        .unwrap();
    seed(&server, "trunc_txn_hide").await;

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_txn_hide").await.unwrap();
    assert_eq!(
        scan_ids(&server, "trunc_txn_hide").await,
        Vec::<String>::new()
    );
    assert_eq!(
        point(&server, "trunc_txn_hide", "a").await,
        Vec::<String>::new()
    );
    assert_eq!(
        region_ids(&server, "trunc_txn_hide", "us").await,
        Vec::<String>::new()
    );
    server.exec("ROLLBACK").await.unwrap();
}

/// An INSERT after the TRUNCATE is visible to the same transaction, including
/// one that re-uses a primary key the base collection still holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_then_insert_reusing_base_key_is_visible() {
    let server = TestServer::start().await;
    create(&server, "trunc_txn_reuse").await;
    server
        .exec("CREATE INDEX ON trunc_txn_reuse(region)")
        .await
        .unwrap();
    seed(&server, "trunc_txn_reuse").await;

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_txn_reuse").await.unwrap();
    // `a` exists in base: without the marker this is a duplicate key.
    insert(&server, "trunc_txn_reuse", "a", "apac").await;
    insert(&server, "trunc_txn_reuse", "z", "us").await;
    assert_eq!(scan_ids(&server, "trunc_txn_reuse").await, ids(&["a", "z"]));
    assert_eq!(
        point(&server, "trunc_txn_reuse", "a").await,
        vec!["apac".to_string()]
    );
    assert_eq!(
        point(&server, "trunc_txn_reuse", "b").await,
        Vec::<String>::new()
    );
    assert_eq!(
        region_ids(&server, "trunc_txn_reuse", "us").await,
        ids(&["z"])
    );
    assert_eq!(
        region_ids(&server, "trunc_txn_reuse", "apac").await,
        ids(&["a"])
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(scan_ids(&server, "trunc_txn_reuse").await, ids(&["a", "z"]));
    assert_eq!(
        point(&server, "trunc_txn_reuse", "a").await,
        vec!["apac".to_string()]
    );
    assert_eq!(
        region_ids(&server, "trunc_txn_reuse", "us").await,
        ids(&["z"])
    );
}

/// An in-transaction vector search excludes every base row after the
/// TRUNCATE and ranks a row inserted after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_vector_search_excludes_base_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION trunc_txn_vec")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_trunc_txn_vec_emb ON trunc_txn_vec METRIC l2 DIM 3")
        .await
        .unwrap();
    for (id, v) in [("v1", "1.0,0.0,0.0"), ("v2", "0.0,1.0,0.0")] {
        server
            .exec(&format!(
                "INSERT INTO trunc_txn_vec (id, embedding) VALUES ('{id}', ARRAY[{v}])"
            ))
            .await
            .unwrap();
    }
    async fn ranked(server: &TestServer) -> Vec<String> {
        let rows = server
            .query_rows(
                "SELECT id FROM trunc_txn_vec \
                 ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0]) LIMIT 5",
            )
            .await
            .unwrap();
        rows.into_iter().map(|r| r[0].clone()).collect()
    }
    assert_eq!(ranked(&server).await, ids(&["v1", "v2"]));

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_txn_vec").await.unwrap();
    assert_eq!(ranked(&server).await, Vec::<String>::new());
    server
        .exec("INSERT INTO trunc_txn_vec (id, embedding) VALUES ('v3', ARRAY[0.9,0.1,0.0])")
        .await
        .unwrap();
    assert_eq!(ranked(&server).await, ids(&["v3"]));
    server.exec("COMMIT").await.unwrap();

    assert_eq!(ranked(&server).await, ids(&["v3"]));
}

/// An in-transaction text search excludes every base row after the
/// TRUNCATE and matches a row inserted after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_fts_search_excludes_base_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION trunc_txn_fts WITH (engine='document_schemaless')")
        .await
        .unwrap();
    for (id, body) in [("a1", "the quick brown fox"), ("a2", "a quick lazy dog")] {
        server
            .exec(&format!(
                "INSERT INTO trunc_txn_fts (id, body) VALUES ('{id}', '{body}')"
            ))
            .await
            .unwrap();
    }
    async fn matched(server: &TestServer) -> Vec<String> {
        let rows = server
            .query_rows("SELECT id FROM trunc_txn_fts WHERE text_match(body, 'quick') ORDER BY id")
            .await
            .unwrap();
        rows.into_iter().map(|r| r[0].clone()).collect()
    }
    assert_eq!(matched(&server).await, ids(&["a1", "a2"]));

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_txn_fts").await.unwrap();
    assert_eq!(matched(&server).await, Vec::<String>::new());
    server
        .exec("INSERT INTO trunc_txn_fts (id, body) VALUES ('a3', 'quick as lightning')")
        .await
        .unwrap();
    assert_eq!(matched(&server).await, ids(&["a3"]));
    server.exec("COMMIT").await.unwrap();

    assert_eq!(matched(&server).await, ids(&["a3"]));
}

/// A second connection keeps seeing the rows while the truncating
/// transaction is open, and sees them gone once it commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_in_transaction_other_connection_still_sees_rows_until_commit() {
    let server = TestServer::start().await;
    create(&server, "trunc_txn_other").await;
    seed(&server, "trunc_txn_other").await;
    let (other, _handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("second connection");
    async fn other_ids(other: &tokio_postgres::Client) -> Vec<String> {
        let rows = other
            .simple_query("SELECT id FROM trunc_txn_other")
            .await
            .expect("scan on the second connection");
        let mut out: Vec<String> = rows
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .collect();
        out.sort();
        out
    }

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE trunc_txn_other").await.unwrap();
    assert_eq!(
        scan_ids(&server, "trunc_txn_other").await,
        Vec::<String>::new()
    );
    assert_eq!(
        other_ids(&other).await,
        ids(&["a", "b", "c"]),
        "an uncommitted TRUNCATE is invisible to another connection"
    );
    server.exec("COMMIT").await.unwrap();
    assert_eq!(other_ids(&other).await, Vec::<String>::new());
}
