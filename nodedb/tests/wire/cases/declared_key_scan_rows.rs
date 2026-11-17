// SPDX-License-Identifier: BUSL-1.1

//! A row of a schemaless collection with a declared key names its identity by
//! that key. No read path adds a synthesized `id` column beside it: not a
//! full-text search, and not a join.

use std::collections::HashMap;

use crate::harness::TestServer;

/// Create `items` keyed by `sku` and `orders` keyed by `oid`, both
/// schemaless, with one item and one order that names it.
async fn seed(srv: &TestServer) {
    srv.exec("CREATE COLLECTION items (sku STRING NOT NULL PRIMARY KEY, name STRING)")
        .await
        .expect("create items");
    srv.exec("CREATE SEARCH INDEX ON items (name)")
        .await
        .expect("index items");
    srv.exec("CREATE COLLECTION orders (oid STRING NOT NULL PRIMARY KEY, sku STRING, qty INT)")
        .await
        .expect("create orders");
    srv.exec("INSERT INTO items (sku, name) VALUES ('p1', 'blue pen')")
        .await
        .expect("insert item");
    srv.exec("INSERT INTO items (sku, name) VALUES ('p2', 'red mug')")
        .await
        .expect("insert item");
    srv.exec("INSERT INTO orders (oid, sku, qty) VALUES ('o1', 'p1', 3)")
        .await
        .expect("insert order");
}

/// Whether `column` names `field`, bare or qualified by a collection.
fn names(column: &str, field: &str) -> bool {
    column == field || column.ends_with(&format!(".{field}"))
}

/// Assert no column of `row` is an `id` column.
fn assert_no_id(row: &HashMap<String, String>) {
    assert!(
        !row.keys().any(|column| names(column, "id")),
        "a declared-key row shows no `id` column: {row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_search_row_names_its_identity_by_the_declared_key() {
    let srv = TestServer::start().await;
    seed(&srv).await;

    let rows = srv
        .query_named_rows("SELECT * FROM items WHERE text_match(name, 'pen')")
        .await
        .expect("text search");
    assert_eq!(rows.len(), 1, "one item matches 'pen': {rows:?}");
    let row = &rows[0];
    assert_no_id(row);
    assert_eq!(
        row.get("sku").map(String::as_str),
        Some("p1"),
        "the key column holds the key: {row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_row_names_its_identity_by_the_declared_key() {
    let srv = TestServer::start().await;
    seed(&srv).await;

    let rows = srv
        .query_named_rows("SELECT * FROM items i JOIN orders o ON o.sku = i.sku")
        .await
        .expect("join");
    assert_eq!(rows.len(), 1, "one order joins one item: {rows:?}");
    let row = &rows[0];
    assert_no_id(row);
    assert!(
        row.iter()
            .any(|(column, value)| names(column, "sku") && value == "p1"),
        "a key column holds the item key: {row:?}"
    );
    assert!(
        row.iter()
            .any(|(column, value)| names(column, "oid") && value == "o1"),
        "the order key column holds the order key: {row:?}"
    );

    // A join that names the declared keys finds them.
    let rows = srv
        .query_rows("SELECT i.sku, o.oid FROM items i JOIN orders o ON o.sku = i.sku")
        .await
        .expect("join on keys");
    assert_eq!(
        rows,
        vec![vec!["p1".to_string(), "o1".to_string()]],
        "the projected keys hold the keys"
    );
}
