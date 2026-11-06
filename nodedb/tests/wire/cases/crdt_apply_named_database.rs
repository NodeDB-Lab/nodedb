// SPDX-License-Identifier: BUSL-1.1

//! `crdt_apply` in a named database, on pgwire and HTTP.
//!
//! A client's Loro delta names the bare collection, because a client does not
//! know a database id. The row reads back through SQL, and the collection's state
//! constraint judges the delta as it does in the default database.

use crate::harness::TestServer;

const COLL: &str = "named_flow";
const DATABASE_HEADER: &str = "x-nodedb-database";

/// A delta from an empty document that writes row `row` of root `root`.
fn row_delta(root: &str, row: &str, status: &str) -> (loro::LoroDoc, String) {
    let doc = loro::LoroDoc::new();
    let empty = doc.oplog_vv();
    let map = doc
        .get_map(root)
        .insert_container(row, loro::LoroMap::new())
        .expect("row container");
    map.insert("id", row).expect("id field");
    map.insert("status", status).expect("status field");
    doc.commit();
    let bytes = doc
        .export(loro::ExportMode::updates(&empty))
        .expect("export loro updates");
    (doc, hex::encode(bytes))
}

/// The updates past `base_doc` that move row `row` to `next`.
fn status_delta(base_doc: &loro::LoroDoc, row: &str, next: &str) -> String {
    let base = base_doc.oplog_vv();
    let fork = base_doc.fork();
    let coll = fork.get_map(COLL);
    let Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) = coll.get(row) else {
        panic!("row {row} must exist in the base document");
    };
    map.insert("status", next).expect("status field");
    fork.commit();
    hex::encode(
        fork.export(loro::ExportMode::updates(&base))
            .expect("export loro updates"),
    )
}

async fn status(server: &TestServer, row: &str) -> String {
    let rows = server
        .query_text(&format!("SELECT status FROM {COLL} WHERE id = '{row}'"))
        .await
        .expect("read status");
    assert_eq!(rows.len(), 1, "row {row} must exist: {rows:?}");
    rows[0].clone()
}

async fn sql_apply(server: &TestServer, row: &str, delta_hex: &str) -> Result<(), String> {
    server
        .exec(&format!(
            "SELECT crdt_apply('{COLL}', '{row}', '{delta_hex}')"
        ))
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

async fn http_apply(
    server: &TestServer,
    database: &str,
    row: &str,
    delta_hex: &str,
) -> (reqwest::StatusCode, String) {
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/collections/{COLL}/crdt/apply",
            server.http_port
        ))
        .header(DATABASE_HEADER, database)
        .json(&serde_json::json!({ "doc_id": row, "delta": delta_hex }))
        .send()
        .await
        .expect("POST crdt/apply");
    let status = response.status();
    let body = response.text().await.expect("read crdt/apply body");
    (status, body)
}

async fn named_database() -> (TestServer, String) {
    let (server, name) = TestServer::with_database("crdt_named").await;
    server
        .exec(&format!(
            "CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, status TEXT) WITH (crdt='true')"
        ))
        .await
        .expect("create collection");
    (server, name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_crdt_apply_in_a_named_database_reads_back_and_obeys_its_constraint() {
    let (server, _) = named_database().await;
    let (doc, delta) = row_delta(COLL, "a", "draft");
    sql_apply(&server, "a", &delta)
        .await
        .unwrap_or_else(|e| panic!("a bare-named delta must apply: {e}"));
    assert_eq!(status(&server, "a").await, "draft");

    server
        .exec(&format!(
            "ALTER COLLECTION {COLL} ADD CONSTRAINT flow \
             ON COLUMN status TRANSITIONS ('draft' -> 'active')"
        ))
        .await
        .expect("add constraint");

    assert!(
        sql_apply(&server, "a", &status_delta(&doc, "a", "closed"))
            .await
            .is_err(),
        "a draft -> closed delta must be refused"
    );
    assert_eq!(status(&server, "a").await, "draft");

    sql_apply(&server, "a", &status_delta(&doc, "a", "active"))
        .await
        .unwrap_or_else(|e| panic!("a draft -> active delta must apply: {e}"));
    assert_eq!(status(&server, "a").await, "active");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_row_and_delta_share_one_document_in_a_named_database() {
    let (server, _) = named_database().await;
    server
        .exec(&format!(
            "INSERT INTO {COLL} (id, status) VALUES ('sql', 'draft')"
        ))
        .await
        .expect("insert");
    let (_, delta) = row_delta(COLL, "delta", "draft");
    sql_apply(&server, "delta", &delta)
        .await
        .unwrap_or_else(|e| panic!("a delta beside a SQL row must apply: {e}"));
    assert_eq!(status(&server, "sql").await, "draft");
    assert_eq!(status(&server, "delta").await, "draft");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_crdt_apply_in_a_named_database_reads_back() {
    let (server, name) = named_database().await;
    let (_, delta) = row_delta(COLL, "h", "draft");
    let (code, body) = http_apply(&server, &name, "h", &delta).await;
    assert_eq!(code, reqwest::StatusCode::OK, "{body}");
    assert_eq!(status(&server, "h").await, "draft");
}

/// A delta for another root is the client's error, and it changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_refuses_a_delta_for_another_collection_with_a_client_status() {
    let (server, name) = named_database().await;
    let (_, delta) = row_delta("another_collection", "h", "draft");
    let (code, body) = http_apply(&server, &name, "h", &delta).await;
    assert_eq!(code, reqwest::StatusCode::CONFLICT, "{body}");
    assert!(body.contains("target mismatch"), "{body}");
    let rows = server
        .query_text(&format!("SELECT id FROM {COLL}"))
        .await
        .expect("read rows");
    assert!(rows.is_empty(), "{rows:?}");
}
