// SPDX-License-Identifier: BUSL-1.1

//! `AS OF SYSTEM TIME NULL` on a bitemporal document collection shows each
//! delete version as a row: the cells of the version it closes, the
//! document's key among them, its own `_ts_system`, and an empty valid
//! interval (`_ts_valid_from = _ts_valid_until`), the convention a bitemporal
//! columnar tombstone follows. A current read finds no row for the deleted
//! document. Strict and schemaless alike.

use crate::harness::TestServer;

async fn audit_shows_the_delete_version(engine: &str, coll: &str) {
    let srv = TestServer::start().await;
    srv.exec(&format!(
        "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='{engine}', bitemporal=true)"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {engine} collection: {e}"));
    srv.exec(&format!(
        "INSERT INTO {coll} (id, value) VALUES ('r1', 'hello')"
    ))
    .await
    .expect("insert row");
    srv.exec(&format!("DELETE FROM {coll} WHERE id = 'r1'"))
        .await
        .expect("delete row");

    let current = srv
        .query_named_rows(&format!("SELECT * FROM {coll}"))
        .await
        .expect("current read");
    assert!(
        current.is_empty(),
        "{engine}: a current read finds no row: {current:?}"
    );

    let rows = srv
        .query_named_rows(&format!("SELECT * FROM {coll} AS OF SYSTEM TIME NULL"))
        .await
        .expect("audit-log read");
    assert_eq!(
        rows.len(),
        2,
        "{engine}: the version and its delete: {rows:?}"
    );
    let empty_interval = |row: &std::collections::HashMap<String, String>| {
        row.get("_ts_valid_from").is_some()
            && row.get("_ts_valid_from") == row.get("_ts_valid_until")
    };
    let deletes: Vec<_> = rows.iter().filter(|row| empty_interval(row)).collect();
    assert_eq!(deletes.len(), 1, "{engine}: one delete version: {rows:?}");
    let delete = deletes[0];
    assert_eq!(
        delete.get("id").map(String::as_str),
        Some("r1"),
        "{engine}: the delete version carries the document's key: {delete:?}"
    );
    assert_eq!(
        delete.get("value").map(String::as_str),
        Some("hello"),
        "{engine}: the delete version repeats the cells of the version it closes: {delete:?}"
    );
    assert_eq!(
        delete.get("_ts_valid_from").map(String::as_str),
        Some(i64::MIN.to_string().as_str()),
        "{engine}: the empty interval is i64::MIN on both bounds: {delete:?}"
    );
    let live = rows
        .iter()
        .find(|row| !empty_interval(row))
        .expect("the live version");
    assert_eq!(live.get("value").map(String::as_str), Some("hello"));
    let stamp = |row: &std::collections::HashMap<String, String>| -> i64 {
        row.get("_ts_system")
            .and_then(|ts| ts.parse().ok())
            .unwrap_or_else(|| panic!("{engine}: no _ts_system: {row:?}"))
    };
    assert!(
        stamp(delete) >= stamp(live),
        "{engine}: the delete stamps at or after the version it closes"
    );

    // A read at the live version's cut returns the row, never the delete.
    let at = srv
        .query_named_rows(&format!(
            "SELECT * FROM {coll} AS OF SYSTEM TIME {}",
            stamp(live)
        ))
        .await
        .expect("read at a cut");
    assert!(
        at.iter().all(|row| !empty_interval(row)),
        "{engine}: a read at a cut emits no delete version: {at:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_audit_log_shows_delete_versions() {
    audit_shows_the_delete_version("document_schemaless", "bt_audit_del_sl").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_audit_log_shows_delete_versions() {
    audit_shows_the_delete_version("document_strict", "bt_audit_del_st").await;
}
