// SPDX-License-Identifier: BUSL-1.1

//! Full-text search over declared fields: refusal of undeclared fields, backfill on create, postings removal on drop, and the identifier rule for declared field names.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::fts_query_semantics::{create_indexed, ids, insert};
use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

/// A search reads only the fields a search index declares. Another field, or
/// a whole-document search on a collection with no index, returns 42703.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_search_on_an_undeclared_field_is_refused() {
    let srv = TestServer::start().await;
    create_indexed(&srv, "fts_declared", "title").await;
    insert(
        &srv,
        "fts_declared",
        "d1",
        "title: 'granite', body: 'granite slab'",
    )
    .await;

    let err = srv
        .query_rows("SELECT id FROM fts_declared WHERE text_match(body, 'granite')")
        .await
        .expect_err("body has no search index");
    assert!(err.contains("42703"), "expected 42703: {err}");
    assert!(err.contains("CREATE SEARCH INDEX"), "names the fix: {err}");

    let rows = srv
        .query_rows("SELECT id FROM fts_declared WHERE text_match(title, 'granite')")
        .await
        .unwrap();
    assert_eq!(
        ids(&rows),
        vec!["d1"],
        "the declared field matches: {rows:?}"
    );

    srv.exec("CREATE COLLECTION fts_no_index WITH (engine='document_schemaless')")
        .await
        .unwrap();
    insert(&srv, "fts_no_index", "n1", "body: 'granite slab'").await;
    let err = srv
        .query_rows("SELECT id FROM fts_no_index WHERE text_match(*, 'granite')")
        .await
        .expect_err("the collection has no search index");
    assert!(err.contains("42703"), "expected 42703: {err}");
}

/// `CREATE SEARCH INDEX` indexes the rows already stored, and the history of
/// a bitemporal collection, so an `AS OF SYSTEM TIME` search finds old text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_search_index_backfills_existing_rows_and_their_history() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION fts_backfill (id STRING PRIMARY KEY, body STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO fts_backfill (id, body) VALUES ('v1', 'amber stone')")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_backfill (id, body) VALUES ('v2', 'granite stone')")
        .await
        .unwrap();

    // Margins on both sides of `before` keep millisecond rounding out.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;

    srv.exec("UPDATE fts_backfill SET body = 'amber resin' WHERE id = 'v2'")
        .await
        .unwrap();
    srv.exec("CREATE SEARCH INDEX ON fts_backfill (body)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM fts_backfill WHERE text_match(body, 'amber') ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["v1", "v2"], "current text: {rows:?}");

    let rows = srv
        .query_rows("SELECT id FROM fts_backfill WHERE text_match(body, 'granite')")
        .await
        .unwrap();
    assert!(rows.is_empty(), "the old text is not current: {rows:?}");

    let rows = srv
        .query_rows(&format!(
            "SELECT id FROM fts_backfill AS OF SYSTEM TIME {before} \
             WHERE text_match(body, 'granite')"
        ))
        .await
        .unwrap();
    assert_eq!(
        ids(&rows),
        vec!["v2"],
        "the old version is searchable: {rows:?}"
    );
}

/// `DROP INDEX` removes the postings, and a search then returns 42703. A new
/// `CREATE SEARCH INDEX` rebuilds them from the stored rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_search_index_removes_its_postings() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_drop WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("CREATE SEARCH INDEX fts_drop_idx ON fts_drop (body)")
        .await
        .unwrap();
    insert(&srv, "fts_drop", "r1", "body: 'basalt column'").await;

    let sql = "SELECT id FROM fts_drop WHERE text_match(body, 'basalt')";
    let rows = srv.query_rows(sql).await.unwrap();
    assert_eq!(ids(&rows), vec!["r1"], "the index finds the row: {rows:?}");

    srv.exec("DROP INDEX fts_drop_idx").await.unwrap();
    let err = srv.query_rows(sql).await.expect_err("the index is dropped");
    assert!(err.contains("42703"), "expected 42703: {err}");

    srv.exec("CREATE SEARCH INDEX fts_drop_idx ON fts_drop (body)")
        .await
        .unwrap();
    let rows = srv.query_rows(sql).await.unwrap();
    assert_eq!(
        ids(&rows),
        vec!["r1"],
        "the rebuild finds the row: {rows:?}"
    );
}

/// Field names in `CREATE SEARCH INDEX` follow the SQL identifier rule. An
/// unquoted name lowercases, so `(Body)` declares `body`. A double-quoted
/// name keeps its case, so `("Title")` declares `Title` and not `title`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_index_field_names_follow_the_sql_identifier_rule() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_case WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("CREATE SEARCH INDEX ON fts_case (Body)")
        .await
        .unwrap();
    srv.exec("CREATE SEARCH INDEX fts_case_title ON fts_case (\"Title\")")
        .await
        .unwrap();
    srv.exec(
        "INSERT INTO fts_case (id, body, \"Title\") \
         VALUES ('c1', 'granite slab', 'basalt column')",
    )
    .await
    .unwrap();

    for column in ["body", "Body"] {
        let rows = srv
            .query_rows(&format!(
                "SELECT id FROM fts_case WHERE text_match({column}, 'granite')"
            ))
            .await
            .unwrap_or_else(|e| panic!("text_match({column}): {e}"));
        assert_eq!(ids(&rows), vec!["c1"], "text_match({column}): {rows:?}");
    }

    let rows = srv
        .query_rows("SELECT id FROM fts_case WHERE text_match(\"Title\", 'basalt')")
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["c1"], "the quoted field matches: {rows:?}");

    let err = srv
        .query_rows("SELECT id FROM fts_case WHERE text_match(title, 'basalt')")
        .await
        .expect_err("title is not the declared field Title");
    assert!(err.contains("42703"), "expected 42703: {err}");
}
