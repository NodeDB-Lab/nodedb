// SPDX-License-Identifier: BUSL-1.1

//! In-transaction full-text SEARCH must observe the transaction's own staged
//! document writes (read-your-own-writes for FTS): a document inserted or
//! updated earlier in the same transaction appears in `text_match(...)`
//! results, and one deleted in the transaction is excluded — all BEFORE
//! COMMIT. FTS indexing is an inline side effect of the document write, not
//! a stageable write of its own, so this is implemented by re-tokenizing
//! and BM25-scoring the transaction's staged document bodies at query time
//! and merging them into the base search result (see
//! `handlers/transaction/overlay/fts_merge.rs`).

use crate::harness::TestServer;

async fn setup(server: &TestServer, coll: &str, storage_mode: &str) {
    match storage_mode {
        "document_strict" => {
            server
                .exec(&format!(
                    "CREATE COLLECTION {coll} \
                     (id STRING NOT NULL PRIMARY KEY, body STRING) \
                     WITH (engine='document_strict')"
                ))
                .await
                .unwrap();
        }
        _ => {
            server
                .exec(&format!(
                    "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
                ))
                .await
                .unwrap();
        }
    }
    for (id, body) in [
        ("a1", "the quick brown fox"),
        ("a2", "a lazy dog sleeps"),
        ("unrelated", "completely different topic"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {coll} (id, body) VALUES ('{id}', '{body}')"
            ))
            .await
            .unwrap();
    }
}

async fn matched_ids(server: &TestServer, coll: &str, term: &str) -> Vec<String> {
    let rows = server
        .query_rows(&format!(
            "SELECT id FROM {coll} WHERE text_match(body, '{term}') ORDER BY id"
        ))
        .await
        .unwrap();
    rows.into_iter().map(|r| r[0].clone()).collect()
}

/// BEGIN; INSERT a doc matching a query term; in-tx SEARCH includes it;
/// COMMIT persists it; a separate run verifies ROLLBACK excludes it.
async fn insert_visible_in_txn_then_commit(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    // Baseline: no doc mentions "elephant" yet.
    let base = matched_ids(&server, coll, "elephant").await;
    assert!(base.is_empty(), "{engine}: baseline must not match yet");

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, body) VALUES ('new1', 'an elephant never forgets')"
        ))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "elephant").await;
    assert_eq!(
        in_txn,
        vec!["new1".to_string()],
        "{engine}: in-tx search must include the staged insert before COMMIT"
    );

    server.client.simple_query("COMMIT").await.unwrap();
    let after_commit = matched_ids(&server, coll, "elephant").await;
    assert_eq!(
        after_commit,
        vec!["new1".to_string()],
        "{engine}: committed insert stays visible to search"
    );
}

/// BEGIN; INSERT a doc matching a query term; ROLLBACK; the doc must never
/// have been durably indexed and must not match post-rollback.
async fn insert_visible_in_txn_then_rollback(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, body) VALUES ('new2', 'a giraffe is very tall')"
        ))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "giraffe").await;
    assert_eq!(
        in_txn,
        vec!["new2".to_string()],
        "{engine}: in-tx search must include the staged insert"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = matched_ids(&server, coll, "giraffe").await;
    assert!(
        after_rollback.is_empty(),
        "{engine}: ROLLBACK must leave no durable index trace: {after_rollback:?}"
    );
}

/// BEGIN; DELETE a base doc that matched a term; in-tx SEARCH excludes it;
/// ROLLBACK restores it.
async fn delete_hides_in_txn_then_rollback_restores(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    let base = matched_ids(&server, coll, "fox").await;
    assert_eq!(base, vec!["a1".to_string()], "{engine}: baseline match");

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("DELETE FROM {coll} WHERE id = 'a1'"))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "fox").await;
    assert!(
        in_txn.is_empty(),
        "{engine}: in-tx search must exclude the staged delete: {in_txn:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = matched_ids(&server, coll, "fox").await;
    assert_eq!(
        after_rollback,
        vec!["a1".to_string()],
        "{engine}: ROLLBACK restores the deleted doc's match"
    );
}

/// An UPDATE that changes the text so it no longer matches (and a second
/// row updated so it newly matches) must be reflected in-tx.
async fn update_changes_match_in_txn(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    // 'a1' currently matches "fox" — update it away from that term.
    server
        .exec(&format!(
            "UPDATE {coll} SET body = 'nothing to see here' WHERE id = 'a1'"
        ))
        .await
        .unwrap();
    // 'a2' currently does not match "fox" — update it to match.
    server
        .exec(&format!(
            "UPDATE {coll} SET body = 'a fox in the henhouse' WHERE id = 'a2'"
        ))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "fox").await;
    assert_eq!(
        in_txn,
        vec!["a2".to_string()],
        "{engine}: in-tx search must reflect both the moved-out and moved-in update"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = matched_ids(&server, coll, "fox").await;
    assert_eq!(
        after_rollback,
        vec!["a1".to_string()],
        "{engine}: ROLLBACK restores base match state"
    );
}

/// Unrelated base documents must be unaffected by staged writes to other
/// rows, and result ordering (by score, surfaced here via presence) must be
/// stable.
async fn unrelated_docs_unaffected(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, body) VALUES ('new3', 'brown fox and a quick dog')"
        ))
        .await
        .unwrap();

    // "dog" still matches the pre-existing 'a2' row plus the new staged one.
    let mut in_txn = matched_ids(&server, coll, "dog").await;
    in_txn.sort();
    assert_eq!(
        in_txn,
        vec!["a2".to_string(), "new3".to_string()],
        "{engine}: base match for 'a2' must be unaffected by an unrelated staged insert"
    );
    // A term present in no document (neither body nor id) matches nothing,
    // staged or not. (Avoid a term that collides with a doc id, since the id
    // field is indexed too.)
    let unrelated = matched_ids(&server, coll, "kangaroo").await;
    assert!(unrelated.is_empty());

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_visible_in_txn_then_commit() {
    insert_visible_in_txn_then_commit("document_schemaless", "fts_ov_ins_c").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_visible_in_txn_then_rollback() {
    insert_visible_in_txn_then_rollback("document_schemaless", "fts_ov_ins_r").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_delete_hides_in_txn_then_rollback_restores() {
    delete_hides_in_txn_then_rollback_restores("document_schemaless", "fts_ov_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_update_changes_match_in_txn() {
    update_changes_match_in_txn("document_schemaless", "fts_ov_upd").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_unrelated_docs_unaffected() {
    unrelated_docs_unaffected("document_schemaless", "fts_ov_unrel").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_visible_in_txn_then_commit() {
    insert_visible_in_txn_then_commit("document_strict", "fts_ov_st_ins_c").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_visible_in_txn_then_rollback() {
    insert_visible_in_txn_then_rollback("document_strict", "fts_ov_st_ins_r").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_delete_hides_in_txn_then_rollback_restores() {
    delete_hides_in_txn_then_rollback_restores("document_strict", "fts_ov_st_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_update_changes_match_in_txn() {
    update_changes_match_in_txn("document_strict", "fts_ov_st_upd").await;
}

// ── Field scoping and residual filters inside a transaction ────────────────

async fn ids_of(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
}

/// A staged insert is visible to a search scoped to the field that holds the
/// term, and invisible to a search scoped to another field.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_insert_is_visible_to_a_field_scoped_search() {
    let server = TestServer::start().await;
    setup(&server, "fts_ov_scoped", "document_schemaless").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "INSERT INTO fts_ov_scoped (id, title, body) \
             VALUES ('new1', 'elephant tales', 'nothing relevant')",
        )
        .await
        .unwrap();
    let by_title = ids_of(
        &server,
        "SELECT id FROM fts_ov_scoped WHERE text_match(title, 'elephant') ORDER BY id",
    )
    .await;
    let by_body = ids_of(
        &server,
        "SELECT id FROM fts_ov_scoped WHERE text_match(body, 'elephant') ORDER BY id",
    )
    .await;
    server.client.simple_query("ROLLBACK").await.unwrap();

    assert_eq!(
        by_title,
        vec!["new1".to_string()],
        "the title scope sees it"
    );
    assert!(by_body.is_empty(), "the body scope does not: {by_body:?}");
}

/// A staged row that fails the WHERE filter never enters the result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_row_failing_the_filter_is_excluded() {
    let server = TestServer::start().await;
    setup(&server, "fts_ov_filtered", "document_schemaless").await;

    server.exec("BEGIN").await.unwrap();
    for (id, tag) in [("keep1", "a"), ("drop1", "b")] {
        server
            .exec(&format!(
                "INSERT INTO fts_ov_filtered (id, tag, body) \
                 VALUES ('{id}', '{tag}', 'an elephant never forgets')"
            ))
            .await
            .unwrap();
    }
    let ids = ids_of(
        &server,
        "SELECT id FROM fts_ov_filtered \
         WHERE text_match(body, 'elephant') AND tag = 'a' ORDER BY id",
    )
    .await;
    server.client.simple_query("ROLLBACK").await.unwrap();

    assert_eq!(ids, vec!["keep1".to_string()], "only the tag a row returns");
}

/// A field that first holds text inside the transaction is searchable there,
/// not refused as a field no document holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_use_of_a_field_inside_a_transaction_is_not_an_error() {
    let server = TestServer::start().await;
    setup(&server, "fts_ov_new_field", "document_schemaless").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "INSERT INTO fts_ov_new_field (id, note) \
             VALUES ('n1', 'brand new elephant note')",
        )
        .await
        .unwrap();
    let result = server
        .query_rows("SELECT id FROM fts_ov_new_field WHERE text_match(note, 'elephant')")
        .await;
    server.client.simple_query("ROLLBACK").await.unwrap();

    let rows = result.expect("a field staged in this transaction must be searchable");
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, vec!["n1"], "the staged note matches");
}
