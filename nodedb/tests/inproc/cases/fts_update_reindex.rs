// SPDX-License-Identifier: BUSL-1.1

//! UPDATE must keep the full-text index in step with the row.
//!
//! An updated row keeps its surrogate while its text changes. After the
//! update it must match the words its new body holds and must stop
//! matching the words only its old body held, for a point update by id and
//! for a bulk update by predicate alike.

use nodedb_test_support::pgwire_harness::TestServer;

/// Ids a full-text match on `body` returns, sorted.
async fn text_ids(server: &TestServer, collection: &str, term: &str) -> Vec<String> {
    text_ids_on(server, collection, "body", term).await
}

/// Ids a full-text match on `field` returns, sorted.
async fn text_ids_on(
    server: &TestServer,
    collection: &str,
    field: &str,
    term: &str,
) -> Vec<String> {
    let mut ids = server
        .query_text(&format!(
            "SELECT id FROM {collection} WHERE text_match({field}, '{term}')"
        ))
        .await
        .unwrap();
    ids.sort();
    ids
}

async fn seeded(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    for (id, body) in [("u1", "alpha first"), ("u2", "alpha second")] {
        server
            .exec(&format!(
                "INSERT INTO {collection} {{ id: '{id}', body: '{body}', kind: 'k' }}"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_moves_the_row_to_its_new_terms() {
    let server = TestServer::start().await;
    seeded(&server, "fu_point").await;

    server
        .exec("UPDATE fu_point SET body = 'beta moved' WHERE id = 'u1'")
        .await
        .unwrap();

    assert_eq!(text_ids(&server, "fu_point", "alpha").await, ["u2"]);
    assert_eq!(text_ids(&server, "fu_point", "beta").await, ["u1"]);
}

/// An update that moves a word from one field to another retracts it from
/// the old field's index and adds it to the new one, and the per-field
/// indexes hold that state across a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_across_fields_keeps_field_scopes_after_restart() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION fu_fields WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO fu_fields { id: 'f1', title: 'delta heading', body: 'plain body' }")
        .await
        .unwrap();
    server
        .exec("UPDATE fu_fields SET title = 'plain heading', body = 'delta body' WHERE id = 'f1'")
        .await
        .unwrap();

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    assert!(
        text_ids_on(&server, "fu_fields", "title", "delta")
            .await
            .is_empty(),
        "the title index must no longer hold the moved word"
    );
    assert_eq!(
        text_ids_on(&server, "fu_fields", "body", "delta").await,
        ["f1"]
    );
    assert_eq!(
        text_ids_on(&server, "fu_fields", "*", "delta").await,
        ["f1"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_moves_every_row_to_its_new_terms() {
    let server = TestServer::start().await;
    seeded(&server, "fu_bulk").await;

    server
        .exec("UPDATE fu_bulk SET body = 'gamma bulk' WHERE kind = 'k'")
        .await
        .unwrap();

    assert!(text_ids(&server, "fu_bulk", "alpha").await.is_empty());
    assert_eq!(text_ids(&server, "fu_bulk", "gamma").await, ["u1", "u2"]);
}

/// `UPDATE ... FROM` writes through its own row-by-row transaction, not the
/// point/bulk UPDATE paths above, so it must reindex text on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_join_moves_the_row_to_its_new_terms() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION fu_join_target (\
                id TEXT PRIMARY KEY, sku TEXT, body TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION fu_join_source (\
                id TEXT PRIMARY KEY, sku TEXT, new_body TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO fu_join_target (id, sku, body) VALUES ('t1', 'k1', 'alpha original')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO fu_join_source (id, sku, new_body) VALUES ('s1', 'k1', 'beta updated')")
        .await
        .unwrap();

    server
        .exec(
            "UPDATE fu_join_target SET body = s.new_body \
             FROM fu_join_source s WHERE fu_join_target.sku = s.sku",
        )
        .await
        .unwrap();

    assert!(
        text_ids(&server, "fu_join_target", "alpha")
            .await
            .is_empty()
    );
    assert_eq!(text_ids(&server, "fu_join_target", "beta").await, ["t1"]);
}

/// `CONVERT COLLECTION` re-encodes every row against the target schema — a
/// schema that declares every field the source rows carry converts cleanly,
/// and every field's words stay findable afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_collection_to_a_full_schema_keeps_every_fields_words() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION fu_convert_full WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO fu_convert_full { id: 'c1', title: 'alpha kept', notes: 'beta kept' }")
        .await
        .unwrap();

    server
        .exec(
            "CONVERT COLLECTION fu_convert_full TO document_strict \
             (id TEXT PRIMARY KEY, title TEXT, notes TEXT)",
        )
        .await
        .unwrap();

    assert_eq!(
        text_ids_on(&server, "fu_convert_full", "title", "alpha").await,
        ["c1"]
    );
    assert_eq!(
        text_ids_on(&server, "fu_convert_full", "notes", "beta").await,
        ["c1"]
    );
}

/// `CONVERT COLLECTION` to a schema that omits a field a source row carries
/// is refused rather than silently dropping data: the row's own primary key
/// names it, the missing column and collection are named, the error is a
/// typed SQLSTATE 22000 (data_exception) rather than an internal fault, and
/// the collection — and its full-text index — are left exactly as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_collection_to_a_partial_schema_is_refused_and_leaves_the_collection_unchanged() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION fu_convert_partial WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO fu_convert_partial \
             { id: 'c1', title: 'alpha kept', notes: 'beta dropped' }",
        )
        .await
        .unwrap();

    let err = server
        .exec(
            "CONVERT COLLECTION fu_convert_partial TO document_strict \
             (id TEXT PRIMARY KEY, title TEXT)",
        )
        .await
        .expect_err("CONVERT must refuse a schema that drops a populated field");

    assert!(
        err.contains("(SQLSTATE 22000)"),
        "expected SQLSTATE 22000 (data_exception), got: {err}"
    );
    assert!(
        err.contains("column \"notes\""),
        "error must name the missing column: {err}"
    );
    assert!(
        err.contains("collection \"fu_convert_partial\""),
        "error must name the collection: {err}"
    );
    assert!(
        err.contains("row \"c1\""),
        "error must name the row by its primary key, not an internal surrogate: {err}"
    );
    assert!(
        !err.contains("Internal"),
        "a schema mismatch is a client error, not an internal fault: {err}"
    );

    // Nothing converted: the collection's full-text index still holds the
    // row's original body, on its original schemaless engine.
    assert_eq!(
        text_ids_on(&server, "fu_convert_partial", "notes", "beta").await,
        ["c1"]
    );
}
