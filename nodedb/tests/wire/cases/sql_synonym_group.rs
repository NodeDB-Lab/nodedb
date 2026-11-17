// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the `CREATE SYNONYM GROUP` / `DROP SYNONYM GROUP` /
//! `SHOW SYNONYM GROUPS` DDL surface and end-to-end FTS query expansion.
//!
//! Scenarios covered:
//! 1. Create a synonym group and verify SHOW lists it.
//! 2. Drop a synonym group (unconditional) — SHOW no longer lists it.
//! 3. DROP IF EXISTS on a non-existent group — no error.
//! 4. Duplicate CREATE — returns an error.
//! 5. Query-time synonym expansion: querying one term matches documents
//!    that contain a synonym term.
//! 6. Drop removes expansion — post-drop query no longer expands.
//! 7. Restart durability: groups survive server restart.
//! 8. Database scoping: a group belongs to the database that created it, in
//!    the registry and in the FTS backend alike.

use crate::harness::TestServer;

fn row_values(msgs: &[tokio_postgres::SimpleQueryMessage], column: usize) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(column).map(|v| v.to_string()),
            _ => None,
        })
        .collect()
}

/// Create a synonym group and verify SHOW SYNONYM GROUPS reflects it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_show_synonym_group() {
    let srv = TestServer::start().await;

    srv.exec("CREATE SYNONYM GROUP colors AS ('red', 'crimson', 'scarlet')")
        .await
        .expect("create synonym group");

    let rows = srv
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show synonym groups");

    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names.contains(&"colors"),
        "colors group must appear in SHOW SYNONYM GROUPS: {names:?}"
    );

    let colors_row = rows.iter().find(|r| r[0] == "colors").unwrap();
    let terms_str = &colors_row[1];
    assert!(
        terms_str.contains("red"),
        "terms must contain 'red': {terms_str}"
    );
    assert!(
        terms_str.contains("crimson"),
        "terms must contain 'crimson': {terms_str}"
    );
    assert!(
        terms_str.contains("scarlet"),
        "terms must contain 'scarlet': {terms_str}"
    );
}

/// Drop a synonym group and verify SHOW no longer lists it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_synonym_group_removes_from_show() {
    let srv = TestServer::start().await;

    srv.exec("CREATE SYNONYM GROUP mammals AS ('cat', 'feline', 'kitty')")
        .await
        .expect("create synonym group");

    srv.exec("DROP SYNONYM GROUP mammals")
        .await
        .expect("drop synonym group");

    let rows = srv
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show synonym groups");

    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        !names.contains(&"mammals"),
        "mammals group must be absent after drop: {names:?}"
    );
}

/// DROP IF EXISTS on a group that does not exist must succeed (no error).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_if_exists_nonexistent_group_is_ok() {
    let srv = TestServer::start().await;

    srv.exec("DROP SYNONYM GROUP IF EXISTS nonexistent_group_xyz")
        .await
        .expect("DROP IF EXISTS on nonexistent group must not error");
}

/// Creating a synonym group with a name that already exists must return an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_create_synonym_group_errors() {
    let srv = TestServer::start().await;

    srv.exec("CREATE SYNONYM GROUP dupes AS ('alpha', 'beta')")
        .await
        .expect("first create must succeed");

    let result = srv
        .exec("CREATE SYNONYM GROUP dupes AS ('gamma', 'delta')")
        .await;

    assert!(
        result.is_err(),
        "second CREATE with same name must return an error"
    );
}

/// Query-time synonym expansion: document containing a synonym term is
/// returned when the query uses a different term in the same group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fts_query_expands_synonym_terms() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION syn_docs WITH (engine='document_schemaless')")
        .await
        .expect("create collection");
    srv.exec("CREATE SEARCH INDEX ON syn_docs (body)")
        .await
        .expect("declare the body search index");

    // Insert documents using one term from each synonym pair.
    srv.exec("INSERT INTO syn_docs { id: 'a1', body: 'automobile engine repair' }")
        .await
        .expect("insert a1");
    srv.exec("INSERT INTO syn_docs { id: 'a2', body: 'car maintenance guide' }")
        .await
        .expect("insert a2");
    srv.exec("INSERT INTO syn_docs { id: 'a3', body: 'cooking recipe for pasta' }")
        .await
        .expect("insert a3");

    // Before adding synonym group: querying 'automobile' should NOT match 'car'.
    let rows_before = srv
        .query_rows("SELECT id FROM syn_docs WHERE text_match(body, 'automobile') ORDER BY id")
        .await
        .expect("query before synonyms");
    let ids_before: Vec<&str> = rows_before.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_before.contains(&"a1"),
        "a1 must match 'automobile' before synonyms: {ids_before:?}"
    );
    assert!(
        !ids_before.contains(&"a2"),
        "a2 must NOT match 'automobile' before synonyms: {ids_before:?}"
    );

    // Add synonym group: automobile = car.
    srv.exec("CREATE SYNONYM GROUP vehicles AS ('automobile', 'car')")
        .await
        .expect("create synonym group");

    // After adding synonym group: querying 'automobile' should also match 'car'.
    let rows_after = srv
        .query_rows("SELECT id FROM syn_docs WHERE text_match(body, 'automobile') ORDER BY id")
        .await
        .expect("query after synonyms");
    let ids_after: Vec<&str> = rows_after.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_after.contains(&"a1"),
        "a1 must still match 'automobile': {ids_after:?}"
    );
    assert!(
        ids_after.contains(&"a2"),
        "a2 must now match via synonym expansion: {ids_after:?}"
    );
    assert!(
        !ids_after.contains(&"a3"),
        "a3 (unrelated) must not match: {ids_after:?}"
    );
}

/// Drop removes expansion: after dropping a synonym group, querying one term
/// no longer returns documents that only contain a synonym term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fts_expansion_disabled_after_drop() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION syn_drop_docs WITH (engine='document_schemaless')")
        .await
        .expect("create collection");
    srv.exec("CREATE SEARCH INDEX ON syn_drop_docs (body)")
        .await
        .expect("declare the body search index");

    srv.exec("INSERT INTO syn_drop_docs { id: 'b1', body: 'physician visits hospital' }")
        .await
        .expect("insert b1");
    srv.exec("INSERT INTO syn_drop_docs { id: 'b2', body: 'doctor appointments clinic' }")
        .await
        .expect("insert b2");

    srv.exec("CREATE SYNONYM GROUP medical AS ('physician', 'doctor')")
        .await
        .expect("create synonym group");

    // Confirm expansion works.
    let rows_with = srv
        .query_rows("SELECT id FROM syn_drop_docs WHERE text_match(body, 'physician') ORDER BY id")
        .await
        .expect("query with synonyms");
    let ids_with: Vec<&str> = rows_with.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_with.contains(&"b2"),
        "b2 must match via synonym before drop: {ids_with:?}"
    );

    // Drop synonym group.
    srv.exec("DROP SYNONYM GROUP medical")
        .await
        .expect("drop synonym group");

    // After drop, expansion must no longer work.
    let rows_without = srv
        .query_rows("SELECT id FROM syn_drop_docs WHERE text_match(body, 'physician') ORDER BY id")
        .await
        .expect("query without synonyms");
    let ids_without: Vec<&str> = rows_without.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_without.contains(&"b1"),
        "b1 must still match 'physician': {ids_without:?}"
    );
    assert!(
        !ids_without.contains(&"b2"),
        "b2 must NOT match after drop (no expansion): {ids_without:?}"
    );
}

/// Restart durability: synonym groups survive a server restart and expansion
/// still works after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synonym_group_survives_restart() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION durable_fts WITH (engine='document_schemaless')")
        .await
        .expect("create collection");
    srv.exec("CREATE SEARCH INDEX ON durable_fts (body)")
        .await
        .expect("declare the body search index");

    srv.exec("INSERT INTO durable_fts { id: 'c1', body: 'laptop computer hardware' }")
        .await
        .expect("insert c1");
    srv.exec("INSERT INTO durable_fts { id: 'c2', body: 'notebook computing device' }")
        .await
        .expect("insert c2");

    srv.exec("CREATE SYNONYM GROUP computing AS ('laptop', 'notebook')")
        .await
        .expect("create synonym group");

    // Verify it's there before restart.
    let rows_pre = srv
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show pre-restart");
    let names_pre: Vec<&str> = rows_pre.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names_pre.contains(&"computing"),
        "computing must be listed pre-restart: {names_pre:?}"
    );

    // Capture data dir and restart.
    let (srv, data_dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _data_dir) = TestServer::open_on_path(data_dir).await;

    // Verify group still listed after restart.
    let rows_post = srv2
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show post-restart");
    let names_post: Vec<&str> = rows_post.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names_post.contains(&"computing"),
        "computing must survive restart: {names_post:?}"
    );

    // Verify expansion still works after restart.
    let rows_exp = srv2
        .query_rows("SELECT id FROM durable_fts WHERE text_match(body, 'laptop') ORDER BY id")
        .await
        .expect("fts query post-restart");
    let ids_exp: Vec<&str> = rows_exp.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_exp.contains(&"c1"),
        "c1 must match 'laptop': {ids_exp:?}"
    );
    assert!(
        ids_exp.contains(&"c2"),
        "c2 must match via synonym after restart: {ids_exp:?}"
    );
}

/// A group belongs to the database that created it. The same name is accepted
/// in a second database, and each database's FTS backend expands only its own
/// terms.
///
/// The dispatch fans the group out to every core carrying the entry's own
/// `database_id`. Send it under another database's id and the second query
/// below expands `car` and returns an extra row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_expands_only_in_the_database_that_holds_it() {
    let server = TestServer::start().await;
    let client = &*server.client;

    for name in ["syn_scope_a", "syn_scope_b"] {
        client
            .simple_query(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {name}: {e}"));
        client
            .simple_query(&format!("USE DATABASE {name}"))
            .await
            .unwrap_or_else(|e| panic!("USE {name}: {e}"));
        client
            .simple_query("CREATE COLLECTION scoped_docs WITH (engine='document_schemaless')")
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION in {name}: {e}"));
        client
            .simple_query("CREATE SEARCH INDEX ON scoped_docs (body)")
            .await
            .unwrap_or_else(|e| panic!("CREATE SEARCH INDEX in {name}: {e}"));
        client
            .simple_query("INSERT INTO scoped_docs { id: 'd1', body: 'automobile engine repair' }")
            .await
            .unwrap_or_else(|e| panic!("insert d1 in {name}: {e}"));
        client
            .simple_query("INSERT INTO scoped_docs { id: 'd2', body: 'car maintenance guide' }")
            .await
            .unwrap_or_else(|e| panic!("insert d2 in {name}: {e}"));
    }

    client
        .simple_query("USE DATABASE syn_scope_a")
        .await
        .expect("USE syn_scope_a");
    client
        .simple_query("CREATE SYNONYM GROUP vehicles AS ('automobile', 'car')")
        .await
        .expect("CREATE SYNONYM GROUP in the first database");

    let in_a = row_values(
        &client
            .simple_query(
                "SELECT id FROM scoped_docs WHERE text_match(body, 'automobile') ORDER BY id",
            )
            .await
            .expect("query the first database"),
        0,
    );
    assert!(
        in_a.iter().any(|id| id == "d2"),
        "the first database must expand 'car': {in_a:?}"
    );

    // The same name in a second database is a different group, so this CREATE
    // must be accepted rather than refused as a duplicate.
    client
        .simple_query("USE DATABASE syn_scope_b")
        .await
        .expect("USE syn_scope_b");
    client
        .simple_query("CREATE SYNONYM GROUP vehicles AS ('automobile', 'lorry')")
        .await
        .expect("the same group name in a second database must be accepted");

    let in_b = row_values(
        &client
            .simple_query(
                "SELECT id FROM scoped_docs WHERE text_match(body, 'automobile') ORDER BY id",
            )
            .await
            .expect("query the second database"),
        0,
    );
    assert!(
        in_b.iter().any(|id| id == "d1"),
        "the second database must still match the query term: {in_b:?}"
    );
    assert!(
        !in_b.iter().any(|id| id == "d2"),
        "the second database names no 'car' synonym, so its own group is the \
         only one that reached its FTS backend: {in_b:?}"
    );

    let listed = row_values(
        &client
            .simple_query("SHOW SYNONYM GROUPS")
            .await
            .expect("SHOW in the second database"),
        1,
    );
    assert!(
        listed.iter().any(|terms| terms.contains("lorry")),
        "the second database must list its own terms, not the first's: {listed:?}"
    );

    // Dropping the second database's group leaves the first database whole.
    client
        .simple_query("DROP SYNONYM GROUP vehicles")
        .await
        .expect("DROP in the second database");
    client
        .simple_query("USE DATABASE syn_scope_a")
        .await
        .expect("USE syn_scope_a");

    let still_in_a = row_values(
        &client
            .simple_query(
                "SELECT id FROM scoped_docs WHERE text_match(body, 'automobile') ORDER BY id",
            )
            .await
            .expect("re-query the first database"),
        0,
    );
    assert!(
        still_in_a.iter().any(|id| id == "d2"),
        "a drop in the second database must not remove the first's group: {still_in_a:?}"
    );
}
