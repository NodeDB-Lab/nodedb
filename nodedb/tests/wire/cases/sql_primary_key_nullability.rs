// SPDX-License-Identifier: BUSL-1.1

//! A declared `PRIMARY KEY` implies `NOT NULL` on every engine.
//!
//! Row identity is derived from the primary-key value at plan time. A row
//! whose pk is NULL, or whose pk column is omitted, has no identity: it must
//! be refused with `23502` (`not_null_violation`) rather than committed under
//! a minted identity that no uniqueness check can ever see.
//!
//! The empty string is the counterpart case — a real TEXT value, not an
//! absent one, so two rows carrying it collide on the primary key.

use crate::harness::TestServer;

/// SQLSTATE of a statement that must fail, or `None` when it succeeded.
async fn sqlstate_of(server: &TestServer, sql: &str) -> Option<String> {
    match server.client.simple_query(sql).await {
        Ok(_) => None,
        Err(e) => Some(
            e.as_db_error()
                .unwrap_or_else(|| panic!("expected a DbError from {sql}, got: {e}"))
                .code()
                .code()
                .to_string(),
        ),
    }
}

/// Assert `sql` is refused with `23502`, and that the collection is unchanged.
async fn assert_not_null_violation(server: &TestServer, sql: &str) {
    let state = sqlstate_of(server, sql).await.unwrap_or_else(|| {
        panic!("a NULL primary key must be refused, but the server accepted: {sql}")
    });
    assert_eq!(
        state, "23502",
        "a NULL primary key is a not_null_violation, got SQLSTATE {state} for: {sql}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_schemaless_refuses_explicit_null_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_doc_null (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_doc_null");

    assert_not_null_violation(
        &server,
        "INSERT INTO pk_doc_null (id, v) VALUES (NULL, 'explicit-null')",
    )
    .await;

    let rows = server
        .query_text("SELECT v FROM pk_doc_null")
        .await
        .expect("scan pk_doc_null");
    assert!(
        rows.is_empty(),
        "a refused insert must store nothing, found: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_schemaless_refuses_omitted_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_doc_omit (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_doc_omit");

    assert_not_null_violation(&server, "INSERT INTO pk_doc_omit (v) VALUES ('omitted-pk')").await;

    let rows = server
        .query_text("SELECT v FROM pk_doc_omit")
        .await
        .expect("scan pk_doc_omit");
    assert!(
        rows.is_empty(),
        "a refused insert must store nothing, found: {rows:?}"
    );
}

/// The uniqueness check is what a minted identity bypasses: two rows that
/// both omit the pk must not coexist under distinct synthetic identities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn null_primary_keys_never_accumulate_in_one_collection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_doc_accum (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_doc_accum");

    let _ = server
        .client
        .simple_query("INSERT INTO pk_doc_accum (id, v) VALUES (NULL, 'explicit-null')")
        .await;
    let _ = server
        .client
        .simple_query("INSERT INTO pk_doc_accum (v) VALUES ('omitted-pk')")
        .await;

    let rows = server
        .query_text("SELECT v FROM pk_doc_accum")
        .await
        .expect("scan pk_doc_accum");
    assert!(
        rows.is_empty(),
        "no row may be stored without a primary key, found {} row(s): {rows:?}",
        rows.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_value_refuses_omitted_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_kv_omit (k TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .expect("create pk_kv_omit");

    assert_not_null_violation(&server, "INSERT INTO pk_kv_omit (v) VALUES ('kv-omitted')").await;

    let rows = server
        .query_text("SELECT v FROM pk_kv_omit")
        .await
        .expect("scan pk_kv_omit");
    assert!(
        rows.is_empty(),
        "a refused insert must store nothing, found: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_value_refuses_explicit_null_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_kv_null (k TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .expect("create pk_kv_null");

    assert_not_null_violation(
        &server,
        "INSERT INTO pk_kv_null (k, v) VALUES (NULL, 'kv-null')",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_refuses_null_primary_key() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION pk_col_null (id INT PRIMARY KEY, v TEXT) \
             WITH (engine = 'columnar')",
        )
        .await
        .expect("create pk_col_null");

    assert_not_null_violation(
        &server,
        "INSERT INTO pk_col_null (id, v) VALUES (NULL, 'columnar-null')",
    )
    .await;
    assert_not_null_violation(
        &server,
        "INSERT INTO pk_col_null (v) VALUES ('columnar-omitted')",
    )
    .await;
}

/// The upsert path mints its own identity for a pk-less row, so it needs the
/// same guard as the plain insert path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_refuses_null_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_upsert (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_upsert");

    assert_not_null_violation(
        &server,
        "INSERT INTO pk_upsert (id, v) VALUES (NULL, 'upsert-null') \
         ON CONFLICT (id) DO UPDATE SET v = 'updated'",
    )
    .await;
}

/// The object-literal form reaches the same conversion helpers as the
/// VALUES form, and must refuse an absent primary key identically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_refuses_absent_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_object (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_object");

    assert_not_null_violation(&server, "INSERT INTO pk_object { v: 'object-literal' }").await;
}

/// A projected NULL is the same violation as a literal NULL — the guard
/// belongs where identity is derived, not on the literal in the VALUES list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_refuses_null_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_src (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_src");
    server
        .exec("CREATE COLLECTION pk_dst (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_dst");
    server
        .exec("INSERT INTO pk_src (id, v) VALUES (1, 'from-source')")
        .await
        .expect("seed pk_src");

    assert_not_null_violation(
        &server,
        "INSERT INTO pk_dst (id, v) SELECT NULL, v FROM pk_src",
    )
    .await;

    let rows = server
        .query_text("SELECT v FROM pk_dst")
        .await
        .expect("scan pk_dst");
    assert!(
        rows.is_empty(),
        "a refused INSERT ... SELECT must store nothing, found: {rows:?}"
    );
}

/// Nulling out a stored primary key is the same violation reached from the
/// update path — the row would keep an identity its own column denies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_refuses_to_null_out_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_update (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_update");
    server
        .exec("INSERT INTO pk_update (id, v) VALUES (1, 'keep')")
        .await
        .expect("seed pk_update");

    assert_not_null_violation(&server, "UPDATE pk_update SET id = NULL WHERE v = 'keep'").await;

    let rows = server
        .query_text("SELECT id FROM pk_update")
        .await
        .expect("scan pk_update");
    assert_eq!(rows, vec!["1".to_string()], "the stored key must survive");
}

/// The empty string is a value, not an absence: it identifies a row, so the
/// second insert of it is a duplicate key, and the row is addressable by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_string_primary_key_is_a_value_and_stays_unique() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_empty (id TEXT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_empty");

    server
        .exec("INSERT INTO pk_empty (id, v) VALUES ('', 'first')")
        .await
        .expect("an empty-string primary key is a legal value");

    let state = sqlstate_of(
        &server,
        "INSERT INTO pk_empty (id, v) VALUES ('', 'second')",
    )
    .await
    .expect("a second row under the same empty-string key must be refused");
    assert_eq!(
        state, "23505",
        "a repeated empty-string key is a unique_violation, got SQLSTATE {state}"
    );

    let rows = server
        .query_text("SELECT v FROM pk_empty WHERE id = ''")
        .await
        .expect("point read on the empty-string key");
    assert_eq!(
        rows,
        vec!["first".to_string()],
        "the row must be addressable by its empty-string key"
    );
}

/// The identity choke point shared by INSERT ... SELECT, MERGE, and
/// UPDATE ... FROM keys rows on the same values the plain insert path does.
/// An empty-string key identifies one row there too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_keys_an_empty_string_like_any_other_value() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_es_src (id TEXT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_es_src");
    server
        .exec("CREATE COLLECTION pk_es_dst (id TEXT PRIMARY KEY, v TEXT)")
        .await
        .expect("create pk_es_dst");
    server
        .exec("INSERT INTO pk_es_src (id, v) VALUES ('a', 'first'), ('b', 'second')")
        .await
        .expect("seed pk_es_src");

    server
        .exec("INSERT INTO pk_es_dst (id, v) SELECT '', v FROM pk_es_src")
        .await
        .expect("an empty-string key is a legal value");

    let rows = server
        .query_text("SELECT v FROM pk_es_dst WHERE id = ''")
        .await
        .expect("point read on the empty-string key");
    assert_eq!(
        rows.len(),
        1,
        "both source rows carry the same key, so one row survives: {rows:?}"
    );
}
