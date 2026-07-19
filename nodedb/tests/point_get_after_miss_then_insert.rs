// SPDX-License-Identifier: BUSL-1.1

//! A `WHERE <pk> = X` that MISSES (key absent), followed by an INSERT that
//! creates key X, must then be visible to a subsequent `WHERE <pk> = X`: a
//! read that misses must never cache emptiness for the key such that a later
//! write stays invisible. Covered for the schemaless, strict, and key-value
//! engines, across point-get / compound-predicate / full-scan reads, in
//! autocommit.

mod common;
use common::pgwire_harness::TestServer;

async fn assert_miss_then_insert_then_hit(srv: &TestServer, create: &str, pk_col: &str) {
    srv.exec(create).await.expect("create collection");

    // 1. Point-get on an absent key — must miss (0 rows), and must NOT poison
    //    any subsequent read of the same key.
    let miss = srv
        .query_rows(&format!(
            "SELECT {pk_col} FROM poison WHERE {pk_col} = 'k1'"
        ))
        .await
        .expect("point-get miss");
    assert_eq!(
        miss.len(),
        0,
        "key k1 must be absent before insert, got: {miss:?}"
    );

    // 2. Insert the key that just missed.
    srv.exec(&format!(
        "INSERT INTO poison ({pk_col}, v) VALUES ('k1', 'hello')"
    ))
    .await
    .expect("insert k1");

    // 3. The same point-get must now see the row.
    let hit = srv
        .query_rows(&format!(
            "SELECT {pk_col}, v FROM poison WHERE {pk_col} = 'k1'"
        ))
        .await
        .expect("point-get hit after insert");
    assert_eq!(
        hit.len(),
        1,
        "key k1 must be visible after the insert that followed a miss (point-get poisoning), got: {hit:?}"
    );
    assert_eq!(hit[0][0], "k1");
    assert_eq!(hit[0][1], "hello");

    // 4. A compound predicate and a plain scan must also see the row (no read
    //    path may observe a stale emptiness for the key after the insert).
    let compound = srv
        .query_rows(&format!(
            "SELECT {pk_col} FROM poison WHERE {pk_col} = 'k1' AND v = 'hello'"
        ))
        .await
        .expect("compound-predicate read after insert");
    assert_eq!(
        compound.len(),
        1,
        "compound predicate must see key k1 after insert-following-miss, got: {compound:?}"
    );
    let scan = srv
        .query_rows("SELECT v FROM poison")
        .await
        .expect("full scan after insert");
    assert_eq!(
        scan.len(),
        1,
        "full scan must see the inserted row, got: {scan:?}"
    );

    srv.exec("DROP COLLECTION poison").await.expect("drop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_simple_query_replans_absent_point_identity_after_insert() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cached_identity (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create strict collection");

    let lookup = "SELECT v FROM cached_identity WHERE id = 'k1'";
    let miss = srv.query_rows(lookup).await.expect("initial point miss");
    assert!(miss.is_empty(), "key must initially be absent: {miss:?}");

    srv.exec("INSERT INTO cached_identity (id, v) VALUES ('k1', 'visible')")
        .await
        .expect("insert previously absent key");

    let hit = srv
        .query_rows(lookup)
        .await
        .expect("identical point lookup after insert");
    assert_eq!(
        hit,
        vec![vec!["visible".to_string()]],
        "byte-identical simple query must resolve the identity created by the committed insert"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_simple_query_replans_absent_point_identity_after_insert() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cached_schemaless (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_schemaless')",
    )
    .await
    .expect("create schemaless collection");

    let lookup = "SELECT v FROM cached_schemaless WHERE id = 'k1'";
    assert!(
        srv.query_rows(lookup)
            .await
            .expect("initial point miss")
            .is_empty()
    );
    srv.exec("INSERT INTO cached_schemaless (id, v) VALUES ('k1', 'visible')")
        .await
        .expect("insert previously absent key");

    let hit = srv
        .query_rows(lookup)
        .await
        .expect("identical point lookup after insert");
    assert_eq!(
        hit,
        vec![vec!["visible".to_string()]],
        "schemaless point lookup must resolve the identity created after its initial miss"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parameterless_extended_query_replans_absent_point_identity_after_insert() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cached_extended (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create strict collection");

    let statement = srv
        .client
        .prepare("SELECT v FROM cached_extended WHERE id = 'k1'")
        .await
        .expect("prepare literal point lookup");
    let miss = srv
        .client
        .query(&statement, &[])
        .await
        .expect("initial prepared point miss");
    assert!(miss.is_empty(), "key must initially be absent");

    srv.exec("INSERT INTO cached_extended (id, v) VALUES ('k1', 'visible')")
        .await
        .expect("insert previously absent key");

    let hit = srv
        .client
        .query(&statement, &[])
        .await
        .expect("repeat prepared point lookup after insert");
    assert_eq!(
        hit.len(),
        1,
        "parameterless extended query must observe the committed insert"
    );
    let value: &str = hit[0].get("v");
    assert_eq!(value, "visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_replans_identity_created_after_initial_no_match() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cached_update (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create strict collection");

    let update = "UPDATE cached_update SET v = 'updated' WHERE id = 'k1'";
    srv.exec(update).await.expect("initial no-match update");
    srv.exec("INSERT INTO cached_update (id, v) VALUES ('k1', 'initial')")
        .await
        .expect("insert previously absent key");
    srv.exec(update)
        .await
        .expect("repeat identical update after insert");

    let rows = srv
        .query_rows("SELECT v FROM cached_update WHERE id = 'k1'")
        .await
        .expect("read updated row");
    assert_eq!(
        rows,
        vec![vec!["updated".to_string()]],
        "point UPDATE must resolve a key created after the statement first matched no row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_delete_replans_identity_created_after_initial_no_match() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cached_delete (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create strict collection");

    let delete = "DELETE FROM cached_delete WHERE id = 'k1'";
    srv.exec(delete).await.expect("initial no-match delete");
    srv.exec("INSERT INTO cached_delete (id, v) VALUES ('k1', 'present')")
        .await
        .expect("insert previously absent key");
    srv.exec(delete)
        .await
        .expect("repeat identical delete after insert");

    let rows = srv
        .query_rows("SELECT v FROM cached_delete WHERE id = 'k1'")
        .await
        .expect("verify point delete");
    assert!(
        rows.is_empty(),
        "point DELETE must resolve and remove a key created after the statement first matched no row: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_update_replans_identities_missing_from_initial_target_set() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cached_multi_update (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create strict collection");
    srv.exec("INSERT INTO cached_multi_update (id, v) VALUES ('a', 'initial')")
        .await
        .expect("insert initial key");

    let update = "UPDATE cached_multi_update SET v = 'updated' WHERE id IN ('a', 'b')";
    srv.exec(update)
        .await
        .expect("initial partial-target update");
    srv.exec("INSERT INTO cached_multi_update (id, v) VALUES ('b', 'initial')")
        .await
        .expect("insert key missing from initial target set");
    srv.exec(update)
        .await
        .expect("repeat identical multi-key update");

    let rows = srv
        .query_rows("SELECT v FROM cached_multi_update WHERE id = 'b'")
        .await
        .expect("read later-created target");
    assert_eq!(
        rows,
        vec![vec!["updated".to_string()]],
        "multi-key UPDATE must not permanently omit identities absent during initial planning"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_delete_replans_identities_missing_from_initial_target_set() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cached_multi_delete (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create strict collection");
    srv.exec("INSERT INTO cached_multi_delete (id, v) VALUES ('a', 'present')")
        .await
        .expect("insert initial key");

    let delete = "DELETE FROM cached_multi_delete WHERE id IN ('a', 'b')";
    srv.exec(delete)
        .await
        .expect("initial partial-target delete");
    srv.exec("INSERT INTO cached_multi_delete (id, v) VALUES ('b', 'present')")
        .await
        .expect("insert key missing from initial target set");
    srv.exec(delete)
        .await
        .expect("repeat identical multi-key delete");

    let rows = srv
        .query_rows("SELECT v FROM cached_multi_delete WHERE id = 'b'")
        .await
        .expect("verify later-created target deletion");
    assert!(
        rows.is_empty(),
        "multi-key DELETE must not permanently omit identities absent during initial planning: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_point_get_after_miss_then_insert_is_visible() {
    let srv = TestServer::start().await;
    assert_miss_then_insert_then_hit(
        &srv,
        "CREATE COLLECTION poison (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_schemaless')",
        "id",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_point_get_after_miss_then_insert_is_visible() {
    let srv = TestServer::start().await;
    assert_miss_then_insert_then_hit(
        &srv,
        "CREATE COLLECTION poison (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
        "id",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_point_get_after_miss_then_insert_is_visible() {
    let srv = TestServer::start().await;
    assert_miss_then_insert_then_hit(
        &srv,
        "CREATE COLLECTION poison (key STRING PRIMARY KEY, v STRING) \
         WITH (engine='kv')",
        "key",
    )
    .await;
}
