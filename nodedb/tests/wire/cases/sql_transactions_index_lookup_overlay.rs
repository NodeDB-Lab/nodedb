// SPDX-License-Identifier: BUSL-1.1

//! In-transaction `WHERE indexed_field = value` reads must observe the
//! transaction's own staged point writes (read-your-own-writes for
//! secondary-index lookups).
//!
//! Unlike a scan, an equality predicate on a field with `CREATE INDEX`
//! rewrites to `DocumentOp::IndexedFetch` / `IndexLookup`, which resolves
//! doc IDs from the durable `INDEXES` table (never staged) and then fetches
//! bodies. This exercises the overlay merge added to that path specifically —
//! see `sql_transactions_scan_overlay.rs` for the analogous scan-path
//! coverage on non-indexed predicates.
//!
//! Every write here is a point write (by primary key). Predicate DML staging
//! is covered by `sql_transactions_bulk_dml_overlay.rs` and
//! `sql_transactions_kv_predicate_overlay.rs`.

use crate::harness::TestServer;

/// Collect the single-column string result of an indexed-equality SELECT as
/// a sorted vec, so assertions are independent of base/overlay row ordering.
async fn select_ids(server: &TestServer, sql: &str) -> Vec<String> {
    let mut v = server.query_text(sql).await.unwrap();
    v.sort_unstable();
    v
}

async fn setup(server: &TestServer, coll: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} \
             (id STRING NOT NULL PRIMARY KEY, region STRING) WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("CREATE INDEX ON {coll}(region)"))
        .await
        .unwrap();
    for (id, region) in [("a", "us"), ("b", "us"), ("unrelated", "eu")] {
        server
            .exec(&format!(
                "INSERT INTO {coll} (id, region) VALUES ('{id}', '{region}')"
            ))
            .await
            .unwrap();
    }
}

/// In-tx INSERT (by PK, so it is staged) of a row matching the indexed
/// predicate must appear in the index-lookup result.
async fn index_lookup_sees_own_insert(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, region) VALUES ('c', 'us')"
        ))
        .await
        .unwrap();

    let seen = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(
        seen,
        vec!["a", "b", "c"],
        "{engine}: in-tx index lookup must include the staged insert"
    );

    server.client.simple_query("COMMIT").await.unwrap();
    let after = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(
        after,
        vec!["a", "b", "c"],
        "{engine}: staged insert persists"
    );
}

/// In-tx DELETE (by PK) of a row matching the indexed predicate must
/// disappear from the index-lookup result, and ROLLBACK restores it.
async fn index_lookup_excludes_own_delete(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("DELETE FROM {coll} WHERE id = 'b'"))
        .await
        .unwrap();

    let seen = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(
        seen,
        vec!["a"],
        "{engine}: in-tx index lookup must hide the staged delete"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(
        after,
        vec!["a", "b"],
        "{engine}: ROLLBACK restores the base row"
    );
}

/// In-tx UPDATE (by PK) that moves a row's indexed value must be reflected
/// both at the old value (excluded) and the new value (included).
async fn index_lookup_reflects_value_move(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("UPDATE {coll} SET region = 'eu' WHERE id = 'a'"))
        .await
        .unwrap();

    let us = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(
        us,
        vec!["b"],
        "{engine}: staged update must remove 'a' from the old value's lookup"
    );

    let eu = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'eu'"),
    )
    .await;
    assert_eq!(
        eu,
        vec!["a", "unrelated"],
        "{engine}: staged update must add 'a' to the new value's lookup"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_us = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(after_us, vec!["a", "b"], "ROLLBACK: base index lookup only");
}

async fn setup_residual(server: &TestServer, coll: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} \
             (id STRING NOT NULL PRIMARY KEY, region STRING, score INT) WITH (engine='{engine}')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("CREATE INDEX ON {coll}(region)"))
        .await
        .unwrap();
    for (id, region, score) in [("a", "us", 100), ("b", "us", 5), ("unrelated", "eu", 100)] {
        server
            .exec(&format!(
                "INSERT INTO {coll} (id, region, score) VALUES ('{id}', '{region}', {score})"
            ))
            .await
            .unwrap();
    }
}

/// A compound-predicate index lookup (`WHERE region = 'us' AND score > 10`)
/// resolves `region = 'us'` via the secondary index and applies `score > 10`
/// as the residual. A staged row must honor the residual exactly like a
/// committed row: matching the indexed term alone must not be enough to
/// surface it in-transaction (regression for the overlay merge leaking
/// residual-failing staged rows).
async fn index_lookup_applies_residual_to_overlay(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup_residual(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();

    // Staged insert matching the indexed term but failing the residual
    // (score = 1, not > 10) must NOT leak into the in-tx result.
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, region, score) VALUES ('fail', 'us', 1)"
        ))
        .await
        .unwrap();

    // Staged insert matching both the indexed term and the residual must
    // still be visible (read-your-own-writes holds under a compound WHERE).
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, region, score) VALUES ('pass', 'us', 50)"
        ))
        .await
        .unwrap();

    let seen = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us' AND score > 10"),
    )
    .await;
    assert_eq!(
        seen,
        vec!["a", "pass"],
        "{engine}: a staged row matching the indexed term but failing the residual must be \
         excluded, while a staged row satisfying both must be included"
    );

    // A staged UPDATE that moves an already-matching row's residual column
    // out of range (without touching the indexed column) must also drop it
    // -- the supersede path must re-check the residual, not just the term.
    server
        .exec(&format!("UPDATE {coll} SET score = 1 WHERE id = 'a'"))
        .await
        .unwrap();

    let after_update = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us' AND score > 10"),
    )
    .await;
    assert_eq!(
        after_update,
        vec!["pass"],
        "{engine}: a staged update that moves a row's residual value out of range must exclude it"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us' AND score > 10"),
    )
    .await;
    assert_eq!(
        after_rollback,
        vec!["a"],
        "{engine}: ROLLBACK restores the base index lookup + residual result"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_index_lookup_applies_residual_to_overlay() {
    index_lookup_applies_residual_to_overlay("document_schemaless", "il_res").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_index_lookup_applies_residual_to_overlay() {
    index_lookup_applies_residual_to_overlay("document_strict", "il_res_st").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_index_lookup_sees_own_insert() {
    index_lookup_sees_own_insert("document_schemaless", "il_ins").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_index_lookup_excludes_own_delete() {
    index_lookup_excludes_own_delete("document_schemaless", "il_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_index_lookup_reflects_value_move() {
    index_lookup_reflects_value_move("document_schemaless", "il_mov").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_index_lookup_sees_own_insert() {
    index_lookup_sees_own_insert("document_strict", "il_ins_st").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_index_lookup_excludes_own_delete() {
    index_lookup_excludes_own_delete("document_strict", "il_del_st").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_index_lookup_reflects_value_move() {
    index_lookup_reflects_value_move("document_strict", "il_mov_st").await;
}

// ── Bitemporal (versioned-index) coverage ───────────────────────────────────

async fn setup_bitemporal(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, region STRING) \
             WITH (engine='document_schemaless', bitemporal=true)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("CREATE INDEX ON {coll}(region)"))
        .await
        .unwrap();
    for (id, region) in [("a", "us"), ("b", "us"), ("unrelated", "eu")] {
        server
            .exec(&format!(
                "INSERT INTO {coll} (id, region) VALUES ('{id}', '{region}')"
            ))
            .await
            .unwrap();
    }
}

/// On a bitemporal collection the equality predicate routes through the
/// versioned-index lookup (`versioned_index_lookup_as_of`). The overlay
/// merge must apply there too: staged insert/delete/update against the
/// indexed field must be observed in-transaction and reverted on ROLLBACK.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_index_lookup_sees_own_writes() {
    let server = TestServer::start().await;
    let coll = "bt_il";
    setup_bitemporal(&server, coll).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, region) VALUES ('c', 'us')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("DELETE FROM {coll} WHERE id = 'b'"))
        .await
        .unwrap();
    server
        .exec(&format!("UPDATE {coll} SET region = 'eu' WHERE id = 'a'"))
        .await
        .unwrap();

    let us = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(
        us,
        vec!["c"],
        "bitemporal versioned-index lookup must merge staged insert+delete+update"
    );

    let eu = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'eu'"),
    )
    .await;
    assert_eq!(eu, vec!["a", "unrelated"]);

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after = select_ids(
        &server,
        &format!("SELECT id FROM {coll} WHERE region = 'us'"),
    )
    .await;
    assert_eq!(
        after,
        vec!["a", "b"],
        "ROLLBACK: bitemporal versioned-index lookup sees base only"
    );
}
