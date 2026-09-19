// SPDX-License-Identifier: BUSL-1.1

//! In-transaction VECTOR search must observe the transaction's own staged
//! document writes (read-your-own-writes for Vector): a row whose vector
//! field was inserted earlier in the same transaction appears in
//! `ORDER BY vector_distance(...)` / kNN results, ranked by TRUE distance,
//! before COMMIT.
//!
//! A vector is a FIELD on a document, not a standalone stageable write --
//! the HNSW index update is an inline side effect of the ordinary document
//! `PointInsert`/`PointPut`, so the document body (which already carries the
//! vector field) is staged by the normal point-write staging path. The merge
//! at query time re-reads those staged document bodies, extracts the queried
//! vector field, and re-scores it against the query vector
//! (`handlers/transaction/overlay/vector_merge.rs`), mirroring the FTS
//! overlay-merge pattern.
//!
//! Scope: single-vector search merge on a document collection. A
//! vector-primary collection's staged DML is covered by
//! `sql_transactions_vector_primary_overlay.rs`; `MultiSearch` and
//! `SetParams` staging are not covered here.

use crate::harness::TestServer;

async fn create_vector_collection(server: &TestServer, name: &str, dim: usize) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} METRIC l2 DIM {dim}"
        ))
        .await
        .unwrap();
}

async fn insert_vector(server: &TestServer, coll: &str, id: &str, v: &[f32]) {
    let arr = v
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, embedding) VALUES ('{id}', ARRAY[{arr}])"
        ))
        .await
        .unwrap();
}

/// `SELECT id FROM coll ORDER BY vector_distance(embedding, ARRAY[...]) LIMIT k`,
/// returning the ids in ascending true-distance (nearest-first) order. This is
/// the supported vector-search projection surface (the distance value itself is
/// not a selectable column); id ordering fully captures the ranking under test.
async fn ranked_ids(server: &TestServer, coll: &str, query: &[f32], k: usize) -> Vec<String> {
    let arr = query
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let rows = server
        .query_rows(&format!(
            "SELECT id FROM {coll} \
             ORDER BY vector_distance(embedding, ARRAY[{arr}]) \
             LIMIT {k}"
        ))
        .await
        .unwrap();
    rows.into_iter().map(|r| r[0].clone()).collect()
}

/// BEGIN; INSERT a vector CLOSE to the query vector; in-tx search includes
/// it ranked near the top by true distance; COMMIT persists it; a fresh
/// (post-commit, no txn) search still finds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_close_vector_visible_in_txn_ranked_then_commit_persists() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_ov_close", 3).await;

    insert_vector(&server, "vec_ov_close", "base_far", &[10.0, 10.0, 10.0]).await;

    server.exec("BEGIN").await.unwrap();
    insert_vector(&server, "vec_ov_close", "staged_close", &[0.0, 0.0, 0.1]).await;

    let query = [0.0, 0.0, 0.0];
    let in_txn = ranked_ids(&server, "vec_ov_close", &query, 2).await;
    // Both rows are returned and the staged vector, being nearest, ranks first.
    // Every hit projects its user PK: the staged row resolves via the overlay's
    // staged body, the base row via its committed surrogate → PK binding.
    assert_eq!(
        in_txn.len(),
        2,
        "expected both rows visible in-tx: {in_txn:?}"
    );
    assert_eq!(
        in_txn[0], "staged_close",
        "staged vector close to the query must rank first before COMMIT: {in_txn:?}"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    // Load-bearing assertion: post-commit the vector-search `id`
    // projection must resolve to the committed row's user PK ('staged_close'),
    // NOT its raw surrogate. The staged insert's global surrogate is now bound
    // in the durable HNSW index (identical to an autocommit insert), so the
    // Control-Plane response boundary maps surrogate → PK for the search hit.
    let after_commit = ranked_ids(&server, "vec_ov_close", &query, 2).await;
    assert_eq!(
        after_commit.len(),
        2,
        "both rows remain searchable after COMMIT: {after_commit:?}"
    );
    assert_eq!(
        after_commit[0], "staged_close",
        "post-commit vector search must project the committed row's PK \
         'staged_close' (nearest to the query), not its surrogate: {after_commit:?}"
    );
    assert_eq!(
        after_commit[1], "base_far",
        "the base row must also project its PK 'base_far', proving surrogate → PK \
         resolution holds for the whole document+vector-index class: {after_commit:?}"
    );

    // Additional durability check: the committed row is also reachable by a
    // direct PK point lookup.
    let persisted = server
        .query_rows("SELECT id FROM vec_ov_close WHERE id = 'staged_close'")
        .await
        .unwrap();
    assert_eq!(
        persisted.len(),
        1,
        "committed insert must persist under its PK: {persisted:?}"
    );
}

/// BEGIN; INSERT a vector; ROLLBACK; the vector must never have been
/// durably indexed and must not appear post-rollback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_visible_in_txn_then_rollback_excludes() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_ov_rb", 3).await;
    insert_vector(&server, "vec_ov_rb", "base1", &[5.0, 5.0, 5.0]).await;

    server.exec("BEGIN").await.unwrap();
    insert_vector(&server, "vec_ov_rb", "staged_rb", &[0.0, 0.0, 0.0]).await;

    let query = [0.0, 0.0, 0.0];
    let in_txn = ranked_ids(&server, "vec_ov_rb", &query, 5).await;
    assert!(
        in_txn.iter().any(|id| id == "staged_rb"),
        "in-tx search must include the staged insert: {in_txn:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after_rollback = ranked_ids(&server, "vec_ov_rb", &query, 5).await;
    assert!(
        after_rollback.iter().all(|id| id != "staged_rb"),
        "ROLLBACK must leave no durable trace of the staged insert: {after_rollback:?}"
    );
}

/// A staged vector FAR from the query only appears once `k` is large enough
/// to include it, and always ranks last (by true distance) among returned
/// rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn far_staged_vector_ranks_last_and_needs_larger_k() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_ov_far", 3).await;

    for i in 0..3 {
        insert_vector(
            &server,
            "vec_ov_far",
            &format!("near{i}"),
            &[i as f32 * 0.1, 0.0, 0.0],
        )
        .await;
    }

    server.exec("BEGIN").await.unwrap();
    insert_vector(&server, "vec_ov_far", "staged_far", &[100.0, 100.0, 100.0]).await;

    let query = [0.0, 0.0, 0.0];

    // k=3: the three near base rows fill every slot; the far staged vector
    // is correctly excluded (its true distance loses to all three).
    let small_k = ranked_ids(&server, "vec_ov_far", &query, 3).await;
    assert_eq!(small_k.len(), 3);
    assert!(
        small_k.iter().all(|id| id != "staged_far"),
        "far staged vector must not displace closer base rows at k=3: {small_k:?}"
    );

    // k=4: now there is room for the far staged vector, and it must be
    // ranked strictly last by true distance.
    let large_k = ranked_ids(&server, "vec_ov_far", &query, 4).await;
    assert_eq!(large_k.len(), 4, "expected 4 rows at k=4: {large_k:?}");
    assert_eq!(
        large_k[3], "staged_far",
        "far staged vector must rank last at k=4: {large_k:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// A base (already-committed) vector plus a staged vector are both present
/// and correctly ordered relative to each other and the query by true
/// distance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn base_and_staged_vectors_interleave_by_true_distance() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_ov_mix", 3).await;

    // Base rows at distances (from origin) 1, 5, 9 along the x-axis.
    insert_vector(&server, "vec_ov_mix", "base_1", &[1.0, 0.0, 0.0]).await;
    insert_vector(&server, "vec_ov_mix", "base_5", &[5.0, 0.0, 0.0]).await;
    insert_vector(&server, "vec_ov_mix", "base_9", &[9.0, 0.0, 0.0]).await;

    server.exec("BEGIN").await.unwrap();
    // Staged rows interleaved at distances 3 and 7.
    insert_vector(&server, "vec_ov_mix", "staged_3", &[3.0, 0.0, 0.0]).await;
    insert_vector(&server, "vec_ov_mix", "staged_7", &[7.0, 0.0, 0.0]).await;

    let query = [0.0, 0.0, 0.0];
    let ranked = ranked_ids(&server, "vec_ov_mix", &query, 5).await;
    // Staged rows at true distances 3 and 7 must land at ranks 1 and 3 —
    // strictly between the base rows at 1/5/9 — proving the merge re-scores
    // staged vectors by true distance rather than appending them. Base rows
    // occupy ranks 0/2/4; their own id resolution is a separate concern.
    assert_eq!(ranked.len(), 5, "expected all five rows: {ranked:?}");
    assert_eq!(
        ranked[1], "staged_3",
        "staged vector at distance 3 must rank second (between base 1 and 5): {ranked:?}"
    );
    assert_eq!(
        ranked[3], "staged_7",
        "staged vector at distance 7 must rank fourth (between base 5 and 9): {ranked:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}
