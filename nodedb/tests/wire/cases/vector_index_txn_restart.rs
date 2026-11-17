// SPDX-License-Identifier: BUSL-1.1

//! WAL-only restart durability for in-transaction writes into vector/FTS-indexed
//! collections — the core regression the statement-time-staging redesign targets.
//!
//! Single-shard COMMIT journals the transaction as one replayable
//! `TransactionRedo` WAL record (built overlay-driven from the transaction's
//! staged post-images by `MetaOp::ResolveTxnPart`), not the non-replayable
//! `Transaction` placeholder. On a WAL-only restart (no vector checkpoint), the
//! base document/KV rows survive via redb, but the in-memory secondary indexes
//! (HNSW vector graph, FTS postings) have no other durable backing — they are
//! rebuilt by replaying the `TransactionRedo` record's engine-native sub-records.
//! Before the redo switch, an in-transaction write into a vector-indexed
//! collection survived as a document but its vector was NEVER re-inserted into
//! the HNSW, so a post-restart vector search returned empty.
//!
//! Each test wraps its writes in an explicit transaction, uses explicit `id`
//! primary keys, performs a WAL-only restart, then asserts BOTH a plain scan
//! (redb durability) AND an index search (the redo-driven index rebuild).

use crate::harness::TestServer;

/// Create a schemaless document collection with a secondary cosine vector index
/// on `embedding` (DIM 4).
async fn create_vec_collection(srv: &TestServer, coll: &str, idx: &str) {
    srv.exec(&format!("CREATE COLLECTION {coll} TYPE document"))
        .await
        .unwrap();
    srv.exec(&format!(
        "CREATE VECTOR INDEX {idx} ON {coll} (embedding) METRIC cosine DIM 4"
    ))
    .await
    .unwrap();
}

/// Autocommit-insert one `(id, sku, embedding)` row.
async fn insert_row(srv: &TestServer, coll: &str, id: &str, sku: &str, emb: [f32; 4]) {
    srv.exec(&format!(
        "INSERT INTO {coll} (id, sku, embedding) VALUES \
         ('{id}', '{sku}', ARRAY[{},{},{},{}])",
        emb[0], emb[1], emb[2], emb[3]
    ))
    .await
    .unwrap();
}

/// Nearest-neighbour `id` to `axis` on the collection's vector index (empty when
/// the index has no reachable rows).
async fn nearest(srv: &TestServer, coll: &str, axis: [f32; 4]) -> Vec<String> {
    srv.query_text(&format!(
        "SELECT id FROM {coll} \
         ORDER BY vector_distance(embedding, ARRAY[{},{},{},{}]) LIMIT 1",
        axis[0], axis[1], axis[2], axis[3]
    ))
    .await
    .unwrap()
}

/// All `id`s in the collection, sorted (base-storage scan; independent of any
/// secondary index).
async fn scan_ids(srv: &TestServer, coll: &str) -> Vec<String> {
    let mut ids = srv
        .query_text(&format!("SELECT id FROM {coll}"))
        .await
        .unwrap();
    ids.sort();
    ids
}

/// WAL-only restart: shut the server down cleanly and reopen against the same
/// data directory (no vector checkpoint) — the exact path the redo record targets.
async fn wal_only_restart(srv: TestServer) -> TestServer {
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    srv2
}

/// (1) THE CORE REGRESSION. `BEGIN; INSERT (id, embedding); COMMIT` into a
/// vector-indexed collection; WAL-only restart; the row survives a scan AND a
/// vector search near its embedding returns it — proving the in-memory HNSW was
/// rebuilt from the `TransactionRedo` record (pre-fix: document survived, vector
/// search returned empty).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_plain_insert_vector_survives_restart() {
    let srv = TestServer::start().await;
    create_vec_collection(&srv, "pv_docs", "idx_pv").await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO pv_docs (id, embedding) VALUES ('a', ARRAY[1.0, 0.0, 0.0, 0.0])")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Pre-restart: the vector is in the live HNSW.
    let pre = nearest(&srv, "pv_docs", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        pre.first().map(String::as_str),
        Some("a"),
        "PRE-RESTART x-axis search must return 'a': {pre:?}"
    );

    let srv2 = wal_only_restart(srv).await;

    assert_eq!(
        scan_ids(&srv2, "pv_docs").await,
        vec!["a".to_string()],
        "post-restart scan must return the committed row (redb durability)"
    );
    let post = nearest(&srv2, "pv_docs", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        post.first().map(String::as_str),
        Some("a"),
        "post-restart x-axis search must return 'a' — the HNSW must be rebuilt from \
         the TransactionRedo record; got {post:?} (pre-fix: empty)"
    );
}

/// (2) `BEGIN; MERGE ... WHEN NOT MATCHED THEN INSERT; COMMIT` into a
/// vector-indexed target; WAL-only restart; the merge-inserted row survives a
/// scan AND is searchable at its axis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_merge_insert_survives_restart() {
    let srv = TestServer::start().await;
    create_vec_collection(&srv, "mt_target", "idx_mt").await;
    srv.exec("CREATE COLLECTION mt_source TYPE document")
        .await
        .unwrap();
    insert_row(&srv, "mt_source", "mx", "mx", [1.0, 0.0, 0.0, 0.0]).await;
    insert_row(&srv, "mt_source", "mz", "mz", [0.0, 0.0, 1.0, 0.0]).await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec(
        "MERGE INTO mt_target t \
         USING mt_source s ON t.sku = s.sku \
         WHEN NOT MATCHED THEN INSERT (id, sku, embedding) \
             VALUES (s.id, s.sku, s.embedding)",
    )
    .await
    .unwrap();
    srv.exec("COMMIT").await.unwrap();

    let srv2 = wal_only_restart(srv).await;

    assert_eq!(
        scan_ids(&srv2, "mt_target").await,
        vec!["mx".to_string(), "mz".to_string()],
        "post-restart scan must return both merge-inserted rows (redb durability)"
    );
    let near_x = nearest(&srv2, "mt_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        near_x.first().map(String::as_str),
        Some("mx"),
        "post-restart x-axis search must return the merge-inserted 'mx': {near_x:?}"
    );
    let near_z = nearest(&srv2, "mt_target", [0.0, 0.0, 1.0, 0.0]).await;
    assert_eq!(
        near_z.first().map(String::as_str),
        Some("mz"),
        "post-restart z-axis search must return the merge-inserted 'mz': {near_z:?}"
    );
}

/// (3) `BEGIN; UPDATE t SET embedding = s.new_embedding FROM s WHERE ...; COMMIT`
/// against a vector-indexed, pre-seeded target; WAL-only restart; the updated row
/// is searchable at its NEW embedding and its OLD axis resolves to an off-axis
/// anchor — no resurrection of the pre-update vector.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_update_from_join_survives_restart() {
    let srv = TestServer::start().await;
    create_vec_collection(&srv, "uj_target", "idx_uj").await;
    srv.exec("CREATE COLLECTION uj_source TYPE document")
        .await
        .unwrap();

    // `mover` starts on the x-axis and is moved to the w-axis. `anchor_x` sits
    // just off the x-axis so it becomes the unique nearest neighbour of the
    // x-axis query once `mover` leaves — a resurrected pre-update vector
    // (cosine distance 0) would beat it.
    insert_row(&srv, "uj_target", "mover", "mover", [1.0, 0.0, 0.0, 0.0]).await;
    insert_row(&srv, "uj_target", "anchor_x", "ax", [0.85, 0.1, 0.0, 0.0]).await;
    srv.exec(
        "INSERT INTO uj_source (id, sku, new_embedding) VALUES \
         ('s_mover', 'mover', ARRAY[0.0, 0.0, 0.0, 1.0])",
    )
    .await
    .unwrap();

    srv.exec("BEGIN").await.unwrap();
    srv.exec(
        "UPDATE uj_target SET embedding = s.new_embedding \
         FROM uj_source s WHERE uj_target.sku = s.sku",
    )
    .await
    .unwrap();
    srv.exec("COMMIT").await.unwrap();

    let srv2 = wal_only_restart(srv).await;

    assert_eq!(
        scan_ids(&srv2, "uj_target").await,
        vec!["anchor_x".to_string(), "mover".to_string()],
        "post-restart scan must return both rows (redb durability)"
    );
    // NEW axis returns the moved row.
    let w = nearest(&srv2, "uj_target", [0.0, 0.0, 0.0, 1.0]).await;
    assert_eq!(
        w.first().map(String::as_str),
        Some("mover"),
        "post-restart w-axis search must return the updated 'mover': {w:?}"
    );
    // OLD axis resolves to the anchor, not the resurrected pre-update vector.
    let x = nearest(&srv2, "uj_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        x.first().map(String::as_str),
        Some("anchor_x"),
        "mover's pre-update x-axis vector must not resurrect after restart: {x:?}"
    );
}

/// (4) `BEGIN; INSERT INTO tgt SELECT * FROM src; COMMIT` into a vector-indexed
/// target; WAL-only restart; the copied rows survive a scan AND are searchable at
/// their embeddings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_insert_select_survives_restart() {
    let srv = TestServer::start().await;
    create_vec_collection(&srv, "is_target", "idx_is").await;
    srv.exec("CREATE COLLECTION is_source TYPE document")
        .await
        .unwrap();
    for (id, v) in [
        ("alpha", [1.0f32, 0.0, 0.0, 0.0]),
        ("beta", [0.0, 0.0, 0.0, 1.0]),
    ] {
        srv.exec(&format!(
            "INSERT INTO is_source (id, embedding) VALUES ('{id}', ARRAY[{},{},{},{}])",
            v[0], v[1], v[2], v[3]
        ))
        .await
        .unwrap();
    }

    srv.exec("BEGIN").await.unwrap();
    srv.client
        .simple_query("INSERT INTO is_target SELECT * FROM is_source")
        .await
        .expect("in-tx insert-select should succeed");
    srv.exec("COMMIT").await.unwrap();

    let srv2 = wal_only_restart(srv).await;

    assert_eq!(
        scan_ids(&srv2, "is_target").await,
        vec!["alpha".to_string(), "beta".to_string()],
        "post-restart scan must return both copied rows (redb durability)"
    );
    let near_a = nearest(&srv2, "is_target", [1.0, 0.0, 0.0, 0.0]).await;
    assert_eq!(
        near_a.first().map(String::as_str),
        Some("alpha"),
        "post-restart search near E1 must return the copied 'alpha': {near_a:?}"
    );
    let near_b = nearest(&srv2, "is_target", [0.0, 0.0, 0.0, 1.0]).await;
    assert_eq!(
        near_b.first().map(String::as_str),
        Some("beta"),
        "post-restart search near E4 must return the copied 'beta': {near_b:?}"
    );
}

/// All `id`s a full-text `text_match(body, term)` returns, sorted.
async fn fts_matched(srv: &TestServer, coll: &str, term: &str) -> Vec<String> {
    let mut ids = srv
        .query_text(&format!(
            "SELECT id FROM {coll} WHERE text_match(body, '{term}')"
        ))
        .await
        .unwrap();
    ids.sort();
    ids
}

/// (5) `BEGIN; INSERT (id, body); COMMIT` into an FTS-indexed collection;
/// WAL-only restart; the row survives a scan AND a full-text `text_match` still
/// returns it — proving the FTS postings were re-derived from the document `Put`
/// redo sub-record (`index_text:true`) on replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_fts_insert_survives_restart() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_docs WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("CREATE SEARCH INDEX ON fts_docs (body)")
        .await
        .unwrap();

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO fts_docs (id, body) VALUES ('e1', 'an elephant never forgets')")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Pre-restart: the term matches via the live FTS index.
    assert_eq!(
        fts_matched(&srv, "fts_docs", "elephant").await,
        vec!["e1".to_string()],
        "PRE-RESTART text_match must return the committed row"
    );

    let srv2 = wal_only_restart(srv).await;

    assert_eq!(
        scan_ids(&srv2, "fts_docs").await,
        vec!["e1".to_string()],
        "post-restart scan must return the committed row (redb durability)"
    );
    assert_eq!(
        fts_matched(&srv2, "fts_docs", "elephant").await,
        vec!["e1".to_string()],
        "post-restart text_match must return the row — FTS postings must be \
         re-derived from the TransactionRedo document Put on replay"
    );
}
