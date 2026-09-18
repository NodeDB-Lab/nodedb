// SPDX-License-Identifier: BUSL-1.1

//! Live (no-restart) regression coverage for `INSERT INTO <target> SELECT ...
//! FROM <source>`.
//!
//! `INSERT ... SELECT` historically copied each source row into the target on
//! the DATA PLANE, REUSING the source row's surrogate and registering NO target
//! catalog binding (surrogate registration is Control-Plane-only). The copied
//! row was therefore:
//!   * cross-engine-UNRESOLVABLE — a vector / FTS hit on the target carried the
//!     source's surrogate, which has no binding in the target collection, so the
//!     hit resolved to a raw internal id (or nothing) instead of the target
//!     row's primary key; and
//!   * a violation of the global-uniqueness invariant — the target row shared
//!     the source row's surrogate instead of owning a fresh one.
//!
//! The fix orchestrates the copy on the Control Plane: the source is scanned,
//! each target row is assigned its OWN fresh, registered surrogate, and the rows
//! are written via an atomic `BatchInsert` that maintains every cross-engine
//! index. All three assertions below fail on the pre-fix code.

use crate::harness::TestServer;

/// A copied row must be resolvable through the target's vector index, its FTS
/// index, and a plain scan — proving the target row carries its OWN registered
/// surrogate (not the source's, which has no target binding).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_row_visible_to_vector_fts_and_scan() {
    let server = TestServer::start().await;

    // Target: an initially-empty document collection carrying BOTH a secondary
    // vector index and an FTS search index.
    server.exec("CREATE COLLECTION isc_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_isc_target_emb ON isc_target METRIC cosine DIM 4")
        .await
        .unwrap();
    server
        .exec(
            "CREATE SEARCH INDEX idx_isc_target_fts ON isc_target FIELDS body ANALYZER 'standard'",
        )
        .await
        .unwrap();

    // Source: two rows, each with text + an embedding.
    server.exec("CREATE COLLECTION isc_source").await.unwrap();
    for (id, body, v) in [
        (
            "alpha",
            "quantum computing breakthrough",
            [1.0f32, 0.0, 0.0, 0.0],
        ),
        ("beta", "photosynthesis in plants", [0.0, 0.0, 0.0, 1.0]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO isc_source (id, body, embedding) VALUES \
                 ('{id}', '{body}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // Copy every source row into the target.
    server
        .exec("INSERT INTO isc_target SELECT * FROM isc_source")
        .await
        .unwrap();

    // (c) Normal scan sees both copied rows.
    let scanned = server
        .query_text("SELECT id FROM isc_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec!["alpha".to_string(), "beta".to_string()],
        "scan must return both copied rows; got {scanned:?}"
    );

    // (a) Vector search near E1 resolves the copied `alpha` to its target PK.
    // Pre-fix the row carried the source surrogate (no target binding), so the
    // hit did not resolve to `alpha`.
    let near_e1 = server
        .query_text(
            "SELECT id FROM isc_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("alpha"),
        "vector search near E1 must resolve the copied 'alpha'; got {near_e1:?} \
         (pre-fix: source surrogate unbound in target → unresolvable)"
    );

    let near_e2 = server
        .query_text(
            "SELECT id FROM isc_target \
             WHERE embedding <-> ARRAY[0.0, 0.0, 0.0, 1.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("beta"),
        "vector search near E2 must resolve the copied 'beta'; got {near_e2:?}"
    );

    // (b) FTS text search resolves the copied `alpha` by its body text.
    let fts = server
        .query_text("SELECT id FROM isc_target WHERE text_match(body, 'quantum')")
        .await
        .unwrap();
    assert_eq!(
        fts.iter().map(String::as_str).collect::<Vec<_>>(),
        vec!["alpha"],
        "FTS search for 'quantum' must resolve the copied 'alpha'; got {fts:?} \
         (pre-fix: source surrogate unbound in target → unresolvable)"
    );
}

/// The copied target row must carry a FRESH identity, distinct from the source
/// row: both collections are independently vector-searchable and each resolves
/// to its OWN primary key. If the target reused the source's surrogate, the
/// target hit would fail to resolve (no target binding) — pre-fix behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_target_row_has_fresh_registered_identity() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION isf_source").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_isf_source_emb ON isf_source METRIC cosine DIM 4")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION isf_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_isf_target_emb ON isf_target METRIC cosine DIM 4")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO isf_source (id, embedding) VALUES \
             ('gamma', ARRAY[1.0, 0.0, 0.0, 0.0])",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO isf_target SELECT * FROM isf_source")
        .await
        .unwrap();

    // The source row still resolves to its own id in the source collection.
    let src_hit = server
        .query_text(
            "SELECT id FROM isf_source \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        src_hit.first().map(String::as_str),
        Some("gamma"),
        "source row must resolve to its own id; got {src_hit:?}"
    );

    // The copied target row independently resolves to its own id in the target
    // collection — only possible if it owns a fresh surrogate registered under
    // (isf_target, 'gamma'). A reused source surrogate has no target binding.
    let tgt_hit = server
        .query_text(
            "SELECT id FROM isf_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        tgt_hit.first().map(String::as_str),
        Some("gamma"),
        "copied target row must resolve to its own id via a fresh registered \
         surrogate; got {tgt_hit:?} (pre-fix: reused source surrogate → unresolvable)"
    );
}

/// A page that would violate a target constraint (a duplicate value on a UNIQUE
/// secondary index) must abort atomically, leaving the target unchanged: none
/// of the copied rows land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_constraint_violation_is_atomic() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION isa_source").await.unwrap();
    server.exec("CREATE COLLECTION isa_target").await.unwrap();
    server
        .exec("CREATE UNIQUE INDEX idx_isa_target_code ON isa_target (code)")
        .await
        .unwrap();

    // Pre-existing target row holding code = 'X'.
    server
        .exec("INSERT INTO isa_target (id, code) VALUES ('pre', 'X')")
        .await
        .unwrap();

    // Source rows: one is fine, one collides with the pre-existing 'X'.
    server
        .exec("INSERT INTO isa_source (id, code) VALUES ('s1', 'Y')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO isa_source (id, code) VALUES ('s2', 'X')")
        .await
        .unwrap();

    // The copy must fail on the UNIQUE violation.
    let result = server
        .exec("INSERT INTO isa_target SELECT * FROM isa_source")
        .await;
    assert!(
        result.is_err(),
        "INSERT ... SELECT that violates a UNIQUE constraint must error, not \
         silently partially insert"
    );

    // The target is unchanged: only the pre-existing row remains — the
    // non-conflicting 's1' did NOT land (all-or-nothing within the page).
    let ids = server
        .query_text("SELECT id FROM isa_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        ids,
        vec!["pre".to_string()],
        "a violating INSERT ... SELECT must leave the target unchanged; got {ids:?}"
    );
}

/// An autocommit `INSERT ... SELECT` whose SOURCE is a STRICT document
/// collection must normalize each scanned Binary Tuple to msgpack before the
/// copy: rows persist and resolve through the target's vector index to their own
/// PKs. Without normalization the strict source's raw tuple bytes are copied
/// through unchanged and PK extraction / vector indexing both fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_from_strict_source_normalizes_and_resolves() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION iss_ac_source \
             (id STRING NOT NULL PRIMARY KEY, embedding VECTOR(4)) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for (id, v) in [
        ("alpha", [1.0f32, 0.0, 0.0, 0.0]),
        ("beta", [0.0, 0.0, 0.0, 1.0]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO iss_ac_source (id, embedding) VALUES \
                 ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    server
        .exec("CREATE COLLECTION iss_ac_target")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_iss_ac_target_emb ON iss_ac_target METRIC cosine DIM 4")
        .await
        .unwrap();

    server
        .exec("INSERT INTO iss_ac_target SELECT * FROM iss_ac_source")
        .await
        .unwrap();

    let scanned = server
        .query_text("SELECT id FROM iss_ac_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec!["alpha".to_string(), "beta".to_string()],
        "autocommit copy from a strict source must persist both rows; got {scanned:?}"
    );

    let near_e1 = server
        .query_text(
            "SELECT id FROM iss_ac_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("alpha"),
        "vector search must resolve the copied strict-source 'alpha'; got {near_e1:?}"
    );
}

/// A kv-engine source keeps nothing in the document store, so the document
/// materializer read zero rows and the statement reported `INSERT 0 0`. The
/// route scans the kv source through its own engine, and the copied rows must
/// carry expression cells with the same semantics as a document source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_copies_a_kv_source_through_its_own_engine() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION isk_src (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO isk_src (id, v) VALUES (1, 'hello')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO isk_src (id, v) VALUES (2, 'world')")
        .await
        .unwrap();

    server.exec("CREATE COLLECTION isk_dst").await.unwrap();
    server
        .exec("INSERT INTO isk_dst (id, v) SELECT id, upper(v) FROM isk_src")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, v FROM isk_dst ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "HELLO".to_string()],
            vec!["2".to_string(), "WORLD".to_string()],
        ],
        "both rows must copy, with expression cells evaluated: {rows:?}"
    );
}

/// A source engine with no `INSERT ... SELECT` materializer is refused by name:
/// scanning it with the document materializer would copy nothing and report
/// `INSERT 0 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_refuses_a_source_it_cannot_scan() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION isk_nosrc_src (id TEXT PRIMARY KEY) WITH (engine = 'columnar')")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION isk_nosrc_dst")
        .await
        .unwrap();

    server
        .expect_error(
            "INSERT INTO isk_nosrc_dst SELECT * FROM isk_nosrc_src",
            "Columnar",
        )
        .await;
}

/// A kv collection whose primary key column is `key` stores the key outside the
/// row body. The copy must still carry it: a NULL key column is silent data
/// loss, and the target's own key would be minted from the wrong column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_copies_a_kv_key_column() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION isk_key_src (key TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO isk_key_src (key, v) VALUES ('k1', 'hello')")
        .await
        .unwrap();

    server.exec("CREATE COLLECTION isk_key_dst").await.unwrap();
    server
        .exec("INSERT INTO isk_key_dst (key, v) SELECT key, v FROM isk_key_src")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT key, v FROM isk_key_dst")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![vec!["k1".to_string(), "hello".to_string()]],
        "the copied key column must carry the source key: {rows:?}"
    );
}
