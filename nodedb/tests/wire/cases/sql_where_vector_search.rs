// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for vector-search triggers in WHERE clauses.
//!
//! `try_extract_where_search` matches `TextMatch` and the spatial predicates
//! but falls through silently on `VectorSearch`, `MultiVectorSearch`, and
//! `HybridSearch`. The rewritten `vector_distance(...)` call then survives
//! into the scan filter as a non-boolean (float) predicate, which the
//! evaluator silently rejects — every documented `WHERE embedding <-> $q`
//! shape returns zero rows even though the index is populated and reachable
//! via SEARCH and via `vector_distance` in the SELECT list.
//!
//! The docs (`docs/full-text-search.md`, `docs/graph.md`,
//! `docs/ai/rag-pipelines.md`, `docs/ai/multi-modal-search.md`,
//! `docs/ai/agent-memory.md`, ...) all advertise the canonical
//! `WHERE embedding <-> $query_vector LIMIT N` shape as a vector search.
//! These tests pin that contract for every distance operator the
//! preprocessor rewrites: `<->` (L2), `<=>` (cosine), `<#>` (inner product).

use crate::harness::TestServer;

async fn create_vector_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} METRIC cosine DIM 4"
        ))
        .await
        .unwrap();
    for (id, v) in [
        ("r0", [0.10f32, 0.20, 0.30, 0.40]),
        ("r1", [0.11, 0.21, 0.31, 0.41]),
        ("r2", [0.90, 0.80, 0.70, 0.60]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, embedding) VALUES \
                 ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrow_distance_in_where_returns_nearest_rows() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_arrow").await;

    let rows = server
        .query_text(
            "SELECT id FROM vec_arrow \
             WHERE embedding <-> ARRAY[0.1, 0.2, 0.3, 0.4] \
             LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "WHERE embedding <-> qvec LIMIT 2 must trigger a vector top-K, not a scalar predicate that silently matches no rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cosine_distance_in_where_returns_nearest_rows() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_cos").await;

    let rows = server
        .query_text(
            "SELECT id FROM vec_cos \
             WHERE embedding <=> ARRAY[0.1, 0.2, 0.3, 0.4] \
             LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "WHERE embedding <=> qvec LIMIT 2 must trigger a cosine vector search"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inner_product_distance_in_where_returns_nearest_rows() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_ip").await;

    let rows = server
        .query_text(
            "SELECT id FROM vec_ip \
             WHERE embedding <#> ARRAY[0.1, 0.2, 0.3, 0.4] \
             LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "WHERE embedding <#> qvec LIMIT 2 must trigger an inner-product vector search"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_distance_function_in_where_returns_nearest_rows() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_func").await;

    // The preprocessor rewrites `<->` to `vector_distance(...)`. The
    // user-written form must behave identically — both flow through
    // `try_extract_where_search` with the same `SearchTrigger::VectorSearch`
    // tag and must not fall through to a scalar match-none.
    let rows = server
        .query_text(
            "SELECT id FROM vec_func \
             WHERE vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]) \
             LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "WHERE vector_distance(field, qvec) LIMIT 2 must trigger a vector top-K"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrow_distance_with_and_filter_in_where_returns_nearest_rows() {
    // Pattern from docs/full-text-search.md and docs/ai/rag-pipelines.md:
    //   WHERE other_predicate AND embedding <-> $q LIMIT N
    // The AND branch in `try_extract_where_search` recurses on both sides,
    // so a vector trigger sitting next to a non-search predicate must
    // still be detected — and the non-search predicate must be carried
    // through as a scan filter, not silently dropped.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vec_filter").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vec_filter_emb ON vec_filter METRIC cosine DIM 4")
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO vec_filter (id, tenant, embedding) \
             VALUES ('a', 't1', ARRAY[0.10, 0.20, 0.30, 0.40])",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO vec_filter (id, tenant, embedding) \
             VALUES ('b', 't1', ARRAY[0.11, 0.21, 0.31, 0.41])",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO vec_filter (id, tenant, embedding) \
             VALUES ('c', 't2', ARRAY[0.10, 0.20, 0.30, 0.40])",
        )
        .await
        .unwrap();

    // Baseline: same vector search WITHOUT the tenant predicate must return
    // all 3 inserted rows (LIMIT 5, only 3 candidates).
    let unfiltered = server
        .query_text(
            "SELECT id FROM vec_filter \
             WHERE embedding <-> ARRAY[0.1, 0.2, 0.3, 0.4] \
             LIMIT 5",
        )
        .await
        .unwrap();
    assert_eq!(
        unfiltered.len(),
        3,
        "baseline (no tenant filter) must return all 3 inserted rows; got {unfiltered:?}"
    );

    // With AND-sibling tenant filter, the t2 row must be excluded — only the
    // 2 t1 rows survive the filter even though all 3 are within `LIMIT 5`.
    let filtered = server
        .query_text(
            "SELECT id FROM vec_filter \
             WHERE tenant = 't1' AND embedding <-> ARRAY[0.1, 0.2, 0.3, 0.4] \
             LIMIT 5",
        )
        .await
        .unwrap();
    assert_eq!(
        filtered.len(),
        2,
        "tenant='t1' AND embedding <-> q must filter the t2 row out; \
         3 rows means the AND-sibling filter was silently dropped; got {filtered:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrow_distance_in_where_does_not_silently_match_none_for_delete() {
    // Silent-failure regression guard: if the WHERE-clause vector trigger
    // were ever "fixed" by treating the float predicate as truthy (≠ 0),
    // a `DELETE WHERE embedding <-> q` would wipe every row whose distance
    // is non-zero — i.e. nearly the entire table. A correct fix turns the
    // predicate into a top-K, and DELETE on a top-K WHERE is currently
    // expected to either return a typed error or scope to the K matched
    // rows. Either way, a 3-row table must NOT be left empty.
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_delete").await;

    // Best-effort DELETE; either rejection or scoped delete is acceptable,
    // but a silent table-wipe is not.
    let _ = server
        .exec(
            "DELETE FROM vec_delete \
             WHERE embedding <-> ARRAY[0.1, 0.2, 0.3, 0.4] \
             LIMIT 1",
        )
        .await;

    let remaining = server
        .query_text("SELECT id FROM vec_delete")
        .await
        .unwrap();
    assert!(
        !remaining.is_empty(),
        "DELETE with a vector-search WHERE must not silently wipe the table"
    );
    assert!(
        remaining.len() >= 2,
        "at most one row should be deleted; got {} rows remaining",
        remaining.len()
    );
}

const RLS_PASSWORD: &str = "vec-where-rls-secret-7";

/// Run `sql` as `user` and return the first column of each delivered row.
async fn ids_as(server: &TestServer, user: &str, sql: &str) -> Vec<String> {
    let (client, handle) = server
        .connect_as(user, RLS_PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{user} runs {sql}: {e}"));
    let mut out = Vec::new();
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            out.push(row.get(0).unwrap_or("").to_string());
        }
    }
    drop(client);
    handle.abort();
    out
}

/// The statement's own WHERE predicate and a read policy share the vector
/// search's post-filter slot. The policy joins the predicate and never
/// replaces it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_where_filter_survives_a_read_policy() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vec_rls").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vec_rls_emb ON vec_rls METRIC cosine DIM 4")
        .await
        .unwrap();
    for (id, owner, tag, v) in [
        ("m1", "vec_rls_reader", "a", "0.10, 0.20, 0.30, 0.40"),
        ("m2", "vec_rls_reader", "b", "0.11, 0.21, 0.31, 0.41"),
        ("m3", "alice", "a", "0.12, 0.22, 0.32, 0.42"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO vec_rls (id, owner, tag, embedding) \
                 VALUES ('{id}', '{owner}', '{tag}', ARRAY[{v}])"
            ))
            .await
            .unwrap();
    }
    server
        .exec(&format!(
            "CREATE USER vec_rls_reader PASSWORD '{RLS_PASSWORD}'"
        ))
        .await
        .unwrap();
    server
        .exec("GRANT ROLE readwrite TO vec_rls_reader")
        .await
        .unwrap();
    server
        .exec(
            "CREATE RLS POLICY vec_rls_owner ON vec_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .unwrap();

    let ids = ids_as(
        &server,
        "vec_rls_reader",
        "SELECT id FROM vec_rls \
         WHERE tag = 'a' AND embedding <-> ARRAY[0.1, 0.2, 0.3, 0.4] LIMIT 10",
    )
    .await;
    assert_eq!(
        ids,
        vec!["m1".to_string()],
        "both the tag filter and the owner policy must hold"
    );
}
