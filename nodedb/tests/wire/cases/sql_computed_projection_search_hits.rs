// SPDX-License-Identifier: BUSL-1.1

//! Computed SELECT-list columns over text, vector, sparse, and hybrid
//! searches.
//!
//! A search returns hit rows and evaluates no expression. Each computed
//! column must still carry its value, beside the search's own score, and a
//! sort on a computed alias must read it.

use crate::harness::TestServer;

fn number(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

#[tokio::test]
async fn text_match_evaluates_computed_columns() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION tx_hits WITH (engine='document_schemaless')")
        .await
        .unwrap_or_else(|e| panic!("create tx_hits: {e}"));
    srv.exec("CREATE SEARCH INDEX ON tx_hits (body)")
        .await
        .unwrap_or_else(|e| panic!("index tx_hits: {e}"));
    for (id, body, title, n) in [
        ("a", "rust lang", "Rust", 3),
        ("b", "python lang", "Python", 5),
    ] {
        srv.exec(&format!(
            "INSERT INTO tx_hits {{ id: '{id}', body: '{body}', title: '{title}', n: {n} }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    let rows = srv
        .query_rows("SELECT id, upper(title), n * 2 FROM tx_hits WHERE text_match(body, 'rust')")
        .await
        .unwrap_or_else(|e| panic!("text match with computed columns: {e}"));
    assert_eq!(
        rows,
        vec![vec!["a".to_string(), "RUST".to_string(), "6".to_string()]],
        "the matched row carries its computed columns"
    );

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(body, 'lang') AS s, upper(title) AS u FROM tx_hits \
             WHERE text_match(body, 'lang') ORDER BY u",
        )
        .await
        .unwrap_or_else(|e| panic!("text match with a score and a computed sort: {e}"));
    let ids: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(ids, ["b", "a"], "rows sort by the computed alias: {rows:?}");
    for row in &rows {
        assert!(number(&row[1]) >= 0.0, "the score stays beside it: {row:?}");
    }
    assert_eq!(rows[0][2], "PYTHON");
    assert_eq!(rows[1][2], "RUST");
}

#[tokio::test]
async fn vector_search_evaluates_computed_columns_and_its_distance() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vx_hits WITH (engine='document_schemaless')")
        .await
        .unwrap_or_else(|e| panic!("create vx_hits: {e}"));
    srv.exec("CREATE INDEX ON vx_hits (embedding)")
        .await
        .unwrap_or_else(|e| panic!("index vx_hits: {e}"));
    for (id, embedding, label) in [
        ("v1", "[1.0, 0.0, 0.0]", "cat"),
        ("v2", "[0.0, 1.0, 0.0]", "dog"),
    ] {
        srv.exec(&format!(
            "INSERT INTO vx_hits {{ id: '{id}', embedding: {embedding}, label: '{label}' }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    let rows = srv
        .query_rows(
            "SELECT id, upper(label), vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]) AS d \
             FROM vx_hits ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]) LIMIT 2",
        )
        .await
        .unwrap_or_else(|e| panic!("vector search with computed columns: {e}"));
    assert_eq!(rows.len(), 2, "both hits: {rows:?}");
    assert_eq!(rows[0][0], "v1", "the nearest hit first: {rows:?}");
    assert_eq!(
        rows[0][1], "CAT",
        "a computed column over the body: {rows:?}"
    );
    assert_eq!(
        rows[1][1], "DOG",
        "a computed column over the body: {rows:?}"
    );
    assert!(
        number(&rows[0][2]) <= number(&rows[1][2]),
        "the distance column carries each hit's distance: {rows:?}"
    );
}

#[tokio::test]
async fn sparse_search_evaluates_computed_columns_and_its_score() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE sx_hits (id TEXT PRIMARY KEY, terms SPARSEVECTOR)")
        .await
        .unwrap_or_else(|e| panic!("create sx_hits: {e}"));
    for (id, terms) in [("a", "{3: 1.0, 7: 1.0}"), ("b", "{3: 1.0}")] {
        srv.exec(&format!(
            "INSERT INTO sx_hits (id, terms) VALUES ('{id}', '{terms}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    let rows = srv
        .query_rows(
            "SELECT id, upper(id), sparse_score(terms, '{3: 1.0, 7: 0.5}') AS s FROM sx_hits \
             ORDER BY sparse_score(terms, '{3: 1.0, 7: 0.5}') DESC LIMIT 2",
        )
        .await
        .unwrap_or_else(|e| panic!("sparse search with computed columns: {e}"));
    let ids: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(ids, ["a", "b"], "ranked by dot product: {rows:?}");
    assert_eq!(rows[0][1], "A");
    assert_eq!(rows[1][1], "B");
    assert!(
        number(&rows[0][2]) >= number(&rows[1][2]),
        "the score column carries each hit's score: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_search_evaluates_computed_columns_beside_its_score() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION hx_hits")
        .await
        .unwrap_or_else(|e| panic!("create hx_hits: {e}"));
    srv.exec("CREATE VECTOR INDEX idx_hx_hits_emb ON hx_hits METRIC cosine DIM 4")
        .await
        .unwrap_or_else(|e| panic!("vector index: {e}"));
    srv.exec("CREATE SEARCH INDEX idx_hx_hits_fts ON hx_hits FIELDS content ANALYZER 'standard'")
        .await
        .unwrap_or_else(|e| panic!("search index: {e}"));
    for (id, content, embedding) in [
        ("a", "consensus algorithm", "ARRAY[0.1, 0.2, 0.3, 0.4]"),
        ("b", "distributed consensus", "ARRAY[0.2, 0.3, 0.4, 0.5]"),
    ] {
        srv.exec(&format!(
            "INSERT INTO hx_hits (id, content, embedding) VALUES ('{id}', '{content}', {embedding})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    let rows = srv
        .query_rows(
            "SELECT id, upper(id) AS uid, \
                    rrf_score(\
                      vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]), \
                      bm25_score(content, 'consensus')\
                    ) AS score \
             FROM hx_hits ORDER BY score DESC LIMIT 5",
        )
        .await
        .unwrap_or_else(|e| panic!("hybrid search with a computed column: {e}"));
    assert!(!rows.is_empty(), "fused rows expected");
    for row in &rows {
        assert_eq!(
            row[1],
            row[0].to_uppercase(),
            "the computed column: {row:?}"
        );
        assert!(
            number(&row[2]) > 0.0,
            "the RRF score stays beside it: {row:?}"
        );
    }
}
