// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for NOT-operator negation in FTS queries.
//!
//! Covers `NOT <term>` (keyword syntax) and `-<term>` (Lucene-style prefix),
//! multiple negations, NOT-only error, parenthesised-NOT error, negation of a
//! nonexistent term, and synonym interaction with negated terms.

use crate::harness::TestServer;

/// Fixture helper: creates a collection and inserts three standard documents.
///
/// Returns a `TestServer` with the collection and docs already present:
/// - d1: rust + python
/// - d2: rust + ruby
/// - d3: python + ruby
async fn make_fixture(collection: &str) -> TestServer {
    let srv = TestServer::start().await;
    srv.exec(&format!(
        "CREATE COLLECTION {collection} WITH (engine='document_schemaless')"
    ))
    .await
    .expect("create collection");
    srv.exec(&format!("CREATE SEARCH INDEX ON {collection} (body)"))
        .await
        .expect("declare the body search index");
    srv.exec(&format!(
        "INSERT INTO {collection} {{ id: 'd1', body: 'rust python programming language' }}"
    ))
    .await
    .expect("insert d1");
    srv.exec(&format!(
        "INSERT INTO {collection} {{ id: 'd2', body: 'rust ruby programming language' }}"
    ))
    .await
    .expect("insert d2");
    srv.exec(&format!(
        "INSERT INTO {collection} {{ id: 'd3', body: 'python ruby programming language' }}"
    ))
    .await
    .expect("insert d3");
    srv
}

/// Test 1: `NOT <term>` excludes documents containing the negated term.
///
/// Fixture: d1 (rust+python), d2 (rust+ruby), d3 (python+ruby).
/// Query `rust NOT python` must return d2, must not return d1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_keyword_excludes_matching_documents() {
    let srv = make_fixture("fts_not_kw").await;

    let rows = srv
        .query_rows(
            "SELECT id FROM fts_not_kw WHERE text_match(body, 'rust NOT python') ORDER BY id",
        )
        .await
        .expect("not-keyword query");

    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();

    assert!(
        ids.contains(&"d2"),
        "d2 (rust+ruby) must appear in results: {ids:?}"
    );
    assert!(
        !ids.contains(&"d1"),
        "d1 (rust+python) must be excluded: {ids:?}"
    );
    assert!(
        !ids.contains(&"d3"),
        "d3 (python+ruby) must be excluded (no rust): {ids:?}"
    );
}

/// Test 2: `-<term>` (Lucene-style prefix) has the same semantics as `NOT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dash_prefix_excludes_matching_documents() {
    let srv = make_fixture("fts_not_dash").await;

    let rows = srv
        .query_rows(
            "SELECT id FROM fts_not_dash WHERE text_match(body, 'rust -python') ORDER BY id",
        )
        .await
        .expect("dash-prefix query");

    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();

    assert!(
        ids.contains(&"d2"),
        "d2 (rust+ruby) must appear in results: {ids:?}"
    );
    assert!(
        !ids.contains(&"d1"),
        "d1 (rust+python) must be excluded: {ids:?}"
    );
}

/// Test 3: Multiple negations exclude all negated terms independently.
///
/// `rust NOT python NOT ruby` must return only documents containing rust
/// and neither python nor ruby.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_negations_exclude_all_negated_terms() {
    let srv = TestServer::start().await;
    let col = "fts_multi_not";
    srv.exec(&format!(
        "CREATE COLLECTION {col} WITH (engine='document_schemaless')"
    ))
    .await
    .expect("create collection");
    srv.exec(&format!("CREATE SEARCH INDEX ON {col} (body)"))
        .await
        .expect("declare the body search index");
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd1', body: 'rust python programming' }}"
    ))
    .await
    .expect("insert d1");
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd2', body: 'rust ruby programming' }}"
    ))
    .await
    .expect("insert d2");
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd3', body: 'rust systems programming' }}"
    ))
    .await
    .expect("insert d3");

    let rows = srv
        .query_rows(&format!(
            "SELECT id FROM {col} WHERE text_match(body, 'rust NOT python NOT ruby') ORDER BY id"
        ))
        .await
        .expect("multi-NOT query");

    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();

    assert!(
        ids.contains(&"d3"),
        "d3 (rust+systems) must appear: {ids:?}"
    );
    assert!(
        !ids.contains(&"d1"),
        "d1 (rust+python) must be excluded: {ids:?}"
    );
    assert!(
        !ids.contains(&"d2"),
        "d2 (rust+ruby) must be excluded: {ids:?}"
    );
}

/// Test 4: A NOT-only query (no positive terms) returns a typed error.
///
/// `NOT python` alone is ill-defined; the server must reject it with an error
/// containing the phrase "positive term".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_only_query_returns_error() {
    let srv = make_fixture("fts_not_only").await;

    srv.expect_error(
        "SELECT id FROM fts_not_only WHERE text_match(body, 'NOT python')",
        "positive term",
    )
    .await;
}

/// Test 5: `NOT (x OR y)` (parenthesised group) returns a typed error pointing
/// at the workaround (use flat negations instead).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_with_parentheses_returns_error() {
    let srv = make_fixture("fts_not_paren").await;

    srv.expect_error(
        "SELECT id FROM fts_not_paren WHERE text_match(body, 'rust NOT (python OR ruby)')",
        "parenthes",
    )
    .await;
}

/// Test 6: Synonym interaction — negating a synonym term also excludes
/// documents containing the synonym's expansion.
///
/// Group: `py` → {`python`, `py`}. Query `rust NOT py` should exclude
/// documents that contain either `python` or `py`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_with_synonym_excludes_expanded_terms() {
    let srv = TestServer::start().await;
    let col = "fts_not_syn";

    srv.exec(&format!(
        "CREATE COLLECTION {col} WITH (engine='document_schemaless')"
    ))
    .await
    .expect("create collection");
    srv.exec(&format!("CREATE SEARCH INDEX ON {col} (body)"))
        .await
        .expect("declare the body search index");

    // d1: contains the raw synonym abbreviation 'py'
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd1', body: 'rust py programming shorthand' }}"
    ))
    .await
    .expect("insert d1");
    // d2: contains the full term 'python'
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd2', body: 'rust python programming' }}"
    ))
    .await
    .expect("insert d2");
    // d3: contains neither synonym variant
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd3', body: 'rust systems software' }}"
    ))
    .await
    .expect("insert d3");

    // Create synonym group: querying 'py' also matches 'python' and vice versa.
    srv.exec("CREATE SYNONYM GROUP py AS ('python', 'py')")
        .await
        .expect("create synonym group");

    // Negate 'py' — should also exclude docs containing 'python'.
    let rows = srv
        .query_rows(&format!(
            "SELECT id FROM {col} WHERE text_match(body, 'rust NOT py') ORDER BY id"
        ))
        .await
        .expect("synonym-NOT query");

    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();

    assert!(
        ids.contains(&"d3"),
        "d3 (rust+systems) must appear: {ids:?}"
    );
    assert!(
        !ids.contains(&"d1"),
        "d1 (rust+py) must be excluded via synonym expansion: {ids:?}"
    );
    assert!(
        !ids.contains(&"d2"),
        "d2 (rust+python) must be excluded via synonym expansion: {ids:?}"
    );
}

/// Test 7: NOT with a nonexistent negative term returns same results as
/// the plain positive query — the negative bitmap is empty, nothing to subtract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_nonexistent_term_does_not_remove_results() {
    let srv = TestServer::start().await;
    let col = "fts_not_missing";

    srv.exec(&format!(
        "CREATE COLLECTION {col} WITH (engine='document_schemaless')"
    ))
    .await
    .expect("create collection");
    srv.exec(&format!("CREATE SEARCH INDEX ON {col} (body)"))
        .await
        .expect("declare the body search index");
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd1', body: 'rust programming' }}"
    ))
    .await
    .expect("insert d1");
    srv.exec(&format!(
        "INSERT INTO {col} {{ id: 'd2', body: 'rust systems' }}"
    ))
    .await
    .expect("insert d2");

    let plain = srv
        .query_rows(&format!(
            "SELECT id FROM {col} WHERE text_match(body, 'rust') ORDER BY id"
        ))
        .await
        .expect("plain query");

    let with_not = srv
        .query_rows(&format!(
            "SELECT id FROM {col} WHERE text_match(body, 'rust NOT nonexistentxyzterm') ORDER BY id"
        ))
        .await
        .expect("not-missing query");

    let plain_ids: Vec<&str> = plain.iter().map(|r| r[0].as_str()).collect();
    let not_ids: Vec<&str> = with_not.iter().map(|r| r[0].as_str()).collect();

    assert_eq!(
        plain_ids, not_ids,
        "NOT nonexistent-term must not remove any results: plain={plain_ids:?} not={not_ids:?}"
    );
}
