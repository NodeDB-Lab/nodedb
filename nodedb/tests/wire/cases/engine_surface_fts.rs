// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Full-Text Search overlay: `text_match`
//! search, `bm25_score` projection, phrase search, fuzzy matching, and
//! AND/OR query combinations.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_text_match_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_basic WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_basic { id: 'd1', body: 'The quick brown fox' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_basic { id: 'd2', body: 'A lazy dog sleeps' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_basic { id: 'd3', body: 'Fox hunting is banned' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM fts_basic WHERE text_match(body, 'fox') ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"d1"), "d1 should match 'fox': {ids:?}");
    assert!(ids.contains(&"d3"), "d3 should match 'fox': {ids:?}");
    assert!(!ids.contains(&"d2"), "d2 should not match 'fox': {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_projection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_bm25 WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_bm25 { id: 'b1', content: 'database systems architecture' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_bm25 { id: 'b2', content: 'cooking recipes and food' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_bm25 { id: 'b3', content: 'distributed database performance' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_bm25 { id: 'b4', other: 'no content field' }")
        .await
        .unwrap();

    // A match scores its BM25 value. A row the `content` index holds that
    // does not match scores 0. A row with no `content` text scores NULL.
    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(content, 'database') AS score \
             FROM fts_bm25 \
             ORDER BY score DESC NULLS LAST, id",
        )
        .await
        .unwrap();
    let mut top = ids(&rows[..2]);
    top.sort_unstable();
    assert_eq!(top, vec!["b1", "b3"], "matches rank first: {rows:?}");
    for row in &rows[..2] {
        let score: f64 = row[1].parse().expect("a match carries a score");
        assert!(score > 0.0, "a match scores above 0: {rows:?}");
    }
    assert_eq!(rows[2][0], "b2", "the held miss follows: {rows:?}");
    let miss: f64 = rows[2][1].parse().expect("a held miss carries a score");
    assert_eq!(miss, 0.0, "a held miss scores 0: {rows:?}");
    assert_eq!(rows[3][0], "b4", "the unheld row is last: {rows:?}");
    assert!(rows[3][1].is_empty(), "an unheld row scores NULL: {rows:?}");

    // With no NULLS clause a DESC key places NULL first, as every ORDER BY
    // key does.
    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(content, 'database') AS score \
             FROM fts_bm25 \
             ORDER BY score DESC",
        )
        .await
        .unwrap();
    assert_eq!(rows[0][0], "b4", "NULL sorts first under DESC: {rows:?}");
    assert!(rows[0][1].is_empty(), "b4 scores NULL: {rows:?}");

    // ASC places NULL last.
    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(content, 'database') AS score \
             FROM fts_bm25 \
             ORDER BY score",
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0][0], "b2",
        "the 0 score sorts first under ASC: {rows:?}"
    );
    assert_eq!(rows[3][0], "b4", "NULL sorts last under ASC: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phrase_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_phrase WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_phrase { id: 'p1', text: 'quick brown fox jumps' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_phrase { id: 'p2', text: 'brown quick fox' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_phrase { id: 'p3', text: 'the quick brown fox' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM fts_phrase WHERE text_match(text, '\"quick brown\"') ORDER BY id",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"p1"), "p1 should match phrase: {ids:?}");
    assert!(ids.contains(&"p3"), "p3 should match phrase: {ids:?}");
    assert!(!ids.contains(&"p2"), "p2 should not match phrase: {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzzy_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_fuzzy WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_fuzzy { id: 'f1', body: 'distributed database systems' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM fts_fuzzy WHERE text_match(body, 'databse', fuzzy => true)")
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"f1"), "fuzzy should match with typo: {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn and_or_query_combinations() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_logic WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO fts_logic { id: 'l1', body: 'rust programming language' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_logic { id: 'l2', body: 'python programming tutorial' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_logic { id: 'l3', body: 'rust systems software' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM fts_logic WHERE text_match(body, 'rust programming') ORDER BY id",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"l1"), "l1 should match: {ids:?}");
}

/// Two documents whose `rust` sits in different fields.
async fn crossed_docs(srv: &TestServer, coll: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {coll} {{ id: 'd1', title: 'rust guide', body: 'an introduction' }}"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {coll} {{ id: 'd2', title: 'cooking', body: 'the rust belt' }}"
    ))
    .await
    .unwrap();
}

fn ids(rows: &[Vec<String>]) -> Vec<&str> {
    rows.iter().map(|r| r[0].as_str()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_match_reads_only_the_named_field() {
    let srv = TestServer::start().await;
    crossed_docs(&srv, "fts_scoped").await;

    let rows = srv
        .query_rows("SELECT id FROM fts_scoped WHERE text_match(title, 'rust') ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["d1"], "title scope must ignore body text");

    let rows = srv
        .query_rows("SELECT id FROM fts_scoped WHERE text_match(body, 'rust') ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["d2"], "body scope must ignore title text");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_match_star_reads_the_whole_document() {
    let srv = TestServer::start().await;
    crossed_docs(&srv, "fts_star").await;

    let rows = srv
        .query_rows("SELECT id FROM fts_star WHERE text_match(*, 'rust') ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["d1", "d2"]);
}

/// Rows for the score-column tests: `a` matches both, `b` only the body,
/// `c` only the title.
async fn score_docs(srv: &TestServer, coll: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
    ))
    .await
    .unwrap();
    for (id, title, body) in [
        ("a", "xenon lamp", "yellow light"),
        ("b", "plain", "yellow paint"),
        ("c", "xenon gas", "nothing here"),
    ] {
        srv.exec(&format!(
            "INSERT INTO {coll} {{ id: '{id}', title: '{title}', body: '{body}' }}"
        ))
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_score_on_another_field_is_zero_where_its_text_does_not_match() {
    let srv = TestServer::start().await;
    score_docs(&srv, "fts_cross_score").await;

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(title, 'xenon') AS s FROM fts_cross_score \
             WHERE text_match(body, 'yellow') ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["a", "b"], "only body matches: {rows:?}");
    let a_score: f64 = rows[0][1].parse().expect("a holds xenon in its title");
    assert!(a_score > 0.0, "a's title score must be positive: {rows:?}");
    let b_score: f64 = rows[1][1].parse().expect("b holds a title");
    assert_eq!(b_score, 0.0, "b's title lacks xenon: {rows:?}");
}

/// A score column beside the match it scores keeps the match shape: only
/// matching rows return.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_score_beside_its_own_match_returns_only_matches() {
    let srv = TestServer::start().await;
    score_docs(&srv, "fts_same_score").await;

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(body, 'yellow') AS s FROM fts_same_score \
             WHERE text_match(body, 'yellow') ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["a", "b"], "c must not return: {rows:?}");
    for row in &rows {
        assert!(
            !row[1].is_empty(),
            "every match carries its score: {rows:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_residual_filter_restricts_matches_before_the_limit() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_filtered WITH (engine='document_schemaless')")
        .await
        .unwrap();
    for i in 0..10 {
        let tag = if i % 3 == 0 { "a" } else { "b" };
        srv.exec(&format!(
            "INSERT INTO fts_filtered {{ id: 'w{i}', tag: '{tag}', body: 'widget number {i}' }}"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows(
            "SELECT id, tag FROM fts_filtered \
             WHERE text_match(body, 'widget') AND tag = 'a' LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "4 tagged matches exist, LIMIT 3: {rows:?}");
    assert!(
        rows.iter().all(|r| r[1] == "a"),
        "every row is tag a: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_match_with_no_limit_returns_every_match() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_unbounded WITH (engine='document_schemaless')")
        .await
        .unwrap();
    for chunk in 0..3 {
        let values: Vec<String> = (0..500)
            .map(|i| {
                let n = chunk * 500 + i;
                format!("('m{n}', 'marker row {n}')")
            })
            .collect();
        srv.exec(&format!(
            "INSERT INTO fts_unbounded (id, body) VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT id FROM fts_unbounded WHERE text_match(body, 'marker')")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1500, "no LIMIT must not cap the matches");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_score_scan_keeps_its_filter_order_and_limit() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fts_scan_tail WITH (engine='document_schemaless')")
        .await
        .unwrap();
    for (id, tag, body) in [
        ("k1", "b", "xylophone music"),
        ("k2", "a", "plain text"),
        ("k3", "a", "xylophone lesson"),
        ("k4", "a", "another row"),
    ] {
        srv.exec(&format!(
            "INSERT INTO fts_scan_tail {{ id: '{id}', tag: '{tag}', body: '{body}' }}"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(body, 'xylophone') FROM fts_scan_tail \
             WHERE tag = 'a' ORDER BY id LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&rows),
        vec!["k2", "k3"],
        "first two tagged rows: {rows:?}"
    );
    let k2: f64 = rows[0][1].parse().expect("k2 holds body text");
    assert_eq!(k2, 0.0, "k2 lacks the term: {rows:?}");
    let k3: f64 = rows[1][1].parse().expect("k3 holds body text");
    assert!(k3 > 0.0, "k3 holds the term: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_field_no_document_holds_is_an_undefined_column() {
    let srv = TestServer::start().await;
    crossed_docs(&srv, "fts_ghost").await;

    let err = srv
        .query_text("SELECT id FROM fts_ghost WHERE text_match(ghost, 'rust')")
        .await
        .expect_err("a field no document holds must be refused");
    assert!(err.contains("42703"), "expected 42703: {err}");
    assert!(err.contains("ghost"), "the message names the field: {err}");
    assert!(
        err.contains("fts_ghost"),
        "the message names the collection: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_literal_column_argument_is_a_datatype_mismatch() {
    let srv = TestServer::start().await;
    crossed_docs(&srv, "fts_literal").await;

    let err = srv
        .query_text("SELECT id FROM fts_literal WHERE text_match('lit', 'rust')")
        .await
        .expect_err("a literal names no column");
    assert!(err.contains("42804"), "expected 42804: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_order_by_score_with_a_foreign_qualifier_is_an_unknown_table() {
    let srv = TestServer::start().await;
    crossed_docs(&srv, "fts_foreign").await;

    let err = srv
        .query_text("SELECT id FROM fts_foreign ORDER BY bm25_score(zz.body, 'rust')")
        .await
        .expect_err("zz names no relation");
    assert!(err.contains("zz"), "the error names the qualifier: {err}");
    assert!(err.contains("42P01"), "expected undefined_table: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_score_desc_lists_matches_first() {
    let srv = TestServer::start().await;
    score_docs(&srv, "fts_rank_order").await;
    srv.exec("INSERT INTO fts_rank_order { id: 'd', body: 'no title here' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_rank_order { id: 'e', title: 'xenon xenon', body: 'bright' }")
        .await
        .unwrap();

    // `e` holds the term twice in a title as short as the others, so it
    // scores highest. `d` has no title, so it scores NULL: `NULLS LAST`
    // keeps it behind the matches under DESC.
    let rows = srv
        .query_rows(
            "SELECT id FROM fts_rank_order \
             ORDER BY bm25_score(title, 'xenon') DESC NULLS LAST LIMIT 3",
        )
        .await
        .unwrap();
    let got = ids(&rows);
    assert_eq!(got.len(), 3, "three xenon titles exist: {rows:?}");
    assert_eq!(got[0], "e", "the strongest match ranks first: {rows:?}");
    assert!(
        got[1..].iter().all(|id| *id == "a" || *id == "c"),
        "the tied matches follow, before the unmatched rows: {rows:?}"
    );
    assert_ne!(got[1], got[2], "each tied match appears once: {rows:?}");
}
