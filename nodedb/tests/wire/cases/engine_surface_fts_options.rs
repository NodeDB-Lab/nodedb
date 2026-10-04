// SPDX-License-Identifier: BUSL-1.1

//! Named query options of the text-search calls over pgwire:
//! `text_match(col, 'q', mode => 'and' | 'or', fuzzy => true | false)` and
//! the same options on `bm25_score`. Omitted options run
//! `TextSearchParams::default()`: `mode => 'or'`, `fuzzy => false`.

use crate::harness::TestServer;

/// `m1` holds both query terms, `m2` only `machine`, `m3` neither.
async fn mode_docs(srv: &TestServer, coll: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
    ))
    .await
    .unwrap();
    for (id, body) in [
        ("m1", "machine learning is everywhere"),
        ("m2", "machine shop tools"),
        ("m3", "garden flowers"),
    ] {
        srv.exec(&format!(
            "INSERT INTO {coll} {{ id: '{id}', body: '{body}' }}"
        ))
        .await
        .unwrap();
    }
}

fn ids(rows: &[Vec<String>]) -> Vec<&str> {
    rows.iter().map(|r| r[0].as_str()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mode_or_and_mode_and_return_different_rows() {
    let srv = TestServer::start().await;
    mode_docs(&srv, "fts_mode").await;

    let and_rows = srv
        .query_rows(
            "SELECT id FROM fts_mode \
             WHERE text_match(body, 'machine learning', mode => 'and') ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&and_rows),
        vec!["m1"],
        "And keeps the row with both terms"
    );

    let or_rows = srv
        .query_rows(
            "SELECT id FROM fts_mode \
             WHERE text_match(body, 'machine learning', mode => 'or') ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&or_rows),
        vec!["m1", "m2"],
        "Or keeps a row with any term"
    );

    // No option runs the default mode, `or`.
    let default_rows = srv
        .query_rows(
            "SELECT id FROM fts_mode WHERE text_match(body, 'machine learning') ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&default_rows), vec!["m1", "m2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_mode_scores_a_partial_match_only_under_or() {
    let srv = TestServer::start().await;
    mode_docs(&srv, "fts_score_mode").await;

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(body, 'machine learning', mode => 'and') AS s \
             FROM fts_score_mode ORDER BY id",
        )
        .await
        .unwrap();
    let score = |rows: &[Vec<String>], id: &str| -> f64 {
        let row = rows
            .iter()
            .find(|r| r[0] == id)
            .unwrap_or_else(|| panic!("row {id} missing: {rows:?}"));
        row[1]
            .parse()
            .unwrap_or_else(|e| panic!("score {:?}: {e}", row[1]))
    };
    assert!(score(&rows, "m1") > 0.0, "{rows:?}");
    assert_eq!(
        score(&rows, "m2"),
        0.0,
        "And does not score a partial match"
    );

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(body, 'machine learning', mode => 'or') AS s \
             FROM fts_score_mode ORDER BY id",
        )
        .await
        .unwrap();
    assert!(
        score(&rows, "m2") > 0.0,
        "Or scores a partial match: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzzy_on_matches_a_typo_and_fuzzy_off_does_not() {
    let srv = TestServer::start().await;
    mode_docs(&srv, "fts_fuzzy_opt").await;

    let off = srv
        .query_rows(
            "SELECT id FROM fts_fuzzy_opt WHERE text_match(body, 'machime', fuzzy => false)",
        )
        .await
        .unwrap();
    assert!(off.is_empty(), "no exact match for a typo: {off:?}");

    // No option runs the default, `fuzzy => false`.
    let default_rows = srv
        .query_rows("SELECT id FROM fts_fuzzy_opt WHERE text_match(body, 'machime')")
        .await
        .unwrap();
    assert!(default_rows.is_empty(), "{default_rows:?}");

    let on = srv
        .query_rows(
            "SELECT id FROM fts_fuzzy_opt \
             WHERE text_match(body, 'machime', fuzzy => true) ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&on), vec!["m1", "m2"], "fuzzy matches the typo");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_options_are_refused() {
    let srv = TestServer::start().await;
    mode_docs(&srv, "fts_bad_opt").await;

    for (sql, needle) in [
        (
            "SELECT id FROM fts_bad_opt WHERE text_match(body, 'x', boost => 2)",
            "unknown text-search option",
        ),
        (
            "SELECT id FROM fts_bad_opt WHERE text_match(body, 'x', mode = 'or')",
            "=>",
        ),
        (
            "SELECT id FROM fts_bad_opt WHERE text_match(body, 'x', 'or')",
            "third positional argument",
        ),
        (
            "SELECT id FROM fts_bad_opt WHERE text_match(body, 'x', mode => 'xor')",
            "'or' or 'and'",
        ),
    ] {
        let err = srv
            .query_text(sql)
            .await
            .expect_err("a malformed option must be refused");
        assert!(err.contains(needle), "{sql}: expected {needle:?} in {err}");
    }
}
