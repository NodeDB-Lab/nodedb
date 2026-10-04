// SPDX-License-Identifier: BUSL-1.1

//! Full-text query semantics over the wire: negation and AND matching
//! before the LIMIT, RLS folded in before ranking, a transaction's staged
//! rows ranked before the cut, `id` filters on key-only rows, redaction
//! refusal, bitemporal score scans, and TRUNCATE emptying the index.

use crate::harness::TestServer;

const PASSWORD: &str = "fts-semantics-secret-3";

fn ids(rows: &[Vec<String>]) -> Vec<&str> {
    rows.iter().map(|r| r[0].as_str()).collect()
}

async fn create(srv: &TestServer, coll: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
    ))
    .await
    .unwrap();
}

async fn insert(srv: &TestServer, coll: &str, id: &str, extra: &str) {
    srv.exec(&format!("INSERT INTO {coll} {{ id: '{id}', {extra} }}"))
        .await
        .unwrap();
}

/// Run `sql` as `user`, returning each row's first cell, or the error text.
async fn first_cells_as(srv: &TestServer, user: &str, sql: &str) -> Result<Vec<String>, String> {
    let (client, handle) = srv
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = match client.simple_query(sql).await {
        Ok(messages) => Ok(messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(0).unwrap_or("").to_string())
                }
                _ => None,
            })
            .collect()),
        Err(e) => Err(e.to_string()),
    };
    drop(client);
    handle.abort();
    result
}

async fn create_reader(srv: &TestServer, user: &str) {
    srv.exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap();
    srv.exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap();
}

/// The best `rust` rows hold `python`. Negation drops them before the cut,
/// so `LIMIT 2` still returns the two surviving rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negated_terms_are_excluded_before_the_limit() {
    let srv = TestServer::start().await;
    create(&srv, "fts_not_limit").await;
    for i in 0..4 {
        insert(
            &srv,
            "fts_not_limit",
            &format!("p{i}"),
            "body: 'rust rust rust python'",
        )
        .await;
    }
    insert(&srv, "fts_not_limit", "k1", "body: 'rust tooling'").await;
    insert(&srv, "fts_not_limit", "k2", "body: 'rust compiler'").await;

    let rows = srv
        .query_rows("SELECT id FROM fts_not_limit WHERE text_match(body, 'rust -python') LIMIT 2")
        .await
        .unwrap();
    let mut got = ids(&rows);
    got.sort_unstable();
    assert_eq!(
        got,
        vec!["k1", "k2"],
        "two rows survive the negation: {rows:?}"
    );
}

/// Many rows out-score the single AND match on one word. The AND match is
/// found under `LIMIT 3`, and the query does not fall back to OR.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_and_match_is_found_past_single_word_out_scorers() {
    let srv = TestServer::start().await;
    create(&srv, "fts_and_limit").await;
    for i in 0..30 {
        insert(
            &srv,
            "fts_and_limit",
            &format!("a{i}"),
            "body: 'alpha alpha alpha'",
        )
        .await;
        insert(
            &srv,
            "fts_and_limit",
            &format!("b{i}"),
            "body: 'bravo bravo bravo'",
        )
        .await;
    }
    insert(
        &srv,
        "fts_and_limit",
        "both",
        "body: 'alpha bravo and more words'",
    )
    .await;

    let rows = srv
        .query_rows(
            "SELECT id FROM fts_and_limit \
             WHERE text_match(body, 'alpha bravo', mode => 'and') LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(ids(&rows), vec!["both"], "only the AND match: {rows:?}");
}

/// A read policy admits half the matches. `LIMIT 3` returns three admitted
/// rows, ranked among admitted rows only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rls_restricts_matches_before_the_limit() {
    let srv = TestServer::start().await;
    let user = "fts_rls_reader";
    create(&srv, "fts_rls_limit").await;
    for i in 0..8 {
        let owner = if i % 2 == 0 { user } else { "other" };
        // The rows the policy hides hold the term more often, so they would
        // fill the limit if the policy ran after ranking.
        let body = if i % 2 == 0 {
            "widget"
        } else {
            "widget widget widget"
        };
        insert(
            &srv,
            "fts_rls_limit",
            &format!("w{i}"),
            &format!(
                "owner: '{owner}', tag: '{}', body: '{body}'",
                if i < 4 { "x" } else { "y" }
            ),
        )
        .await;
    }
    create_reader(&srv, user).await;
    srv.exec(
        "CREATE RLS POLICY fts_rls_owner ON fts_rls_limit FOR READ \
         USING (owner = $auth.username)",
    )
    .await
    .unwrap();

    let rows = first_cells_as(
        &srv,
        user,
        "SELECT id FROM fts_rls_limit WHERE text_match(body, 'widget') LIMIT 3",
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 3, "three admitted matches exist: {rows:?}");
    for id in &rows {
        assert!(
            ["w0", "w2", "w4", "w6"].contains(&id.as_str()),
            "{id} is admitted: {rows:?}"
        );
    }

    // With a residual filter too, both restrict the candidates.
    let rows = first_cells_as(
        &srv,
        user,
        "SELECT id FROM fts_rls_limit WHERE text_match(body, 'widget') AND tag = 'x' LIMIT 5",
    )
    .await
    .unwrap();
    let mut got: Vec<&str> = rows.iter().map(String::as_str).collect();
    got.sort_unstable();
    assert_eq!(got, vec!["w0", "w2"], "admitted rows with tag x: {rows:?}");
}

/// A staged update that removes the best hit leaves `LIMIT 2` full, and a
/// staged AND query keeps AND semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_rows_are_ranked_before_the_limit() {
    let srv = TestServer::start().await;
    create(&srv, "fts_txn_limit").await;
    insert(&srv, "fts_txn_limit", "top", "body: 'rust rust rust rust'").await;
    insert(&srv, "fts_txn_limit", "mid", "body: 'rust rust lang'").await;
    insert(
        &srv,
        "fts_txn_limit",
        "low",
        "body: 'rust tooling compiler words'",
    )
    .await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec("UPDATE fts_txn_limit SET body = 'golang only' WHERE id = 'top'")
        .await
        .unwrap();
    let rows = srv
        .query_rows("SELECT id FROM fts_txn_limit WHERE text_match(body, 'rust') LIMIT 2")
        .await
        .unwrap();
    let mut got = ids(&rows);
    got.sort_unstable();
    assert_eq!(got, vec!["low", "mid"], "the limit stays full: {rows:?}");

    srv.exec("INSERT INTO fts_txn_limit { id: 'one', body: 'rust alone' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_txn_limit { id: 'both', body: 'rust lang pairing' }")
        .await
        .unwrap();
    let rows = srv
        .query_rows(
            "SELECT id FROM fts_txn_limit \
             WHERE text_match(body, 'rust lang', mode => 'and') ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&rows),
        vec!["both", "mid"],
        "AND matches only: {rows:?}"
    );
    srv.exec("ROLLBACK").await.unwrap();
}

/// A row inserted without an `id` carries its identity in its key only. A
/// filter on `id` still finds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_id_filter_matches_a_key_only_row() {
    let srv = TestServer::start().await;
    create(&srv, "fts_key_id").await;
    srv.exec("INSERT INTO fts_key_id { body: 'quartz crystal' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_key_id { body: 'quartz watch' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM fts_key_id WHERE text_match(body, 'crystal')")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one crystal row: {rows:?}");
    let id = rows[0][0].clone();
    assert!(!id.is_empty(), "the row carries its identity: {rows:?}");

    let rows = srv
        .query_rows(&format!(
            "SELECT id FROM fts_key_id WHERE text_match(body, 'quartz') AND id = '{id}'"
        ))
        .await
        .unwrap();
    assert_eq!(
        ids(&rows),
        vec![id.as_str()],
        "the id filter admits it: {rows:?}"
    );
}

/// A redacted column cannot be matched or scored: both are computed over
/// the stored text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_reads_of_a_redacted_column_are_refused() {
    let srv = TestServer::start().await;
    let user = "fts_redact_reader";
    create(&srv, "fts_redact").await;
    insert(&srv, "fts_redact", "r1", "ssn: '123 456', bio: 'gardener'").await;
    create_reader(&srv, user).await;
    srv.exec(
        "CREATE REDACTION POLICY fts_mask_ssn ON fts_redact FOR ROLE readwrite (ssn MASK '***')",
    )
    .await
    .unwrap();

    for sql in [
        "SELECT id FROM fts_redact WHERE text_match(ssn, '123')",
        "SELECT id, bm25_score(ssn, '123') FROM fts_redact",
        "SELECT id FROM fts_redact WHERE text_match(*, '123')",
    ] {
        let result = first_cells_as(&srv, user, sql).await;
        assert!(result.is_err(), "{sql} must be refused: {result:?}");
    }
    let allowed = first_cells_as(
        &srv,
        user,
        "SELECT id FROM fts_redact WHERE text_match(bio, 'gardener')",
    )
    .await
    .unwrap();
    assert_eq!(allowed, vec!["r1".to_string()]);
}

/// A score scan over a bitemporal collection reads each row's current
/// version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bitemporal_score_scan_reads_current_versions() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION fts_bitemp (id STRING PRIMARY KEY, body STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO fts_bitemp (id, body) VALUES ('v1', 'amber stone')")
        .await
        .unwrap();
    srv.exec("INSERT INTO fts_bitemp (id, body) VALUES ('v2', 'granite stone')")
        .await
        .unwrap();
    srv.exec("UPDATE fts_bitemp SET body = 'amber resin' WHERE id = 'v2'")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id, bm25_score(body, 'amber') AS s FROM fts_bitemp \
             ORDER BY s DESC NULLS LAST, id",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "one row per current version: {rows:?}");
    for row in &rows {
        let score: f64 = row[1].parse().expect("both rows hold amber");
        assert!(score > 0.0, "both current versions match: {rows:?}");
    }
}

/// TRUNCATE empties the collection's inverted index and keeps it usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_empties_the_text_index() {
    let srv = TestServer::start().await;
    create(&srv, "fts_truncate").await;
    for i in 0..5 {
        insert(
            &srv,
            "fts_truncate",
            &format!("t{i}"),
            "body: 'basalt column'",
        )
        .await;
    }
    srv.exec("TRUNCATE fts_truncate").await.unwrap();

    let rows = srv
        .query_rows("SELECT id FROM fts_truncate WHERE text_match(body, 'basalt')")
        .await
        .unwrap();
    assert!(rows.is_empty(), "no text survives TRUNCATE: {rows:?}");

    insert(&srv, "fts_truncate", "n1", "body: 'basalt again'").await;
    let rows = srv
        .query_rows("SELECT id FROM fts_truncate WHERE text_match(body, 'basalt')")
        .await
        .unwrap();
    assert_eq!(
        ids(&rows),
        vec!["n1"],
        "new text indexes after TRUNCATE: {rows:?}"
    );
}
