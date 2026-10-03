// SPDX-License-Identifier: BUSL-1.1

//! Full-text search coverage for strict-doc collections.
//!
//! A search index attached to a strict-doc collection must observe writes
//! the same way it observes writes to a schemaless collection: every
//! INSERT must populate the inverted index for the indexed text fields,
//! so `bm25_score(field, term)` returns a non-NULL score for rows whose
//! `field` contains `term`, regardless of underlying storage mode.
//!
//! The class of bug captured here is "the strict-doc INSERT path and the
//! shared FTS / stats / aggregate-cache side-effect block silently
//! diverge" — a format-detection helper that does not auto-detect Binary
//! Tuple bytes, called inside an `if let Some(..)` guard, drops every
//! side-effect for strict rows without surfacing an error.
//!
//! Tests assert the spec via the `bm25_score` projection so the
//! silent-skip failure mode appears as a literal NULL cell on rows whose
//! field contains the term — directly visible at the wire, no count
//! aggregation in the path.
//!
//! `CREATE FULLTEXT INDEX` is the documented keyword alias of
//! `CREATE SEARCH INDEX`; both keywords must populate the same indexer, and
//! both must read the same statement the same way — the two spellings of the
//! column list, the analyzer name, and any token neither understands.

use crate::harness::TestServer;

const SCHEMALESS_DDL: &str = "CREATE COLLECTION docs_schemaless";

const STRICT_DDL: &str = "CREATE COLLECTION docs_strict TYPE DOCUMENT STRICT (\
     id STRING PRIMARY KEY,\
     content STRING\
   )";

async fn seed_three(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, content) VALUES \
             ('r0', 'consensus algorithm distributed'), \
             ('r1', 'consensus memory replication'), \
             ('r2', 'cats and dogs')"
        ))
        .await
        .unwrap();
}

/// Pull `(id, bm25_score)` from each row. NULL/empty score → `None`.
fn id_score_pairs(rows: &[Vec<String>]) -> Vec<(String, Option<f64>)> {
    rows.iter()
        .map(|r| {
            let id = r[0].trim().to_string();
            let cell = r[1].trim();
            let score = if cell.is_empty() {
                None
            } else {
                cell.parse::<f64>().ok()
            };
            (id, score)
        })
        .collect()
}

fn pair<'a>(pairs: &'a [(String, Option<f64>)], id: &str) -> &'a (String, Option<f64>) {
    pairs
        .iter()
        .find(|(i, _)| i == id)
        .unwrap_or_else(|| panic!("row {id} missing from result {pairs:?}"))
}

fn assert_term_rows_scored(pairs: &[(String, Option<f64>)], context: &str) {
    // Spec: rows whose `content` contains 'consensus' (r0, r1) must have
    // a positive numeric bm25_score; the row that does not (r2) is allowed
    // to be NULL or zero — only the term-bearing rows are load-bearing.
    for id in ["r0", "r1"] {
        let (_, score) = pair(pairs, id);
        assert!(
            score.is_some(),
            "[{context}] bm25_score for row {id} was NULL — \
             the inverted index was never populated for this row. \
             This is the silent-skip class: the FTS write site's \
             format-detection guard failed and dropped the row."
        );
        let s = score.unwrap();
        assert!(
            s > 0.0,
            "[{context}] bm25_score for row {id} must be positive when \
             the term occurs in `content`; got {s}"
        );
    }
}

// ── 1. bm25_score on strict-doc must return non-NULL for indexed rows ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_strict_returns_non_null_for_indexed_rows() {
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .exec(
            "CREATE SEARCH INDEX idx_strict_bm25 ON docs_strict FIELDS content ANALYZER 'standard'",
        )
        .await
        .unwrap();
    seed_three(&server, "docs_strict").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score projection must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / standard analyzer");
}

// ── 2. Schemaless control: same INSERTs / index produce non-NULL scores ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_schemaless_control_returns_non_null_for_indexed_rows() {
    // Control case: confirms the bug class is strict-specific. If this
    // test fails alongside the strict tests, the fix has regressed the
    // schemaless path — the side-effect block must run for both modes.
    let server = TestServer::start().await;
    server.exec(SCHEMALESS_DDL).await.unwrap();
    server
        .exec("CREATE SEARCH INDEX idx_schemaless_bm25 ON docs_schemaless FIELDS content ANALYZER 'standard'")
        .await
        .unwrap();
    seed_three(&server, "docs_schemaless").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_schemaless ORDER BY id")
        .await
        .expect("bm25_score projection on schemaless must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "schemaless control");
}

// ── 3. Strict-doc must work under a stemming language analyzer ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_strict_works_under_language_analyzer() {
    // The bug is at the write site, not the analyzer — every analyzer must
    // see the same indexed content. If only the default worked, the fix
    // would be analyzer-specific and the systemic flaw would remain.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .exec("CREATE SEARCH INDEX idx_strict_standard ON docs_strict FIELDS content ANALYZER 'english'")
        .await
        .unwrap();
    seed_three(&server, "docs_strict").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score under a language analyzer must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / english analyzer");
}

// ── 4. CREATE FULLTEXT INDEX (alias) must wire the same indexer ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fulltext_index_keyword_populates_strict_inverted_index() {
    // `CREATE FULLTEXT INDEX` and `CREATE SEARCH INDEX` are documented as
    // equivalents. The same observable failure mode (NULL bm25 score on rows
    // that contain the term) is captured for both keywords so neither can be
    // wired without the other.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .exec("CREATE FULLTEXT INDEX idx_strict_fulltext ON docs_strict FIELDS content ANALYZER 'standard'")
        .await
        .unwrap();
    seed_three(&server, "docs_strict").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score after CREATE FULLTEXT INDEX must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / FULLTEXT keyword");
}

// ── 5. Index-creation order must not matter (insert-then-index) ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_score_strict_indexes_existing_rows_when_index_created_after_insert() {
    // If a fix only patches the INSERT side-effect block, rows inserted
    // *before* the index existed will still be invisible to the index.
    // The spec: creating a search index after the data is already there
    // must backfill (or otherwise make discoverable) those rows. Same
    // systemic gap (DDL not reaching the data plane) exposed from the
    // other direction.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    seed_three(&server, "docs_strict").await;
    server
        .exec(
            "CREATE SEARCH INDEX idx_strict_after ON docs_strict FIELDS content ANALYZER 'standard'",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score against post-hoc index must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");

    let pairs = id_score_pairs(&rows);
    assert_term_rows_scored(&pairs, "strict / DDL after INSERT");
}

// ── 6. CREATE FULLTEXT INDEX statement shapes ──────────────────────────────
//
// The handler reads the field name out of a fixed token position, so the
// documented `collection(field)` spelling fails the length check and a
// comma-separated field list is read as a single field named `title,` with
// the remaining columns dropped. Both mean the statement the user wrote and
// the index the server built do not agree.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_fulltext_index_accepts_documented_paren_column_form() {
    // `CREATE FULLTEXT INDEX <name> ON <collection>(<field>)` is the form
    // shown in `docs/query-language.md`.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .exec("CREATE FULLTEXT INDEX idx_strict_paren ON docs_strict(content)")
        .await
        .expect("documented collection(field) form must be accepted");
    seed_three(&server, "docs_strict").await;

    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'consensus') FROM docs_strict ORDER BY id")
        .await
        .expect("bm25_score after the documented CREATE form must succeed");
    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");
    assert_term_rows_scored(&id_score_pairs(&rows), "strict / documented paren form");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_fulltext_index_covers_every_field_in_a_list() {
    // A comma-separated field list must index every named field — the
    // `CREATE SEARCH INDEX ... FIELDS a, b` alias accepts one, so users
    // reasonably write one here. Today only the first token is read (with
    // its trailing comma attached) and the rest are dropped in silence.
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION docs_two TYPE DOCUMENT STRICT (\
               id STRING PRIMARY KEY, title STRING, content STRING\
             )",
        )
        .await
        .unwrap();
    server
        .exec("CREATE FULLTEXT INDEX idx_two ON docs_two (title, content)")
        .await
        .expect("multi-field CREATE FULLTEXT INDEX must be accepted");
    server
        .exec(
            "INSERT INTO docs_two (id, title, content) VALUES \
             ('r0', 'consensus notes', 'replication log')",
        )
        .await
        .unwrap();

    // The *second* field in the list is the one silently dropped today.
    let rows = server
        .query_rows("SELECT id, bm25_score(content, 'replication') FROM docs_two ORDER BY id")
        .await
        .expect("bm25_score on the second listed field must succeed");
    assert_eq!(rows.len(), 1, "expected 1 row, got {rows:?}");
    let score = rows[0][1].trim().parse::<f64>().ok();
    assert!(
        score.is_some_and(|s| s > 0.0),
        "bm25_score on `content` must be positive — a NULL cell means the \
         second field of the list was never indexed; got {:?}",
        rows[0][1]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_fulltext_index_rejects_unrecognized_trailing_tokens() {
    // Tokens past the field name are dropped, so an unsupported clause reads
    // as a successful CREATE and the option it carried is never applied.
    let server = TestServer::start().await;
    server.exec(STRICT_DDL).await.unwrap();
    server
        .expect_error(
            "CREATE FULLTEXT INDEX idx_strict_tail ON docs_strict (content) \
             WITH (analyzer = 'simple')",
            "unrecognized option 'WITH'",
        )
        .await;
}

// ── Column scoping on a strict schema ──────────────────────────────────────

const TYPED_DDL: &str = "CREATE COLLECTION docs_typed TYPE DOCUMENT STRICT (\
     id STRING PRIMARY KEY,\
     title STRING,\
     body STRING,\
     views INT\
   )";

async fn seed_typed(server: &TestServer) {
    server.exec(TYPED_DDL).await.unwrap();
    server
        .exec(
            "INSERT INTO docs_typed (id, title, body, views) VALUES \
             ('t1', 'consensus primer', 'nothing else', 1), \
             ('t2', 'gardening', 'consensus in the body', 2)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undeclared_strict_column_is_an_undefined_column() {
    let server = TestServer::start().await;
    seed_typed(&server).await;
    let err = server
        .query_text("SELECT id FROM docs_typed WHERE text_match(summary, 'consensus')")
        .await
        .expect_err("summary is not a column of docs_typed");
    assert!(err.contains("42703"), "expected 42703: {err}");
    assert!(
        err.contains("docs_typed"),
        "the message names the collection: {err}"
    );
    assert!(
        err.contains("summary"),
        "the message names the column: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_int_strict_column_is_a_datatype_mismatch() {
    let server = TestServer::start().await;
    seed_typed(&server).await;
    let err = server
        .query_text("SELECT id FROM docs_typed WHERE text_match(views, 'consensus')")
        .await
        .expect_err("views is not a text column");
    assert!(err.contains("42804"), "expected 42804: {err}");
    assert!(err.contains("views"), "the message names the column: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_strict_column_scopes_the_search() {
    let server = TestServer::start().await;
    seed_typed(&server).await;
    let rows = server
        .query_rows("SELECT id FROM docs_typed WHERE text_match(title, 'consensus') ORDER BY id")
        .await
        .expect("a declared text column must be searchable");
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, vec!["t1"], "only the title match returns: {rows:?}");

    let rows = server
        .query_rows("SELECT id FROM docs_typed WHERE text_match(body, 'consensus') ORDER BY id")
        .await
        .expect("a declared text column must be searchable");
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, vec!["t2"], "only the body match returns: {rows:?}");
}
