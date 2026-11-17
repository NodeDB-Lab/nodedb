// SPDX-License-Identifier: BUSL-1.1

//! The full-text-search read path must decode a row's body with the encoding
//! its collection actually uses, and must gate it on policy rather than on a
//! format mismatch.
//!
//! Three encodings share the sparse store: schemaless document bodies
//! (standard MessagePack), strict document bodies (Binary Tuples), and
//! vector-primary metadata sidecars (`zerompk` TAGGED
//! `HashMap<String, Value>`). A tagged map and a plain document map are both
//! valid MessagePack maps beginning with the same map header, so no inspection
//! of the stored bytes can tell them apart — a reader that sniffs necessarily
//! mis-decodes one and hands back `[4,"alice"]` where the caller asked for
//! `alice`.
//!
//! Two independent failures are pinned here, because they are two different
//! consequences of the same missing input:
//!
//! - **Projection.** The BM25 score scan returns EVERY row of the collection
//!   (a row with no hit gets a null score), so it reads vector-primary
//!   sidecars whether or not the inverted index holds anything for the
//!   collection. Decoded as document bodies, their payload columns render as
//!   tag arrays.
//! - **Row-level security.** The policy predicate must run against the
//!   NORMALIZED image — the same bytes the projection sees — so the gate and
//!   the output agree. Run against the stored bytes, a strict Binary Tuple
//!   exposes no field the predicate recognizes, so every row is dropped: the
//!   check fails closed on a format mismatch rather than on policy, and an
//!   FTS search under any read policy returns nothing at all.

use crate::harness::TestServer;

const PASSWORD: &str = "fts-body-secret-41";

/// Run `sql` as `user` and return the delivered rows, each cell joined by `|`.
async fn rows_as(server: &TestServer, user: &str, sql: &str) -> Vec<String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{user} runs {sql}: {e}"));
    let mut out = Vec::new();
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            let cells: Vec<String> = (0..row.len())
                .map(|i| row.get(i).unwrap_or("").to_string())
                .collect();
            out.push(cells.join("|"));
        }
    }
    drop(client);
    handle.abort();
    out
}

/// A vector-primary payload column read through the FTS score scan must come
/// back as its VALUE, not as the `zerompk` tag array it is stored as.
///
/// The score scan is the FTS read path that does not need any postings to
/// reach a row: it scans every document in the collection and injects a null
/// score for the ones the inverted index does not know. That makes it the
/// reachable route by which a vector-primary sidecar arrives at an FTS decoder
/// — and the decoder must be told more than "strict schema or not", a
/// two-way answer that cannot express "tagged sidecar". Telling it only that
/// falls through to the document decoder, which ACCEPTS the bytes and yields
/// `[4,"alice"]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_primary_payload_column_survives_the_bm25_score_scan() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION vp_fts_body (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])",
        )
        .await
        .expect("create vector-primary collection");
    server
        .exec("CREATE SEARCH INDEX ON vp_fts_body (owner)")
        .await
        .expect("declare the owner search index");
    server
        .exec(
            "INSERT INTO vp_fts_body (id, vec, owner) \
             VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("vector-primary insert must succeed");

    let rows = server
        .query_named_rows("SELECT id, owner, bm25_score(owner, 'alice') AS s FROM vp_fts_body")
        .await
        .expect("a bm25 score projection over a vector-primary collection must succeed");

    assert_eq!(rows.len(), 1, "one stored row: {rows:?}");
    assert_eq!(
        rows[0].get("owner").map(String::as_str),
        Some("alice"),
        "a vector-primary payload column must read back as its value through the \
         FTS score scan, not as a zerompk tag array: {rows:?}"
    );
    assert_eq!(
        rows[0].get("id").map(String::as_str),
        Some("r1"),
        "the declared primary-key column must read back: {rows:?}"
    );
}

/// An FTS search under a read policy must return the rows the policy admits.
///
/// The policy predicate reads `owner` out of the row. A strict collection
/// stores its rows as Binary Tuples, which are not MessagePack maps at all, so
/// evaluating the predicate against the STORED bytes finds no `owner` field
/// and denies — fail-closed on a format mismatch rather than on policy. The
/// symptom is total: every row disappears from every FTS search on any
/// collection carrying a read policy, including the rows the policy exists to
/// admit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_fts_search_admits_the_rows_a_read_policy_allows() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION fts_rls_body (\
                 id TEXT PRIMARY KEY, owner TEXT, content TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict collection");
    server
        .exec("CREATE SEARCH INDEX idx_fts_rls_body ON fts_rls_body FIELDS content")
        .await
        .expect("create search index");
    server
        .exec(
            "INSERT INTO fts_rls_body (id, owner, content) VALUES \
             ('r1', 'fts_rls_reader', 'consensus algorithm distributed'), \
             ('r2', 'alice', 'consensus memory replication')",
        )
        .await
        .expect("seed rows");
    server
        .exec(&format!("CREATE USER fts_rls_reader PASSWORD '{PASSWORD}'"))
        .await
        .expect("create reader");
    server
        .exec("GRANT ROLE readwrite TO fts_rls_reader")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY fts_rls_owner ON fts_rls_body FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy");

    let rows = rows_as(
        &server,
        "fts_rls_reader",
        "SELECT id FROM fts_rls_body WHERE text_match(content, 'consensus')",
    )
    .await;

    assert_eq!(
        rows.len(),
        1,
        "the search must deliver the one row the policy admits — an empty result \
         means the predicate was evaluated against the stored Binary Tuple and \
         denied on a format mismatch rather than on policy: {rows:?}"
    );
    assert!(
        rows[0].contains("r1"),
        "the admitted row must be the one the policy names: {rows:?}"
    );
}

/// …and the same search must still exclude the rows the policy denies.
///
/// The companion to the test above: normalizing the body before the predicate
/// runs must not turn a fail-closed check into a fail-open one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_fts_search_excludes_the_rows_a_read_policy_denies() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION fts_rls_deny (\
                 id TEXT PRIMARY KEY, owner TEXT, content TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict collection");
    server
        .exec("CREATE SEARCH INDEX idx_fts_rls_deny ON fts_rls_deny FIELDS content")
        .await
        .expect("create search index");
    server
        .exec(
            "INSERT INTO fts_rls_deny (id, owner, content) VALUES \
             ('r1', 'alice', 'consensus algorithm distributed'), \
             ('r2', 'alice', 'consensus memory replication')",
        )
        .await
        .expect("seed rows");
    server
        .exec(&format!("CREATE USER fts_rls_denied PASSWORD '{PASSWORD}'"))
        .await
        .expect("create reader");
    server
        .exec("GRANT ROLE readwrite TO fts_rls_denied")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY fts_rls_deny_owner ON fts_rls_deny FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy");

    let rows = rows_as(
        &server,
        "fts_rls_denied",
        "SELECT id FROM fts_rls_deny WHERE text_match(content, 'consensus')",
    )
    .await;

    assert!(
        rows.is_empty(),
        "the search surfaced rows the read policy excludes: {rows:?}"
    );
}
