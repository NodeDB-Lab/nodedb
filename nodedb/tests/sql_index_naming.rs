//! Integration tests for `CREATE INDEX` on document collections:
//! naming, registration, uniqueness enforcement, and planner visibility.
//!
//! Silent no-op (DDL parses, audit records ownership, but the secondary
//! index is never actually registered against the collection config, never
//! populated on subsequent writes, and never picked up by the planner) is
//! the regression mode these tests guard. Both schemaless and strict
//! document modes must honor CREATE INDEX, and UNIQUE / COLLATE NOCASE
//! modifiers that parse must also enforce.

mod common;

use common::pgwire_harness::TestServer;

/// Return all rows of EXPLAIN <sql> concatenated, lowercased.
///
/// Used to assert that the chosen physical plan references an index lookup
/// rather than a full scan when a WHERE predicate lands on an indexed field.
async fn explain_lower(server: &TestServer, sql: &str) -> String {
    let rows = server
        .query_text(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN must succeed");
    rows.join("\n").to_lowercase()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_named() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_named").await.unwrap();
    server
        .exec("INSERT INTO idx_named { id: 'a', role: 'admin' }")
        .await
        .unwrap();

    // Named index — standard SQL form.
    server
        .exec("CREATE INDEX my_idx ON idx_named(role)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_unnamed_auto_name() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_unnamed").await.unwrap();
    server
        .exec("INSERT INTO idx_unnamed { id: 'a', email: 'a@b.com' }")
        .await
        .unwrap();

    // No name — should auto-generate name and succeed.
    server
        .exec("CREATE INDEX ON idx_unnamed(email)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_fields_keyword() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_fields").await.unwrap();
    server
        .exec("INSERT INTO idx_fields { id: 'a', tag: 'rust' }")
        .await
        .unwrap();

    // FIELDS keyword form — should succeed.
    server
        .exec("CREATE INDEX ON idx_fields FIELDS tag")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_unique_index_unnamed() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_unique").await.unwrap();
    server
        .exec("INSERT INTO idx_unique { id: 'a', code: 'ABC' }")
        .await
        .unwrap();

    // Unnamed UNIQUE index.
    server
        .exec("CREATE UNIQUE INDEX ON idx_unique(code)")
        .await
        .unwrap();
}

// ───────────────────────── Behavior: index must actually be registered ─────────────────────────
//
// The tests below assert the SPEC, not current behavior. CREATE INDEX today
// parses, writes an ownership record, and returns success — but never
// registers the field onto the collection's secondary-index configuration,
// so writes do not populate it and the planner never picks it up. Each of
// these tests fails today for reasons traceable to
// `control/server/pgwire/ddl/collection/index.rs::create_index`.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_on_strict_document_used_by_planner() {
    let server = TestServer::start().await;

    // Exact scenario from the reporter's bug repro.
    server
        .exec(
            "CREATE COLLECTION events TYPE DOCUMENT STRICT (\
               id STRING PRIMARY KEY, \
               tenant_id STRING NOT NULL, \
               user_id STRING NOT NULL, \
               created_at TIMESTAMP)",
        )
        .await
        .unwrap();
    server
        .exec("CREATE INDEX idx_events_lookup ON events(tenant_id)")
        .await
        .unwrap();

    // After CREATE INDEX succeeds, a point-lookup query on the indexed
    // column must be planned as an index lookup, not a full table/collection
    // scan. Asserting the plan shape (rather than just the result set) is
    // what catches the silent-no-op failure mode: a full scan returns the
    // same rows but with linear cost in tenant size.
    let plan = explain_lower(&server, "SELECT id FROM events WHERE tenant_id = 'acme'").await;

    assert!(
        plan.contains("indexedfetch")
            || plan.contains("indexlookup")
            || plan.contains("index lookup")
            || plan.contains("index_lookup"),
        "plan for WHERE on indexed column must reference an index lookup, \
         got: {plan}"
    );
    // Silent-failure regression guard: the plan must not fall back to a
    // generic scan/table-walk shape once an index exists on the filter column.
    assert!(
        !plan.contains("full scan") && !plan.contains("fulltablescan"),
        "plan must not full-scan when indexed column has a CREATE INDEX, \
         got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_on_schemaless_document_used_by_planner() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_schemaless")
        .await
        .unwrap();
    server
        .exec("INSERT INTO idx_schemaless { id: 'a', role: 'admin' }")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX ON idx_schemaless(role)")
        .await
        .unwrap();

    let plan = explain_lower(
        &server,
        "SELECT id FROM idx_schemaless WHERE role = 'admin'",
    )
    .await;

    assert!(
        plan.contains("indexedfetch")
            || plan.contains("indexlookup")
            || plan.contains("index lookup")
            || plan.contains("index_lookup"),
        "schemaless CREATE INDEX must wire into the planner the same way as \
         a strict column index; got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_unique_index_rejects_duplicate_insert() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_unique_enforce")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX ON idx_unique_enforce(email)")
        .await
        .unwrap();

    // First insert must succeed.
    server
        .exec("INSERT INTO idx_unique_enforce { id: 'a', email: 'x@y.z' }")
        .await
        .unwrap();

    // Second insert with the same indexed value must be rejected. Today the
    // UNIQUE keyword is parsed (`is_unique`) but never persisted anywhere,
    // so duplicates succeed silently — a correctness bug that is part of
    // the same design flaw as the reporter's point-lookup issue: CREATE
    // INDEX DDL modifiers are parsed but not dispatched to the config or
    // enforcement layer.
    server
        .expect_error(
            "INSERT INTO idx_unique_enforce { id: 'b', email: 'x@y.z' }",
            "unique",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_removes_planner_awareness() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_drop").await.unwrap();
    server
        .exec("CREATE INDEX idx_drop_role ON idx_drop(role)")
        .await
        .unwrap();
    server.exec("DROP INDEX idx_drop_role").await.unwrap();

    // After DROP INDEX, queries on the formerly-indexed field must no longer
    // be planned as index lookups (the index entries, in-memory config, and
    // planner hints must all be torn down). Same design flaw as CREATE
    // INDEX: DROP only rewrites the ownership record.
    let plan = explain_lower(&server, "SELECT id FROM idx_drop WHERE role = 'admin'").await;

    assert!(
        !plan.contains("indexedfetch")
            && !plan.contains("indexlookup")
            && !plan.contains("index lookup")
            && !plan.contains("index_lookup"),
        "after DROP INDEX the planner must not pick an index path, got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_backfills_existing_rows() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION bf_col").await.unwrap();
    server
        .exec("INSERT INTO bf_col { id: 'a', email: 'one@x.com' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO bf_col { id: 'b', email: 'two@x.com' }")
        .await
        .unwrap();

    // CREATE INDEX runs AFTER the rows exist. The two-phase
    // Building→Ready backfill pipeline must populate the index from the
    // pre-existing documents before flipping to Ready; otherwise a
    // subsequent lookup against the index would miss the rows (same
    // silent-miss class as the original reporter's bug).
    server.exec("CREATE INDEX ON bf_col(email)").await.unwrap();

    // After CREATE INDEX completes (backfill done, state=Ready), the
    // planner rewrites the equality query to IndexedFetch — and the
    // executor returns the backfilled row through the index path.
    // We assert the row is found (doc id appears in the response) rather
    // than pinning the exact column layout: the scan-compatible
    // `{id, data}` envelope is the current wire shape and extracting
    // projection fields inside the indexed-fetch handler is additive
    // work; an empty result here is the actual silent-miss bug.
    let rows = server
        .query_text("SELECT id FROM bf_col WHERE email = 'one@x.com'")
        .await
        .expect("indexed SELECT must succeed");
    assert_eq!(
        rows.len(),
        1,
        "indexed SELECT must return exactly one row, got: {rows:?}"
    );
    assert!(
        rows[0].contains("\"a\""),
        "indexed SELECT row must reference doc id 'a', got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_unique_index_rejects_existing_duplicates() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION bf_unique").await.unwrap();
    server
        .exec("INSERT INTO bf_unique { id: 'a', code: 'ABC' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO bf_unique { id: 'b', code: 'ABC' }")
        .await
        .unwrap();

    // CREATE UNIQUE INDEX on a collection that already contains
    // duplicates must fail at the backfill phase — detecting the
    // violation before the Ready flip so the catalog never advertises
    // an index that doesn't actually enforce uniqueness.
    server
        .expect_error("CREATE UNIQUE INDEX ON bf_unique(code)", "unique")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_indexes_lists_created_index() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_show").await.unwrap();
    server
        .exec("CREATE INDEX idx_show_role ON idx_show(role)")
        .await
        .unwrap();

    // Positive lock-in: SHOW INDEXES must list a freshly created index. This
    // is the user-visible confirmation that creation succeeded; without it,
    // operators have no feedback channel. (First-column read of `query_text`
    // returns the index name.)
    let names = server
        .query_text("SHOW INDEXES")
        .await
        .expect("SHOW INDEXES must succeed");
    assert!(
        names.iter().any(|n| n == "idx_show_role"),
        "SHOW INDEXES must list created index, got: {names:?}"
    );
}
