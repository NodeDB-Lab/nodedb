//! Partial indexes: `CREATE INDEX ... WHERE <predicate>`.
//!
//! `StoredIndex.predicate` is stored but no layer enforces it: writes
//! index every row, backfill indexes every existing row, and the
//! planner rewrites equality queries to `IndexedFetch` even when the
//! query doesn't entail the index predicate — so UNIQUE constraints
//! over-trigger and point lookups return rows that should have been
//! excluded.

use super::common::pgwire_harness::TestServer;
use super::helpers::explain_lower;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_unique_index_excludes_predicate_mismatches() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_partial_uniq")
        .await
        .unwrap();

    // Partial UNIQUE: uniqueness only enforced where status = 'active'.
    server
        .exec(
            "CREATE UNIQUE INDEX ON idx_partial_uniq(email) \
             WHERE status = 'active'",
        )
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO idx_partial_uniq \
             { id: 'a', email: 'x@y.z', status: 'disabled' }",
        )
        .await
        .unwrap();
    // Second disabled row with the same email — predicate is FALSE
    // for both, so no unique-index entry is created and the insert
    // must succeed. Today the predicate is ignored: both rows land
    // in the unique index and the second INSERT is rejected.
    server
        .exec(
            "INSERT INTO idx_partial_uniq \
             { id: 'b', email: 'x@y.z', status: 'disabled' }",
        )
        .await
        .expect(
            "partial UNIQUE index must not flag duplicates where the \
             WHERE predicate evaluates FALSE — the predicate field is \
             ignored today, so this INSERT is rejected for a phantom \
             unique violation.",
        );

    // And the active-row predicate must still enforce uniqueness.
    server
        .exec(
            "INSERT INTO idx_partial_uniq \
             { id: 'c', email: 'c@y.z', status: 'active' }",
        )
        .await
        .unwrap();
    server
        .expect_error(
            "INSERT INTO idx_partial_uniq \
             { id: 'd', email: 'c@y.z', status: 'active' }",
            "unique",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_index_backfill_excludes_predicate_mismatches() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_partial_bf")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO idx_partial_bf \
             { id: 'a', email: 'x@y.z', status: 'active' }",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO idx_partial_bf \
             { id: 'b', email: 'x@y.z', status: 'disabled' }",
        )
        .await
        .unwrap();

    // Partial UNIQUE index — duplicate `email` exists across the full
    // collection, but the duplicate spans an active and a disabled row.
    // The predicate excludes the disabled row, so the backfill must
    // filter it out and the CREATE must succeed.
    //
    // Today the backfill ignores the predicate and flags the duplicate
    // — CREATE fails at the Building→Ready transition.
    server
        .exec(
            "CREATE UNIQUE INDEX ON idx_partial_bf(email) \
             WHERE status = 'active'",
        )
        .await
        .expect(
            "partial UNIQUE backfill must filter out rows where the \
             predicate is FALSE; the duplicate across (active, \
             disabled) rows is not a true violation of the partial \
             unique index.",
        );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_index_planner_rejects_non_entailing_query() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_partial_plan")
        .await
        .unwrap();
    server
        .exec(
            "CREATE INDEX ON idx_partial_plan(email) \
             WHERE status = 'active'",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO idx_partial_plan \
             { id: 'a', email: 'x@y.z', status: 'active' }",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO idx_partial_plan \
             { id: 'b', email: 'x@y.z', status: 'disabled' }",
        )
        .await
        .unwrap();

    let plan = explain_lower(
        &server,
        "SELECT id FROM idx_partial_plan WHERE email = 'x@y.z'",
    )
    .await;
    assert!(
        !plan.contains("indexedfetch")
            && !plan.contains("indexlookup")
            && !plan.contains("index lookup")
            && !plan.contains("index_lookup"),
        "planner must not rewrite to an index fetch when the query's \
         WHERE does not entail the partial-index predicate; using the \
         index here silently omits rows that don't match the index \
         predicate. got plan: {plan}"
    );

    let rows = server
        .query_text("SELECT id FROM idx_partial_plan WHERE email = 'x@y.z'")
        .await
        .expect("non-entailing query must succeed");
    assert_eq!(
        rows.len(),
        2,
        "non-entailing query must return every row matching the query's \
         WHERE, regardless of the index's partial predicate; got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_index_planner_uses_index_for_entailing_query() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_partial_use")
        .await
        .unwrap();
    server
        .exec(
            "CREATE INDEX ON idx_partial_use(email) \
             WHERE status = 'active'",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO idx_partial_use \
             { id: 'a', email: 'x@y.z', status: 'active' }",
        )
        .await
        .unwrap();

    let plan = explain_lower(
        &server,
        "SELECT id FROM idx_partial_use \
         WHERE email = 'x@y.z' AND status = 'active'",
    )
    .await;
    assert!(
        plan.contains("indexedfetch")
            || plan.contains("indexlookup")
            || plan.contains("index lookup")
            || plan.contains("index_lookup"),
        "planner must rewrite equality-on-indexed-field to IndexedFetch \
         when the query's WHERE entails the partial-index predicate; \
         got plan: {plan}"
    );
}
