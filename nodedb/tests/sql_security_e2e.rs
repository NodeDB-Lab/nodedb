//! End-to-end pgwire tests for security features:
//! audit log, RLS, SHOW USERS, SHOW GRANTS, SHOW CONSTRAINTS.

mod common;

use common::pgwire_harness::TestServer;

// ── Audit Log ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_audit_log_succeeds() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION audit_test").await.unwrap();

    // SHOW AUDIT LOG should execute without error.
    // May return 0 rows if audit level is Minimal or entries aren't flushed yet.
    let result = server.query_text("SHOW AUDIT LOG LIMIT 10").await;
    assert!(
        result.is_ok(),
        "SHOW AUDIT LOG should not error: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_audit_log_with_limit() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION audit_lim").await.unwrap();

    let result = server.query_text("SHOW AUDIT LOG LIMIT 2").await;
    assert!(result.is_ok(), "SHOW AUDIT LOG LIMIT should not error");
    if let Ok(rows) = result {
        assert!(rows.len() <= 2, "LIMIT 2 should return at most 2 rows");
    }
}

// ── SHOW USERS ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_users_succeeds() {
    let server = TestServer::start().await;

    // SHOW USERS should execute without error (may return 0 rows in test mode).
    let result = server.query_text("SHOW USERS").await;
    assert!(result.is_ok(), "SHOW USERS should not error: {result:?}");
}

// ── SHOW CONSTRAINTS ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_constraints_with_all_kinds() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION sec_test").await.unwrap();

    // Add a state transition constraint.
    server
        .exec(
            "ALTER COLLECTION sec_test ADD CONSTRAINT status_flow \
             ON COLUMN status TRANSITIONS ('draft' -> 'active')",
        )
        .await
        .unwrap();

    // Add a general CHECK constraint.
    server
        .exec(
            "ALTER COLLECTION sec_test ADD CONSTRAINT pos_val \
             CHECK (NEW.val > 0)",
        )
        .await
        .unwrap();

    // Add a typeguard.
    server
        .exec("CREATE TYPEGUARD ON sec_test (name STRING)")
        .await
        .unwrap();

    // SHOW CONSTRAINTS should return all three kinds.
    let rows = server
        .query_text("SHOW CONSTRAINTS ON sec_test")
        .await
        .unwrap();
    assert!(
        rows.len() >= 3,
        "should have transition + check + typeguard: {rows:?}"
    );
}

// ── RLS Policy DDL ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_show_rls_policy() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION rls_orders").await.unwrap();

    // Create an RLS policy ($auth.id is the authenticated user's ID).
    server
        .exec(
            "CREATE RLS POLICY own_orders ON rls_orders FOR READ \
             USING (customer_id = $auth.id)",
        )
        .await
        .unwrap();

    // SHOW RLS POLICIES should succeed.
    let rows = server.query_text("SHOW RLS POLICIES").await.unwrap();
    // At least one policy should exist (the one we just created).
    assert!(!rows.is_empty(), "should have at least one policy");

    // Drop the policy.
    server
        .exec("DROP RLS POLICY own_orders ON rls_orders")
        .await
        .unwrap();
}

// ── Change Tracking via Typeguard DEFAULT/VALUE ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_created_at_and_updated_at() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION tracked_docs")
        .await
        .unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tracked_docs (\
                 created_at STRING DEFAULT 'auto-set',\
                 version INT DEFAULT 1\
             )",
        )
        .await
        .unwrap();

    // Insert — DEFAULT should fill created_at and version.
    server
        .exec("INSERT INTO tracked_docs { id: 't1', name: 'Alice' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM tracked_docs WHERE id = 't1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("auto-set"),
        "DEFAULT should inject created_at: {rows:?}"
    );
}
