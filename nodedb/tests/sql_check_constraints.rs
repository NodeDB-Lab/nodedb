//! Integration tests for general CHECK constraints.
//!
//! Verifies that `ALTER COLLECTION ... ADD CONSTRAINT name CHECK (expr)` enforces
//! the constraint on INSERT, UPSERT, and UPDATE — including cross-field checks
//! and subquery checks. Also tests DROP CONSTRAINT.

mod common;

use common::pgwire_harness::TestServer;

// ── Simple CHECK: single-field value constraint ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_constraint_rejects_invalid_insert() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION orders").await.unwrap();

    server
        .exec(
            "ALTER COLLECTION orders ADD CONSTRAINT positive_amount \
             CHECK (NEW.amount > 0)",
        )
        .await
        .unwrap();

    // Valid insert — should pass.
    server
        .exec("INSERT INTO orders { id: 'o1', amount: 50 }")
        .await
        .unwrap();

    // Invalid insert — amount <= 0 should fail.
    let err = server
        .exec("INSERT INTO orders { id: 'o2', amount: -5 }")
        .await;
    assert!(err.is_err(), "negative amount should be rejected");
    let msg = err.unwrap_err();
    assert!(
        msg.contains("positive_amount"),
        "error should mention constraint name: {msg}"
    );

    // Verify only the valid row exists.
    let rows = server.query_text("SELECT * FROM orders").await.unwrap();
    assert_eq!(rows.len(), 1, "only valid row should exist: {rows:?}");
}

// ── CHECK on standard SQL INSERT (VALUES form) ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_constraint_values_form() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION items").await.unwrap();

    server
        .exec(
            "ALTER COLLECTION items ADD CONSTRAINT valid_qty \
             CHECK (NEW.qty >= 1)",
        )
        .await
        .unwrap();

    // Valid via VALUES form.
    server
        .exec("INSERT INTO items (id, qty) VALUES ('i1', 10)")
        .await
        .unwrap();

    // Invalid via VALUES form.
    let err = server
        .exec("INSERT INTO items (id, qty) VALUES ('i2', 0)")
        .await;
    assert!(err.is_err(), "qty=0 should be rejected: {err:?}");
}

// ── Cross-field CHECK on UPDATE ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_constraint_cross_field_update() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION events").await.unwrap();

    // Insert a row first (no constraints yet).
    server
        .exec("INSERT INTO events { id: 'e1', start_val: 10, end_val: 20 }")
        .await
        .unwrap();

    // Add a cross-field constraint: end_val must be > start_val.
    server
        .exec(
            "ALTER COLLECTION events ADD CONSTRAINT end_after_start \
             CHECK (NEW.end_val > NEW.start_val)",
        )
        .await
        .unwrap();

    // Valid update: end_val stays > start_val.
    server
        .exec("UPDATE events SET end_val = 15 WHERE id = 'e1'")
        .await
        .unwrap();

    // Invalid update: end_val < start_val (start_val=10 from existing doc).
    let err = server
        .exec("UPDATE events SET end_val = 5 WHERE id = 'e1'")
        .await;
    assert!(
        err.is_err(),
        "end_val < start_val should be rejected: {err:?}"
    );
}

// ── UPSERT enforcement ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_constraint_upsert() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION products").await.unwrap();

    server
        .exec(
            "ALTER COLLECTION products ADD CONSTRAINT valid_price \
             CHECK (NEW.price > 0)",
        )
        .await
        .unwrap();

    // Valid upsert (new doc).
    server
        .exec("UPSERT INTO products { id: 'p1', price: 9.99 }")
        .await
        .unwrap();

    // Valid upsert (update existing).
    server
        .exec("UPSERT INTO products { id: 'p1', price: 12.50 }")
        .await
        .unwrap();

    // Invalid upsert.
    let err = server
        .exec("UPSERT INTO products { id: 'p2', price: -1 }")
        .await;
    assert!(err.is_err(), "negative price should be rejected");
}

// ── DROP CONSTRAINT removes enforcement ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_check_constraint() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION widgets").await.unwrap();

    server
        .exec(
            "ALTER COLLECTION widgets ADD CONSTRAINT min_weight \
             CHECK (NEW.weight >= 0)",
        )
        .await
        .unwrap();

    // Should fail.
    let err = server
        .exec("INSERT INTO widgets { id: 'w1', weight: -1 }")
        .await;
    assert!(err.is_err(), "negative weight should be rejected");

    // Drop the constraint.
    server
        .exec("DROP CONSTRAINT min_weight ON widgets")
        .await
        .unwrap();

    // Now should succeed.
    server
        .exec("INSERT INTO widgets { id: 'w1', weight: -1 }")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM widgets").await.unwrap();
    assert_eq!(rows.len(), 1);
}

// ── Multiple constraints on same collection ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_check_constraints() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION users").await.unwrap();

    server
        .exec(
            "ALTER COLLECTION users ADD CONSTRAINT name_required \
             CHECK (NEW.name != '')",
        )
        .await
        .unwrap();

    server
        .exec(
            "ALTER COLLECTION users ADD CONSTRAINT age_valid \
             CHECK (NEW.age >= 0)",
        )
        .await
        .unwrap();

    // Passes both.
    server
        .exec("INSERT INTO users { id: 'u1', name: 'Alice', age: 25 }")
        .await
        .unwrap();

    // Fails age_valid.
    let err = server
        .exec("INSERT INTO users { id: 'u2', name: 'Bob', age: -1 }")
        .await;
    assert!(err.is_err(), "negative age should be rejected");
}

// ── Duplicate constraint name rejected ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_constraint_name_rejected() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION dup_test").await.unwrap();

    server
        .exec(
            "ALTER COLLECTION dup_test ADD CONSTRAINT my_check \
             CHECK (NEW.x > 0)",
        )
        .await
        .unwrap();

    // Same name again should fail.
    let err = server
        .exec(
            "ALTER COLLECTION dup_test ADD CONSTRAINT my_check \
             CHECK (NEW.y > 0)",
        )
        .await;
    assert!(err.is_err(), "duplicate constraint name should be rejected");
    assert!(err.unwrap_err().contains("already exists"));
}

// ── CHECK with subquery (cross-collection) ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_constraint_with_subquery() {
    let server = TestServer::start().await;

    // Create a roles collection with known values.
    server.exec("CREATE COLLECTION roles").await.unwrap();
    server
        .exec("INSERT INTO roles { id: 'r1', name: 'admin' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO roles { id: 'r2', name: 'editor' }")
        .await
        .unwrap();

    // Create a users collection with a CHECK referencing roles.
    server.exec("CREATE COLLECTION sub_users").await.unwrap();

    server
        .exec(
            "ALTER COLLECTION sub_users ADD CONSTRAINT valid_role \
             CHECK (NEW.role IN (SELECT name FROM roles))",
        )
        .await
        .unwrap();

    // Valid: role 'admin' exists in roles.
    server
        .exec("INSERT INTO sub_users { id: 'u1', role: 'admin' }")
        .await
        .unwrap();

    // Invalid: role 'superuser' does not exist in roles.
    let err = server
        .exec("INSERT INTO sub_users { id: 'u2', role: 'superuser' }")
        .await;
    assert!(
        err.is_err(),
        "non-existent role should be rejected: {err:?}"
    );
}
