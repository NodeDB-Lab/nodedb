//! End-to-end tests for trigger execution: CREATE/DROP lifecycle,
//! BEFORE validation, INSTEAD OF, ALTER ENABLE/DISABLE, SECURITY DEFINER.

mod common;

use common::pgwire_harness::TestServer;

/// CREATE TRIGGER succeeds and DROP removes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_drop_trigger() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION orders").await.unwrap();

    let result = server
        .exec(
            "CREATE TRIGGER audit_orders AFTER INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO audit_log (order_id) VALUES (NEW.id); END",
        )
        .await;
    assert!(result.is_ok(), "CREATE TRIGGER failed: {:?}", result);

    // DROP succeeds (proves it was stored).
    server.exec("DROP TRIGGER audit_orders").await.unwrap();

    // DROP again fails.
    server
        .expect_error("DROP TRIGGER audit_orders", "does not exist")
        .await;
}

/// BEFORE trigger that always rejects via RAISE EXCEPTION.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_trigger_unconditional_reject() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION orders").await.unwrap();

    // Unconditional RAISE — no condition evaluation needed.
    server
        .exec(
            "CREATE TRIGGER block_all BEFORE INSERT ON orders FOR EACH ROW \
             BEGIN \
               RAISE EXCEPTION 'inserts are blocked'; \
             END",
        )
        .await
        .unwrap();

    // Any insert should be rejected.
    server
        .expect_error("INSERT INTO orders (id) VALUES ('ord-1')", "blocked")
        .await;

    server.exec("DROP TRIGGER block_all").await.unwrap();
}

/// ALTER TRIGGER ENABLE/DISABLE works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_trigger_enable_disable() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION items").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER t1 AFTER INSERT ON items FOR EACH ROW \
             BEGIN INSERT INTO log (id) VALUES (NEW.id); END",
        )
        .await
        .unwrap();

    // Disable.
    server.exec("ALTER TRIGGER t1 DISABLE").await.unwrap();

    // Re-enable.
    server.exec("ALTER TRIGGER t1 ENABLE").await.unwrap();

    // Cleanup.
    server.exec("DROP TRIGGER t1").await.unwrap();
}

/// INSTEAD OF trigger creation and lifecycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instead_of_trigger_lifecycle() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION view_orders").await.unwrap();

    // Create INSTEAD OF trigger — verifies DDL parsing for this timing mode.
    let result = server
        .exec(
            "CREATE TRIGGER redirect INSTEAD OF INSERT ON view_orders FOR EACH ROW \
             BEGIN DECLARE x INT := 0; END",
        )
        .await;
    assert!(result.is_ok(), "INSTEAD OF CREATE failed: {:?}", result);

    server.exec("DROP TRIGGER redirect").await.unwrap();
}

/// SECURITY DEFINER trigger creation succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn security_definer_trigger() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION secure_data").await.unwrap();

    let result = server
        .exec(
            "CREATE TRIGGER admin_audit AFTER INSERT ON secure_data FOR EACH ROW \
             SECURITY DEFINER \
             BEGIN INSERT INTO audit (id) VALUES (NEW.id); END",
        )
        .await;
    assert!(
        result.is_ok(),
        "SECURITY DEFINER trigger failed: {:?}",
        result
    );

    server.exec("DROP TRIGGER admin_audit").await.unwrap();
}
