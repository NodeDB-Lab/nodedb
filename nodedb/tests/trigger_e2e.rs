//! End-to-end tests for trigger execution: CREATE/DROP lifecycle,
//! BEFORE validation, INSTEAD OF, ALTER ENABLE/DISABLE, SECURITY DEFINER.

mod common;

use std::time::Duration;

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

/// Multi-statement scripts containing CREATE TRIGGER ... BEGIN ... END split correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trigger_batch_script_executes_after_trigger_body() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION after_src;\n\
             CREATE SYNC TRIGGER log_insert AFTER INSERT ON after_src FOR EACH ROW\n\
             BEGIN\n\
                 DECLARE noop INT := 0;\n\
             END;\n\
             INSERT INTO after_src (id, name, val) VALUES ('as1', 'Alpha', 10);\n\
             INSERT INTO after_src (id, name, val) VALUES ('as2', 'Beta', 20);\n\
             DROP TRIGGER log_insert;",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM after_src ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].contains("\"id\":\"as1\""), "got: {:?}", rows);
    assert!(rows[1].contains("\"id\":\"as2\""), "got: {:?}", rows);

    server
        .expect_error("DROP TRIGGER log_insert", "does not exist")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_trigger_assignment_body_parses() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION trigger_test").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER normalize_status BEFORE INSERT ON trigger_test \
             FOR EACH ROW \
             BEGIN \
                 NEW.status := 'active'; \
             END;",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_trigger_insert_body_parses() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION after_src").await.unwrap();
    server.exec("CREATE COLLECTION after_log").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER log_insert AFTER INSERT ON after_src \
             FOR EACH ROW \
             BEGIN \
                 INSERT INTO after_log (id, src_id, action) VALUES (NEW.id || '_log', NEW.id, 'inserted'); \
             END;",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_async_trigger_insert_persists_side_effect() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION after_src").await.unwrap();
    server.exec("CREATE COLLECTION after_log").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER log_insert AFTER INSERT ON after_src \
             FOR EACH ROW \
             BEGIN \
                 INSERT INTO after_log (id, src_id, action) VALUES (NEW.id || '_log', NEW.id, 'inserted'); \
             END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO after_src (id, name, val) VALUES ('as1', 'Alpha', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO after_src (id, name, val) VALUES ('as2', 'Beta', 20)")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let rows = server
            .query_text("SELECT id, src_id, action FROM after_log ORDER BY id")
            .await
            .unwrap();
        if rows.len() == 2 {
            assert!(rows[0].contains("\"id\":\"as1_log\""), "got: {:?}", rows);
            assert!(rows[0].contains("\"src_id\":\"as1\""), "got: {:?}", rows);
            assert!(rows[1].contains("\"id\":\"as2_log\""), "got: {:?}", rows);
            assert!(rows[1].contains("\"src_id\":\"as2\""), "got: {:?}", rows);
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for async trigger side effect, got: {:?}",
            rows
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_sync_trigger_insert_persists_side_effect() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION after_src").await.unwrap();
    server.exec("CREATE COLLECTION after_log").await.unwrap();

    server
        .exec(
            "CREATE SYNC TRIGGER log_insert AFTER INSERT ON after_src \
             FOR EACH ROW \
             BEGIN \
                 INSERT INTO after_log (id, src_id, action) VALUES (NEW.id || '_log', NEW.id, 'inserted'); \
             END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO after_src (id, name, val) VALUES ('as1', 'Alpha', 10)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id, src_id, action FROM after_log ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "got: {:?}", rows);
    assert!(rows[0].contains("\"id\":\"as1_log\""), "got: {:?}", rows);
    assert!(rows[0].contains("\"src_id\":\"as1\""), "got: {:?}", rows);
}
