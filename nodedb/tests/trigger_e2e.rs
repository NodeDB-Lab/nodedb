// SPDX-License-Identifier: BUSL-1.1

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

/// BEFORE trigger rejection ensures the row is NOT persisted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_trigger_reject_does_not_persist_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION guarded (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();

    server
        .exec(
            "CREATE TRIGGER guard BEFORE INSERT ON guarded FOR EACH ROW \
             BEGIN \
               RAISE EXCEPTION 'rejected'; \
             END",
        )
        .await
        .unwrap();

    // Insert should fail.
    server
        .expect_error(
            "INSERT INTO guarded (id, val) VALUES ('g1', 100)",
            "rejected",
        )
        .await;

    // Row should NOT exist.
    let rows = server
        .query_text("SELECT id FROM guarded WHERE id = 'g1'")
        .await
        .unwrap();
    assert!(rows.is_empty(), "rejected row should not be persisted");

    server.exec("DROP TRIGGER guard").await.unwrap();
}

/// A BEFORE INSERT trigger that divides by zero in a NEW.* assignment must
/// reject the insert with a "division by zero" error instead of silently
/// writing NULL, and the row must NOT be persisted — the same fail-closed
/// contract as any other BEFORE trigger rejection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_trigger_division_by_zero_rejects_insert() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION ratios (id TEXT PRIMARY KEY, numerator INT, \
             denominator INT, ratio INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec(
            "CREATE TRIGGER compute_ratio BEFORE INSERT ON ratios FOR EACH ROW \
             BEGIN \
               NEW.ratio := NEW.numerator / NEW.denominator; \
             END",
        )
        .await
        .unwrap();

    // Insert with a zero denominator should fail with a division-by-zero error.
    server
        .expect_error(
            "INSERT INTO ratios (id, numerator, denominator) VALUES ('r1', 10, 0)",
            "division by zero",
        )
        .await;

    // Row should NOT exist.
    let rows = server
        .query_text("SELECT id FROM ratios WHERE id = 'r1'")
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "row rejected by division-by-zero trigger should not be persisted"
    );

    server.exec("DROP TRIGGER compute_ratio").await.unwrap();
}

/// Happy-path control for `before_trigger_division_by_zero_rejects_insert`:
/// a non-zero denominator computes normally and the row persists with the
/// computed value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_trigger_nonzero_division_persists_computed_value() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION ratios2 (id TEXT PRIMARY KEY, numerator INT, \
             denominator INT, ratio INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec(
            "CREATE TRIGGER compute_ratio2 BEFORE INSERT ON ratios2 FOR EACH ROW \
             BEGIN \
               NEW.ratio := NEW.numerator / NEW.denominator; \
             END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO ratios2 (id, numerator, denominator) VALUES ('r2', 10, 2)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT ratio FROM ratios2 WHERE id = 'r2'")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["5".to_string()],
        "expected computed ratio 10/2 = 5"
    );

    server.exec("DROP TRIGGER compute_ratio2").await.unwrap();
}

/// An `EXCEPTION WHEN DIVISION_BY_ZERO` handler inside the trigger body must
/// catch the division-by-zero signal end-to-end: the insert succeeds and the
/// handler's fallback assignment is applied, proving the previously-dormant
/// `DIVISION_BY_ZERO` named condition (see `exception::exception_matches`)
/// now actually fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_trigger_exception_handler_catches_division_by_zero() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION ratios_safe (id TEXT PRIMARY KEY, numerator INT, \
             denominator INT, ratio INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec(
            "CREATE TRIGGER compute_ratio_safe BEFORE INSERT ON ratios_safe FOR EACH ROW \
             BEGIN \
               NEW.ratio := NEW.numerator / NEW.denominator; \
             EXCEPTION WHEN DIVISION_BY_ZERO THEN \
               NEW.ratio := 0; \
             END",
        )
        .await
        .unwrap();

    // Insert with a zero denominator: the EXCEPTION handler catches the
    // division-by-zero error and the insert succeeds with ratio = 0.
    server
        .exec("INSERT INTO ratios_safe (id, numerator, denominator) VALUES ('s1', 10, 0)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT ratio FROM ratios_safe WHERE id = 's1'")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["0".to_string()],
        "EXCEPTION handler should have set ratio to 0"
    );

    server
        .exec("DROP TRIGGER compute_ratio_safe")
        .await
        .unwrap();
}

/// AFTER ASYNC trigger failure does NOT affect the parent write.
/// The write should succeed; trigger error is handled by retry/DLQ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_async_trigger_failure_does_not_block_write() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION async_src (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();

    // Create an ASYNC AFTER trigger that targets a nonexistent collection (will fail).
    server
        .exec(
            "CREATE TRIGGER fail_async AFTER INSERT ON async_src FOR EACH ROW \
             BEGIN \
                 INSERT INTO nonexistent_collection (id) VALUES (NEW.id); \
             END",
        )
        .await
        .unwrap();

    // Insert should succeed despite the trigger targeting a bad collection.
    server
        .exec("INSERT INTO async_src (id, val) VALUES ('a1', 42)")
        .await
        .unwrap();

    // Verify the row was persisted.
    let rows = server
        .query_text("SELECT id FROM async_src WHERE id = 'a1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "write should succeed despite async trigger failure"
    );

    server.exec("DROP TRIGGER fail_async").await.unwrap();
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
    assert_eq!(rows[0], "as1", "got: {:?}", rows);
    assert_eq!(rows[1], "as2", "got: {:?}", rows);

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
            // query_text returns first column (id) for each row
            assert_eq!(rows[0], "as1_log", "got: {:?}", rows);
            assert_eq!(rows[1], "as2_log", "got: {:?}", rows);
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
    // query_text returns first column (id)
    assert_eq!(rows[0], "as1_log", "got: {:?}", rows);
}

/// A trigger body containing `PUBLISH TO <topic> ...` must parse and compile.
/// `PUBLISH TO` is a first-class statement at the SQL top-level and is the
/// documented mechanism for reactive pipelines (CDC → topic). It must be
/// usable inside trigger bodies, not only as a standalone statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_to_inside_trigger_body_parses() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TOPIC profile_events WITH (RETENTION = '1 hour')")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION memories").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER memory_publish AFTER INSERT ON memories FOR EACH ROW \
             BEGIN \
                 PUBLISH TO profile_events NEW.user_id; \
             END",
        )
        .await
        .expect("CREATE TRIGGER with PUBLISH TO body should succeed");

    server.exec("DROP TRIGGER memory_publish").await.unwrap();
}

/// A string-literal `PUBLISH TO` form (no NEW.* substitution) inside a trigger
/// body must also parse. Guards the parser specifically, independent of
/// expression substitution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_to_literal_inside_trigger_body_parses() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TOPIC audit_events WITH (RETENTION = '1 hour')")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION audited").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER audit_pub AFTER INSERT ON audited FOR EACH ROW \
             BEGIN \
                 PUBLISH TO audit_events 'row inserted'; \
             END",
        )
        .await
        .expect("CREATE TRIGGER with literal PUBLISH TO body should succeed");

    server.exec("DROP TRIGGER audit_pub").await.unwrap();
}

/// End-to-end: an AFTER INSERT trigger that PUBLISHes must deliver the message
/// to the topic so a consumer group can read it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_to_inside_trigger_body_delivers_message() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TOPIC order_events WITH (RETENTION = '1 hour')")
        .await
        .unwrap();
    server
        .exec("CREATE CONSUMER GROUP processors ON order_events")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION orders").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER orders_publish AFTER INSERT ON orders FOR EACH ROW \
             BEGIN \
                 PUBLISH TO order_events 'order inserted'; \
             END",
        )
        .await
        .expect("trigger with PUBLISH TO body should be created");

    server
        .exec("INSERT INTO orders (id) VALUES ('o1')")
        .await
        .unwrap();

    // Give the event plane a moment to dispatch the AFTER trigger.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let rows = server
        .query_text("SELECT * FROM TOPIC order_events CONSUMER GROUP processors LIMIT 10")
        .await
        .expect("SELECT FROM TOPIC should succeed");

    assert!(
        !rows.is_empty(),
        "expected at least one published message, got: {:?}",
        rows
    );
}

/// A PUBLISH TO payload containing an SQL-escaped quote (`''` → `'`) must be
/// delivered to the topic with the quote unescaped, not as the literal two-char
/// sequence. Guards against naive outer-quote stripping.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_to_payload_unescapes_doubled_quotes() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TOPIC quote_events WITH (RETENTION = '1 hour')")
        .await
        .unwrap();
    server
        .exec("CREATE CONSUMER GROUP qg ON quote_events")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION quotes").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER quotes_pub AFTER INSERT ON quotes FOR EACH ROW \
             BEGIN \
                 PUBLISH TO quote_events 'it''s fine'; \
             END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO quotes (id) VALUES ('q1')")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let combined = topic_rows_joined(
        &server,
        "SELECT * FROM TOPIC quote_events CONSUMER GROUP qg LIMIT 10",
    )
    .await;
    assert!(
        combined.contains("it's fine"),
        "payload should be unescaped to `it's fine`, got: {combined:?}"
    );
    assert!(
        !combined.contains("it''s fine"),
        "payload must not retain SQL-escaped form `it''s fine`, got: {combined:?}"
    );
}

async fn topic_rows_joined(server: &TestServer, sql: &str) -> String {
    let msgs = server.client.simple_query(sql).await.unwrap();
    let mut out = String::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            for i in 0..row.len() {
                if let Some(v) = row.get(i) {
                    out.push_str(v);
                    out.push('\t');
                }
            }
            out.push('\n');
        }
    }
    out
}

/// An empty quoted payload (`''`) must deliver an empty string — not be
/// rejected, not be treated as a bare payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_to_empty_quoted_payload_delivers() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TOPIC empty_events WITH (RETENTION = '1 hour')")
        .await
        .unwrap();
    server
        .exec("CREATE CONSUMER GROUP eg ON empty_events")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION empties").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER empties_pub AFTER INSERT ON empties FOR EACH ROW \
             BEGIN \
                 PUBLISH TO empty_events ''; \
             END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO empties (id) VALUES ('e1')")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let rows = server
        .query_text("SELECT * FROM TOPIC empty_events CONSUMER GROUP eg LIMIT 10")
        .await
        .unwrap();

    assert!(
        !rows.is_empty(),
        "empty-payload publish should still deliver a message row, got: {rows:?}"
    );
}

/// A PUBLISH TO with NEW.* substitution must produce a quoted literal that
/// survives payload parsing. This is the documented trigger pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_to_new_field_substitution_delivers_value() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TOPIC user_events WITH (RETENTION = '1 hour')")
        .await
        .unwrap();
    server
        .exec("CREATE CONSUMER GROUP ug ON user_events")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION memories (id TEXT PRIMARY KEY, user_id TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();

    server
        .exec(
            "CREATE TRIGGER memories_pub AFTER INSERT ON memories FOR EACH ROW \
             BEGIN \
                 PUBLISH TO user_events NEW.user_id; \
             END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO memories (id, user_id) VALUES ('m1', 'alice')")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let combined = topic_rows_joined(
        &server,
        "SELECT * FROM TOPIC user_events CONSUMER GROUP ug LIMIT 10",
    )
    .await;
    assert!(
        combined.contains("alice"),
        "expected substituted value `alice` in published payload, got: {combined:?}"
    );
}
