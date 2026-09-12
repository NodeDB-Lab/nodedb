// SPDX-License-Identifier: BUSL-1.1

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_strict_from_typeguards() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION conv_tg").await.unwrap();

    // Add typeguards with types and CHECK.
    server
        .exec(
            "CREATE TYPEGUARD ON conv_tg (\
                 name STRING REQUIRED,\
                 age INT CHECK (age >= 0)\
             )",
        )
        .await
        .unwrap();

    // Insert valid data.
    server
        .exec("INSERT INTO conv_tg { id: 'c1', name: 'Alice', age: 25 }")
        .await
        .unwrap();

    // Convert to strict WITHOUT explicit column defs — should infer from typeguards.
    server
        .exec("CONVERT COLLECTION conv_tg TO document_strict")
        .await
        .unwrap();

    // Typeguards should be gone.
    let tg_rows = server
        .query_text("SHOW TYPEGUARD ON conv_tg")
        .await
        .unwrap();
    assert_eq!(
        tg_rows.len(),
        0,
        "typeguards should be cleared: {tg_rows:?}"
    );

    // CHECK constraints should be carried over.
    let constraint_rows = server
        .query_text("SHOW CONSTRAINTS ON conv_tg")
        .await
        .unwrap();
    assert!(
        constraint_rows.iter().any(|r| r.contains("_guard_age")),
        "CHECK from typeguard should carry over: {constraint_rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_strict_no_typeguards_no_cols_errors() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION conv_empty").await.unwrap();

    // No typeguards, no column defs — should fail.
    let err = server
        .exec("CONVERT COLLECTION conv_empty TO document_strict")
        .await;
    assert!(
        err.is_err(),
        "should fail without typeguards or column defs: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_strict_with_explicit_cols() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION conv_explicit")
        .await
        .unwrap();

    server
        .exec("INSERT INTO conv_explicit { id: 'e1', val: 42 }")
        .await
        .unwrap();

    // Convert with explicit column defs (should still work as before).
    let result = server
        .exec("CONVERT COLLECTION conv_explicit TO document_strict (id TEXT, val INT)")
        .await;
    assert!(result.is_ok(), "explicit convert should work");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_strict_preserves_a_minted_rows_identity() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION conv_minted").await.unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON conv_minted (\
                 name STRING REQUIRED\
             )",
        )
        .await
        .unwrap();

    // No declared id — the row's identity lives only in its storage key.
    server
        .exec("INSERT INTO conv_minted { name: 'bob' }")
        .await
        .unwrap();

    let before = server
        .query_text("SELECT id FROM conv_minted")
        .await
        .unwrap();
    assert_eq!(before.len(), 1, "insert must produce one row: {before:?}");
    let identity = before[0].clone();

    server
        .exec("CONVERT COLLECTION conv_minted TO document_strict")
        .await
        .unwrap();

    let after = server
        .query_text("SELECT id FROM conv_minted")
        .await
        .unwrap();
    assert_eq!(
        after.len(),
        1,
        "the minted row must survive conversion: {after:?}"
    );
    assert_eq!(
        after[0], identity,
        "conversion must not change the row's client-visible identity"
    );

    let names = server
        .query_text("SELECT name FROM conv_minted")
        .await
        .unwrap();
    assert_eq!(names, vec!["bob".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_schemaless_reencodes_rows_from_a_strict_source() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION conv_strict_src (\
                 id TEXT PRIMARY KEY,\
                 name TEXT,\
                 age INT\
             ) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO conv_strict_src (id, name, age) VALUES ('s1', 'carol', 30)")
        .await
        .unwrap();

    server
        .exec("CONVERT COLLECTION conv_strict_src TO document_schemaless")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT * FROM conv_strict_src WHERE id = 's1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the Binary Tuple row must survive conversion: {rows:?}"
    );
    assert_eq!(
        rows[0].get("name").map(String::as_str),
        Some("carol"),
        "row: {:?}",
        rows[0]
    );
    assert_eq!(
        rows[0].get("age").map(String::as_str),
        Some("30"),
        "row: {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_strict_fails_the_statement_when_a_row_cannot_encode() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION conv_fail_encode")
        .await
        .unwrap();

    // No 'name' field — the target schema below requires it.
    server
        .exec("INSERT INTO conv_fail_encode { id: 'f1' }")
        .await
        .unwrap();

    server
        .expect_error(
            "CONVERT COLLECTION conv_fail_encode TO document_strict \
             (id TEXT, name TEXT NOT NULL)",
            "NOT NULL",
        )
        .await;

    // The catalog must still show the collection as schemaless: a failed
    // conversion must not flip the stored type over unconverted data.
    let rows = server
        .query_text_joined("SELECT * FROM conv_fail_encode WHERE id = 'f1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the row must remain readable under the original type: {rows:?}"
    );
}
