// SPDX-License-Identifier: BUSL-1.1

use crate::harness::TestServer;

// ── VALIDATE TYPEGUARD ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_typeguard_no_violations() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION val_clean").await.unwrap();

    // Insert valid data first.
    server
        .exec("INSERT INTO val_clean { id: 'v1', name: 'Alice', age: 25 }")
        .await
        .unwrap();

    // Add type guard after data.
    server
        .exec(
            "CREATE TYPEGUARD ON val_clean (\
                 name STRING,\
                 age INT\
             )",
        )
        .await
        .unwrap();

    // Validate — all docs should pass.
    let rows = server
        .query_text("VALIDATE TYPEGUARD ON val_clean")
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "no violations expected: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_typeguard_finds_violations() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION val_dirty").await.unwrap();

    // Insert data that will violate a future type guard.
    server
        .exec("INSERT INTO val_dirty { id: 'd1', name: 'Alice', score: 42 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO val_dirty { id: 'd2', name: 123, score: 99 }")
        .await
        .unwrap();

    // Add type guard — name must be STRING.
    server
        .exec(
            "CREATE TYPEGUARD ON val_dirty (\
                 name STRING\
             )",
        )
        .await
        .unwrap();

    // Validate — d2 has name=123 (INT, not STRING).
    let rows = server
        .query_text("VALIDATE TYPEGUARD ON val_dirty")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "should find at least one violation: {rows:?}"
    );
    // First column is document_id — should be d2.
    assert!(
        rows.iter().any(|r| r.contains("d2")),
        "violation should reference d2: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_typeguard_no_guards() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION val_noguard").await.unwrap();

    server
        .exec("INSERT INTO val_noguard { id: 'n1', x: 1 }")
        .await
        .unwrap();

    // No typeguard — should return empty result.
    let rows = server
        .query_text("VALIDATE TYPEGUARD ON val_noguard")
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

// ── Unresolvable declared type ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_unresolvable_type_is_refused_at_declaration() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION tg_bad_type").await.unwrap();

    // A type name the engine resolves to nothing.
    server
        .expect_error("CREATE TYPEGUARD ON tg_bad_type (gadget WIDGET)", "42601")
        .await;

    // A trailing word that reading the leading token alone would ignore.
    server
        .expect_error(
            "CREATE TYPEGUARD ON tg_bad_type (at TIMESTAMP GARBAGE)",
            "42601",
        )
        .await;

    // ALTER carries the same refusal.
    server
        .expect_error("ALTER TYPEGUARD ON tg_bad_type ADD gadget WIDGET", "42601")
        .await;

    // A refused declaration reaches no storage.
    let rows = server
        .query_text("SHOW TYPEGUARD ON tg_bad_type")
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "refused guard must not be stored: {rows:?}");
}
