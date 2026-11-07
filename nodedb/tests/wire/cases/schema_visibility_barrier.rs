// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the cross-core schema visibility barrier.
//!
//! These tests verify that ALTER DDL does not return success until every
//! Data Plane core has applied the updated schema.  Without the barrier a
//! write arriving on a core that has not yet processed the Register plan
//! would encode the row at the old schema version, producing a mismatch
//! that surfaces as a corrupt or incomplete read later.
//!
//! Tests use a multi-core harness (4 cores) so the fan-out path is
//! exercised.  `online_alter_add_column.rs` covers ALTER ADD COLUMN on a
//! single-core server.

use crate::harness::TestServer;

/// After ALTER ADD COLUMN returns success, writes on every core must use
/// the new schema version.  Verified by inserting rows on all cores
/// (the dispatcher round-robins across cores) immediately after the
/// DDL returns and reading them back — the new column must appear with
/// its default value on pre-existing rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_add_column_waits_for_all_cores_to_register() {
    let srv = TestServer::start_multicores(4).await;

    srv.exec(
        "CREATE COLLECTION barrier_add (id TEXT PRIMARY KEY, val INT NOT NULL DEFAULT 0) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION");

    // Insert a row before the ALTER so we can verify default materialization.
    srv.exec("INSERT INTO barrier_add (id, val) VALUES ('pre', 1)")
        .await
        .expect("pre-alter insert");

    // ALTER ADD COLUMN must not return until every core has the new schema.
    srv.exec("ALTER TABLE barrier_add ADD COLUMN extra INT NOT NULL DEFAULT 42")
        .await
        .expect("ALTER ADD COLUMN must succeed with barrier");

    // Post-alter inserts — dispatched across all cores via round-robin.
    // If any core is still on the old schema, encoding would fail or produce
    // a version mismatch detectable at read time.
    for i in 0..8 {
        srv.exec(&format!(
            "INSERT INTO barrier_add (id, val, extra) VALUES ('post{i}', {i}, {i})",
        ))
        .await
        .unwrap_or_else(|e| panic!("post-alter insert {i} failed: {e}"));
    }

    // Read back the pre-existing row — it must have the default value (42) for `extra`.
    let rows = srv
        .query_rows("SELECT id, val, extra FROM barrier_add WHERE id = 'pre'")
        .await
        .expect("SELECT pre-existing row");
    assert!(!rows.is_empty(), "pre-existing row not found after ALTER");
    let extra_val = &rows[0][2];
    assert_eq!(
        extra_val, "42",
        "pre-existing row must carry default value 42 for new column, got: {extra_val}"
    );

    // All post-alter rows must also be readable.
    let all_rows = srv
        .query_rows("SELECT id FROM barrier_add")
        .await
        .expect("SELECT all rows");
    assert_eq!(
        all_rows.len(),
        9,
        "expected 9 rows (1 pre + 8 post), got {}",
        all_rows.len()
    );
}

/// After ALTER TABLE DROP COLUMN returns success, all cores must be on
/// the new schema.  Post-alter inserts must not reference the dropped
/// column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_column_also_waits_for_all_cores() {
    let srv = TestServer::start_multicores(4).await;

    srv.exec(
        "CREATE COLLECTION barrier_drop (id TEXT PRIMARY KEY, val INT NOT NULL DEFAULT 0, \
         removeme TEXT) WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION");

    srv.exec("INSERT INTO barrier_drop (id, val, removeme) VALUES ('r1', 1, 'gone')")
        .await
        .expect("pre-drop insert");

    // DROP COLUMN must not return until every core has the new schema.
    srv.exec("ALTER COLLECTION barrier_drop DROP COLUMN removeme")
        .await
        .expect("ALTER DROP COLUMN must succeed with barrier");

    // Post-drop inserts on all cores.
    for i in 0..8 {
        srv.exec(&format!(
            "INSERT INTO barrier_drop (id, val) VALUES ('d{i}', {i})",
        ))
        .await
        .unwrap_or_else(|e| panic!("post-drop insert {i} failed: {e}"));
    }

    let all_rows = srv
        .query_rows("SELECT id FROM barrier_drop")
        .await
        .expect("SELECT after drop");
    assert_eq!(all_rows.len(), 9, "expected 9 rows, got {}", all_rows.len());
}

/// After ALTER TABLE (any variant) returns success, writes that arrive on
/// any core must use the new schema.  This test interleaves ALTER with
/// concurrent inserts to verify the barrier holds under concurrent load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_after_alter_success_use_new_schema() {
    let srv = TestServer::start_multicores(4).await;
    let srv = std::sync::Arc::new(srv);

    srv.exec(
        "CREATE COLLECTION barrier_concurrent (id TEXT PRIMARY KEY, n INT NOT NULL DEFAULT 0) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION");

    // Spawn writers that write continuously in the background.
    let srv2 = std::sync::Arc::clone(&srv);
    let writer = tokio::spawn(async move {
        for i in 0..32 {
            let _ = srv2
                .exec(&format!(
                    "INSERT INTO barrier_concurrent (id, n) VALUES ('bg{i}', {i})",
                ))
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    });

    // Small delay so some background writes happen before ALTER.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // ALTER — must not return until all cores are on the new schema.
    srv.exec("ALTER TABLE barrier_concurrent ADD COLUMN score INT NOT NULL DEFAULT 0")
        .await
        .expect("ALTER must succeed");

    // Wait for writers to finish.
    writer.await.expect("writer task panicked");

    // All rows must be readable.
    let rows = srv
        .query_rows("SELECT id FROM barrier_concurrent")
        .await
        .expect("SELECT after concurrent ALTER");
    assert!(!rows.is_empty(), "no rows found after concurrent ALTER");

    // Post-alter inserts referencing the new column must succeed.
    srv.exec("INSERT INTO barrier_concurrent (id, n, score) VALUES ('post_alter', 999, 100)")
        .await
        .expect("post-alter insert with new column");

    let check = srv
        .query_rows("SELECT score FROM barrier_concurrent WHERE id = 'post_alter'")
        .await
        .expect("SELECT new column");
    assert_eq!(check.len(), 1, "post-alter row not found");
    assert_eq!(check[0][0], "100", "score column value mismatch");
}

/// ALTER COLUMN RENAME also fans out to all cores (covers that the
/// rename path goes through the same barrier).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_column_waits_for_all_cores() {
    let srv = TestServer::start_multicores(4).await;

    srv.exec(
        "CREATE COLLECTION barrier_rename (id TEXT PRIMARY KEY, oldname INT NOT NULL DEFAULT 7) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION");

    srv.exec("INSERT INTO barrier_rename (id, oldname) VALUES ('r1', 7)")
        .await
        .expect("pre-rename insert");

    srv.exec("ALTER COLLECTION barrier_rename RENAME COLUMN oldname TO newname")
        .await
        .expect("ALTER RENAME COLUMN must succeed with barrier");

    // Post-rename inserts using the new column name.
    for i in 0..4 {
        srv.exec(&format!(
            "INSERT INTO barrier_rename (id, newname) VALUES ('rn{i}', {i})",
        ))
        .await
        .unwrap_or_else(|e| panic!("post-rename insert {i} failed: {e}"));
    }

    let rows = srv
        .query_rows("SELECT id FROM barrier_rename")
        .await
        .expect("SELECT after rename");
    assert_eq!(rows.len(), 5, "expected 5 rows, got {}", rows.len());
}
