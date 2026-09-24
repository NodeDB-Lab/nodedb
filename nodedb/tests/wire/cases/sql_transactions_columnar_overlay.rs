// SPDX-License-Identifier: BUSL-1.1

//! Columnar-engine batch `INSERT` executes at STATEMENT time inside a
//! transaction -- staged into the per-transaction overlay with
//! read-your-own-writes on columnar scans, a real affected-row count, and
//! `ROLLBACK` discarding the staged rows -- mirroring the Document/KV/FTS
//! staging already in place. COMMIT resolves the staged rows into the
//! transaction's redo record, which the redo install applies.
//!
//! Columnar is the first non-point-write, non-Document/KV engine wired into
//! the staging overlay; row identity is the cross-engine surrogate rather
//! than a document primary key.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a
/// simple-query response (PostgreSQL's `INSERT 0 N` count).
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

fn rows_of(msgs: &[SimpleQueryMessage], col: &str) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(col).map(str::to_string),
            _ => None,
        })
        .collect()
}

async fn setup(server: &TestServer) {
    server
        .exec("CREATE COLLECTION m (id INT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m (id, v) VALUES (1, 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m (id, v) VALUES (2, 20)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_insert_returns_real_tag_and_is_visible_in_tx() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // A batch INSERT of two new rows returns INSERT 0 2 at the statement,
    // not a bare OK deferred to COMMIT.
    let msgs = server
        .client
        .simple_query("INSERT INTO m (id, v) VALUES (3, 30), (4, 40)")
        .await
        .expect("in-tx columnar insert should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(2),
        "in-tx columnar INSERT must report the real row count at statement time"
    );

    // Read-your-own-writes: both staged rows are visible to a SELECT inside
    // the same transaction, alongside the pre-existing base rows.
    let rows = server
        .client
        .simple_query("SELECT v FROM m WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&rows, "v"),
        vec!["30"],
        "staged columnar insert must be visible in the same transaction"
    );

    let all = server
        .client
        .simple_query("SELECT id FROM m ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&all, "id"),
        vec!["1", "2", "3", "4"],
        "unrelated base rows must remain alongside the staged rows"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .query_text("SELECT v FROM m WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(
        committed,
        vec!["30"],
        "committed columnar insert must persist"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_insert_rollback_discards_staged_rows() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO m (id, v) VALUES (9, 90)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));

    // Visible in-tx before rollback.
    let in_tx = server
        .client
        .simple_query("SELECT v FROM m WHERE id = 9")
        .await
        .unwrap();
    assert_eq!(rows_of(&in_tx, "v"), vec!["90"]);

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = server
        .query_text("SELECT v FROM m WHERE id = 9")
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "rolled-back columnar insert must not persist, got {after:?}"
    );

    // Original rows are unaffected by the rollback.
    let base = server
        .query_text("SELECT id FROM m ORDER BY id")
        .await
        .unwrap();
    assert_eq!(base, vec!["1", "2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_insert_where_filtered_scan_sees_only_matching_staged_rows() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("INSERT INTO m (id, v) VALUES (5, 50), (6, 999)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(2));

    // A WHERE-filtered in-tx scan must only surface the staged row that
    // matches the predicate.
    let filtered = server
        .client
        .simple_query("SELECT id FROM m WHERE v = 50")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&filtered, "id"),
        vec!["5"],
        "only the staged row matching the WHERE predicate must appear"
    );

    let filtered_out = server
        .client
        .simple_query("SELECT id FROM m WHERE v = 999999")
        .await
        .unwrap();
    assert!(
        rows_of(&filtered_out, "id").is_empty(),
        "a predicate matching no staged or base row must return nothing"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_insert_into_brand_new_collection_is_visible_in_tx() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION fresh (id INT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    // No durable insert has ever landed in `fresh` on this core: the staged
    // insert must still register a schema so the in-transaction SELECT
    // resolves it instead of hitting the scan's "unknown collection" path.
    let msgs = server
        .client
        .simple_query("INSERT INTO fresh (id, v) VALUES (1, 100)")
        .await
        .expect("staged insert into a never-durably-written collection must succeed");
    assert_eq!(command_count(&msgs), Some(1));

    let rows = server
        .client
        .simple_query("SELECT v FROM fresh WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&rows, "v"),
        vec!["100"],
        "staged insert into a brand-new collection must be visible in the same transaction"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .query_text("SELECT v FROM fresh WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(committed, vec!["100"]);
}

/// An `INSERT ... ON CONFLICT DO UPDATE` stages the MERGED row, not the row the
/// statement submitted.
///
/// The overlay's job is to show what this transaction has written, and after a
/// conflict merge that is the stored row with the assignments applied — which
/// is also exactly what `execute_columnar_insert` persists at COMMIT. Staging
/// the submitted body instead made the two disagree: the in-transaction
/// `SELECT` showed `note = 'new'` (submitted) while COMMIT produced
/// `note = 'orig'` (untouched by the `SET` list). `note` is the discriminator
/// here for that reason — `v` looks the same either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_on_conflict_update_stages_the_merged_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION upsert_tx (id TEXT PRIMARY KEY, v INT, note TEXT) \
             WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX upsert_tx_pk ON upsert_tx (id)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO upsert_tx (id, v, note) VALUES ('a', 1, 'orig')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO upsert_tx (id, v, note) VALUES ('a', 7, 'new') \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        )
        .await
        .expect("staged upsert must succeed at the statement");
    assert_eq!(command_count(&msgs), Some(1));

    let staged = server
        .client
        .simple_query("SELECT v, note FROM upsert_tx WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&staged, "v"),
        vec!["7"],
        "the merged row takes `v` from EXCLUDED"
    );
    assert_eq!(
        rows_of(&staged, "note"),
        vec!["orig"],
        "the merged row keeps the column the SET list did not touch — staging the \
         submitted body would show 'new' here and disagree with what COMMIT writes"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .query_rows("SELECT v, note FROM upsert_tx WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        committed,
        vec![vec!["7".to_string(), "orig".to_string()]],
        "COMMIT must leave exactly the values the transaction was shown"
    );
}
