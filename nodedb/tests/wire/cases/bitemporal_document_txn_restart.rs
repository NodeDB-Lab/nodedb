// SPDX-License-Identifier: BUSL-1.1

//! WAL-only restart fidelity for an in-transaction INSERT into a
//! `bitemporal=true` document collection — the durability regression the
//! resolve-time bitemporal-stamp mechanism targets.
//!
//! `BEGIN; INSERT; COMMIT` on a bitemporal collection journals the transaction
//! as one `TransactionRedo` WAL record. The document `Put`
//! sub-record must carry a bitemporal stamp: at WAL replay `doc_configs` is
//! EMPTY (replay runs before the `Register` ops that repopulate it), so a
//! sub-record with no stamp makes `is_bitemporal` return false, landing the
//! replayed put on the PLAIN table
//! (invisible to the versioned reads `AS OF SYSTEM TIME` / current bitemporal
//! reads consult) — data loss in the crash window, or an orphan plain row plus
//! a double-stamped version in the normal case.
//!
//! The fix carries the resolve-time stamp in the redo verbatim (an 8-tuple) and
//! applies it into the versioned store on replay, independent of `doc_configs`.
//! The same stamp is used by the commit-time base install and by replay, so a
//! normal restart does NOT write a second version of the row.
//!
//! ## Strict shares this code path
//!
//! A `document_strict` bitemporal collection resolves through the SAME
//! `serialize_document_collection` / `apply_point_put` path: the resolver
//! decodes the strict Binary Tuple back to MessagePack and carries the identical
//! three stamp fields (`sys_from`, `valid_from_ms`, `valid_until_ms`), and
//! the replay decoder / versioned install are storage-mode agnostic. The
//! schemaless coverage below therefore exercises the exact stamp-carrying
//! mechanism the strict path relies on; strict is intentionally NOT re-tested
//! here because the strict READ-BACK depends on a separate, pre-existing concern
//! (strict document replay stores MessagePack when `doc_configs` is empty at
//! replay, since the Binary Tuple re-encode needs the schema) that is outside
//! this fix's scope.

use crate::harness::TestServer;

/// Create a schemaless `bitemporal=true` document collection with an explicit
/// `id` primary key and a `value` column.
async fn create_bitemporal_schemaless(srv: &TestServer, name: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {name} (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_schemaless', bitemporal=true)"
    ))
    .await
    .unwrap();
}

/// All rows a current SELECT returns, as `(id, value)` pairs, sorted by id.
async fn current_rows(srv: &TestServer, coll: &str) -> Vec<(String, String)> {
    let rows = srv
        .query_rows(&format!("SELECT id, value FROM {coll}"))
        .await
        .unwrap();
    let mut out: Vec<(String, String)> = rows
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect();
    out.sort();
    out
}

/// All `(id, value)` a point `AS OF SYSTEM TIME <ms>` read returns, sorted.
async fn rows_as_of(srv: &TestServer, coll: &str, sys_ms: i64) -> Vec<(String, String)> {
    let rows = srv
        .query_rows(&format!(
            "SELECT id, value FROM {coll} AS OF SYSTEM TIME {sys_ms}"
        ))
        .await
        .unwrap();
    let mut out: Vec<(String, String)> = rows
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect();
    out.sort();
    out
}

/// The `_ts_system` stamps of every version the audit query
/// (`AS OF SYSTEM TIME NULL`) surfaces for the collection, sorted.
async fn audit_system_stamps(srv: &TestServer, coll: &str) -> Vec<i64> {
    let rows = srv
        .query_named_rows(&format!("SELECT * FROM {coll} AS OF SYSTEM TIME NULL"))
        .await
        .unwrap();
    let mut stamps: Vec<i64> = rows
        .iter()
        .filter_map(|r| r.get("_ts_system").and_then(|s| s.parse::<i64>().ok()))
        .collect();
    stamps.sort_unstable();
    stamps
}

/// WAL-only restart: shut the server down cleanly and reopen against the same
/// data directory (no checkpoint) — `doc_configs` is empty when replay runs,
/// the exact boot ordering the fix has to survive.
async fn wal_only_restart(srv: TestServer) -> TestServer {
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    srv2
}

/// THE CORE REGRESSION. `BEGIN; INSERT; COMMIT` into a bitemporal schemaless
/// collection; capture the committed `_ts_system`; WAL-only restart; then the
/// row is visible to a CURRENT read AND to `AS OF SYSTEM TIME <captured>`, and
/// the audit log holds EXACTLY ONE version (no duplicate from a mismatched
/// replay stamp, no orphan plain row).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_insert_bitemporal_survives_wal_only_restart() {
    let srv = TestServer::start().await;
    create_bitemporal_schemaless(&srv, "bt_txr").await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO bt_txr (id, value) VALUES ('r1', 'v1')")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    // Pre-restart: current read sees the committed row.
    assert_eq!(
        current_rows(&srv, "bt_txr").await,
        vec![("r1".to_string(), "v1".to_string())],
        "PRE-RESTART current read must see the committed transactional INSERT"
    );

    // Capture the committed system-time stamp via the audit query. Exactly one
    // version must exist before restart too.
    let pre_stamps = audit_system_stamps(&srv, "bt_txr").await;
    assert_eq!(
        pre_stamps.len(),
        1,
        "PRE-RESTART audit must show exactly one version, got {pre_stamps:?}"
    );
    let committed_ts = pre_stamps[0];

    let srv2 = wal_only_restart(srv).await;

    // Post-restart: current read still sees the row (versioned store, not an
    // orphaned plain row).
    assert_eq!(
        current_rows(&srv2, "bt_txr").await,
        vec![("r1".to_string(), "v1".to_string())],
        "post-restart current read must return the row from the versioned store — \
         pre-fix the replayed put landed on the plain table and was invisible here"
    );

    // Post-restart: `AS OF SYSTEM TIME <committed_ts>` resolves the row, proving
    // the replay stamp equals the commit-time stamp (a re-derived replay-clock
    // stamp would sit ABOVE this cutoff and the row would be invisible).
    assert_eq!(
        rows_as_of(&srv2, "bt_txr", committed_ts).await,
        vec![("r1".to_string(), "v1".to_string())],
        "post-restart AS OF SYSTEM TIME at the committed stamp must resolve the row"
    );

    // Post-restart: the audit log holds EXACTLY ONE version at the SAME stamp —
    // no second version from a mismatched replay stamp.
    let post_stamps = audit_system_stamps(&srv2, "bt_txr").await;
    assert_eq!(
        post_stamps,
        vec![committed_ts],
        "post-restart audit must hold exactly one version at the committed stamp \
         (no duplicate from a mismatched replay stamp), got {post_stamps:?}"
    );
}

/// A multi-row transactional INSERT into a bitemporal collection: every row
/// survives a WAL-only restart on the versioned store, and each carries exactly
/// one audit version (no per-row duplication on replay).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_multi_insert_bitemporal_survives_wal_only_restart() {
    let srv = TestServer::start().await;
    create_bitemporal_schemaless(&srv, "bt_multi").await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO bt_multi (id, value) VALUES ('a', '1'), ('b', '2'), ('c', '3')")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    let expected = vec![
        ("a".to_string(), "1".to_string()),
        ("b".to_string(), "2".to_string()),
        ("c".to_string(), "3".to_string()),
    ];
    assert_eq!(current_rows(&srv, "bt_multi").await, expected);
    let pre_stamps = audit_system_stamps(&srv, "bt_multi").await;
    assert_eq!(
        pre_stamps.len(),
        3,
        "PRE-RESTART audit must show three versions (one per row), got {pre_stamps:?}"
    );

    let srv2 = wal_only_restart(srv).await;

    assert_eq!(
        current_rows(&srv2, "bt_multi").await,
        expected,
        "post-restart current read must return all three rows from the versioned store"
    );
    let post_stamps = audit_system_stamps(&srv2, "bt_multi").await;
    assert_eq!(
        post_stamps, pre_stamps,
        "post-restart audit must hold the SAME three stamps — no per-row \
         duplication from replay, got {post_stamps:?}"
    );
}
