// SPDX-License-Identifier: BUSL-1.1

//! Regression: after a committed transactional DELETE, the PK
//! point-lookup path and the full-scan path must agree — both must report
//! the row gone. A prior bug left the point-lookup index stale while the
//! scan (or vice versa) still reflected the deleted row.
//!
//! The same "point-lookup and scan must agree" invariant is exercised here
//! along two further axes that a divergence bug exposes:
//!   * operand order — `id = 'x'` is answered by a primary-key point-lookup
//!     (`SqlPlan::PointGet`) while `'x' = id` stays a sequential scan; the two
//!     spellings are logically identical and must never select operators that
//!     disagree;
//!   * a restart — a scan-visible row that no point-lookup (nor `COUNT(*)`)
//!     can reach is a ghost tuple, the failure mode where a restart leaves the
//!     PK index diverged from the stored tuples.

use crate::harness::TestServer;

/// Assert that both a PK point-lookup (`WHERE id = ...`) and a full scan
/// (`COUNT(*)`) agree that the given id is absent.
async fn assert_point_and_scan_agree_absent(server: &TestServer, table: &str, id: &str) {
    let point = server
        .query_text(&format!("SELECT id FROM {table} WHERE id = '{id}'"))
        .await
        .unwrap();
    assert!(
        point.is_empty(),
        "PK point-lookup must not see the deleted row '{id}', got {point:?}"
    );

    let scan = server
        .query_rows(&format!("SELECT id FROM {table}"))
        .await
        .unwrap();
    assert!(
        scan.iter().all(|row| row[0] != id),
        "full scan must not see the deleted row '{id}', got {scan:?}"
    );

    let count = server
        .query_text(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap();
    assert_eq!(
        count,
        vec![scan.len().to_string()],
        "COUNT(*) must agree with the scan row count"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_tx_delete_agrees_between_point_lookup_and_scan() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION pk_scan (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();

    // Seed two rows so the scan path has other rows to compare against.
    server
        .exec("INSERT INTO pk_scan (id, val) VALUES ('x', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk_scan (id, val) VALUES ('keep', 2)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM pk_scan WHERE id = 'x'")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_point_and_scan_agree_absent(&server, "pk_scan", "x").await;

    // The untouched row must still be visible on both paths.
    let point_keep = server
        .query_text("SELECT id FROM pk_scan WHERE id = 'keep'")
        .await
        .unwrap();
    assert_eq!(
        point_keep.len(),
        1,
        "untouched row must still be visible via point-lookup, got {point_keep:?}"
    );
    let scan_keep = server.query_rows("SELECT id FROM pk_scan").await.unwrap();
    assert_eq!(
        scan_keep.len(),
        1,
        "scan must show exactly the one remaining row, got {scan_keep:?}"
    );
}

/// A committed row must be reachable identically whether the primary-key
/// equality is written column-first (`id = 'x'`) or literal-first
/// (`'x' = id`). These spellings are logically identical, but the planner
/// answers the column-first form with a primary-key point-lookup
/// (`SqlPlan::PointGet`) and the literal-first form with a sequential scan —
/// two different physical operators. They must never disagree: a row the
/// scan can see must also be reachable through the point-lookup, regardless
/// of operand order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn equality_operand_order_agrees_on_primary_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_order (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk_order (id, val) VALUES ('smoke_32ea5f88', 1)")
        .await
        .unwrap();

    let column_first = server
        .query_text("SELECT id FROM pk_order WHERE id = 'smoke_32ea5f88'")
        .await
        .unwrap();
    let literal_first = server
        .query_text("SELECT id FROM pk_order WHERE 'smoke_32ea5f88' = id")
        .await
        .unwrap();

    assert_eq!(
        column_first,
        vec!["smoke_32ea5f88".to_string()],
        "column-first `id = 'x'` must match the committed row via the point-lookup path"
    );
    assert_eq!(
        literal_first, column_first,
        "literal-first `'x' = id` must return the identical row set as `id = 'x'`; \
         operand order must not route the same predicate to a physical operator that disagrees"
    );
}

/// Operand-order agreement must also hold in the negative: a primary key
/// that matches no row returns empty through both spellings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn equality_operand_order_agrees_when_absent() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_order_absent (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk_order_absent (id, val) VALUES ('present', 1)")
        .await
        .unwrap();

    let column_first = server
        .query_text("SELECT id FROM pk_order_absent WHERE id = 'absent'")
        .await
        .unwrap();
    let literal_first = server
        .query_text("SELECT id FROM pk_order_absent WHERE 'absent' = id")
        .await
        .unwrap();

    assert!(
        column_first.is_empty(),
        "column-first `id = 'absent'` must not match any row, got {column_first:?}"
    );
    assert_eq!(
        literal_first, column_first,
        "literal-first `'absent' = id` must also return empty for an absent key"
    );
}

/// The primary-key point-lookup index and the sequential-scan surface must
/// stay in agreement across a restart. The reporter's ghost tuples arose
/// after restart cycles left the PK index diverged from the stored tuples:
/// the scan returned rows the point-lookup (and `COUNT(*)`) could not reach.
/// After a clean shutdown and WAL-replay reopen, every scanned row must
/// remain reachable by its own primary-key point-lookup, and `COUNT(*)` must
/// equal the scanned row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_lookup_scan_and_count_agree_after_restart() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_restart (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    for i in 0..7u32 {
        server
            .exec(&format!(
                "INSERT INTO pk_restart (id, val) VALUES ('row_{i}', {i})"
            ))
            .await
            .unwrap();
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let scan = server
        .query_rows("SELECT id FROM pk_restart")
        .await
        .unwrap();
    assert_eq!(
        scan.len(),
        7,
        "all 7 rows must survive the restart, got {scan:?}"
    );

    // Every scan-visible row must be reachable via its own PK point-lookup —
    // a scan-visible row unreachable by point-lookup is a ghost tuple.
    for row in &scan {
        let id = &row[0];
        let point = server
            .query_text(&format!("SELECT id FROM pk_restart WHERE id = '{id}'"))
            .await
            .unwrap();
        assert_eq!(
            point,
            vec![id.clone()],
            "scanned row '{id}' must be reachable by primary-key point-lookup after restart"
        );
    }

    let count = server
        .query_text("SELECT COUNT(*) FROM pk_restart")
        .await
        .unwrap();
    assert_eq!(
        count,
        vec![scan.len().to_string()],
        "COUNT(*) must equal the scanned row count after restart, got {count:?}"
    );
}

// ── Minted identity (no declared PRIMARY KEY) ───────────────────────────────
//
// A schemaless collection with no declared `PRIMARY KEY` mints its row
// identity from the surrogate counter. That identity must be one value, not
// one per read path: `RETURNING id`, `SELECT id`, `SELECT *`, the scan
// predicate, the aggregate predicate, and the UPDATE key must all name it the
// same way. The tests above cover the declared-key case only, where the body
// carries `id` and every path reads it from there.

/// Insert one row into a collection with no declared `PRIMARY KEY` and return
/// the id `RETURNING` reports.
async fn insert_minted_row(server: &TestServer, table: &str) -> String {
    let returned = server
        .query_text(&format!(
            "INSERT INTO {table} (v) VALUES ('x') RETURNING id"
        ))
        .await
        .unwrap();
    assert_eq!(
        returned.len(),
        1,
        "RETURNING id must report exactly one id, got {returned:?}"
    );
    returned[0].clone()
}

/// `RETURNING id` and `SELECT id` name the same row, so they must answer the
/// same string. Two encodings of one surrogate is one identity too many.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minted_identity_agrees_between_returning_and_projection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mint_ret (v TEXT)")
        .await
        .unwrap();

    let returned = insert_minted_row(&server, "mint_ret").await;
    let projected = server.query_text("SELECT id FROM mint_ret").await.unwrap();

    assert_eq!(
        projected,
        vec![returned.clone()],
        "SELECT id must answer the same identity RETURNING id reported ('{returned}')"
    );
}

/// The identity a read returns must address the row it came from. A client
/// that reads an id and cannot fetch that row again holds a value that names
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minted_identity_addresses_its_own_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mint_addr (v TEXT)")
        .await
        .unwrap();

    let returned = insert_minted_row(&server, "mint_addr").await;
    let projected = server.query_text("SELECT id FROM mint_addr").await.unwrap();

    for id in [returned.as_str(), projected[0].as_str()] {
        let rows = server
            .query_text(&format!("SELECT v FROM mint_addr WHERE id = '{id}'"))
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec!["x".to_string()],
            "id '{id}' was returned by a read of this row and must address it"
        );
    }
}

/// The scan path and the aggregate path answer the same predicate over `id`
/// identically. A predicate that finds the row on one path and misses it on
/// the other is a silently wrong count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minted_identity_scan_and_aggregate_agree_on_equality() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mint_agg (v TEXT)")
        .await
        .unwrap();

    let returned = insert_minted_row(&server, "mint_agg").await;
    let projected = server.query_text("SELECT id FROM mint_agg").await.unwrap();

    // Both encodings a read has ever produced for this row are probed. Any
    // value that addresses the row on one path must address it on both.
    for id in [returned.as_str(), projected[0].as_str()] {
        let scan = server
            .query_text(&format!("SELECT v FROM mint_agg WHERE id = '{id}'"))
            .await
            .unwrap();
        let count = server
            .query_text(&format!("SELECT count(*) FROM mint_agg WHERE id = '{id}'"))
            .await
            .unwrap();
        assert_eq!(
            count,
            vec![scan.len().to_string()],
            "scan and count(*) must agree on `id = '{id}'`; scan got {scan:?}, count got {count:?}"
        );
    }
}

/// `SELECT *` exposes `id` whenever `SELECT id` resolves it. A projection that
/// drops the row's only identity leaves the client no way to address it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minted_identity_star_projection_includes_id() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mint_star (v TEXT)")
        .await
        .unwrap();

    let returned = insert_minted_row(&server, "mint_star").await;
    let star = server.query_rows("SELECT * FROM mint_star").await.unwrap();

    assert_eq!(star.len(), 1, "one row inserted, got {star:?}");
    assert!(
        star[0].iter().any(|cell| cell == &returned),
        "SELECT * must carry the row's identity '{returned}', got {star:?}"
    );
}

/// A read-then-write round trip addresses the row by the identity the read
/// reported. `UPDATE` keyed on that value touches that row and no other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minted_identity_addresses_its_own_row_for_update() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mint_upd (v TEXT)")
        .await
        .unwrap();

    let returned = insert_minted_row(&server, "mint_upd").await;
    server
        .exec(&format!(
            "UPDATE mint_upd SET v = 'y' WHERE id = '{returned}'"
        ))
        .await
        .unwrap();

    let rows = server.query_text("SELECT v FROM mint_upd").await.unwrap();
    assert_eq!(
        rows,
        vec!["y".to_string()],
        "UPDATE keyed on the id RETURNING reported ('{returned}') must reach the row, got {rows:?}"
    );

    // The update addressed an existing row, so it minted no second one.
    let count = server
        .query_text("SELECT count(*) FROM mint_upd")
        .await
        .unwrap();
    assert_eq!(
        count,
        vec!["1".to_string()],
        "the collection must still hold exactly one row, got {count:?}"
    );
}
