//! Integration coverage for SQL JOIN correctness.
//!
//! Covers: RIGHT JOIN plan conversion (inline swap), multi-predicate join
//! conditions (non-equi predicates), and NATURAL JOIN handling.

mod common;

use common::pgwire_harness::TestServer;

// ---------------------------------------------------------------------------
// Helper: set up three related tables for join tests
// ---------------------------------------------------------------------------

async fn setup_join_tables(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION j_t1 TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                x INT)",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION j_t2 TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                t1_id TEXT, \
                y INT)",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION j_t3 TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                t2_id TEXT, \
                z INT)",
        )
        .await
        .unwrap();

    // t1 data
    server
        .exec("INSERT INTO j_t1 (id, name, x) VALUES ('a', 'Alice', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO j_t1 (id, name, x) VALUES ('b', 'Bob', 20)")
        .await
        .unwrap();

    // t2 data — references t1
    server
        .exec("INSERT INTO j_t2 (id, t1_id, y) VALUES ('p', 'a', 100)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO j_t2 (id, t1_id, y) VALUES ('q', 'a', 200)")
        .await
        .unwrap();

    // t3 data — references t2, plus one unmatched
    server
        .exec("INSERT INTO j_t3 (id, t2_id, z) VALUES ('x', 'p', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO j_t3 (id, t2_id, z) VALUES ('y', 'q', 2)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO j_t3 (id, t2_id, z) VALUES ('z', 'NONE', 3)")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// RIGHT JOIN with nested join on the left
// ---------------------------------------------------------------------------

/// A RIGHT JOIN where the left side is itself a join. The planner rewrites
/// RIGHT → LEFT by swapping sides, but must also swap inline_left/inline_right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn right_join_with_nested_left_join_returns_correct_rows() {
    let server = TestServer::start().await;
    setup_join_tables(&server).await;

    // t3 has 3 rows. RIGHT JOIN means all t3 rows appear, with NULLs for
    // unmatched left-side (t1 JOIN t2) rows.
    let rows = server
        .query_text(
            "SELECT j_t3.id FROM j_t1 \
             INNER JOIN j_t2 ON j_t1.id = j_t2.t1_id \
             RIGHT JOIN j_t3 ON j_t2.id = j_t3.t2_id",
        )
        .await
        .unwrap();

    // All 3 t3 rows must appear (x, y, z). z has no match in the left join
    // but RIGHT JOIN preserves it.
    assert_eq!(
        rows.len(),
        3,
        "RIGHT JOIN should preserve all right-side rows: got {rows:?}"
    );
}

/// Simple RIGHT JOIN (no nested join) as a baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_right_join_preserves_right_rows() {
    let server = TestServer::start().await;
    setup_join_tables(&server).await;

    let rows = server
        .query_text(
            "SELECT j_t3.id FROM j_t2 \
             RIGHT JOIN j_t3 ON j_t2.id = j_t3.t2_id",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        3,
        "simple RIGHT JOIN should return all 3 t3 rows: got {rows:?}"
    );
}

// ---------------------------------------------------------------------------
// Multiple non-equi predicates in JOIN ON
// ---------------------------------------------------------------------------

/// All non-equi predicates in a JOIN ON clause must be preserved.
/// Only keeping the first one silently drops filter conditions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_preserves_all_non_equi_predicates() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION jn_left TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                x INT, \
                y INT)",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION jn_right TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                x INT, \
                y INT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO jn_left (id, x, y) VALUES ('L1', 10, 5)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO jn_left (id, x, y) VALUES ('L2', 10, 50)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO jn_right (id, x, y) VALUES ('R1', 10, 20)")
        .await
        .unwrap();

    // Both non-equi conditions must apply:
    // L1: x=10 matches R1.x=10, L1.x(10) > R1.x(10) is FALSE → no match
    // Actually let me redesign: equi on id won't work. Let's use a simpler setup.
    // L1: x=10, y=5. R1: x=10, y=20.
    // Condition: jn_left.x = jn_right.x AND jn_left.y < jn_right.y AND jn_left.y > 0
    // L1 matches: x=x, 5 < 20, 5 > 0 → yes
    // L2 matches: x=x, 50 < 20 is FALSE → no

    let rows = server
        .query_text(
            "SELECT jn_left.id FROM jn_left \
             JOIN jn_right ON jn_left.x = jn_right.x \
             AND jn_left.y < jn_right.y \
             AND jn_left.y > 0",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "only L1 should match all three join conditions: got {rows:?}"
    );
    assert!(
        rows[0].contains("L1"),
        "matched row should be L1: got {:?}",
        rows[0]
    );
}

/// If the second non-equi predicate is dropped, L2 would incorrectly appear.
/// This is a regression guard: assert L2 is NOT in the result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_does_not_drop_second_non_equi_predicate() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION jd_a TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                val INT, \
                lo INT, \
                hi INT)",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION jd_b TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                val INT, \
                bound INT)",
        )
        .await
        .unwrap();

    // A: val=1, lo=0, hi=100. B: val=1, bound=50.
    // Condition: a.val = b.val AND a.lo < b.bound AND a.hi > b.bound
    // → 0 < 50 AND 100 > 50 → match.
    server
        .exec("INSERT INTO jd_a (id, val, lo, hi) VALUES ('a1', 1, 0, 100)")
        .await
        .unwrap();
    // A2: val=1, lo=0, hi=10.
    // → 0 < 50 AND 10 > 50 → no match (second non-equi fails).
    server
        .exec("INSERT INTO jd_a (id, val, lo, hi) VALUES ('a2', 1, 0, 10)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO jd_b (id, val, bound) VALUES ('b1', 1, 50)")
        .await
        .unwrap();

    let rows = server
        .query_text(
            "SELECT jd_a.id FROM jd_a \
             JOIN jd_b ON jd_a.val = jd_b.val \
             AND jd_a.lo < jd_b.bound \
             AND jd_a.hi > jd_b.bound",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1, "only a1 should match: got {rows:?}");
    assert!(
        rows[0].contains("a1"),
        "matched row should be a1: got {:?}",
        rows[0]
    );
    // Regression guard: if the second non-equi (`hi > bound`) was dropped,
    // a2 would also appear.
    assert!(
        !rows.iter().any(|r| r.contains("a2")),
        "a2 should NOT match — hi=10 is not > bound=50: got {rows:?}"
    );
}

// ---------------------------------------------------------------------------
// NATURAL JOIN
// ---------------------------------------------------------------------------

/// NATURAL JOIN should either compute the shared column set and produce a
/// correct equi-join, or return an explicit unsupported error. It must NOT
/// silently produce a cartesian product.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn natural_join_is_not_cartesian() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION nat_a TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                shared_col TEXT)",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION nat_b TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                shared_col TEXT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO nat_a (id, shared_col) VALUES ('a1', 'x')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO nat_a (id, shared_col) VALUES ('a2', 'y')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO nat_b (id, shared_col) VALUES ('b1', 'x')")
        .await
        .unwrap();

    let result = server
        .query_text("SELECT nat_a.id FROM nat_a NATURAL JOIN nat_b")
        .await;

    match result {
        Ok(rows) => {
            // If NATURAL JOIN is supported, shared_col='x' should produce
            // a match on a1↔b1 only. Cartesian would give 2 rows.
            assert!(
                rows.len() <= 1,
                "NATURAL JOIN should match on shared_col, not produce cartesian ({} rows): {rows:?}",
                rows.len()
            );
        }
        Err(msg) => {
            // An explicit "unsupported" error is acceptable.
            assert!(
                msg.to_lowercase().contains("natural")
                    || msg.to_lowercase().contains("unsupported")
                    || msg.to_lowercase().contains("not supported"),
                "error should mention NATURAL JOIN: {msg}"
            );
        }
    }
}
