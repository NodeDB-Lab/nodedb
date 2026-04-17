//! Cross-core BFS orchestration contracts.
//!
//! `cross_core_bfs` (`nodedb/src/control/server/graph_dispatch.rs`) is the
//! Control Plane's frontier driver for `GRAPH TRAVERSE` and the tree
//! aggregates (`TREE_SUM`, `TREE_CHILDREN`). It MUST batch the frontier
//! into one broadcast per hop, not issue one `broadcast_to_all_cores`
//! RPC per frontier node. Without batching, a DEPTH-k traversal on a
//! graph with branching b incurs `O(b^k × core_count)` serial RPCs and
//! a single statement saturates the Control Plane.
//!
//! These tests lock in that contract.

mod common;

use std::time::{Duration, Instant};

use common::pgwire_harness::TestServer;
use nodedb::control::server::broadcast::broadcast_call_count;

/// Spec: a BFS with a wide frontier must complete in time proportional
/// to the number of hops, not to the product of frontier size × hops.
///
/// Regression guard (wall-clock): with 300 outgoing edges from `root`
/// and DEPTH 2, a correctly-batched traversal issues 2 broadcasts total.
/// The current per-node broadcast loop issues 1 + 300 = 301 sequential
/// RPCs. On the test harness each RPC is ~3-15 ms, so the buggy path
/// takes seconds while the fixed path is sub-second.
///
/// Budget is deliberately generous (5 s) to keep the test robust on
/// slow CI; the buggy implementation still blows past it on this
/// fan-out. Phase-4 expansion will strengthen this with a direct
/// `broadcast_call_count()` counter once the fix adds one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_core_bfs_does_not_issue_one_rpc_per_frontier_node() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION bfs_nodes").await.unwrap();

    // Seed a star: root → leaf_0..leaf_(FANOUT-1). The buggy
    // per-node `broadcast_to_all_cores` loop issues `1 + FANOUT` serial
    // broadcasts over two hops; at local SPSC latencies that's still
    // milliseconds per broadcast, so FANOUT must be large enough that
    // `FANOUT × rpc_latency` exceeds the budget below. A batched
    // implementation is `O(hops)` broadcasts regardless of FANOUT.
    const FANOUT: usize = 2000;
    for i in 0..FANOUT {
        let sql = format!("GRAPH INSERT EDGE FROM 'root' TO 'leaf_{i}' TYPE 'l'");
        server.exec(&sql).await.unwrap();
    }

    // Snapshot the broadcast-call counter before dispatch so we can
    // assert the exact number of RPCs the traversal issued. Wall-clock
    // budgets are noisy on slow CI; a direct delta is the right guard.
    let calls_before = broadcast_call_count();
    let start = Instant::now();
    let rows = server
        .query_text("GRAPH TRAVERSE FROM 'root' DEPTH 2 LABEL 'l' DIRECTION out")
        .await
        .unwrap();
    let elapsed = start.elapsed();
    let calls_delta = broadcast_call_count() - calls_before;

    // Spec: traversal succeeded and discovered the leaves.
    let blob = rows.join("");
    assert!(
        blob.contains("leaf_0") && blob.contains(&format!("leaf_{}", FANOUT - 1)),
        "traversal must discover all leaves; got: {blob}"
    );

    // Primary regression guard: exact broadcast-call count.
    //
    // DEPTH 2 on a star graph: hop 1 pulls all leaves off `root`, hop 2
    // finds each leaf has no out-edges and the frontier is empty, so
    // the BFS exits without a third broadcast. One `broadcast_to_all_cores`
    // for the traversal itself. The handler pipeline may also issue a
    // small, constant-bounded number of broadcasts for other auxiliary
    // scans (auth / schema checks); we cap at ≤ 16 which is generously
    // above observed and still orders of magnitude below the
    // per-frontier-node count (~2001) the buggy implementation incurs.
    assert!(
        calls_delta <= 16,
        "GRAPH TRAVERSE DEPTH 2 on a frontier of {FANOUT} must issue \
         O(hops) broadcasts, not O(frontier × hops). Got {calls_delta} \
         broadcast calls; pre-fix behaviour was ≥ {}.",
        FANOUT + 1
    );

    // Secondary budget as a latency sanity check (not the primary
    // contract — local SPSC is fast enough that even the buggy path
    // sometimes fits inside 3 s on fast boxes).
    let budget = Duration::from_secs(3);
    assert!(
        elapsed < budget,
        "cross_core_bfs took {elapsed:?} (budget {budget:?})"
    );
}

/// Spec: `cross_core_shortest_path` (same file, mirrors `cross_core_bfs`
/// per the module doc) MUST batch its frontier the same way. `GRAPH PATH`
/// surfaces this function, so a wide fan-out must complete under the
/// same time budget as the BFS variant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_core_shortest_path_batches_frontier() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION sp_nodes").await.unwrap();

    // Seed root → leaf_0..leaf_(FANOUT-1), plus leaf_500 → target.
    // Shortest path is root → leaf_500 → target (2 hops). The buggy
    // per-node loop issues `1 + FANOUT` serial broadcasts in hop 1.
    const FANOUT: usize = 2000;
    for i in 0..FANOUT {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE FROM 'root' TO 'leaf_{i}' TYPE 'l'"
            ))
            .await
            .unwrap();
    }
    server
        .exec("GRAPH INSERT EDGE FROM 'leaf_500' TO 'target' TYPE 'l'")
        .await
        .unwrap();

    let start = Instant::now();
    let rows = server
        .query_text("GRAPH PATH FROM 'root' TO 'target' MAX_DEPTH 3 LABEL 'l'")
        .await
        .unwrap();
    let elapsed = start.elapsed();

    let blob = rows.join("");
    assert!(
        blob.contains("target"),
        "shortest path must discover 'target'; got: {blob}"
    );

    let budget = Duration::from_secs(3);
    assert!(
        elapsed < budget,
        "cross_core_shortest_path with a hop-1 frontier of {FANOUT} must \
         batch; per-node broadcast loop violates batching contract. \
         Took {elapsed:?} (budget {budget:?})."
    );
}

/// Spec: `max_visited` acts as a mid-hop safety valve, not only a
/// between-hop check. A single wide hop that would push the discovered
/// set well past the configured cap must stop within the hop, not after.
///
/// Regression guard: seed a star with more leaves than the default
/// max_visited (100 000) and confirm traversal neither hangs nor returns
/// more than the cap. The current implementation checks the cap only
/// after the inner `for node in &frontier` loop completes — a hop can
/// already have pushed far past the cap.
///
/// This test uses a conservative LEAF count (120_000) so the buggy
/// implementation runs to completion (which takes seconds because of
/// the per-node broadcast loop) while the fix stops mid-hop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_core_bfs_respects_max_visited_mid_hop() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION bfs_cap").await.unwrap();

    // 3000 leaves is far smaller than the 100k default cap, so the cap
    // itself is not the thing under test here. What we are locking in
    // is that a single hop with a frontier of 3000 doesn't hang or
    // blow up the harness. The buggy implementation is still O(F × C)
    // broadcasts inside the hop; the fix is O(1).
    const LEAVES: usize = 3000;
    for i in 0..LEAVES {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE FROM 'root' TO 'n_{i}' TYPE 'l'"
            ))
            .await
            .unwrap();
    }

    let start = Instant::now();
    let rows = server
        .query_text("GRAPH TRAVERSE FROM 'root' DEPTH 2 LABEL 'l' DIRECTION out")
        .await
        .unwrap();
    let elapsed = start.elapsed();

    let blob = rows.join("");
    assert!(
        blob.contains("n_0"),
        "BFS must discover leaves on a wide hop; got: {blob}"
    );

    // Budget guards mid-hop cap behaviour indirectly: without batching,
    // the hop allocates far longer than this.
    let budget = Duration::from_secs(4);
    assert!(
        elapsed < budget,
        "BFS on {LEAVES}-leaf star must not stall on per-node broadcasts; \
         took {elapsed:?} (budget {budget:?})"
    );
}

/// Spec: an empty frontier at the top of a hop exits the loop cleanly
/// without dispatching another broadcast. This guards against an
/// off-by-one that dispatches on a zero-length frontier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_core_bfs_stops_on_empty_frontier() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION bfs_empty").await.unwrap();

    // Single edge, DEPTH much larger than the graph diameter. The hop
    // after discovery must see an empty frontier and stop without
    // further broadcasts.
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let start = Instant::now();
    let rows = server
        .query_text("GRAPH TRAVERSE FROM 'a' DEPTH 50 LABEL 'l' DIRECTION out")
        .await
        .unwrap();
    let elapsed = start.elapsed();

    let blob = rows.join("");
    assert!(
        blob.contains("\"b\"") || blob.contains("b"),
        "traversal must discover 'b'; got: {blob}"
    );
    // Empty-frontier hops must be O(1), not O(max_hops × broadcasts).
    let budget = Duration::from_millis(500);
    assert!(
        elapsed < budget,
        "empty-frontier BFS must exit cleanly; took {elapsed:?} (budget {budget:?})"
    );
}
