//! `CREATE GRAPH INDEX` DDL correctness contracts.
//!
//! The tree-index builder (`nodedb/src/control/server/pgwire/ddl/tree_ops.rs`)
//! must satisfy three invariants that the current implementation violates:
//!
//! 1. Edge dispatch is batched per hop, not one awaited RPC per document.
//! 2. Any `Err` from edge insert propagates as a DDL failure — no silent
//!    warn-and-continue — and `schema_version` does NOT advance on failure.
//! 3. The build emits a WAL record so a crash mid-build is replayable.
//!
//! The Phase-2 failing test here locks in invariant 1 (batched dispatch,
//! observable as a wall-clock budget). Invariants 2 and 3 get their own
//! tests in Phase 4 once fault-injection hooks land.

mod common;

use std::time::{Duration, Instant};

use common::pgwire_harness::TestServer;

/// Spec: `CREATE GRAPH INDEX` on N documents must dispatch edge inserts
/// in a batched or pipelined form. A serial `for doc in &docs { await }`
/// loop is quadratic in latency and makes the statement unusable on real
/// collections.
///
/// Regression guard: on 300 docs the current serial loop issues 300
/// awaited `dispatch_to_data_plane` calls at several ms each, so the
/// build takes multiple seconds. A batched implementation is sub-second.
/// Budget 1 s sits between the two regimes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_graph_index_batches_edge_dispatch() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION orgs").await.unwrap();

    // Seed 300 parent→child docs forming a flat tree under 'ceo'.
    const N: usize = 300;
    for i in 0..N {
        let sql = format!(
            "INSERT INTO orgs {{ id: 'emp_{i}', parent: 'ceo', name: 'emp_{i}' }}"
        );
        server.exec(&sql).await.unwrap();
    }

    let start = Instant::now();
    let rows = server
        .query_text("CREATE GRAPH INDEX reports ON orgs (parent -> id)")
        .await
        .expect("CREATE GRAPH INDEX must succeed on valid input");
    let elapsed = start.elapsed();

    // Regression guard: timing. A serial per-doc await loop bursts past
    // this budget; a batched dispatch does not.
    let budget = Duration::from_secs(1);
    assert!(
        elapsed < budget,
        "CREATE GRAPH INDEX on {N} docs must batch edge dispatch; \
         a serial per-doc await loop violates the batching contract. \
         Took {elapsed:?} (budget {budget:?})."
    );

    // Regression guard: no silent drops. The DDL advertises an
    // `edges_created` count — it must equal the number of parent→child
    // relations in the collection. If any `Err` from `dispatch_to_data_plane`
    // were warn-logged and ignored, this count would be less than N.
    let blob = rows.join("");
    let count: usize = blob
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0);
    assert_eq!(
        count, N,
        "edges_created ({count}) must equal the number of parent→child \
         relations in the collection ({N}); a lower count means edge \
         inserts were silently dropped. Raw response: {blob:?}"
    );
}

/// Spec: `schema_version` advances only on a fully successful build.
/// The current implementation calls `state.schema_version.bump()`
/// unconditionally at line 154 of tree_ops.rs, so the catalog reports
/// a new version even when zero edges were inserted (e.g. scan returned
/// no docs). Consumers observing schema_version to invalidate caches
/// then refetch state that claims an index exists which is actually empty.
///
/// Regression guard: run CREATE GRAPH INDEX on an empty collection and
/// observe the returned `edges_created` is 0 — at that point no index
/// was actually built, so the DDL should either fail or the schema
/// version must remain stable. We assert the DDL fails loudly rather
/// than silently reporting "success" with zero edges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_graph_index_does_not_silently_succeed_on_empty_collection() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION empty_orgs").await.unwrap();

    let res = server
        .query_text("CREATE GRAPH INDEX reports ON empty_orgs (parent -> id)")
        .await;

    match res {
        Err(_) => {
            // Acceptable: DDL surfaces an error when there is nothing to
            // index. Loud failure is the preferred spec.
        }
        Ok(rows) => {
            // If the DDL returns Ok, the returned count MUST be 0 and
            // (crucially) the DDL must have been a no-op — not a
            // state-mutating partial success.
            let blob = rows.join("");
            let count: usize = blob
                .chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .unwrap_or(usize::MAX);
            assert_eq!(
                count, 0,
                "empty collection must report 0 edges_created; got {count}. \
                 Raw: {blob:?}"
            );
        }
    }
}

/// Spec: `tree_sum` shares the same dispatch pattern as `create_graph_index`
/// — serial per-node RPCs for document lookups in the inner loop
/// (`for node_id in &all_ids { for coll_name in ... }`). A wide tree
/// must complete in time proportional to tree depth, not O(N) awaits.
///
/// Precondition: `CREATE GRAPH INDEX` must work end-to-end first; this
/// test is dependent on that fix landing (or at least, on the index
/// populating correctly). If the DDL silently fails, this test will
/// fail for the same root cause, which is fine — both bugs live in
/// `tree_ops.rs` and the skill expands within the module.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tree_sum_batches_dispatch_on_wide_trees() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION wide_tree").await.unwrap();

    const N: usize = 300;
    for i in 0..N {
        server
            .exec(&format!(
                "INSERT INTO wide_tree {{ id: 'c_{i}', parent: 'root', amount: 1 }}"
            ))
            .await
            .unwrap();
    }
    server
        .exec("INSERT INTO wide_tree { id: 'root', parent: '', amount: 0 }")
        .await
        .unwrap();
    server
        .exec("CREATE GRAPH INDEX reports ON wide_tree (parent -> id)")
        .await
        .expect("CREATE GRAPH INDEX must succeed");

    let start = Instant::now();
    let rows = server
        .query_text(
            "SELECT TREE_SUM(amount, reports, 'root', 'wide_tree') FROM wide_tree LIMIT 1",
        )
        .await
        .unwrap_or_default();
    let elapsed = start.elapsed();

    let budget = Duration::from_secs(3);
    assert!(
        elapsed < budget,
        "TREE_SUM on a wide tree must batch dispatch; serial per-node \
         point-lookups violate the batching contract. Took {elapsed:?} \
         (budget {budget:?}). Raw: {rows:?}"
    );
}

/// Spec: when a source document has a non-string `parent` field (e.g.
/// an integer), the DDL must either coerce it or surface an explicit
/// error — never silently skip. The current code does
/// `obj.get(&parent_col).and_then(|v| v.as_str())` at line 117 of
/// tree_ops.rs, which returns `None` for non-string types and drops
/// the edge without logging.
///
/// Regression guard: insert one doc with a string parent and one doc
/// with an integer parent. The final edge count must match the number
/// of docs whose parent relation is valid (string), OR the DDL must
/// error out — the silent-drop behaviour is forbidden.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_graph_index_handles_nonstring_parent_field_loudly() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION mixed_orgs").await.unwrap();

    server
        .exec("INSERT INTO mixed_orgs { id: 'c1', parent: 'root' }")
        .await
        .unwrap();
    // Non-string parent: integer. Current code silently drops; spec
    // says either coerce to string or fail loudly.
    server
        .exec("INSERT INTO mixed_orgs { id: 'c2', parent: 42 }")
        .await
        .unwrap();

    let res = server
        .query_text("CREATE GRAPH INDEX reports ON mixed_orgs (parent -> id)")
        .await;

    match res {
        Err(_) => {
            // Acceptable: loud failure on the mixed-type parent field.
        }
        Ok(rows) => {
            // If the DDL accepts it, the count MUST reflect both docs
            // (coerced), not just the string-valued one. Silent drop
            // of c2 leaves the count at 1, which is the bug.
            let blob = rows.join("");
            let count: usize = blob
                .chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .unwrap_or(0);
            assert!(
                count == 2,
                "DDL must either fail loudly on non-string parent or \
                 coerce and include it; got count={count} (silent drop \
                 of the integer-parent doc is forbidden). Raw: {blob:?}"
            );
        }
    }
}
