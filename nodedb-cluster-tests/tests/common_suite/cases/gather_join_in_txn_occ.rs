// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard OCC read validation for an in-transaction DISTRIBUTED
//! GATHER-JOIN read, exercised on a single single-node-calvin node.
//!
//! ## What this pins (and how it differs from the shuffle-JOIN suite)
//!
//! A cross-vShard equi-join lowers to `Exchange{Gather}` wrapping a `HashJoin`
//! BY DEFAULT — no `nodedb.force_shuffle_join`, no ANALYZE stats. The coordinator
//! routes the whole `HashJoin` to the LEFT (probe) collection's owning vShard,
//! where the left side is scanned locally, and GATHERS the RIGHT (build)
//! collection across all vShards (`gather_join_build_side`) to inline it as a
//! `ProviderScan`. Each of those two gathers observes its own collection's
//! write version at read time.
//!
//! The GATHER path must not have a serializability HOLE where only the plan's
//! collapsed left collection reaches the read-set (`extract_collection` of an
//! `Exchange{Gather{HashJoin}}` returns the left collection), while the build
//! side's gathered read-version is DISCARDED — that would leave its vShard
//! never validated at commit. A concurrent write to the build collection
//! between the in-txn read and COMMIT must be detected, not silently commit
//! a stale join.
//! The fix threads a per-collection capture accumulator through the Gather
//! resolution so BOTH sides are recorded, mirroring the shuffle path.
//!
//! Three cases bound the fixed behavior:
//!
//!  * `commits_when_neither_side_concurrently_written` — neither join side is
//!    written during the txn, so both recorded real versions still validate and
//!    COMMIT must SUCCEED (proves the captures carry the sound per-collection
//!    write version, not an inflated global watermark that would over-abort).
//!  * `occ_aborts_on_stale_probe_read` — a confirmed-visible concurrent write to
//!    the LEFT/probe collection advances it past the captured probe version, so
//!    COMMIT must abort with 40001.
//!  * `occ_aborts_on_stale_build_read` — THE LOAD-BEARING CASE: a
//!    confirmed-visible concurrent write to the RIGHT/build collection ONLY must
//!    STILL abort with 40001. Pre-fix, no read-set entry existed for the build
//!    collection, so this would have SILENTLY committed. This case proves the
//!    hole is closed on the NATURAL Gather path (no forced shuffle).
//!
//! ## Why a single node reproduces it
//!
//! The OCC hazard is cross-vShard, NOT cross-node. A single single-node-calvin
//! node with 4 Data-Plane cores leads every data group locally AND has the
//! gateway installed, so it BOTH resolves a real `Exchange{Gather{HashJoin}}`
//! (routing the join to the probe vShard and gathering the build collection
//! across vShards) AND routes a cross-shard COMMIT through the multi-participant
//! Calvin barrier that revalidates read-only participants.
//!
//! The transaction WRITES two collections on DISTINCT vShards (`w1`, `w2`) so its
//! write set spans two vShards → participant floor `>= 2` → COMMIT routes through
//! the Calvin barrier rather than the single-shard fast path. It READS two more
//! collections (`left`, `right`, each on a distinct vShard) via the distributed
//! gather join, making both join sides' vShards read-only participants whose
//! captured read slices the barrier revalidates.
//!
//! ## Determinism
//!
//! No fixed sleep governs correctness. Each concurrent write is issued on a
//! separate connection and confirmed VISIBLE (via an autocommit `SELECT` on a
//! third connection) BEFORE the coordinator's COMMIT is issued, so the join read
//! is provably stale at commit time — the abort is guaranteed, never racy.

use crate::common;

use std::time::Duration;

use nodedb::types::DatabaseId;
use tokio_postgres::SimpleQueryMessage;

use common::cluster_harness::{TestClusterNode, wait_for, wait_for_async};
use common::occ_shuffle::{
    admitted_total, count_rows, has_id, open_client, pg_detail, pg_sqlstate, sequencer_leader,
};

/// Four collection names whose vShard ids are pairwise distinct, so a transaction
/// that reads two of them (the join sides) and writes the other two is genuinely
/// multi-vShard on both its read set and its write set. Deterministic:
/// `VShardId::from_collection` is a pure function of the database id +
/// collection-name bytes.
fn distinct_vshard_quad() -> (String, String, String, String) {
    let mut chosen: Vec<(String, u32)> = Vec::new();
    for i in 0u32..2048 {
        let name = format!("gather_join_occ_{i}");
        let v = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
            .vshard()
            .as_u32();
        if chosen.iter().all(|(_, cv)| *cv != v) {
            chosen.push((name, v));
            if chosen.len() == 4 {
                let mut it = chosen.into_iter().map(|(n, _)| n);
                return (
                    it.next().expect("left name"),
                    it.next().expect("right name"),
                    it.next().expect("w1 name"),
                    it.next().expect("w2 name"),
                );
            }
        }
    }
    panic!("could not find four pairwise-distinct-vShard collection names in 2048 tries");
}

/// The distributed equi-join over the seeded `left`/`right` collections (their
/// real generated names — chosen by `distinct_vshard_quad` for vShard placement,
/// so NOT literal "left"/"right"). `left` is the probe side (its column is the
/// LEFT of the `on` pair), `right` is the build side. Lowers to the default
/// `Exchange{Gather{HashJoin}}` plan — NO forced shuffle.
fn join_sql(left: &str, right: &str) -> String {
    format!("SELECT l.id AS lid, r.id AS rid FROM {left} l JOIN {right} r ON l.jk = r.jk")
}

/// Concatenate an `EXPLAIN` result's `QUERY PLAN` rows into one string.
async fn explain_text(client: &tokio_postgres::Client, sql: &str) -> String {
    let msgs = client
        .simple_query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN {sql}: {}", pg_detail(&e)));
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get("QUERY PLAN").map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Spawn a 4-core single-node-calvin node, create `left`/`right` (document_strict,
/// each seeded with three matching join keys) plus `w1`/`w2` on distinct vShards,
/// select strict cross-shard mode, and confirm the join lowers to the DEFAULT
/// distributed gather plan (`Exchange{Gather}`, NOT shuffle). Returns the node,
/// its data-dir guard, and the four collection names `(left, right, w1, w2)`.
async fn spawn_node_with_collections() -> (
    TestClusterNode,
    tempfile::TempDir,
    String,
    String,
    String,
    String,
) {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir.path().to_path_buf())
        .await
        .expect("spawn standalone single-node-calvin server on path");

    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    let (left, right, w1, w2) = distinct_vshard_quad();

    // left/right carry the join rows (document_strict); w1/w2 are the two
    // buffered-write collections that push COMMIT onto the multi-participant
    // Calvin barrier.
    for coll in [&left, &right] {
        node.client
            .simple_query(&format!(
                "CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, jk TEXT, val BIGINT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {coll}: {}", pg_detail(&e)));
    }
    for coll in [&w1, &w2] {
        node.client
            .simple_query(&format!(
                "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, value STRING) \
                 WITH (engine='document_schemaless')"
            ))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {coll}: {}", pg_detail(&e)));
    }
    wait_for(
        "all four collections visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 4,
    )
    .await;

    // Seed left and right with three matching join keys so the join returns real
    // rows AND each side's committed write version is non-zero (gives the
    // stale-read cases a real baseline version to advance past, and the
    // commits-clean case a non-zero version that must still validate).
    node.client
        .simple_query(&format!(
            "INSERT INTO {left} (id, jk, val) VALUES \
             ('l1', 'k1', 10), ('l2', 'k2', 20), ('l3', 'k3', 30)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {left}: {}", pg_detail(&e)));
    node.client
        .simple_query(&format!(
            "INSERT INTO {right} (id, jk, val) VALUES \
             ('r1', 'k1', 1), ('r2', 'k2', 2), ('r3', 'k3', 3)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {right}: {}", pg_detail(&e)));

    // Strict mode so COMMIT's multi-shard path routes through the Calvin barrier.
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // The join MUST default to the distributed GATHER plan (not shuffle): confirm
    // the physical plan carries an `Exchange{Gather}` and NOT a `Shuffle`. If this
    // ever regresses to shuffle in the harness, the test is exercising the wrong
    // path and must fail loudly here rather than silently passing.
    let plan = explain_text(&node.client, &join_sql(&left, &right)).await;
    assert!(
        plan.contains("Exchange") && plan.contains("Gather"),
        "cross-vShard join must lower to Exchange{{Gather}} by default; EXPLAIN was:\n{plan}"
    );
    assert!(
        !plan.contains("Shuffle"),
        "cross-vShard join must NOT be a shuffle join in this test (no force override); \
         EXPLAIN was:\n{plan}"
    );

    // Sanity: the distributed gather join resolves and returns the 3 matched rows
    // in autocommit before any transaction — localizes a plan/resolve regression
    // away from the OCC assertions below. (A read; writes nothing.)
    let joined = node
        .client
        .simple_query(&join_sql(&left, &right))
        .await
        .unwrap_or_else(|e| panic!("autocommit distributed gather join: {}", pg_detail(&e)));
    assert_eq!(
        count_rows(&joined),
        3,
        "distributed gather join must return the 3 matched rows"
    );

    (node, data_dir, left, right, w1, w2)
}

/// Open a cross-shard txn: BEGIN + the distributed gather join read of
/// `left`/`right` + two buffered writes on distinct vShards, leaving the
/// transaction OPEN. The two INSERTs buffer on the coordinator, so at COMMIT the
/// write set spans two vShards → the Calvin barrier; the join SELECT registers
/// BOTH join sides' vShards as read-only participants with captured real read
/// versions.
async fn begin_join_read_and_buffer_writes(
    node: &TestClusterNode,
    left: &str,
    right: &str,
    w1: &str,
    w2: &str,
) {
    let join = join_sql(left, right);
    let block = format!(
        "BEGIN; \
         {join}; \
         INSERT INTO {w1} (id, value) VALUES ('a', '1'); \
         INSERT INTO {w2} (id, value) VALUES ('b', '2');"
    );
    node.client
        .simple_query(&block)
        .await
        .expect("open cross-shard txn: distributed gather join read + buffer writes");
}

/// Neither join side is concurrently written, so both recorded real read versions
/// still validate at the barrier and COMMIT must SUCCEED. Proves the captured
/// read-versions are the sound per-collection write versions — an inflated
/// global watermark would over-abort this clean commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gather_join_commits_when_neither_side_concurrently_written() {
    let (node, _data_dir, left, right, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    begin_join_read_and_buffer_writes(&node, &left, &right, &w1, &w2).await;

    // No concurrent write to either join side: both read slices stay current.
    // COMMIT must succeed through the multi-participant Calvin barrier.
    node.client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "COMMIT of an in-txn distributed gather join whose sides were NOT concurrently \
                 written must succeed; got: {}",
                pg_detail(&e)
            )
        });

    // The batch reached the sequencer — proving the commit went through the
    // multi-participant barrier (which validates read-only participants), not a
    // no-op / single-shard path.
    wait_for(
        "calvin admitted the committed cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Both committed writes become visible (the redo install lands asynchronously).
    wait_for_async(
        "both committed writes visible after COMMIT",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let rows_w1 = node
                .client
                .simple_query(&format!("SELECT id FROM {w1} WHERE id = 'a'"))
                .await
                .expect("SELECT w1 after commit");
            let rows_w2 = node
                .client
                .simple_query(&format!("SELECT id FROM {w2} WHERE id = 'b'"))
                .await
                .expect("SELECT w2 after commit");
            count_rows(&rows_w1) == 1 && count_rows(&rows_w2) == 1
        },
    )
    .await;

    node.shutdown().await;
}

/// A confirmed-visible concurrent write to the LEFT/probe collection advances it
/// past the captured probe read version. COMMIT must abort with SQLSTATE 40001 —
/// the barrier revalidates the probe side's read-only slice and finds it stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gather_join_occ_aborts_on_stale_probe_read() {
    let (node, _data_dir, left, right, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    begin_join_read_and_buffer_writes(&node, &left, &right, &w1, &w2).await;

    // Concurrent writer on a SEPARATE connection advances the LEFT (probe)
    // collection past the captured probe read version by inserting a new row.
    let (writer, writer_conn) = open_client(&node).await;
    writer
        .simple_query(&format!(
            "INSERT INTO {left} (id, jk, val) VALUES ('rival', 'k1', 999)"
        ))
        .await
        .unwrap_or_else(|e| panic!("concurrent write to {left}: {}", pg_detail(&e)));

    // Confirm the rival row is applied/visible via a THIRD autocommit connection
    // BEFORE issuing COMMIT — this makes the stale read (and abort) deterministic.
    let (probe, probe_conn) = open_client(&node).await;
    wait_for_async(
        "rival left-side row visible before COMMIT",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let msgs = probe
                .simple_query(&format!("SELECT id FROM {left}"))
                .await
                .expect("probe autocommit read of left");
            has_id(&msgs, "rival")
        },
    )
    .await;

    let err = node
        .client
        .simple_query("COMMIT")
        .await
        .expect_err("COMMIT of a stale in-txn distributed gather join (probe side) must abort");
    assert_eq!(
        pg_sqlstate(&err).as_deref(),
        Some("40001"),
        "expected serialization_failure (40001) for the stale probe-side read, got: {}",
        pg_detail(&err)
    );

    wait_for(
        "calvin admitted the aborted cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    probe_conn.abort();
    writer_conn.abort();
    node.shutdown().await;
}

/// THE LOAD-BEARING CASE. A confirmed-visible concurrent write to the RIGHT/build
/// collection ONLY must STILL abort the COMMIT with SQLSTATE 40001. Pre-fix, the
/// Gather resolver DISCARDED the build side's gathered read-version and
/// `extract_collection(Exchange{Gather{HashJoin}})` returned only the left
/// collection, so NO read-set entry existed for the build collection — its vShard
/// was never validated at the barrier and this transaction would have SILENTLY
/// committed a stale join result. The abort here proves the build side is now
/// recorded on the NATURAL gather path and the serializability hole is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gather_join_occ_aborts_on_stale_build_read() {
    let (node, _data_dir, left, right, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    begin_join_read_and_buffer_writes(&node, &left, &right, &w1, &w2).await;

    // Concurrent writer advances the RIGHT (build) collection ONLY — the LEFT
    // side stays current. Pre-fix this write went undetected at commit.
    let (writer, writer_conn) = open_client(&node).await;
    writer
        .simple_query(&format!(
            "INSERT INTO {right} (id, jk, val) VALUES ('rival', 'k1', 999)"
        ))
        .await
        .unwrap_or_else(|e| panic!("concurrent write to {right}: {}", pg_detail(&e)));

    // Confirm the rival row is visible via a THIRD autocommit connection BEFORE
    // COMMIT — deterministic staleness of the build side.
    let (probe, probe_conn) = open_client(&node).await;
    wait_for_async(
        "rival build-side row visible before COMMIT",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let msgs = probe
                .simple_query(&format!("SELECT id FROM {right}"))
                .await
                .expect("probe autocommit read of right");
            has_id(&msgs, "rival")
        },
    )
    .await;

    let err = node.client.simple_query("COMMIT").await.expect_err(
        "COMMIT of a stale in-txn distributed gather join (BUILD side) must abort — the hole \
             this closes; pre-fix it silently committed",
    );
    assert_eq!(
        pg_sqlstate(&err).as_deref(),
        Some("40001"),
        "expected serialization_failure (40001) for the stale BUILD-side read (hole closed), \
         got: {}",
        pg_detail(&err)
    );

    // The aborted transaction reached the sequencer via the multi-participant
    // barrier (the only path that revalidates read-only participants), proving the
    // abort is the barrier finding the build side stale — not an unrelated reason.
    wait_for(
        "calvin admitted the aborted cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Belt-and-suspenders: the aborted transaction's buffered write into w1 must
    // have rolled back — an autocommit read shows no 'a' row.
    let rows_w1 = node
        .client
        .simple_query(&format!("SELECT id FROM {w1} WHERE id = 'a'"))
        .await
        .expect("SELECT w1 after abort");
    assert_eq!(
        count_rows(&rows_w1),
        0,
        "the aborted transaction's buffered write into w1 must not be visible"
    );

    probe_conn.abort();
    writer_conn.abort();
    node.shutdown().await;
}
