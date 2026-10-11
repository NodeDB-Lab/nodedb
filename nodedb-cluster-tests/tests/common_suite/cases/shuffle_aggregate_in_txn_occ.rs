// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard OCC read validation for an in-transaction DISTRIBUTED
//! shuffle-AGGREGATE read, exercised on a single single-node-calvin node.
//!
//! ## The hazard this pins
//!
//! An interactive transaction can WRITE several collections and READ another via
//! a distributed GROUP BY that the planner lowers to `Exchange{ShuffleAggregate}`.
//! The coordinator fans partial-state PRODUCERs to the source collection's
//! owner(s); each producer scans the collection and reports the scanned
//! collection's write version at read time back on its
//! `ShuffleProduceResponse`. The coordinator max-folds those and records the
//! aggregate's read-set entry with that version.
//!
//! The shuffle-aggregate resolver must not record `WriteVersion::ZERO` as the
//! read version. At the cross-shard COMMIT barrier the read-only participant is
//! revalidated with `write_version(coll) <= read_version`. With a zero read
//! version and any prior write to the collection, the write version is above
//! zero, so the comparison is false → a SPURIOUS serialization abort (SQLSTATE 40001) for a
//! read that was never concurrently written. This suite bounds the behavior from
//! BOTH sides:
//!
//! * `commits_when_not_concurrently_written` — the read collection is NOT written
//!   during the txn, so the recorded real version still validates and COMMIT must
//!   SUCCEED. This is exactly what the ZERO→real-version fix unblocks; before the
//!   fix this direction spuriously aborted with 40001.
//! * `aborts_on_stale_read` — a confirmed-visible concurrent write advances the
//!   read collection past the captured version, so COMMIT must STILL abort with
//!   40001. This guards the fix against degrading into a blanket "always valid".
//!
//! ## Why a single node reproduces it
//!
//! The OCC hazard is cross-vShard, NOT cross-node. A single single-node-calvin
//! node with 4 Data-Plane cores leads every data group locally AND has a cluster
//! transport + the shuffle producer hook installed, so it can BOTH resolve a
//! distributed `Exchange{ShuffleAggregate}` (producer/consumer loop back to the
//! same node over the real RPC path, so the producer-side read-version reporting
//! is genuinely exercised) AND route a cross-shard COMMIT through the
//! multi-participant Calvin barrier that revalidates read-only participants.
//!
//! The transaction WRITES two collections on DISTINCT vShards (`w1`, `w2`) so its
//! write set spans two vShards → participant floor `>= 2` → COMMIT routes through
//! the Calvin barrier rather than the single-shard fast path. It READS a third
//! collection (`metrics`, on a distinct vShard) via a forced distributed GROUP BY,
//! making `metrics`'s vShard a read-only participant whose captured read slice the
//! barrier revalidates.
//!
//! ## Determinism
//!
//! No fixed sleep governs correctness. The concurrent write to `metrics` is issued
//! on a separate connection and confirmed VISIBLE (via an autocommit `SELECT` on a
//! third connection) BEFORE the coordinator's COMMIT is issued, so the aggregate
//! read is provably stale at commit time — the abort is guaranteed, never racy.

use crate::common;

use std::time::Duration;

use nodedb::types::DatabaseId;

use common::cluster_harness::{TestClusterNode, wait_for, wait_for_async};
use common::occ_shuffle::{
    admitted_total, count_rows, has_id, open_client, pg_detail, pg_sqlstate, sequencer_leader,
};

/// Three `metrics`/`w1`/`w2` collection names whose vShard ids are pairwise
/// distinct, so a transaction that writes two of them and reads the third is
/// genuinely multi-vShard. Deterministic: `VShardId::from_collection`
/// is a pure function of the database id + collection-name bytes.
fn distinct_vshard_triple() -> (String, String, String) {
    let mut chosen: Vec<(String, u32)> = Vec::new();
    for i in 0u32..1024 {
        let name = format!("shuffle_occ_{i}");
        let v = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
            .vshard()
            .as_u32();
        if chosen.iter().all(|(_, cv)| *cv != v) {
            chosen.push((name, v));
            if chosen.len() == 3 {
                let mut it = chosen.into_iter().map(|(n, _)| n);
                return (
                    it.next().expect("metrics name"),
                    it.next().expect("w1 name"),
                    it.next().expect("w2 name"),
                );
            }
        }
    }
    panic!("could not find three pairwise-distinct-vShard collection names in 1024 tries");
}

/// The GROUP BY aggregate over the seeded `metrics` collection (its real
/// generated name — the collection name is chosen by `distinct_vshard_triple`
/// for vShard placement, so it is NOT the literal "metrics"). Forced onto the
/// distributed `Exchange{ShuffleAggregate}` path by the session override in setup.
fn agg_sql(metrics: &str) -> String {
    format!("SELECT k, COUNT(*) AS cnt, SUM(v) AS s FROM {metrics} GROUP BY k")
}

/// Spawn a 4-core single-node-calvin node, create `metrics` (document_strict,
/// seeded with low-cardinality GROUP BY rows) plus `w1`/`w2` on distinct vShards,
/// select strict cross-shard mode, and force the distributed shuffle-aggregate
/// plan on the driving connection. Returns the node, its data-dir guard, and the
/// three collection names.
async fn spawn_node_with_collections()
-> (TestClusterNode, tempfile::TempDir, String, String, String) {
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

    let (metrics, w1, w2) = distinct_vshard_triple();

    // metrics carries the aggregated rows (document_strict, matching the proven
    // distributed shuffle-aggregate path); w1/w2 are the two buffered-write
    // collections that push COMMIT onto the multi-participant Calvin barrier.
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {metrics} (id TEXT PRIMARY KEY, k TEXT, v BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {metrics}: {}", pg_detail(&e)));
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
        "all three collections visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 3,
    )
    .await;

    // Seed metrics with committed low-cardinality GROUP BY rows so the aggregate
    // read observes real data AND metrics' committed write version is non-zero
    // (the exact precondition that made the pre-fix ZERO read-version abort).
    node.client
        .simple_query(&format!(
            "INSERT INTO {metrics} (id, k, v) VALUES \
             ('r1', 'a', 10), ('r2', 'a', 20), ('r3', 'b', 5), \
             ('r4', 'b', 15), ('r5', 'c', 100), ('r6', 'c', 300)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {metrics}: {}", pg_detail(&e)));

    // Strict mode so COMMIT's multi-shard path routes through the Calvin barrier.
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");
    // Force the whole-aggregate Exchange{ShuffleAggregate} plan so the in-txn read
    // genuinely goes distributed produce → consume (the path under test), with an
    // explicit small partition count to exercise multi-part fan-out.
    node.client
        .simple_query("SET nodedb.force_shuffle_agg = on")
        .await
        .expect("SET nodedb.force_shuffle_agg = on");
    node.client
        .simple_query("SET nodedb.shuffle_agg_num_parts = 2")
        .await
        .expect("SET nodedb.shuffle_agg_num_parts = 2");

    // Sanity: the forced distributed aggregate resolves and returns the 3 groups
    // in autocommit before any transaction — localizes a plan/resolve regression
    // away from the OCC assertions below. (A read; writes nothing.)
    let groups = node
        .client
        .simple_query(&agg_sql(&metrics))
        .await
        .unwrap_or_else(|e| panic!("autocommit distributed aggregate: {}", pg_detail(&e)));
    assert_eq!(
        count_rows(&groups),
        3,
        "forced distributed shuffle-aggregate must return the 3 seeded groups"
    );

    (node, data_dir, metrics, w1, w2)
}

/// The AGGREGATE read is NOT concurrently written, so its recorded real read
/// version still validates at the barrier and COMMIT must SUCCEED. This is the
/// direction the producer-side ZERO→real read-version fix unblocks: before it,
/// the shuffle-aggregate recorded read version `0`, and the barrier's
/// `write_version(metrics) <= 0` check (metrics has committed writes, so its
/// write version is above zero) spuriously aborted the COMMIT with 40001.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_aggregate_read_commits_when_not_concurrently_written() {
    let (node, _data_dir, metrics, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    // ONE simple_query: BEGIN + the distributed GROUP BY read of metrics + both
    // buffered writes, leaving the transaction OPEN. The two INSERTs buffer on the
    // coordinator, so at COMMIT the write set spans two vShards → the Calvin
    // barrier; the aggregate SELECT registers metrics' vShard as a read-only
    // participant with a captured (real, non-zero) read version.
    let agg = agg_sql(&metrics);
    let block = format!(
        "BEGIN; \
         {agg}; \
         INSERT INTO {w1} (id, value) VALUES ('a', '1'); \
         INSERT INTO {w2} (id, value) VALUES ('b', '2');"
    );
    node.client
        .simple_query(&block)
        .await
        .expect("open cross-shard txn: distributed aggregate read + buffer writes");

    // No concurrent write to metrics: the aggregate read stays current. COMMIT
    // must succeed through the multi-participant Calvin barrier.
    node.client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "COMMIT of an in-txn distributed aggregate read that was NOT concurrently \
                 written must succeed (the producer-side read-version fix); got: {}",
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

/// The precision control: the SAME shape, but a confirmed-visible concurrent write
/// advances `metrics` past the captured aggregate read version. COMMIT must STILL
/// abort with SQLSTATE 40001 — the barrier revalidates metrics' read-only slice
/// at its real read version and finds it stale. Guards the fix against becoming a
/// blanket "always valid".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_aggregate_read_occ_aborts_on_stale_read() {
    let (node, _data_dir, metrics, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    let agg = agg_sql(&metrics);
    let block = format!(
        "BEGIN; \
         {agg}; \
         INSERT INTO {w1} (id, value) VALUES ('a', '1'); \
         INSERT INTO {w2} (id, value) VALUES ('b', '2');"
    );
    node.client
        .simple_query(&block)
        .await
        .expect("open cross-shard txn: distributed aggregate read + buffer writes");

    // Concurrent writer on a SEPARATE connection advances metrics past the
    // captured aggregate read version by inserting a new row.
    let (writer, writer_conn) = open_client(&node).await;
    writer
        .simple_query(&format!(
            "INSERT INTO {metrics} (id, k, v) VALUES ('rival', 'a', 999)"
        ))
        .await
        .unwrap_or_else(|e| panic!("concurrent write to {metrics}: {}", pg_detail(&e)));

    // Confirm the rival row is applied/visible via a THIRD autocommit connection
    // BEFORE issuing COMMIT — this is what makes the stale read (and therefore
    // the abort) deterministic rather than racy.
    let (probe, probe_conn) = open_client(&node).await;
    wait_for_async(
        "rival metrics row visible before COMMIT",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let msgs = probe
                .simple_query(&format!("SELECT id FROM {metrics}"))
                .await
                .expect("probe autocommit read of metrics");
            has_id(&msgs, "rival")
        },
    )
    .await;

    // COMMIT must fail: the barrier revalidates metrics' read-only-participant
    // slice and finds it stale → serialization failure (SQLSTATE 40001).
    let err =
        node.client.simple_query("COMMIT").await.expect_err(
            "COMMIT of a stale in-txn distributed aggregate read must abort, not succeed",
        );
    assert_eq!(
        pg_sqlstate(&err).as_deref(),
        Some("40001"),
        "expected serialization_failure (40001) for the stale distributed-aggregate read, got: {}",
        pg_detail(&err)
    );

    // The aborted transaction reached the sequencer via the multi-participant
    // barrier (the only path that revalidates read-only participants), proving the
    // abort is the barrier finding metrics stale, not an unrelated single-shard
    // reason.
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
