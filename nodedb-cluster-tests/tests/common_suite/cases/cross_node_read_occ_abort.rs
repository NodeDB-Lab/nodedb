// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard read-set OCC validation at the interactive cross-shard COMMIT
//! barrier, exercised on a SINGLE single-node-calvin node.
//!
//! ## The hazard this pins
//!
//! An interactive transaction can WRITE several collections and READ another. To
//! exercise the cross-shard OCC read-validation path, the transaction must go
//! through the multi-participant Calvin barrier (`run_commit_calvin`) rather than
//! the single-shard local-WAL `si_conflict_abort` fast path. That barrier is the
//! only place that revalidates a read slice on its owning vShard using the real
//! per-shard `read_lsn`, by dispatching a validate-only task to every read-only
//! participant. If a read-only participant is never validated, a stale read
//! commits silently — a non-serializable execution.
//!
//! ## Why a single node reproduces it
//!
//! The bug is cross-vShard / cross-Raft-group, NOT cross-NODE. A single
//! single-node-calvin node with 4 Data-Plane cores leads every data group
//! locally, so:
//!
//! * every in-transaction write BUFFERS on the coordinator (no remote
//!   forwarding), and
//! * a transaction can still span multiple vShards (distinct cores), including a
//!   read-only participant vShard.
//!
//! The transaction writes two collections on DISTINCT vShards (`w1`, `w2`) so its
//! write set spans two vShards → participant floor `>= 2` → COMMIT routes through
//! the cross-shard Calvin barrier instead of the single-shard fast path. It also
//! READS a third collection on a distinct vShard (`bread`), making `bread`'s
//! vShard a read-only participant whose captured read slice the barrier must
//! revalidate. A validate task must reach `bread`'s vShard — one that does not
//! reach it lets a stale read commit — so the stale read
//! aborts the COMMIT with SQLSTATE 40001.
//!
//! ## Determinism
//!
//! No fixed sleep governs correctness. The concurrent write to `bread` is issued
//! on a separate connection to the SAME node and confirmed VISIBLE (via an
//! autocommit `SELECT` on a third connection) BEFORE the coordinator's COMMIT is
//! issued. So the in-transaction read is provably stale at commit time — the
//! abort is guaranteed, never racy.
//!
//! These two tests bound the behavior from both sides: one forces a stale read
//! and demands the abort; the other keeps the read current and demands the commit
//! succeeds (proving the barrier is precise, not a blanket abort).
//!
//! File lives in the cluster-tests crate so nextest applies the cluster
//! test-group serialization.

use crate::common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::DatabaseId;
use tokio_postgres::SimpleQueryMessage;

use common::cluster_harness::{TestClusterNode, wait_for, wait_for_async};

/// Observed sequencer-group leader id from the node's local Raft status, or `0`
/// if no leader is known yet. Same shape as the `single_node_calvin_*` suite.
fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

/// Count of transactions the single-node sequencer has admitted to an epoch, or
/// `0` if the sequencer metrics handle is not installed yet.
fn admitted_total(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Three `document_schemaless` collection names whose vShard ids are pairwise
/// distinct, so a transaction that writes two of them and reads the third is
/// genuinely multi-vShard. Deterministic: `VShardId::from_collection`
/// is a pure function of the database id + collection-name bytes, so the same
/// scan picks the same names every run.
fn distinct_vshard_triple() -> (String, String, String) {
    let mut chosen: Vec<(String, u32)> = Vec::new();
    for i in 0u32..1024 {
        let name = format!("occ_shard_{i}");
        let v = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
            .vshard()
            .as_u32();
        if chosen.iter().all(|(_, cv)| *cv != v) {
            chosen.push((name, v));
            if chosen.len() == 3 {
                let mut it = chosen.into_iter().map(|(n, _)| n);
                return (
                    it.next().expect("w1 name"),
                    it.next().expect("w2 name"),
                    it.next().expect("bread name"),
                );
            }
        }
    }
    panic!("could not find three pairwise-distinct-vShard collection names in 1024 tries");
}

/// Extract the SQLSTATE code from a `tokio_postgres` error, or `None` if it is
/// not a structured DB error (e.g. a transport failure).
fn pg_sqlstate(e: &tokio_postgres::Error) -> Option<String> {
    e.as_db_error().map(|db| db.code().code().to_string())
}

/// Human-readable `sqlstate: message` rendering for assertion failure context.
fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// Count `Row` messages in a simple-query result set.
fn count_rows(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// `true` if any returned row's `id` column equals `id`.
fn has_id(msgs: &[SimpleQueryMessage], id: &str) -> bool {
    msgs.iter().any(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("id") == Some(id),
        _ => false,
    })
}

/// Open an additional pgwire connection to the SAME single node, returning the
/// driving client plus the spawned connection task (abort it on teardown).
/// Mirrors the harness's own `tokio_postgres::connect` idiom and the extra-socket
/// pattern in `write_admission_concurrent_same_key_replay.rs`.
async fn open_client(
    node: &TestClusterNode,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        node.pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("open extra pgwire connection to the single node");
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, handle)
}

/// Spawn a 4-core single-node-calvin node, create `w1`/`w2`/`bread` on distinct
/// vShards, seed `bread` with one committed row, and select strict cross-shard
/// mode. Returns the node, its data-dir guard (kept alive for the test), and the
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

    let (w1, w2, bread) = distinct_vshard_triple();
    for coll in [&w1, &w2, &bread] {
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

    // Seed bread with one committed row so the in-txn read observes real data
    // and captures bread's committed read watermark.
    node.client
        .simple_query(&format!(
            "INSERT INTO {bread} (id, value) VALUES ('seed', 'v0')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {bread}: {}", pg_detail(&e)));

    // Strict mode so COMMIT's multi-shard path routes through the Calvin barrier
    // (mirrors `calvin_multi_shard_redo_restart.rs`).
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    (node, data_dir, w1, w2, bread)
}

/// A cross-shard txn that READ `bread` and WROTE `w1`/`w2` (both buffered locally
/// because the single node leads their data groups) must ABORT at COMMIT with
/// SQLSTATE 40001 when a concurrent, confirmed-visible write has advanced
/// `bread`'s version past the captured read. This is the barrier revalidating
/// `bread`'s read-only-participant slice and finding it stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_read_occ_abort_on_stale_read() {
    let (node, _data_dir, w1, w2, bread) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    // ONE simple_query carrying BEGIN + the cross-vShard read + both buffered
    // writes, leaving the transaction OPEN (no COMMIT). The two INSERTs buffer on
    // the coordinator, so at COMMIT the write set spans two vShards → participant
    // floor >= 2 → the multi-participant Calvin barrier; the SELECT registers
    // bread's vShard as a read-only participant with a captured read_lsn.
    let block = format!(
        "BEGIN; \
         SELECT * FROM {bread}; \
         INSERT INTO {w1} (id, value) VALUES ('a', '1'); \
         INSERT INTO {w2} (id, value) VALUES ('b', '2');"
    );
    node.client
        .simple_query(&block)
        .await
        .expect("open cross-shard txn: read bread + buffer writes to w1/w2");

    // Concurrent writer on a SEPARATE connection to the SAME node advances bread
    // past the captured read by inserting a new row.
    let (writer, writer_conn) = open_client(&node).await;
    writer
        .simple_query(&format!(
            "INSERT INTO {bread} (id, value) VALUES ('rival', 'v1')"
        ))
        .await
        .unwrap_or_else(|e| panic!("concurrent write to {bread}: {}", pg_detail(&e)));

    // Confirm the rival row is applied/visible via a THIRD autocommit connection
    // BEFORE issuing COMMIT — this is what makes the stale read (and therefore
    // the abort) deterministic rather than racy.
    let (probe, probe_conn) = open_client(&node).await;
    wait_for_async(
        "rival bread row visible cluster-locally before COMMIT",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let msgs = probe
                .simple_query(&format!("SELECT id FROM {bread}"))
                .await
                .expect("probe autocommit read of bread");
            has_id(&msgs, "rival")
        },
    )
    .await;

    // COMMIT must fail: the barrier revalidates bread's read-only-participant
    // slice and finds it stale → serialization failure (SQLSTATE 40001).
    let err = node
        .client
        .simple_query("COMMIT")
        .await
        .expect_err("COMMIT of a stale cross-shard read must abort, not succeed");
    assert_eq!(
        pg_sqlstate(&err).as_deref(),
        Some("40001"),
        "expected serialization_failure (40001) for the stale cross-shard read, got: {}",
        pg_detail(&err)
    );

    // Discriminator: `si_conflict_abort`'s single-shard fast path raises this same
    // SQLSTATE without ever admitting the batch to a sequencer epoch, so the 40001
    // assertion above alone cannot tell "aborted by the multi-participant Calvin
    // barrier revalidating bread's read-only-participant slice" apart from "aborted
    // for some unrelated single-shard reason". Only the barrier path admits the
    // batch, so requiring the admitted count to advance proves the former.
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

/// The precision control: the SAME cross-shard shape (read `bread`, write
/// `w1`/`w2`) with NO concurrent write to `bread` must COMMIT SUCCESSFULLY. The
/// barrier validates `bread`'s read at its real `read_lsn` and finds it still
/// current — proving the abort above is caused by the genuine version advance,
/// not a blanket cross-shard abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_read_occ_commits_when_read_still_current() {
    let (node, _data_dir, w1, w2, bread) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    let block = format!(
        "BEGIN; \
         SELECT * FROM {bread}; \
         INSERT INTO {w1} (id, value) VALUES ('a', '1'); \
         INSERT INTO {w2} (id, value) VALUES ('b', '2');"
    );
    node.client
        .simple_query(&block)
        .await
        .expect("open cross-shard txn: read bread + buffer writes to w1/w2");

    // No concurrent write to bread: the read stays current. COMMIT must succeed
    // through the same MultiShard Calvin barrier.
    node.client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT of a still-current cross-shard read must succeed");

    // The batch reached the sequencer — admitted advanced past baseline, proving
    // the commit went through the multi-participant barrier, not a no-op path.
    wait_for(
        "calvin admitted the committed cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Both committed writes become visible (the redo install lands asynchronously
    // after the completion ack, so poll).
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

    // bread is unchanged: still exactly the single seeded row.
    let rows_bread = node
        .client
        .simple_query(&format!("SELECT id FROM {bread}"))
        .await
        .expect("SELECT bread after commit");
    assert_eq!(
        count_rows(&rows_bread),
        1,
        "bread must still hold exactly the seeded row"
    );

    node.shutdown().await;
}
