// SPDX-License-Identifier: BUSL-1.1

//! An interactive `BEGIN; <cross-shard KV + vector writes>; COMMIT` block
//! COMMITS through the Calvin sequencer, and the committed writes survive a
//! WAL-only restart via the replayable `TransactionRedo` WAL record (see
//! `vector_index_txn_restart.rs` for the single-shard analogue).
//!
//! 1. Two collections — a KV collection and a vector-indexed document
//!    collection — are created on DIFFERENT vShards (`distinct_vshard_
//!    collections`, same technique as `calvin_cluster_pgwire_e2e.rs`).
//! 2. `BEGIN; INSERT INTO <kv>; INSERT INTO <vecdocs>; COMMIT` is sent as ONE
//!    `simple_query` call. tokio-postgres ships this as a single wire
//!    message; the server buffers the two INSERTs during the transaction and,
//!    on COMMIT, `classify_dispatch` sees writes on two vShards → MultiShard.
//!    The neutral commit orchestrator flushes the whole batch through the
//!    leader-routed Calvin submit-and-await, which commits it durably.
//! 3. A WAL-only restart (no vector checkpoint) reopens the same data
//!    directory. The base KV row and the document row survive via redb; the
//!    in-memory HNSW vector graph has no other durable backing and is rebuilt
//!    by replaying the committed transaction's engine-native redo records — so
//!    a post-restart vector search must still return the inserted document.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};
use tokio_postgres::SimpleQueryMessage;

use common::cluster_harness::{TestClusterNode, wait_for};

/// Observed sequencer-group leader id from a node's local Raft status, or `0`
/// if no leader is known yet. Same shape as the sibling `single_node_calvin_*`
/// suite.
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

/// Count of transactions the single-node sequencer has admitted to an epoch,
/// or `0` if the sequencer metrics handle is not installed yet.
fn admitted_total(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// A `(kv_name, vec_name)` pair of collection names whose vShard ids differ,
/// so a transaction writing to both is genuinely multi-shard. Deterministic:
/// `VShardId::from_collection_in_database` is a pure function of the database
/// id + collection name bytes. Same technique as
/// `calvin_cluster_pgwire_e2e.rs::two_distinct_vshard_collections`.
fn distinct_vshard_collections() -> (String, String) {
    let kv_name = "cmr_kv".to_string();
    let vkv = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &kv_name).as_u32();
    for i in 0u32..512 {
        let vec_name = format!("cmr_vecdocs_{i}");
        if VShardId::from_collection_in_database(DatabaseId::DEFAULT, &vec_name).as_u32() != vkv {
            return (kv_name, vec_name);
        }
    }
    panic!(
        "could not find a vector-doc collection name on a distinct vShard from \
         the KV collection in 512 tries"
    );
}

/// Single-row `col` value for `id` in a KV/document collection, or `None` if
/// not visible.
async fn value_of(
    client: &tokio_postgres::Client,
    coll: &str,
    col: &str,
    id: &str,
) -> Option<String> {
    let msgs = client
        .simple_query(&format!("SELECT {col} FROM {coll} WHERE id = '{id}'"))
        .await
        .expect("SELECT by id");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(col).map(str::to_owned),
        _ => None,
    })
}

/// Nearest-neighbour `id` to `axis` on the collection's vector index (`None`
/// when the index has no reachable rows).
async fn nearest_id(client: &tokio_postgres::Client, coll: &str, axis: [f32; 3]) -> Option<String> {
    let msgs = client
        .simple_query(&format!(
            "SELECT id FROM {coll} \
             ORDER BY vector_distance(embedding, ARRAY[{},{},{}]) LIMIT 1",
            axis[0], axis[1], axis[2]
        ))
        .await
        .expect("vector search");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("id").map(str::to_owned),
        _ => None,
    })
}

/// A multi-shard write (KV row + vector-indexed document row) buffered inside
/// an explicit `BEGIN ... COMMIT` block COMMITS through Calvin, and both the KV
/// base row and the vector-indexed document row (plus its HNSW entry) survive a
/// WAL-only restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_multi_shard_write_in_explicit_block_commits_and_survives_restart() {
    // The node's own data directory (kept alive across both bring-ups).
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_dir_path = data_dir.path().to_path_buf();

    // 4 Data-Plane cores so distinct vShards land on distinct cores — a
    // genuine cross-core, cross-shard write.
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir_path.clone())
        .await
        .expect("spawn standalone single-node-calvin server on path");

    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    let (kv, vecdocs) = distinct_vshard_collections();

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {kv} (id TEXT PRIMARY KEY, v INT) WITH (engine='kv')"
        ))
        .await
        .expect("CREATE COLLECTION kv");
    node.client
        .simple_query(&format!("CREATE COLLECTION {vecdocs} TYPE document"))
        .await
        .expect("CREATE COLLECTION vecdocs");
    node.client
        .simple_query(&format!(
            "CREATE VECTOR INDEX idx_{vecdocs} ON {vecdocs} (embedding) METRIC cosine DIM 3"
        ))
        .await
        .expect("CREATE VECTOR INDEX");
    wait_for(
        "both collections visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 2,
    )
    .await;

    // Strict cross-shard mode so COMMIT's multi-shard path routes through
    // Calvin (mirrors `calvin_cluster_pgwire_e2e.rs`).
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    let admitted_before = admitted_total(&node);

    // ONE `simple_query` call carrying the whole transaction: the two INSERTs
    // are buffered during the block, and on COMMIT `classify_dispatch` sees the
    // full task set spanning two vShards → MultiShard → leader-routed Calvin
    // flush.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {kv} (id, v) VALUES ('k1', 42); \
         INSERT INTO {vecdocs} (id, body, embedding) VALUES ('d1', 'hello', ARRAY[0.1,0.2,0.3]); \
         COMMIT"
    );
    node.client
        .simple_query(&txn_sql)
        .await
        .expect("interactive cross-shard COMMIT must succeed through the Calvin barrier");

    // The batch reached the sequencer inbox — admitted advanced past baseline.
    wait_for(
        "calvin admitted the committed cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Pre-restart: both rows are visible and the vector is in the live HNSW.
    // The Calvin flush lands asynchronously after the completion ack, so poll.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let kv_v = value_of(&node.client, &kv, "v", "k1").await;
        let near = nearest_id(&node.client, &vecdocs, [0.1, 0.2, 0.3]).await;
        if kv_v.as_deref() == Some("42") && near.as_deref() == Some("d1") {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("pre-restart committed rows not visible within 10s: kv={kv_v:?} vec={near:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // WAL-only restart: shut down cleanly (flushing the WAL, releasing every
    // redb handle) and reopen against the SAME directory — no vector
    // checkpoint, so the HNSW must be rebuilt purely from the committed
    // transaction's replayed redo records.
    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir_path.clone())
        .await
        .expect("reopen standalone single-node-calvin server on the same path");
    wait_for(
        "both collections visible after WAL-only restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 2,
    )
    .await;

    // Post-restart: the KV base row survives (redb), the document row survives
    // (redb), and a vector search near the inserted embedding still returns it
    // — proving the in-memory HNSW was rebuilt from the committed cross-shard
    // transaction's redo records on replay.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let kv_v = value_of(&node.client, &kv, "v", "k1").await;
        let doc_v = value_of(&node.client, &vecdocs, "body", "d1").await;
        let near = nearest_id(&node.client, &vecdocs, [0.1, 0.2, 0.3]).await;
        if kv_v.as_deref() == Some("42")
            && doc_v.as_deref() == Some("hello")
            && near.as_deref() == Some("d1")
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "post-restart survival check failed within 10s: \
                 kv={kv_v:?} doc={doc_v:?} vec={near:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    node.shutdown().await;
}
