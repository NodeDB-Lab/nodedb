// SPDX-License-Identifier: BUSL-1.1
//! Cross-node shuffle PRODUCER (E4a) integration test.
//!
//! Drives the `ShuffleProduce` trigger end-to-end over real QUIC: a coordinator
//! sends a `ShuffleProduceRequest` to a producer node carrying an inline
//! `QueryOp::ProviderScan` scan fragment. The producer node executes the scan
//! through its local streaming executor, hash-partitions each output row on the
//! join key, and fans the rows out to the per-part owners:
//!   - part 0 is owned by the producer node itself → LOOPBACK into its own
//!     receiver registry (no QUIC round-trip to self);
//!   - part 1 is owned by a remote node → a real cross-node `ShufflePush` stream.
//!
//! Asserts that each part's staged frame-file contains EXACTLY the rows whose
//! `partition_hash % num_parts` maps there (both the loopback and the remote
//! part), that every per-part build barrier completed, and that the producer
//! replied with a clean (no-error) `ShuffleProduceResponse`.
//!
//! `ProviderScan` is used as the scan fragment because it carries its rows inline
//! (no collection / storage dependency) yet streams through the SAME Data-Plane
//! dispatch + streaming path the fan-out sink consumes — so the test exercises
//! the real produce pipeline, not a stub.

use crate::common;
use common::cluster_harness::TestCluster;

use std::path::Path;
use std::time::Duration;

use nodedb_cluster::rpc_codec::{RaftRpc, ShuffleProduceResponse};
use nodedb_cluster::{PartNodeEntry, ShuffleProduceRequest};
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

const NUM_PARTS: u32 = 2;

/// Poll `cond` until it returns true or the deadline elapses.
async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A msgpack map row `{ <fields> }` — the per-row shape the staged-file reader
/// and the producer's hash-partitioner operate on.
fn row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert((*k).to_string(), v.clone());
    }
    nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).expect("encode row")
}

/// Wrap `rows` in a single msgpack ARRAY — the `ProviderScan.rows` shape (and
/// the same flat row-array the producer's fan-out re-explodes).
fn msgpack_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    let n = rows.len();
    if n < 16 {
        out.push(0x90 | n as u8);
    } else if n <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for r in rows {
        out.extend_from_slice(r);
    }
    out
}

/// Parse a staged `[u32 LE len][row-bytes]` frame file into per-row byte vectors
/// (the format the Data Plane's `FrameStreamReader` consumes).
fn read_frames(path: &Path) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(path).expect("read staged frame file");
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("len")) as usize;
        pos += 4;
        assert!(pos + len <= bytes.len(), "frame body truncated");
        out.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    assert_eq!(pos, bytes.len(), "trailing bytes after last frame");
    out
}

/// The partition a row routes to: `partition_hash(row, keys) % NUM_PARTS`.
/// Uses the SAME shared hash the producer uses, so the test's expectations and
/// the producer's routing cannot drift.
fn expected_part(row: &[u8]) -> u32 {
    (nodedb_query::partition_hash(row, &["k"]) % NUM_PARTS as u64) as u32
}

/// A `ProviderScan` plan that streams `rows` inline (no collection needed).
fn provider_scan_plan(rows: &[Vec<u8>]) -> Vec<u8> {
    let plan = PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows: msgpack_array(rows),
        filters: Vec::new(),
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    });
    plan_wire::encode(&plan).expect("encode provider scan plan")
}

/// Build-side fixtures spanning BOTH partitions (verified below via
/// `expected_part`), with duplicate keys so a partition genuinely holds several
/// rows.
fn fixtures() -> Vec<Vec<u8>> {
    vec![
        row(&[("k", serde_json::json!(1)), ("v", serde_json::json!("a"))]),
        row(&[("k", serde_json::json!(2)), ("v", serde_json::json!("b"))]),
        row(&[("k", serde_json::json!(3)), ("v", serde_json::json!("c"))]),
        row(&[("k", serde_json::json!(4)), ("v", serde_json::json!("d"))]),
        row(&[("k", serde_json::json!(2)), ("v", serde_json::json!("b2"))]),
    ]
}

/// Partition `rows` by `expected_part` into `NUM_PARTS` buckets, preserving order
/// within each bucket (the producer fans rows out in scan order).
fn partitioned(rows: &[Vec<u8>]) -> Vec<Vec<Vec<u8>>> {
    let mut buckets = vec![Vec::new(); NUM_PARTS as usize];
    for r in rows {
        buckets[expected_part(r) as usize].push(r.clone());
    }
    buckets
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn shuffle_produce_partitions_and_fans_out_across_nodes() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Coordinator = node 0; producer node = node 0 itself (it both runs the scan
    // and owns part 0 via loopback). Remote part owner = node 1.
    let producer = &cluster.nodes[0];
    let remote = &cluster.nodes[1];
    let producer_addr = producer.listen_addr;

    let rows = fixtures();
    let buckets = partitioned(&rows);
    // Sanity: the fixtures must actually exercise BOTH partitions, otherwise the
    // loopback or remote path would be vacuously "correct".
    assert!(
        !buckets[0].is_empty() && !buckets[1].is_empty(),
        "fixtures must span both partitions: part0={} part1={}",
        buckets[0].len(),
        buckets[1].len()
    );

    let shuffle_id = 7001u64;
    let side = 0u8; // build

    let req = ShuffleProduceRequest {
        shuffle_id,
        side,
        num_parts: NUM_PARTS,
        producer_count: 1,
        keys: vec!["k".into()],
        part_node_map: vec![
            PartNodeEntry {
                part: 0,
                node_id: producer.node_id,
            },
            PartNodeEntry {
                part: 1,
                node_id: remote.node_id,
            },
        ],
        plan_bytes: provider_scan_plan(&rows),
        tenant_id: 0,
        database_id: 0,
        deadline_remaining_ms: 10_000,
        trace_id: [0u8; 16],
        descriptor_versions: vec![], // ProviderScan touches no catalog collection
    };

    // The coordinator drives the producer node directly via its QUIC address and
    // awaits the terminal ShuffleProduceResponse.
    let coordinator_transport = producer
        .shared
        .cluster_transport
        .as_ref()
        .expect("coordinator has a cluster transport")
        .clone();

    let resp = coordinator_transport
        .send_rpc_to_addr(producer_addr, RaftRpc::ShuffleProduceRequest(req))
        .await
        .expect("send shuffle produce request");

    match resp {
        RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse { error: None, .. }) => {}
        RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse { error: Some(e), .. }) => {
            panic!("produce returned terminal error: {e:?}")
        }
        other => panic!("expected ShuffleProduceResponse, got {other:?}"),
    }

    // Part 0 = LOOPBACK: staged on the PRODUCER node's own registry.
    let producer_registry = producer.shared.shuffle_registry.clone();
    // Part 1 = REMOTE: staged on node 1's registry by its inbound read-loop.
    let remote_registry = remote.shared.shuffle_registry.clone();

    let staged = wait_until(Duration::from_secs(10), || {
        let p0 = producer_registry.get((shuffle_id, 0, side));
        let p1 = remote_registry.get((shuffle_id, 1, side));
        matches!((&p0, &p1), (Some(a), Some(b)) if a.barrier_complete() && b.barrier_complete())
    })
    .await;
    assert!(
        staged,
        "both the loopback (part 0) and remote (part 1) barriers must fire within 10s"
    );

    let inbox0 = producer_registry
        .get((shuffle_id, 0, side))
        .expect("loopback part-0 inbox");
    let inbox1 = remote_registry
        .get((shuffle_id, 1, side))
        .expect("remote part-1 inbox");

    assert!(inbox0.take_error().is_none(), "clean loopback EOF");
    assert!(inbox1.take_error().is_none(), "clean remote EOF");
    assert_eq!(inbox0.ends_received(), 1, "single producer End for part 0");
    assert_eq!(inbox1.ends_received(), 1, "single producer End for part 1");

    // Each part's staged frame-file must hold EXACTLY the rows that hash there,
    // in scan order — proving both the local-loopback deposit and the cross-node
    // push partition identically to the shared `partition_hash`.
    let staged0 = read_frames(inbox0.staged_path());
    let staged1 = read_frames(inbox1.staged_path());
    assert_eq!(
        staged0, buckets[0],
        "loopback part-0 staged rows must equal the part-0 bucket, in order"
    );
    assert_eq!(
        staged1, buckets[1],
        "remote part-1 staged rows must equal the part-1 bucket, in order"
    );

    // And together they account for every scanned row exactly once.
    let total_staged = staged0.len() + staged1.len();
    assert_eq!(
        total_staged,
        rows.len(),
        "every scanned row is staged exactly once across the two parts"
    );

    cluster.shutdown().await;
}
