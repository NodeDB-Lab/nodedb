// SPDX-License-Identifier: BUSL-1.1
//! Cross-node shuffle CONSUMER (E4b) integration test.
//!
//! Drives a full distributed shuffle join end-to-end over real QUIC:
//!
//! 1. PRODUCE both sides. For the BUILD side (side 0) and the PROBE side
//!    (side 1) the coordinator sends a `ShuffleProduceRequest` carrying an inline
//!    `QueryOp::ProviderScan` of that side's rows. Each producer hash-partitions
//!    its rows on the shared key `k` (`partition_hash % NUM_PARTS`) and fans them
//!    out to the per-part owners:
//!    part 0 → node 0 (loopback into its own registry), part 1 → node 1 (a real
//!    cross-node `ShufflePush` stream). Because BOTH sides hash with the SAME
//!    `partition_hash`, a build row and a probe row with equal `k` always land in
//!    the SAME part — so the per-part grace join sees every match (co-location
//!    holds).
//!
//! 2. CONSUME each part. The coordinator sends a `ShuffleConsumeRequest` to each
//!    part-owner; that node waits for both staged sides of its part to finalize,
//!    runs the node-local grace-hash join, and returns the joined rows.
//!
//! Asserts that the UNION of the two parts' returned rows equals the expected
//! INNER join of build ⋈ probe on `k` (computed independently in the test), that
//! matched probe-side data appears in the output, and that a genuinely empty
//! shuffle yields zero join rows on both parts (not an error).

use crate::common;
use common::cluster_harness::TestCluster;

use std::net::SocketAddr;

use nodedb_cluster::rpc_codec::{
    JoinKeyPair, RaftRpc, ShuffleConsumeResponse, ShuffleProduceResponse,
};
use nodedb_cluster::{PartNodeEntry, ShuffleConsumeRequest, ShuffleProduceRequest};
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

const NUM_PARTS: u32 = 2;
const SIDE_BUILD: u8 = 0;
const SIDE_PROBE: u8 = 1;

/// A msgpack map row `{ k: <key>, <label>: <marker> }` paired with its integer
/// key. Pairing the key alongside the bytes lets the test compute the expected
/// join without decoding msgpack (no rmpv dependency).
struct Row {
    key: i64,
    bytes: Vec<u8>,
}

fn row(key: i64, label: &str, marker: &str) -> Row {
    let bytes = nodedb_types::json_to_msgpack(&serde_json::json!({ "k": key, label: marker }))
        .expect("encode row");
    Row { key, bytes }
}

/// Wrap `rows` in a single msgpack ARRAY — the `ProviderScan.rows` shape.
fn msgpack_array(rows: &[&Row]) -> Vec<u8> {
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
        out.extend_from_slice(&r.bytes);
    }
    out
}

/// Count the elements in a msgpack row array (a `ShuffleConsumeResponse.rows`
/// payload). The join encoder emits a fixarray / array16 / array32 header; the
/// element COUNT is all the cardinality assertion needs. An empty payload is
/// zero rows.
fn row_array_len(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }
    let b0 = bytes[0];
    if b0 & 0xf0 == 0x90 {
        (b0 & 0x0f) as usize
    } else if b0 == 0xdc {
        u16::from_be_bytes([bytes[1], bytes[2]]) as usize
    } else if b0 == 0xdd {
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize
    } else {
        panic!("payload does not start with a msgpack array header: {b0:#x}");
    }
}

/// The partition a row routes to: `partition_hash(row, ["k"]) % NUM_PARTS`.
fn expected_part(row: &Row) -> u32 {
    (nodedb_query::partition_hash(&row.bytes, &["k"]) % NUM_PARTS as u64) as u32
}

/// A `ProviderScan` plan that streams `rows` inline (no collection needed).
fn provider_scan_plan(rows: &[&Row]) -> Vec<u8> {
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

/// Expected INNER-join cardinality: one output row per (probe, matching build)
/// pair on equal `k`.
fn expected_inner_pairs(build: &[Row], probe: &[Row]) -> usize {
    let mut pairs = 0usize;
    for p in probe {
        for b in build {
            if b.key == p.key {
                pairs += 1;
            }
        }
    }
    pairs
}

/// Send a `ShuffleProduceRequest` for one side and assert a clean reply.
async fn produce_side(
    transport: &nodedb_cluster::NexarTransport,
    producer_addr: SocketAddr,
    shuffle_id: u64,
    side: u8,
    rows: &[&Row],
    part_node_map: Vec<PartNodeEntry>,
) {
    let req = ShuffleProduceRequest {
        shuffle_id,
        side,
        num_parts: NUM_PARTS,
        producer_count: 1,
        keys: vec!["k".into()],
        part_node_map,
        plan_bytes: provider_scan_plan(rows),
        tenant_id: 0,
        database_id: 0,
        deadline_remaining_ms: 15_000,
        trace_id: [0u8; 16],
        descriptor_versions: vec![],
    };
    let resp = transport
        .send_rpc_to_addr(producer_addr, RaftRpc::ShuffleProduceRequest(req))
        .await
        .expect("send shuffle produce request");
    match resp {
        RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse { error: None, .. }) => {}
        RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse { error: Some(e), .. }) => {
            panic!("produce (side {side}) returned terminal error: {e:?}")
        }
        other => panic!("expected ShuffleProduceResponse, got {other:?}"),
    }
}

/// Send a `ShuffleConsumeRequest` to a part-owner and return its raw joined-row
/// array payload.
async fn consume_part(
    transport: &nodedb_cluster::NexarTransport,
    owner_addr: SocketAddr,
    shuffle_id: u64,
    part: u32,
) -> Vec<u8> {
    let req = ShuffleConsumeRequest {
        shuffle_id,
        part,
        on: vec![JoinKeyPair {
            left: "k".into(),
            right: "k".into(),
        }],
        join_type: "inner".into(),
        limit: u64::MAX,
        probe_qualifier: "l".into(),
        index_qualifier: "r".into(),
        tenant_id: 0,
        database_id: 0,
        deadline_remaining_ms: 15_000,
        trace_id: [0u8; 16],
    };
    let resp = transport
        .send_rpc_to_addr(owner_addr, RaftRpc::ShuffleConsumeRequest(req))
        .await
        .expect("send shuffle consume request");
    match resp {
        RaftRpc::ShuffleConsumeResponse(ShuffleConsumeResponse { rows, error: None }) => rows,
        RaftRpc::ShuffleConsumeResponse(ShuffleConsumeResponse { error: Some(e), .. }) => {
            panic!("consume (part {part}) returned terminal error: {e:?}")
        }
        other => panic!("expected ShuffleConsumeResponse, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn shuffle_consume_joins_staged_parts_across_nodes() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Two part-owners: node 0 (part 0, loopback) and node 1 (part 1, remote).
    // The coordinator drives both producers and both consumers from node 0.
    let node0 = &cluster.nodes[0];
    let node1 = &cluster.nodes[1];
    let node0_addr = node0.listen_addr;
    let node1_addr = node1.listen_addr;

    let part_node_map = || {
        vec![
            PartNodeEntry {
                part: 0,
                node_id: node0.node_id,
            },
            PartNodeEntry {
                part: 1,
                node_id: node1.node_id,
            },
        ]
    };

    // BUILD (right) side and PROBE (left) side. Keys chosen so:
    //  - keys match across sides (k=1, k=2 → join output rows),
    //  - duplicate build key on k=1 (multiset cardinality matters),
    //  - a build-only key (k=9) and a probe-only key (k=7) produce no output.
    let build = vec![
        row(1, "rv", "r1"),
        row(1, "rv", "r1b"),
        row(2, "rv", "r2"),
        row(9, "rv", "r9"),
    ];
    let probe = vec![row(1, "lv", "l1"), row(2, "lv", "l2"), row(7, "lv", "l7")];

    // Sanity: the matching keys must actually span BOTH parts, otherwise one
    // part's join would be vacuously empty and the cross-part claim untested.
    let parts_touched: std::collections::HashSet<u32> = build
        .iter()
        .chain(probe.iter())
        .map(expected_part)
        .collect();
    assert_eq!(
        parts_touched.len(),
        2,
        "fixtures must touch both parts; touched={parts_touched:?}"
    );

    let shuffle_id = 8001u64;

    // Coordinator transport (node 0).
    let coordinator = node0
        .shared
        .cluster_transport
        .as_ref()
        .expect("coordinator has a cluster transport")
        .clone();

    let build_refs: Vec<&Row> = build.iter().collect();
    let probe_refs: Vec<&Row> = probe.iter().collect();

    // 1. PRODUCE both sides — stage build + probe into each part's two inboxes.
    produce_side(
        &coordinator,
        node0_addr,
        shuffle_id,
        SIDE_BUILD,
        &build_refs,
        part_node_map(),
    )
    .await;
    produce_side(
        &coordinator,
        node0_addr,
        shuffle_id,
        SIDE_PROBE,
        &probe_refs,
        part_node_map(),
    )
    .await;

    // 2. CONSUME each part. The consume hook waits for both staged sides of the
    //    part to finalize, then runs the node-local grace join.
    let joined0 = consume_part(&coordinator, node0_addr, shuffle_id, 0).await;
    let joined1 = consume_part(&coordinator, node1_addr, shuffle_id, 1).await;

    // The UNION of the two parts' joined rows must equal the reference inner-join
    // cardinality (one row per (probe, matching build) pair). Co-location
    // guarantees every matching pair lands in exactly one part, so the parts
    // partition the join with no overlap and no loss.
    let total = row_array_len(&joined0) + row_array_len(&joined1);
    let expected = expected_inner_pairs(&build, &probe);
    assert_eq!(
        total,
        expected,
        "union of part joins must equal the reference inner-join cardinality \
         (part0={}, part1={}, expected={expected})",
        row_array_len(&joined0),
        row_array_len(&joined1),
    );

    // Matched probe-side markers must appear in the concatenated output (the
    // inner join emits the left columns for matched rows). k=1 and k=2 match, so
    // `l1` and `l2` must be present across the union; the non-matching `l7` must
    // NOT appear (it has no build match).
    let mut union = joined0.clone();
    union.extend_from_slice(&joined1);
    let contains = |needle: &[u8]| union.windows(needle.len()).any(|w| w == needle);
    assert!(
        contains(b"l1"),
        "matched probe row l1 must be in the output"
    );
    assert!(
        contains(b"l2"),
        "matched probe row l2 must be in the output"
    );
    assert!(
        !contains(b"l7"),
        "unmatched probe row l7 must NOT appear in an inner join"
    );

    // 3. An empty shuffle yields zero join rows on both parts (not an error):
    //    producers `End` every part even with no rows (so both inboxes exist and
    //    finalize), and the consume completes cleanly to an empty result.
    let empty_shuffle = 8002u64;
    produce_side(
        &coordinator,
        node0_addr,
        empty_shuffle,
        SIDE_BUILD,
        &[],
        part_node_map(),
    )
    .await;
    produce_side(
        &coordinator,
        node0_addr,
        empty_shuffle,
        SIDE_PROBE,
        &[],
        part_node_map(),
    )
    .await;
    let empty0 = consume_part(&coordinator, node0_addr, empty_shuffle, 0).await;
    let empty1 = consume_part(&coordinator, node1_addr, empty_shuffle, 1).await;
    assert_eq!(
        row_array_len(&empty0) + row_array_len(&empty1),
        0,
        "an empty shuffle must yield zero join rows across both parts"
    );

    cluster.shutdown().await;
}
