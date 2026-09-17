// SPDX-License-Identifier: BUSL-1.1
//! Cross-node distributed GROUP BY shuffle CONSUMER (E5b) integration test.
//!
//! Drives a full distributed GROUP BY aggregate end-to-end over real QUIC, the
//! SINGLE-SIDED aggregate sibling of the shuffle-join consume test:
//!
//! 1. PRODUCE the single map side. The coordinator sends TWO
//!    `ShuffleProduceRequest`s (producer_count = 2) for side 0, each carrying an
//!    inline `QueryOp::PartialAggregateState` over a `ProviderScan` of that
//!    producer's rows. Each producer accumulates per-group partial `GroupState`s
//!    and emits one state row per group, hash-partitioned on the GROUP BY key `k`
//!    (`partition_hash % NUM_PARTS`) and fanned out to the per-part owners:
//!    part 0 → node 0 (loopback), part 1 → node 1 (a real cross-node
//!    `ShufflePush` stream). The two producers share overlapping group keys, so
//!    each part-owner's inbox barrier waits for 2 `End`s and merges 2 partials
//!    per shared group.
//!
//! 2. CONSUME each part. The coordinator sends a `ShuffleAggregateConsumeRequest`
//!    to each part-owner; that node waits for its part's single staged side to
//!    finalize, merges the partial states, finalizes, and returns the aggregate
//!    rows.
//!
//! Asserts that the UNION of the two parts' returned rows equals the single-node
//! GROUP BY reference (computed independently in-test by direct arithmetic over
//! the union of ALL produced rows), per-group count/sum/avg/min/max, and that
//! each group key appears in exactly ONE part (co-location).

use crate::common;
use common::cluster_harness::TestCluster;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use nodedb_cluster::rpc_codec::{RaftRpc, ShuffleAggregateConsumeResponse, ShuffleProduceResponse};
use nodedb_cluster::{PartNodeEntry, ShuffleAggregateConsumeRequest, ShuffleProduceRequest};
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec, PhysicalPlan, QueryOp};
use nodedb_types::{QualifiedCollection, Value};

const NUM_PARTS: u32 = 2;
const SIDE_PRODUCER: u8 = 0;

/// A msgpack map row `{ k: <key>, v: <value> }` paired with its decoded fields so
/// the test can compute the reference aggregate without re-decoding.
struct Row {
    key: i64,
    value: i64,
    bytes: Vec<u8>,
}

fn row(key: i64, value: i64) -> Row {
    let bytes = nodedb_types::json_to_msgpack(&serde_json::json!({ "k": key, "v": value }))
        .expect("encode row");
    Row { key, value, bytes }
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

/// The partition a row's group key routes to:
/// `partition_hash(row, ["k"]) % NUM_PARTS`.
fn expected_part(row: &Row) -> u32 {
    (nodedb_query::partition_hash(&row.bytes, &["k"]) % NUM_PARTS as u64) as u32
}

/// The aggregate spec the producer accumulates and the consumer finalizes:
/// count(*), sum(v), avg(v), min(v), max(v). `user_alias` pins the output column
/// names so the test reads them deterministically.
fn agg_specs() -> Vec<AggregateSpec> {
    let spec = |function: &str, field: &str, user_alias: &str| AggregateSpec {
        function: function.to_string(),
        alias: format!("{function}({field})"),
        user_alias: Some(user_alias.to_string()),
        field: field.to_string(),
        expr: None,
    };
    vec![
        spec("count", "*", "cnt"),
        spec("sum", "v", "sm"),
        spec("avg", "v", "av"),
        spec("min", "v", "mn"),
        spec("max", "v", "mx"),
    ]
}

/// A producer plan: `PartialAggregateState` over an inline `ProviderScan` of
/// `rows`, grouping on `k`.
fn producer_plan(rows: &[&Row]) -> Vec<u8> {
    let scan = PhysicalPlan::Query(QueryOp::ProviderScan {
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
    let plan = PhysicalPlan::Query(QueryOp::PartialAggregateState {
        collection: QualifiedCollection::new(nodedb_types::id::DatabaseId::DEFAULT, ""),
        input: Some(Box::new(scan)),
        group_by: vec![GroupKeySpec::column("k")],
        aggregates: agg_specs(),
        filters: Vec::new(),
    });
    plan_wire::encode(&plan).expect("encode producer plan")
}

/// Reference finalized aggregate per group key, computed by direct arithmetic
/// over the union of all produced rows.
struct AggRef {
    cnt: i64,
    sum: i64,
    avg: f64,
    min: i64,
    max: i64,
}

fn reference(rows: &[&Row]) -> HashMap<i64, AggRef> {
    let mut groups: HashMap<i64, Vec<i64>> = HashMap::new();
    for r in rows {
        groups.entry(r.key).or_default().push(r.value);
    }
    groups
        .into_iter()
        .map(|(k, vs)| {
            let cnt = vs.len() as i64;
            let sum: i64 = vs.iter().sum();
            let avg = sum as f64 / cnt as f64;
            let min = *vs.iter().min().expect("non-empty group");
            let max = *vs.iter().max().expect("non-empty group");
            (
                k,
                AggRef {
                    cnt,
                    sum,
                    avg,
                    min,
                    max,
                },
            )
        })
        .collect()
}

/// Send one `ShuffleProduceRequest` (producer_count = 2) for the map side and
/// assert a clean reply.
async fn produce(
    transport: &nodedb_cluster::NexarTransport,
    producer_addr: SocketAddr,
    shuffle_id: u64,
    rows: &[&Row],
    part_node_map: Vec<PartNodeEntry>,
) {
    let req = ShuffleProduceRequest {
        shuffle_id,
        side: SIDE_PRODUCER,
        num_parts: NUM_PARTS,
        producer_count: 2,
        keys: vec!["k".into()],
        part_node_map,
        plan_bytes: producer_plan(rows),
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
            panic!("produce returned terminal error: {e:?}")
        }
        other => panic!("expected ShuffleProduceResponse, got {other:?}"),
    }
}

/// Send a `ShuffleAggregateConsumeRequest` to a part-owner and return its raw
/// aggregate-row array payload.
async fn aggregate_part(
    transport: &nodedb_cluster::NexarTransport,
    owner_addr: SocketAddr,
    shuffle_id: u64,
    part: u32,
) -> Vec<u8> {
    let req = ShuffleAggregateConsumeRequest {
        shuffle_id,
        part,
        group_by: vec!["k".into()],
        aggregates_bytes: zerompk::to_msgpack_vec(&agg_specs()).expect("encode agg specs"),
        having: vec![],
        limit: u64::MAX,
        sort_keys: vec![],
        tenant_id: 0,
        database_id: 0,
        deadline_remaining_ms: 15_000,
        trace_id: [0u8; 16],
    };
    let resp = transport
        .send_rpc_to_addr(owner_addr, RaftRpc::ShuffleAggregateConsumeRequest(req))
        .await
        .expect("send shuffle aggregate consume request");
    match resp {
        RaftRpc::ShuffleAggregateConsumeResponse(ShuffleAggregateConsumeResponse {
            rows,
            error: None,
        }) => rows,
        RaftRpc::ShuffleAggregateConsumeResponse(ShuffleAggregateConsumeResponse {
            error: Some(e),
            ..
        }) => panic!("aggregate (part {part}) returned terminal error: {e:?}"),
        other => panic!("expected ShuffleAggregateConsumeResponse, got {other:?}"),
    }
}

/// Decode a `ShuffleAggregateConsumeResponse.rows` payload (a msgpack array of
/// result-row maps) into `(group_key_k, fields)` pairs. An empty payload is zero
/// rows.
fn decode_rows(bytes: &[u8]) -> Vec<HashMap<String, Value>> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let val = nodedb_types::value_from_msgpack(bytes).expect("decode result rows");
    let arr = match val {
        Value::Array(a) => a,
        other => panic!("expected result rows array, got {other:?}"),
    };
    arr.into_iter()
        .map(|v| match v {
            Value::Object(m) => m,
            other => panic!("expected result row object, got {other:?}"),
        })
        .collect()
}

/// Read a field as i64 (accepts integer or float-with-integral-value).
fn as_i64(m: &HashMap<String, Value>, key: &str) -> i64 {
    match m.get(key) {
        Some(Value::Integer(i)) => *i,
        Some(Value::Float(f)) => *f as i64,
        other => panic!("field `{key}` not an integer: {other:?}"),
    }
}

/// Read a field as f64 (accepts float or integer).
fn as_f64(m: &HashMap<String, Value>, key: &str) -> f64 {
    match m.get(key) {
        Some(Value::Float(f)) => *f,
        Some(Value::Integer(i)) => *i as f64,
        other => panic!("field `{key}` not a number: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn shuffle_aggregate_merges_staged_parts_across_nodes() {
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

    // Two producers sharing overlapping group keys, so each part-owner merges 2
    // partials per shared group. Keys chosen so groups span BOTH parts.
    let producer_a = [row(1, 10), row(1, 20), row(2, 5), row(3, 100), row(4, 7)];
    let producer_b = [row(1, 30), row(2, 15), row(2, 25), row(3, 1), row(5, 9)];

    // Sanity: the group keys must span BOTH parts, else one part's merge would be
    // vacuously empty and the cross-part claim untested.
    let parts_touched: HashSet<u32> = producer_a
        .iter()
        .chain(producer_b.iter())
        .map(expected_part)
        .collect();
    assert_eq!(
        parts_touched.len(),
        2,
        "fixtures must touch both parts; touched={parts_touched:?}"
    );

    let shuffle_id = 9001u64;

    // Coordinator transport (node 0).
    let coordinator = node0
        .shared
        .cluster_transport
        .as_ref()
        .expect("coordinator has a cluster transport")
        .clone();

    let a_refs: Vec<&Row> = producer_a.iter().collect();
    let b_refs: Vec<&Row> = producer_b.iter().collect();

    // 1. PRODUCE both producers into the single map side (producer_count = 2).
    produce(
        &coordinator,
        node0_addr,
        shuffle_id,
        &a_refs,
        part_node_map(),
    )
    .await;
    produce(
        &coordinator,
        node0_addr,
        shuffle_id,
        &b_refs,
        part_node_map(),
    )
    .await;

    // 2. CONSUME each part — wait for the single staged side to finalize, merge
    //    the 2 partials per group, and finalize.
    let part0 = aggregate_part(&coordinator, node0_addr, shuffle_id, 0).await;
    let part1 = aggregate_part(&coordinator, node1_addr, shuffle_id, 1).await;

    let rows0 = decode_rows(&part0);
    let rows1 = decode_rows(&part1);

    // Co-location: each group key appears in exactly ONE part.
    let keys0: HashSet<i64> = rows0.iter().map(|m| as_i64(m, "k")).collect();
    let keys1: HashSet<i64> = rows1.iter().map(|m| as_i64(m, "k")).collect();
    assert!(
        keys0.is_disjoint(&keys1),
        "co-location violated: a group key appears in both parts (part0={keys0:?}, part1={keys1:?})"
    );

    // Reference: single-node GROUP BY over the union of all produced rows.
    let mut union_refs: Vec<&Row> = a_refs.clone();
    union_refs.extend(b_refs.clone());
    let expected = reference(&union_refs);

    // The UNION of both parts' rows must equal the reference, per group.
    let mut got: HashMap<i64, HashMap<String, Value>> = HashMap::new();
    for m in rows0.into_iter().chain(rows1.into_iter()) {
        let k = as_i64(&m, "k");
        assert!(
            got.insert(k, m).is_none(),
            "duplicate group key {k} across parts"
        );
    }

    assert_eq!(
        got.len(),
        expected.len(),
        "group count must match the single-node reference"
    );

    for (k, exp) in &expected {
        let m = got
            .get(k)
            .unwrap_or_else(|| panic!("result missing group {k}"));
        assert_eq!(as_i64(m, "cnt"), exp.cnt, "count(*) mismatch for group {k}");
        assert_eq!(as_i64(m, "sm"), exp.sum, "sum(v) mismatch for group {k}");
        assert!(
            (as_f64(m, "av") - exp.avg).abs() < 1e-9,
            "avg(v) mismatch for group {k}: got {}, expected {}",
            as_f64(m, "av"),
            exp.avg
        );
        assert_eq!(as_i64(m, "mn"), exp.min, "min(v) mismatch for group {k}");
        assert_eq!(as_i64(m, "mx"), exp.max, "max(v) mismatch for group {k}");
    }

    cluster.shutdown().await;
}
