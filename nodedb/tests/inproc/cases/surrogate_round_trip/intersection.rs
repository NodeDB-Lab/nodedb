// SPDX-License-Identifier: BUSL-1.1

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::data::executor::response_codec::decode_payload_to_json;
use nodedb::engine::graph::edge_store::Direction;
use nodedb::types::TenantId;
use nodedb_array::query::slice::Slice as ArraySlice;
use nodedb_array::types::ArrayId;
use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_physical::physical_plan::{ArrayOp, GraphOp, PhysicalPlan, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;
use nodedb_types::{Surrogate, SurrogateBitmap};

use super::harness::{ARR_SURS, G_SRC_SURS, V_SURS, extract_vector_surrogates, send_ok};

// ══════════════════════════════════════════════════════════════════════
// Assertion B: Cross-engine bitmap intersection (Vector ∩ Graph ∩ Array).
//
// Design:
//   bitmap_v  = {10, 11, 12, 13}  (all Vector surrogates)
//   bitmap_g  = {10, 12, 30, 31}  (all Graph src surrogates)
//   bitmap_a  = {10, 13, 60, 61}  (all Array cell surrogates)
//   intersection = bitmap_v ∩ bitmap_g ∩ bitmap_a = {10}
//
// For each engine a query runs with `intersection` as the prefilter
// and the assertions check that only surrogate 10 is returned.
// ══════════════════════════════════════════════════════════════════════
pub(super) fn assert_bitmap_intersection(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    let bitmap_v: SurrogateBitmap = V_SURS.iter().copied().map(Surrogate::new).collect();
    let bitmap_g: SurrogateBitmap = G_SRC_SURS.iter().copied().map(Surrogate::new).collect();
    let bitmap_a: SurrogateBitmap = ARR_SURS.iter().copied().map(Surrogate::new).collect();

    let intersection = bitmap_v.intersect(&bitmap_g).intersect(&bitmap_a);

    assert_eq!(
        intersection.len(),
        1,
        "three-way intersection must contain exactly surrogate 10"
    );
    assert!(
        intersection.contains(Surrogate::new(10)),
        "surrogate 10 must be in the intersection"
    );

    // B1 — Vector search with intersection as filter_bitmap.
    //      Unfiltered search returns all four Vector surrogates (11, 12, 13
    //      also match the query vector near [1,0,0]).
    let all_vec = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "vec_col",
            ),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 20,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
        }),
    );
    let all_vec_surs = extract_vector_surrogates(&all_vec);
    assert!(
        all_vec_surs.len() >= 2,
        "unfiltered vector search should return at least 2 results, got {all_vec_surs:?}"
    );

    let filtered_vec = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "vec_col",
            ),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 20,
            ef_search: 0,
            filter_bitmap: Some(intersection.clone()),
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
        }),
    );
    let filtered_vec_surs = extract_vector_surrogates(&filtered_vec);

    // Every returned surrogate must be in the intersection.
    for &s in &filtered_vec_surs {
        assert!(
            intersection.contains(Surrogate::new(s)),
            "vector search returned surrogate {s} not in intersection"
        );
    }
    // Surrogates 11, 12, 13 must not appear (not in intersection).
    for &absent in &[11u32, 12, 13] {
        assert!(
            !filtered_vec_surs.contains(&absent),
            "surrogate {absent} is Vector-only or Vector+one-engine; must be absent from intersection-filtered search"
        );
    }

    // B2 — Graph hop with frontier_bitmap = intersection.
    //      An inserted edge n10 → n11 makes n11 reachable.
    //      With frontier_bitmap = {10}, the hop does not traverse to n11
    //      (surrogate 11 is not in the intersection).
    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "graph_col",
            ),
            src_id: "n10".into(),
            label: "REACH".into(),
            dst_id: "n11".into(),
            properties: vec![],
            src_surrogate: Surrogate::new(10),
            dst_surrogate: Surrogate::new(11),
            valid_from_ms: None,
            valid_until_ms: None,
        }),
    );

    let hop_all = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["n10".into()],
            edge_labels: vec!["REACH".into()],
            direction: Direction::Out,
            depth: 1,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
            collection: None,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
        }),
    );
    let hop_all_json = decode_payload_to_json(&hop_all);
    // Unfiltered: n11 is reachable from n10 via REACH.
    assert!(
        hop_all_json.contains("n11"),
        "unfiltered hop from n10 must reach n11: {hop_all_json}"
    );

    let hop_filtered = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["n10".into()],
            edge_labels: vec!["REACH".into()],
            direction: Direction::Out,
            depth: 1,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: Some(intersection.clone()),
            collection: None,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
        }),
    );
    let hop_filtered_json = decode_payload_to_json(&hop_filtered);
    // Filtered: n11's surrogate (11) is not in the intersection — the hop
    // must not include it.
    assert!(
        !hop_filtered_json.contains("n11"),
        "intersection-filtered hop must not reach n11 (surrogate 11 not in intersection): \
         {hop_filtered_json}"
    );

    // B3 — Array slice with cell_filter = intersection.
    {
        let aid = ArrayId::new(TenantId::new(1), "arr_col");
        // Full slice (no filter).
        // Full slice: no restrictions on any dimension (None = unbounded).
        let full_slice = ArraySlice::new(vec![None]);
        let slice_mp = zerompk::to_msgpack_vec(&full_slice).unwrap();

        let slice_all = send_ok(
            core,
            tx,
            rx,
            PhysicalPlan::Array(ArrayOp::Slice {
                array_id: aid.clone(),
                slice_msgpack: slice_mp.clone(),
                attr_projection: vec![],
                limit: 100,
                cell_filter: None,
                shard_bucket: None,
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                zone_msgpack: Vec::new(),
                physical: false,
            }),
        );
        // Unfiltered: returns all ARR_SURS cells.
        let slice_all_json = decode_payload_to_json(&slice_all);
        assert!(
            !slice_all_json.is_empty(),
            "unfiltered array slice must return rows"
        );

        let slice_filtered = send_ok(
            core,
            tx,
            rx,
            PhysicalPlan::Array(ArrayOp::Slice {
                array_id: aid.clone(),
                slice_msgpack: slice_mp,
                attr_projection: vec![],
                limit: 100,
                cell_filter: Some(intersection.clone()),
                shard_bucket: None,
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                zone_msgpack: Vec::new(),
                physical: false,
            }),
        );
        // Filtered: intersection ∩ ARR_SURS = {10}.  Surrogate 10 is at
        // coord [0] with attr value 10.0 (see the array insert in `inserts`).
        // Surrogates 13 (coord [1]), 60 (coord [2]), 61 (coord [3]) must not
        // appear.  The response is zerompk-encoded ArrayCell values; they
        // are transcoded to JSON for inspection.
        // Local DP slice responses are wrapped as `ArraySliceResponse { rows_msgpack:
        // Vec<u8>, truncated_before_horizon: bool }`. The inner `rows_msgpack` is a
        // separate msgpack array of `ArrayCell` values.
        #[derive(serde::Deserialize, zerompk::FromMessagePack)]
        #[msgpack(map)]
        struct SliceEnvelope {
            rows_msgpack: Vec<u8>,
            truncated_before_horizon: bool,
        }
        fn unwrap_rows(payload: &[u8]) -> serde_json::Value {
            let envelope: SliceEnvelope =
                zerompk::from_msgpack(payload).expect("slice response envelope decodes");
            assert!(
                !envelope.truncated_before_horizon,
                "a current slice is never truncated before the audit horizon"
            );
            let json_str = nodedb_types::msgpack_to_json_string(&envelope.rows_msgpack)
                .expect("slice rows decode");
            serde_json::from_str(&json_str).expect("slice rows are JSON")
        }
        let filtered_cells = unwrap_rows(&slice_filtered);
        let unfiltered_cells = unwrap_rows(&slice_all);
        assert_eq!(
            unfiltered_cells.as_array().map(|a| a.len()).unwrap_or(0),
            ARR_SURS.len(),
            "unfiltered slice must return all {} array cells",
            ARR_SURS.len()
        );

        let filtered_count = filtered_cells.as_array().map(|a| a.len()).unwrap_or(0);
        assert_eq!(
            filtered_count, 1,
            "intersection-filtered slice must return exactly 1 cell (surrogate 10), \
             got {filtered_count}: {filtered_cells}"
        );

        // The surviving cell must have attr value 10.0 (the value stored at
        // coord [0], which received surrogate 10). A slice row names each attr.
        let cell = filtered_cells
            .as_array()
            .and_then(|a| a.first())
            .expect("one filtered cell");
        assert_eq!(
            cell.get("v").and_then(|v| v.as_f64()),
            Some(10.0),
            "filtered cell attr `v` must be 10.0 (surrogate-10's stored value): {cell}"
        );
    }
}
