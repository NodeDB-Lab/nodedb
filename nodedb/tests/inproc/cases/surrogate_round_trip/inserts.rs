// SPDX-License-Identifier: BUSL-1.1

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::engine::array::wal::ArrayPutCell;
use nodedb::types::TenantId;
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarInsertIntent, ColumnarOp, DocumentOp, GraphOp, KvOp, PhysicalPlan, VectorOp,
};
use nodedb_types::Surrogate;

use super::harness::{
    ARR_SURS, COL_SURS, D_SURS, FTS_SURS, G_SRC_SURS, KV_SURS, SPATIAL_SURS, V_SURS,
    array_schema_1d, send_batch_ok, send_ok,
};

// ── 1. Vector inserts ───────────────────────────────────────────────
pub(super) fn insert_vector(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    // Each vector gets a distinct embedding so all appear near [1,0,0].
    let vec_plans: Vec<PhysicalPlan> = V_SURS
        .iter()
        .copied()
        .map(|s| {
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "vec_col",
                ),
                vector: vec![1.0, s as f32 * 0.01, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: Surrogate::new(s),
                pk_bytes: None,
                provenance: None,
            })
        })
        .collect();
    send_batch_ok(core, tx, rx, vec_plans);
}

// ── 2. Document (schemaless) inserts ────────────────────────────────
pub(super) fn insert_document(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    for &s in D_SURS {
        let hex = format!("{s:08x}");
        let doc = serde_json::json!({ "id": hex, "kind": "doc" });
        send_ok(
            core,
            tx,
            rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "doc_col",
                ),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }
}

// ── 3. Graph edge inserts ────────────────────────────────────────────
// Each src node gets a surrogate in G_SRC_SURS; dst node uses s+100.
pub(super) fn insert_graph(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    for &s in G_SRC_SURS {
        let src = format!("n{s}");
        let dst = format!("n{}", s + 100);
        send_ok(
            core,
            tx,
            rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "graph_col",
                ),
                src_id: src.clone(),
                label: "LINKED".into(),
                dst_id: dst.clone(),
                properties: vec![],
                src_surrogate: Surrogate::new(s),
                dst_surrogate: Surrogate::new(s + 100),
                valid_from_ms: None,
                valid_until_ms: None,
            }),
        );
    }
}

// ── 4. KV inserts ────────────────────────────────────────────────────
pub(super) fn insert_kv(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    for &s in KV_SURS {
        let key = format!("k{s}").into_bytes();
        // Minimal Binary Tuple value (empty).
        send_ok(
            core,
            tx,
            rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "kv_col",
                ),
                key: key.clone(),
                value: vec![],
                ttl_ms: 0,
                surrogate: Surrogate::new(s),
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            }),
        );
    }
}

// ── 5. Columnar inserts ──────────────────────────────────────────────
pub(super) fn insert_columnar(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    let rows_json = serde_json::json!(
        COL_SURS
            .iter()
            .map(|&s| serde_json::json!({ "id": format!("{s:08x}"), "val": s }))
            .collect::<Vec<_>>()
    );
    let rows_mp = nodedb_types::json_to_msgpack(&rows_json).unwrap();
    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "col_col",
            ),
            payload: rows_mp,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: COL_SURS.iter().copied().map(Surrogate::new).collect(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
}

// ── 6. Array inserts ─────────────────────────────────────────────────
pub(super) fn insert_array(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    let aid = ArrayId::new(TenantId::new(1), "arr_col");
    let schema = array_schema_1d("arr_col");
    let schema_bytes = zerompk::to_msgpack_vec(&schema).unwrap();

    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id: aid.clone(),
            schema_msgpack: schema_bytes,
            schema_hash: 1,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        }),
    );

    let cells: Vec<ArrayPutCell> = ARR_SURS
        .iter()
        .copied()
        .enumerate()
        .map(|(i, s)| ArrayPutCell {
            coord: vec![CoordValue::Int64(i as i64)],
            attrs: vec![CellValue::Float64(s as f64)],
            surrogate: Surrogate::new(s),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .collect();
    let cells_mp = zerompk::to_msgpack_vec(&cells).unwrap();

    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Array(ArrayOp::Put {
            array_id: aid.clone(),
            cells_msgpack: cells_mp,
            wal_lsn: 1,
            provenance: None,
            vshard_id: 0,
        }),
    );
}

// ── 7. FTS inserts (via Document engine; text is indexed separately) ─
pub(super) fn insert_fts(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    for &s in FTS_SURS {
        let hex = format!("{s:08x}");
        let doc = serde_json::json!({ "id": hex, "body": format!("learning keyword {s}") });
        send_ok(
            core,
            tx,
            rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "fts_col",
                ),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }
}

// ── 8. Spatial inserts (via Document engine; geometry stored as field) ─
pub(super) fn insert_spatial(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
) {
    for &s in SPATIAL_SURS {
        let hex = format!("{s:08x}");
        let lon = s as f64 * 0.01;
        let doc = serde_json::json!({ "id": hex, "geom": { "type": "Point", "coordinates": [lon, 0.0] } });
        send_ok(
            core,
            tx,
            rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "spatial_col",
                ),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }
}
