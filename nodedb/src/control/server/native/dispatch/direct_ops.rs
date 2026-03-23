//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use std::sync::Arc;

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};
use nodedb_types::value::Value;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::data::executor::response_codec;

use super::super::super::dispatch_utils;
use super::{DispatchCtx, error_to_native};

/// Dispatch a direct Data Plane operation by opcode.
pub(crate) async fn handle_direct_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_string();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    let plan = match build_plan(op, fields, &collection) {
        Ok(p) => p,
        Err(e) => return NativeResponse::error(seq, "42601", e),
    };

    // WAL append for writes.
    if let Err(e) = dispatch_utils::wal_append_if_write(&ctx.state.wal, tenant_id, vshard_id, &plan)
    {
        return error_to_native(seq, &e);
    }

    match dispatch_utils::dispatch_to_data_plane(ctx.state, tenant_id, vshard_id, plan, 0).await {
        Ok(resp) => data_plane_response_to_native(seq, &resp),
        Err(e) => error_to_native(seq, &e),
    }
}

fn build_plan(op: OpCode, fields: &TextFields, collection: &str) -> Result<PhysicalPlan, String> {
    match op {
        OpCode::PointGet => {
            let document_id = fields
                .document_id
                .as_ref()
                .ok_or("missing 'document_id'")?
                .clone();
            Ok(PhysicalPlan::PointGet {
                collection: collection.to_string(),
                document_id,
            })
        }
        OpCode::PointPut => {
            let document_id = fields
                .document_id
                .as_ref()
                .ok_or("missing 'document_id'")?
                .clone();
            let value = fields.data.clone().unwrap_or_default();
            Ok(PhysicalPlan::PointPut {
                collection: collection.to_string(),
                document_id,
                value,
            })
        }
        OpCode::PointDelete => {
            let document_id = fields
                .document_id
                .as_ref()
                .ok_or("missing 'document_id'")?
                .clone();
            Ok(PhysicalPlan::PointDelete {
                collection: collection.to_string(),
                document_id,
            })
        }
        OpCode::VectorSearch => {
            let query_vector = fields
                .query_vector
                .as_ref()
                .ok_or("missing 'query_vector'")?;
            let top_k = fields.top_k.unwrap_or(10) as usize;
            Ok(PhysicalPlan::VectorSearch {
                collection: collection.to_string(),
                query_vector: Arc::from(query_vector.as_slice()),
                top_k,
                ef_search: 0,
                filter_bitmap: None,
                field_name: String::new(),
            })
        }
        OpCode::RangeScan => {
            let field = fields.field.as_ref().ok_or("missing 'field'")?.clone();
            let limit = fields.limit.unwrap_or(100) as usize;
            Ok(PhysicalPlan::RangeScan {
                collection: collection.to_string(),
                field,
                lower: None,
                upper: None,
                limit,
            })
        }
        OpCode::CrdtRead => {
            let document_id = fields
                .document_id
                .as_ref()
                .ok_or("missing 'document_id'")?
                .clone();
            Ok(PhysicalPlan::CrdtRead {
                collection: collection.to_string(),
                document_id,
            })
        }
        OpCode::CrdtApply => {
            let document_id = fields
                .document_id
                .as_ref()
                .ok_or("missing 'document_id'")?
                .clone();
            let delta = fields.delta.as_ref().ok_or("missing 'delta'")?.clone();
            let peer_id = fields.peer_id.unwrap_or(0);
            Ok(PhysicalPlan::CrdtApply {
                collection: collection.to_string(),
                document_id,
                delta,
                peer_id,
                mutation_id: 0,
            })
        }
        OpCode::GraphRagFusion => {
            let query_vector = fields
                .query_vector
                .as_ref()
                .ok_or("missing 'query_vector'")?;
            let vector_top_k = fields.vector_top_k.unwrap_or(20) as usize;
            let edge_label = fields.edge_label.clone();
            let direction = parse_direction(fields.direction.as_deref());
            let expansion_depth = fields.expansion_depth.unwrap_or(2) as usize;
            let final_top_k = fields.final_top_k.unwrap_or(10) as usize;
            let vector_k = fields.vector_k.unwrap_or(60.0);
            let graph_k = fields.graph_k.unwrap_or(10.0);
            Ok(PhysicalPlan::GraphRagFusion {
                collection: collection.to_string(),
                query_vector: Arc::from(query_vector.as_slice()),
                vector_top_k,
                edge_label,
                direction,
                expansion_depth,
                final_top_k,
                rrf_k: (vector_k, graph_k),
                options: Default::default(),
            })
        }
        OpCode::AlterCollectionPolicy => {
            let policy = fields.policy.as_ref().ok_or("missing 'policy'")?;
            let policy_json =
                serde_json::to_string(policy).map_err(|e| format!("invalid policy: {e}"))?;
            Ok(PhysicalPlan::SetCollectionPolicy {
                collection: collection.to_string(),
                policy_json,
            })
        }
        OpCode::GraphHop => {
            let start = fields.start_node.as_ref().ok_or("missing 'start_node'")?;
            let depth = fields.depth.unwrap_or(2) as usize;
            let direction = parse_direction(fields.direction.as_deref());
            Ok(PhysicalPlan::GraphHop {
                start_nodes: vec![start.clone()],
                depth,
                edge_label: fields.edge_label.clone(),
                direction,
                options: Default::default(),
            })
        }
        OpCode::GraphNeighbors => {
            let start = fields.start_node.as_ref().ok_or("missing 'start_node'")?;
            let direction = parse_direction(fields.direction.as_deref());
            Ok(PhysicalPlan::GraphNeighbors {
                node_id: start.clone(),
                edge_label: fields.edge_label.clone(),
                direction,
            })
        }
        OpCode::GraphPath => {
            let from = fields.start_node.as_ref().ok_or("missing 'start_node'")?;
            let to = fields.end_node.as_ref().ok_or("missing 'end_node'")?;
            let max_depth = fields.depth.unwrap_or(10) as usize;
            Ok(PhysicalPlan::GraphPath {
                src: from.clone(),
                dst: to.clone(),
                max_depth,
                edge_label: fields.edge_label.clone(),
                options: Default::default(),
            })
        }
        OpCode::GraphSubgraph => {
            let start = fields.start_node.as_ref().ok_or("missing 'start_node'")?;
            let depth = fields.depth.unwrap_or(2) as usize;
            Ok(PhysicalPlan::GraphSubgraph {
                start_nodes: vec![start.clone()],
                depth,
                edge_label: fields.edge_label.clone(),
                options: Default::default(),
            })
        }
        OpCode::EdgePut => {
            let src = fields.from_node.as_ref().ok_or("missing 'from_node'")?;
            let dst = fields.to_node.as_ref().ok_or("missing 'to_node'")?;
            let label = fields.edge_type.as_ref().ok_or("missing 'edge_type'")?;
            let props = fields
                .properties
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .unwrap_or_default();
            Ok(PhysicalPlan::EdgePut {
                src_id: src.clone(),
                label: label.clone(),
                dst_id: dst.clone(),
                properties: props.into_bytes(),
            })
        }
        OpCode::EdgeDelete => {
            let src = fields.from_node.as_ref().ok_or("missing 'from_node'")?;
            let dst = fields.to_node.as_ref().ok_or("missing 'to_node'")?;
            let label = fields.edge_type.as_ref().ok_or("missing 'edge_type'")?;
            Ok(PhysicalPlan::EdgeDelete {
                src_id: src.clone(),
                label: label.clone(),
                dst_id: dst.clone(),
            })
        }
        OpCode::TextSearch => {
            let query_text = fields.query_text.as_ref().ok_or("missing 'query_text'")?;
            let top_k = fields.top_k.unwrap_or(10) as usize;
            Ok(PhysicalPlan::TextSearch {
                collection: collection.to_string(),
                query: query_text.clone(),
                top_k,
                fuzzy: false,
            })
        }
        OpCode::HybridSearch => {
            let query_vector = fields
                .query_vector
                .as_ref()
                .ok_or("missing 'query_vector'")?;
            let query_text = fields.query_text.as_ref().ok_or("missing 'query_text'")?;
            let top_k = fields.top_k.unwrap_or(10) as usize;
            let vector_weight = fields.vector_weight.unwrap_or(0.5) as f32;
            Ok(PhysicalPlan::HybridSearch {
                collection: collection.to_string(),
                query_vector: Arc::from(query_vector.as_slice()),
                query_text: query_text.clone(),
                top_k,
                ef_search: 0,
                fuzzy: false,
                vector_weight,
                filter_bitmap: None,
            })
        }
        _ => Err(format!("operation {op:?} not supported as direct dispatch")),
    }
}

fn parse_direction(s: Option<&str>) -> crate::engine::graph::edge_store::Direction {
    match s {
        Some("in") => crate::engine::graph::edge_store::Direction::In,
        Some("both") => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    }
}

fn data_plane_response_to_native(seq: u64, resp: &Response) -> NativeResponse {
    if resp.status == Status::Error {
        let msg = if resp.payload.is_empty() {
            resp.error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown error".into())
        } else {
            String::from_utf8_lossy(&resp.payload).into_owned()
        };
        return NativeResponse::error(seq, "XX000", msg);
    }

    if resp.payload.is_empty() {
        let mut r = NativeResponse::ok(seq);
        r.watermark_lsn = resp.watermark_lsn.as_u64();
        return r;
    }

    let json_text = response_codec::decode_payload_to_json(&resp.payload);
    NativeResponse {
        seq,
        status: nodedb_types::protocol::ResponseStatus::Ok,
        columns: Some(vec!["result".into()]),
        rows: Some(vec![vec![Value::String(json_text)]]),
        rows_affected: None,
        watermark_lsn: resp.watermark_lsn.as_u64(),
        error: None,
        auth: None,
    }
}
