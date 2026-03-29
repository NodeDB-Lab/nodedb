//! Physical plan construction from NDB opcodes.
//!
//! Converts opcode + TextFields into a PhysicalPlan for Data Plane dispatch.
//! Collection type metadata is consulted for correct engine routing (e.g.,
//! PointPut on a KV collection produces KvOp::Put, not DocumentOp::PointPut).

use std::sync::Arc;

use nodedb_types::protocol::{OpCode, TextFields};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, KvOp, TextOp, TimeseriesOp, VectorOp};

use super::DispatchCtx;

/// Build a PhysicalPlan from an opcode and request fields.
///
/// Uses the catalog (via `ctx`) to determine collection type and route
/// operations to the correct engine.
pub(crate) fn build_plan(
    ctx: &DispatchCtx<'_>,
    op: OpCode,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    match op {
        OpCode::PointGet => build_point_get(ctx, fields, collection),
        OpCode::PointPut => build_point_put(ctx, fields, collection),
        OpCode::PointDelete => build_point_delete(ctx, fields, collection),
        OpCode::VectorSearch => build_vector_search(fields, collection),
        OpCode::RangeScan => build_range_scan(fields, collection),
        OpCode::CrdtRead => build_crdt_read(fields, collection),
        OpCode::CrdtApply => build_crdt_apply(fields, collection),
        OpCode::GraphRagFusion => {
            super::plan_builder_graph::build_graph_rag_fusion(fields, collection)
        }
        OpCode::AlterCollectionPolicy => build_alter_policy(fields, collection),
        OpCode::GraphHop => super::plan_builder_graph::build_graph_hop(fields),
        OpCode::GraphNeighbors => super::plan_builder_graph::build_graph_neighbors(fields),
        OpCode::GraphPath => super::plan_builder_graph::build_graph_path(fields),
        OpCode::GraphSubgraph => super::plan_builder_graph::build_graph_subgraph(fields),
        OpCode::EdgePut => super::plan_builder_graph::build_edge_put(fields),
        OpCode::EdgeDelete => super::plan_builder_graph::build_edge_delete(fields),
        OpCode::TextSearch => build_text_search(fields, collection),
        OpCode::HybridSearch => build_hybrid_search(fields, collection),
        _ => Err(crate::Error::BadRequest {
            detail: format!("operation {op:?} not supported as direct dispatch"),
        }),
    }
}

// ---------------------------------------------------------------------------
// Collection type helper
// ---------------------------------------------------------------------------

/// Check if a collection is KV type via catalog lookup.
fn is_kv(ctx: &DispatchCtx<'_>, collection: &str) -> bool {
    if let Some(catalog) = ctx.state.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(ctx.identity.tenant_id.as_u32(), collection)
    {
        return coll.collection_type.is_kv();
    }
    false
}

/// Check if a collection is timeseries type via catalog lookup.
fn is_timeseries(ctx: &DispatchCtx<'_>, collection: &str) -> bool {
    if let Some(catalog) = ctx.state.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(ctx.identity.tenant_id.as_u32(), collection)
    {
        return coll.collection_type.is_timeseries();
    }
    false
}

// ---------------------------------------------------------------------------
// Point operations (collection-type-aware)
// ---------------------------------------------------------------------------

fn require_doc_id(fields: &TextFields) -> crate::Result<String> {
    fields
        .document_id
        .as_ref()
        .cloned()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'document_id'".to_string(),
        })
}

fn build_point_get(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;

    if is_kv(ctx, collection) {
        return Ok(PhysicalPlan::Kv(KvOp::Get {
            collection: collection.to_string(),
            key: doc_id.into_bytes(),
            rls_filters: Vec::new(),
        }));
    }

    if is_timeseries(ctx, collection) {
        return Err(crate::Error::BadRequest {
            detail:
                "PointGet not supported on timeseries collections (use SQL SELECT with time range)"
                    .to_string(),
        });
    }

    Ok(PhysicalPlan::Document(DocumentOp::PointGet {
        collection: collection.to_string(),
        document_id: doc_id,
        rls_filters: Vec::new(),
    }))
}

fn build_point_put(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();

    if is_kv(ctx, collection) {
        return Ok(PhysicalPlan::Kv(KvOp::Put {
            collection: collection.to_string(),
            key: doc_id.into_bytes(),
            value,
            ttl_ms: 0,
        }));
    }

    if is_timeseries(ctx, collection) {
        // Convert to ILP ingest — same approach as pgwire INSERT.
        let json_str = String::from_utf8_lossy(&value);
        let ilp_line = format!("{collection} value={json_str}\n");
        return Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.to_string(),
            payload: ilp_line.into_bytes(),
            format: "ilp".to_string(),
        }));
    }

    Ok(PhysicalPlan::Document(DocumentOp::PointPut {
        collection: collection.to_string(),
        document_id: doc_id,
        value,
    }))
}

fn build_point_delete(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;

    if is_kv(ctx, collection) {
        return Ok(PhysicalPlan::Kv(KvOp::Delete {
            collection: collection.to_string(),
            keys: vec![doc_id.into_bytes()],
        }));
    }

    if is_timeseries(ctx, collection) {
        return Err(crate::Error::BadRequest {
            detail: "PointDelete not supported on timeseries collections (append-only; use retention policies)".to_string(),
        });
    }

    Ok(PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: collection.to_string(),
        document_id: doc_id,
    }))
}

// ---------------------------------------------------------------------------
// Vector search
// ---------------------------------------------------------------------------

fn build_vector_search(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;
    let field_name = fields.field_name.clone().unwrap_or_default();

    Ok(PhysicalPlan::Vector(VectorOp::Search {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        top_k,
        ef_search,
        filter_bitmap: None,
        field_name,
        rls_filters: Vec::new(),
    }))
}

// ---------------------------------------------------------------------------
// Range scan
// ---------------------------------------------------------------------------

fn build_range_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let field = fields
        .field
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'field'".to_string(),
        })?
        .clone();
    let limit = fields.limit.unwrap_or(100) as usize;

    Ok(PhysicalPlan::Document(DocumentOp::RangeScan {
        collection: collection.to_string(),
        field,
        lower: fields.lower_bound.clone(),
        upper: fields.upper_bound.clone(),
        limit,
    }))
}

// ---------------------------------------------------------------------------
// CRDT
// ---------------------------------------------------------------------------

fn build_crdt_read(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    Ok(PhysicalPlan::Crdt(CrdtOp::Read {
        collection: collection.to_string(),
        document_id,
    }))
}

fn build_crdt_apply(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    let delta = fields
        .delta
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'delta'".to_string(),
        })?
        .clone();
    let peer_id = fields.peer_id.unwrap_or(0);

    // Use provided mutation_id, or generate deterministic one from content hash.
    let mutation_id = fields.mutation_id.unwrap_or_else(|| {
        // FNV-1a hash of (peer_id, delta) for deterministic dedup.
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in peer_id.to_le_bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        for b in &delta {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    });

    Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.to_string(),
        document_id,
        delta,
        peer_id,
        mutation_id,
    }))
}

fn build_alter_policy(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let policy = fields
        .policy
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'policy'".to_string(),
        })?;
    let policy_json = serde_json::to_string(policy).map_err(|e| crate::Error::BadRequest {
        detail: format!("invalid policy: {e}"),
    })?;
    Ok(PhysicalPlan::Crdt(CrdtOp::SetPolicy {
        collection: collection.to_string(),
        policy_json,
    }))
}

// ---------------------------------------------------------------------------
// Text search
// ---------------------------------------------------------------------------

fn build_text_search(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let query_text = fields
        .query_text
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_text'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let fuzzy = fields.fuzzy.unwrap_or(false);

    Ok(PhysicalPlan::Text(TextOp::Search {
        collection: collection.to_string(),
        query: query_text.clone(),
        top_k,
        fuzzy,
        rls_filters: Vec::new(),
    }))
}

fn build_hybrid_search(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    let query_text = fields
        .query_text
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_text'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let vector_weight = fields.vector_weight.unwrap_or(0.5) as f32;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;
    let fuzzy = fields.fuzzy.unwrap_or(false);

    Ok(PhysicalPlan::Text(TextOp::HybridSearch {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        query_text: query_text.clone(),
        top_k,
        ef_search,
        fuzzy,
        vector_weight,
        filter_bitmap: None,
        rls_filters: Vec::new(),
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(super) fn parse_direction(s: Option<&str>) -> crate::engine::graph::edge_store::Direction {
    match s {
        Some("in") => crate::engine::graph::edge_store::Direction::In,
        Some("both") => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    }
}
