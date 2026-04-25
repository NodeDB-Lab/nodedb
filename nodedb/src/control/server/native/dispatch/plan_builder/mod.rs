//! Physical plan construction from NDB opcodes.
//!
//! Each engine has its own sub-module with builder functions.
//! `build_plan()` dispatches to the appropriate engine module.

pub(crate) mod columnar;
pub(crate) mod crdt;
pub(crate) mod document;
pub(crate) mod graph;
pub(crate) mod kv;
pub(crate) mod query;
pub(crate) mod spatial;
pub(crate) mod text;
pub(crate) mod timeseries;
pub(crate) mod vector;

use nodedb_types::protocol::{OpCode, TextFields};

use crate::bridge::envelope::PhysicalPlan;

use super::DispatchCtx;

/// Build a PhysicalPlan from an opcode and request fields.
pub(crate) fn build_plan(
    ctx: &DispatchCtx<'_>,
    op: OpCode,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    match op {
        // Point operations (collection-type-aware).
        OpCode::PointGet => document::build_point_get(ctx, fields, collection),
        OpCode::PointPut => document::build_point_put(ctx, fields, collection),
        OpCode::PointDelete => document::build_point_delete(ctx, fields, collection),
        OpCode::RangeScan => document::build_range_scan(fields, collection),
        OpCode::DocumentBatchInsert => document::build_batch_insert(fields, collection),
        OpCode::DocumentUpdate => document::build_update(ctx, fields, collection),
        OpCode::DocumentScan => document::build_scan(fields, collection),
        OpCode::DocumentUpsert => document::build_upsert(ctx, fields, collection),
        OpCode::DocumentBulkUpdate => document::build_bulk_update(fields, collection),
        OpCode::DocumentBulkDelete => document::build_bulk_delete(fields, collection),
        // Vector.
        OpCode::VectorSearch => vector::build_search(fields, collection),
        OpCode::VectorBatchInsert => vector::build_batch_insert(ctx, fields, collection),
        OpCode::VectorInsert => vector::build_insert(ctx, fields, collection),
        OpCode::VectorMultiSearch => vector::build_multi_search(fields, collection),
        OpCode::VectorDelete => vector::build_delete(fields, collection),
        // Graph.
        OpCode::GraphRagFusion => graph::build_rag_fusion(fields, collection),
        OpCode::GraphHop => graph::build_hop(fields),
        OpCode::GraphNeighbors => graph::build_neighbors(fields),
        OpCode::GraphPath => graph::build_path(fields),
        OpCode::GraphSubgraph => graph::build_subgraph(fields),
        OpCode::EdgePut => graph::build_edge_put(ctx, fields, collection),
        OpCode::EdgeDelete => graph::build_edge_delete(fields, collection),
        // KV.
        OpCode::KvScan => kv::build_scan(fields, collection),
        OpCode::KvExpire => kv::build_expire(fields, collection),
        OpCode::KvPersist => kv::build_persist(fields, collection),
        OpCode::KvGetTtl => kv::build_get_ttl(fields, collection),
        OpCode::KvBatchGet => kv::build_batch_get(fields, collection),
        OpCode::KvBatchPut => kv::build_batch_put(fields, collection),
        OpCode::KvFieldGet => kv::build_field_get(fields, collection),
        OpCode::KvFieldSet => kv::build_field_set(fields, collection),
        // CRDT.
        OpCode::CrdtRead => crdt::build_read(fields, collection),
        OpCode::CrdtApply => crdt::build_apply(ctx.state, fields, collection),
        OpCode::AlterCollectionPolicy => crdt::build_alter_policy(fields, collection),
        // Text/Search.
        OpCode::TextSearch => text::build_search(fields, collection),
        OpCode::HybridSearch => text::build_hybrid_search(fields, collection),
        // Spatial.
        OpCode::SpatialScan => spatial::build_scan(fields, collection),
        // Timeseries.
        OpCode::TimeseriesScan => timeseries::build_scan(fields, collection),
        OpCode::TimeseriesIngest => timeseries::build_ingest(fields, collection),
        // Columnar.
        OpCode::ColumnarScan => columnar::build_scan(fields, collection),
        OpCode::ColumnarInsert => columnar::build_insert(ctx, fields, collection),
        // Graph DDL.
        OpCode::GraphAlgo => graph::build_algo(fields, collection),
        OpCode::GraphMatch => graph::build_match(fields, collection),
        // Document DDL.
        OpCode::DocumentTruncate => document::build_truncate(collection),
        OpCode::DocumentEstimateCount => document::build_estimate_count(fields, collection),
        OpCode::DocumentInsertSelect => document::build_insert_select(fields, collection),
        OpCode::DocumentRegister => document::build_register(fields, collection),
        OpCode::DocumentDropIndex => document::build_drop_index(fields, collection),
        // KV DDL.
        OpCode::KvRegisterIndex => kv::build_register_index(fields, collection),
        OpCode::KvDropIndex => kv::build_drop_index(fields, collection),
        OpCode::KvTruncate => kv::build_truncate(collection),
        // KV atomic operations.
        OpCode::KvIncr => kv::build_incr(collection, fields),
        OpCode::KvIncrFloat => kv::build_incr_float(collection, fields),
        OpCode::KvCas => kv::build_cas(collection, fields),
        OpCode::KvGetSet => kv::build_getset(collection, fields),
        // KV sorted index operations.
        OpCode::KvRegisterSortedIndex => kv::build_register_sorted_index(collection, fields),
        OpCode::KvDropSortedIndex => kv::build_drop_sorted_index(fields),
        OpCode::KvSortedIndexRank => kv::build_sorted_index_rank(fields),
        OpCode::KvSortedIndexTopK => kv::build_sorted_index_top_k(fields),
        OpCode::KvSortedIndexRange => kv::build_sorted_index_range(fields),
        OpCode::KvSortedIndexCount => kv::build_sorted_index_count(fields),
        OpCode::KvSortedIndexScore => kv::build_sorted_index_score(fields),
        // Vector DDL.
        OpCode::VectorSetParams => vector::build_set_params(fields, collection),
        // Query.
        OpCode::RecursiveScan => query::build_recursive_scan(fields, collection),
        _ => Err(crate::Error::BadRequest {
            detail: format!("operation {op:?} not supported as direct dispatch"),
        }),
    }
}

// ── Shared helpers ──────────────────────────────────────────────────

/// Single catalog lookup returning the collection's storage type.
///
/// Returns `None` when: no catalog available, collection not found,
/// or catalog read error. Callers treat `None` as "default to document".
pub(super) fn collection_type(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> Option<nodedb_types::CollectionType> {
    let catalog = ctx.state.credentials.catalog().as_ref()?;
    let coll = catalog
        .get_collection(ctx.identity.tenant_id.as_u32(), collection)
        .ok()??;
    Some(coll.collection_type.clone())
}

/// Extract document_id from request fields.
pub(super) fn require_doc_id(fields: &TextFields) -> crate::Result<String> {
    fields
        .document_id
        .as_ref()
        .cloned()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'document_id'".to_string(),
        })
}

/// Parse direction string for graph operations.
pub(super) fn parse_direction(s: Option<&str>) -> crate::engine::graph::edge_store::Direction {
    match s {
        Some("in") => crate::engine::graph::edge_store::Direction::In,
        Some("both") => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    }
}
