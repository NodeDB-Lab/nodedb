// SPDX-License-Identifier: BUSL-1.1

//! Physical plan construction dispatch: matches an opcode to its
//! per-engine builder function.

use nodedb_types::protocol::{OpCode, TextFields};

use crate::bridge::envelope::PhysicalPlan;

use super::super::DispatchCtx;
use super::{
    columnar, crdt, document, document_bulk, graph, kv, kv_counter, query, spatial, text,
    timeseries, vector,
};

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
        OpCode::RangeScan => document::build_range_scan(ctx, fields, collection),
        OpCode::DocumentBatchInsert => document::build_batch_insert(ctx, fields, collection),
        OpCode::DocumentUpdate => document::build_update(ctx, fields, collection),
        OpCode::DocumentScan => document::build_scan(ctx, fields, collection),
        OpCode::DocumentUpsert => document::build_upsert(ctx, fields, collection),
        OpCode::DocumentBulkUpdate => document_bulk::build_bulk_update(ctx, fields, collection),
        OpCode::DocumentBulkDelete => document_bulk::build_bulk_delete(ctx, fields, collection),
        // Vector.
        OpCode::VectorSearch => vector::build_search(ctx, fields, collection),
        OpCode::VectorBatchInsert => vector::build_batch_insert(ctx, fields, collection),
        OpCode::VectorInsert => vector::build_insert(ctx, fields, collection),
        OpCode::VectorMultiSearch => vector::build_multi_search(ctx, fields, collection),
        OpCode::VectorDelete => vector::build_delete(ctx, fields, collection),
        // Graph.
        OpCode::GraphRagFusion => graph::build_rag_fusion(ctx, fields, collection),
        OpCode::GraphHop => graph::build_hop(ctx, fields),
        OpCode::GraphNeighbors => graph::build_neighbors(ctx, fields),
        OpCode::GraphPath => graph::build_path(ctx, fields),
        OpCode::GraphSubgraph => graph::build_subgraph(ctx, fields),
        OpCode::EdgePut => graph::build_edge_put(ctx, fields, collection),
        OpCode::EdgeDelete => graph::build_edge_delete(ctx, fields, collection),
        // KV.
        OpCode::KvScan => kv::build_scan(ctx, fields, collection),
        OpCode::KvExpire => kv::build_expire(ctx, fields, collection),
        OpCode::KvPersist => kv::build_persist(ctx, fields, collection),
        OpCode::KvGetTtl => kv::build_get_ttl(ctx, fields, collection),
        OpCode::KvBatchGet => kv::build_batch_get(ctx, fields, collection),
        OpCode::KvBatchPut => kv::build_batch_put(ctx, fields, collection),
        OpCode::KvFieldGet => kv::build_field_get(ctx, fields, collection),
        OpCode::KvFieldSet => kv::build_field_set(ctx, fields, collection),
        // CRDT.
        OpCode::CrdtRead => crdt::build_read(ctx, fields, collection),
        OpCode::CrdtApply => crdt::build_apply(ctx, fields, collection),
        OpCode::AlterCollectionPolicy => crdt::build_alter_policy(ctx, fields, collection),
        OpCode::CrdtListInsert => crdt::build_list_insert(ctx, fields, collection),
        OpCode::CrdtListDelete => crdt::build_list_delete(ctx, fields, collection),
        OpCode::CrdtListMove => crdt::build_list_move(ctx, fields, collection),
        // Text/Search.
        OpCode::TextSearch => text::build_search(ctx, fields, collection),
        OpCode::HybridSearch => text::build_hybrid_search(ctx, fields, collection),
        // Spatial.
        OpCode::SpatialScan => spatial::build_scan(ctx, fields, collection),
        // Timeseries.
        OpCode::TimeseriesScan => timeseries::build_scan(ctx, fields, collection),
        OpCode::TimeseriesIngest => timeseries::build_ingest(ctx, fields, collection),
        // Columnar.
        OpCode::ColumnarScan => columnar::build_scan(ctx, fields, collection),
        OpCode::ColumnarInsert => columnar::build_insert(ctx, fields, collection),
        // Graph DDL.
        OpCode::GraphAlgo => graph::build_algo(fields, collection),
        OpCode::GraphMatch => graph::build_match(fields, collection),
        // Document DDL.
        OpCode::DocumentTruncate => document_bulk::build_truncate(ctx, collection),
        OpCode::DocumentEstimateCount => {
            document_bulk::build_estimate_count(ctx, fields, collection)
        }
        OpCode::DocumentInsertSelect => document_bulk::build_insert_select(ctx, fields, collection),
        // KV truncate.
        OpCode::KvTruncate => kv::build_truncate(ctx, collection),
        // KV atomic operations.
        OpCode::KvIncr => kv_counter::build_incr(ctx, collection, fields),
        OpCode::KvIncrFloat => kv_counter::build_incr_float(ctx, collection, fields),
        OpCode::KvCas => kv::build_cas(ctx, collection, fields),
        OpCode::KvGetSet => kv::build_getset(ctx, collection, fields),
        // Query.
        OpCode::RecursiveScan => query::build_recursive_scan(ctx, fields, collection),
        _ => Err(crate::Error::BadRequest {
            detail: format!("operation {op:?} not supported as direct dispatch"),
        }),
    }
}
