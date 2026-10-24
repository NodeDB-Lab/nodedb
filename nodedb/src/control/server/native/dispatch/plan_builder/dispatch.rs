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
pub(crate) async fn build_plan(
    ctx: &DispatchCtx<'_>,
    op: OpCode,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    match op {
        // Point operations (collection-type-aware).
        OpCode::PointGet => document::build_point_get(ctx, fields, collection).await,
        OpCode::PointPut => document::build_point_put(ctx, fields, collection).await,
        OpCode::PointDelete => document::build_point_delete(ctx, fields, collection).await,
        OpCode::RangeScan => document::build_range_scan(ctx, fields, collection).await,
        OpCode::DocumentBatchInsert => document::build_batch_insert(ctx, fields, collection).await,
        OpCode::DocumentUpdate => document::build_update(ctx, fields, collection).await,
        OpCode::DocumentScan => document::build_scan(ctx, fields, collection).await,
        OpCode::DocumentUpsert => document::build_upsert(ctx, fields, collection).await,
        OpCode::DocumentBulkUpdate => {
            document_bulk::build_bulk_update(ctx, fields, collection).await
        }
        OpCode::DocumentBulkDelete => {
            document_bulk::build_bulk_delete(ctx, fields, collection).await
        }
        // Vector.
        OpCode::VectorSearch => vector::build_search(ctx, fields, collection).await,
        OpCode::VectorBatchInsert => vector::build_batch_insert(ctx, fields, collection).await,
        OpCode::VectorInsert => vector::build_insert(ctx, fields, collection).await,
        OpCode::VectorMultiSearch => vector::build_multi_search(ctx, fields, collection).await,
        OpCode::VectorDelete => vector::build_delete(ctx, fields, collection).await,
        // Graph.
        OpCode::GraphRagFusion => graph::build_rag_fusion(ctx, fields, collection).await,
        OpCode::GraphHop => graph::build_hop(ctx, fields).await,
        OpCode::GraphNeighbors => graph::build_neighbors(ctx, fields).await,
        OpCode::GraphPath => graph::build_path(ctx, fields).await,
        OpCode::GraphSubgraph => graph::build_subgraph(ctx, fields).await,
        OpCode::EdgePut => graph::build_edge_put(ctx, fields, collection).await,
        OpCode::EdgeDelete => graph::build_edge_delete(ctx, fields, collection).await,
        // KV.
        OpCode::KvScan => kv::build_scan(ctx, fields, collection).await,
        OpCode::KvExpire => kv::build_expire(ctx, fields, collection).await,
        OpCode::KvPersist => kv::build_persist(ctx, fields, collection).await,
        OpCode::KvGetTtl => kv::build_get_ttl(ctx, fields, collection).await,
        OpCode::KvBatchGet => kv::build_batch_get(ctx, fields, collection).await,
        OpCode::KvBatchPut => kv::build_batch_put(ctx, fields, collection).await,
        OpCode::KvFieldGet => kv::build_field_get(ctx, fields, collection).await,
        OpCode::KvFieldSet => kv::build_field_set(ctx, fields, collection).await,
        // CRDT.
        OpCode::CrdtRead => crdt::build_read(ctx, fields, collection).await,
        OpCode::CrdtApply => crdt::build_apply(ctx, fields, collection).await,
        OpCode::AlterCollectionPolicy => crdt::build_alter_policy(ctx, fields, collection).await,
        OpCode::CrdtListInsert => crdt::build_list_insert(ctx, fields, collection).await,
        OpCode::CrdtListDelete => crdt::build_list_delete(ctx, fields, collection).await,
        OpCode::CrdtListMove => crdt::build_list_move(ctx, fields, collection).await,
        // Text/Search.
        OpCode::TextSearch => text::build_search(ctx, fields, collection).await,
        OpCode::HybridSearch => text::build_hybrid_search(ctx, fields, collection).await,
        // Spatial.
        OpCode::SpatialScan => spatial::build_scan(ctx, fields, collection).await,
        // Timeseries.
        OpCode::TimeseriesScan => timeseries::build_scan(ctx, fields, collection).await,
        OpCode::TimeseriesIngest => timeseries::build_ingest(ctx, fields, collection).await,
        // Columnar.
        OpCode::ColumnarScan => columnar::build_scan(ctx, fields, collection).await,
        OpCode::ColumnarInsert => columnar::build_insert(ctx, fields, collection).await,
        // Graph DDL.
        OpCode::GraphAlgo => graph::build_algo(fields, collection).await,
        OpCode::GraphMatch => graph::build_match(ctx, fields).await,
        // Document DDL.
        OpCode::DocumentTruncate => document_bulk::build_truncate(ctx, collection).await,
        OpCode::DocumentEstimateCount => {
            document_bulk::build_estimate_count(ctx, fields, collection).await
        }
        OpCode::DocumentInsertSelect => {
            document_bulk::build_insert_select(ctx, fields, collection).await
        }
        // KV truncate.
        OpCode::KvTruncate => kv::build_truncate(ctx, collection).await,
        // KV atomic operations.
        OpCode::KvIncr => kv_counter::build_incr(ctx, collection, fields).await,
        OpCode::KvIncrFloat => kv_counter::build_incr_float(ctx, collection, fields).await,
        OpCode::KvCas => kv::build_cas(ctx, collection, fields).await,
        OpCode::KvGetSet => kv::build_getset(ctx, collection, fields).await,
        // Query.
        OpCode::RecursiveScan => query::build_recursive_scan(ctx, fields, collection).await,
        _ => Err(crate::Error::BadRequest {
            detail: format!("operation {op:?} not supported as direct dispatch"),
        }),
    }
}
