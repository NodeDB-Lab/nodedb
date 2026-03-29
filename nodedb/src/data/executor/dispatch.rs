//! Main execute() dispatch: matches on PhysicalPlan and delegates to handlers.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::{
    CrdtOp, DocumentOp, GraphOp, MetaOp, PhysicalPlan, QueryOp, SpatialOp, TextOp, TimeseriesOp,
    VectorOp,
};

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

impl CoreLoop {
    /// Execute a physical plan. Dispatches to the appropriate handler.
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match task.plan() {
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection,
                document_id,
                rls_filters,
            }) => self.execute_point_get(task, tid, collection, document_id, rls_filters),
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                value,
            }) => self.execute_point_put(task, tid, collection, document_id, value),
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection,
                document_id,
            }) => self.execute_point_delete(task, tid, collection, document_id),
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection,
                document_id,
                updates,
            }) => self.execute_point_update(task, tid, collection, document_id, updates),

            PhysicalPlan::Vector(VectorOp::Insert {
                collection,
                vector,
                dim,
                field_name,
                doc_id,
            }) => self.execute_vector_insert(super::handlers::vector::VectorInsertParams {
                task,
                tid,
                collection,
                vector,
                dim: *dim,
                field_name,
                doc_id: doc_id.clone(),
            }),

            PhysicalPlan::Vector(VectorOp::BatchInsert {
                collection,
                vectors,
                dim,
            }) => self.execute_vector_batch_insert(task, tid, collection, vectors, *dim),

            PhysicalPlan::Vector(VectorOp::MultiSearch {
                collection,
                query_vector,
                top_k,
                ef_search,
                filter_bitmap,
                rls_filters,
            }) => self.execute_vector_multi_search(
                super::handlers::vector_search::VectorMultiSearchParams {
                    task,
                    tid,
                    collection,
                    query_vector,
                    top_k: *top_k,
                    ef_search: *ef_search,
                    filter_bitmap: filter_bitmap.as_ref(),
                    rls_filters,
                },
            ),

            PhysicalPlan::Vector(VectorOp::Delete {
                collection,
                vector_id,
            }) => self.execute_vector_delete(task, tid, collection, *vector_id),

            PhysicalPlan::Vector(VectorOp::Search {
                collection,
                query_vector,
                top_k,
                ef_search,
                filter_bitmap,
                field_name,
                rls_filters,
            }) => self.execute_vector_search(super::handlers::vector_search::VectorSearchParams {
                task,
                tid,
                collection,
                query_vector,
                top_k: *top_k,
                ef_search: *ef_search,
                filter_bitmap: filter_bitmap.as_ref(),
                field_name,
                rls_filters,
            }),

            PhysicalPlan::Vector(VectorOp::SetParams {
                collection,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
            }) => self.execute_set_vector_params(super::handlers::vector::SetVectorParamsInput {
                task,
                tid,
                collection,
                m: *m,
                ef_construction: *ef_construction,
                metric,
                index_type,
                pq_m: *pq_m,
                ivf_cells: *ivf_cells,
                ivf_nprobe: *ivf_nprobe,
            }),

            PhysicalPlan::Document(DocumentOp::Scan {
                collection,
                limit,
                offset,
                sort_keys,
                filters,
                distinct,
                projection,
                computed_columns,
                window_functions,
            }) => self.execute_document_scan(
                task,
                tid,
                collection,
                *limit,
                *offset,
                sort_keys,
                filters,
                *distinct,
                projection,
                computed_columns,
                window_functions,
            ),

            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection,
                documents,
            }) => self.execute_document_batch_insert(task, tid, collection, documents),

            PhysicalPlan::Query(QueryOp::Aggregate {
                collection,
                group_by,
                aggregates,
                filters,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
            }) => self.execute_aggregate(
                task,
                tid,
                collection,
                group_by,
                aggregates,
                filters,
                having,
                *limit,
                sub_group_by,
                sub_aggregates,
            ),

            PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
            }) => self.execute_hash_join(
                task,
                tid,
                left_collection,
                right_collection,
                on,
                join_type,
                *limit,
            ),

            PhysicalPlan::Graph(GraphOp::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            }) => self.execute_edge_put(task, src_id, label, dst_id, properties),

            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                src_id,
                label,
                dst_id,
            }) => self.execute_edge_delete(task, src_id, label, dst_id),

            PhysicalPlan::Graph(GraphOp::Hop {
                start_nodes,
                edge_label,
                direction,
                depth,
                options: _,
                rls_filters: _,
            }) => self.execute_graph_hop(task, start_nodes, edge_label, *direction, *depth),

            PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id,
                edge_label,
                direction,
                rls_filters: _,
            }) => self.execute_graph_neighbors(task, node_id, edge_label, *direction),

            PhysicalPlan::Graph(GraphOp::Path {
                src,
                dst,
                edge_label,
                max_depth,
                options: _,
                rls_filters: _,
            }) => self.execute_graph_path(task, src, dst, edge_label, *max_depth),

            PhysicalPlan::Graph(GraphOp::Subgraph {
                start_nodes,
                edge_label,
                depth,
                options: _,
                rls_filters: _,
            }) => self.execute_graph_subgraph(task, start_nodes, edge_label, *depth),

            PhysicalPlan::Graph(GraphOp::RagFusion {
                collection,
                query_vector,
                vector_top_k,
                edge_label,
                direction,
                expansion_depth,
                final_top_k,
                rrf_k,
                options,
            }) => self.execute_graph_rag_fusion(
                task,
                tid,
                collection,
                query_vector,
                *vector_top_k,
                edge_label,
                *direction,
                *expansion_depth,
                *final_top_k,
                *rrf_k,
                options.max_visited,
            ),

            PhysicalPlan::Document(DocumentOp::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
            }) => self.execute_range_scan(
                task,
                tid,
                collection,
                field,
                lower.as_deref(),
                upper.as_deref(),
                *limit,
            ),

            PhysicalPlan::Crdt(CrdtOp::Read {
                collection,
                document_id,
            }) => self.execute_crdt_read(task, collection, document_id),

            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: _,
                document_id: _,
                delta,
                peer_id: _,
                mutation_id: _,
            }) => self.execute_crdt_apply(task, delta),

            PhysicalPlan::Crdt(CrdtOp::SetPolicy {
                collection,
                policy_json,
            }) => self.execute_set_collection_policy(task, collection, policy_json),

            PhysicalPlan::Text(TextOp::Search {
                collection,
                query,
                top_k,
                fuzzy,
                rls_filters,
            }) => {
                self.execute_text_search(task, tid, collection, query, *top_k, *fuzzy, rls_filters)
            }

            PhysicalPlan::Text(TextOp::HybridSearch {
                collection,
                query_vector,
                query_text,
                top_k,
                ef_search,
                fuzzy,
                vector_weight,
                filter_bitmap,
                rls_filters,
            }) => self.execute_hybrid_search(
                task,
                tid,
                collection,
                query_vector,
                query_text,
                *top_k,
                *ef_search,
                *fuzzy,
                *vector_weight,
                filter_bitmap.as_ref(),
                rls_filters,
            ),

            PhysicalPlan::Graph(GraphOp::Algo { algorithm, params }) => {
                self.execute_graph_algo(task, algorithm, params)
            }

            PhysicalPlan::Graph(GraphOp::Match { query }) => self.execute_graph_match(task, query),

            PhysicalPlan::Meta(MetaOp::WalAppend { payload }) => {
                self.execute_wal_append(task, payload)
            }

            PhysicalPlan::Meta(MetaOp::Cancel { target_request_id }) => {
                self.execute_cancel(task, *target_request_id)
            }

            PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                condition,
                join_type,
                limit,
            }) => self.execute_nested_loop_join(
                task,
                tid,
                left_collection,
                right_collection,
                condition,
                join_type,
                *limit,
            ),

            PhysicalPlan::Meta(MetaOp::TransactionBatch { plans }) => {
                self.execute_transaction_batch(task, tid, plans)
            }

            PhysicalPlan::Query(QueryOp::PartialAggregate {
                collection,
                group_by,
                aggregates,
                filters,
            }) => self.execute_aggregate(
                task,
                tid,
                collection,
                group_by,
                aggregates,
                filters,
                &[],
                usize::MAX,
                &[],
                &[],
            ),

            PhysicalPlan::Query(QueryOp::BroadcastJoin {
                large_collection,
                broadcast_data,
                on,
                join_type,
                limit,
            }) => self.execute_broadcast_join(
                task,
                tid,
                large_collection,
                broadcast_data,
                on,
                join_type,
                *limit,
            ),

            PhysicalPlan::Query(QueryOp::ShuffleJoin { .. }) => {
                // Shuffle join phase 2 (local join on repartitioned data) — not yet wired.
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "shuffle join not dispatched to this core".into(),
                    },
                )
            }

            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
            }) => self.execute_bulk_update(task, tid, collection, filters, updates),

            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection,
                filters,
            }) => self.execute_bulk_delete(task, tid, collection, filters),

            PhysicalPlan::Document(DocumentOp::Upsert {
                collection,
                document_id,
                value,
            }) => self.execute_upsert(task, tid, collection, document_id, value),

            PhysicalPlan::Document(DocumentOp::Truncate { collection }) => {
                self.execute_truncate(task, tid, collection)
            }

            PhysicalPlan::Document(DocumentOp::EstimateCount { collection, field }) => {
                self.execute_estimate_count(task, tid, collection, field)
            }

            PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_filters,
                source_limit,
            }) => self.execute_insert_select(
                task,
                tid,
                target_collection,
                source_collection,
                source_filters,
                *source_limit,
            ),

            PhysicalPlan::Meta(MetaOp::CreateSnapshot) => self.execute_create_snapshot(task),

            PhysicalPlan::Meta(MetaOp::Compact) => self.execute_compact(task),

            PhysicalPlan::Meta(MetaOp::Checkpoint) => self.execute_checkpoint(task),

            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection,
                time_range,
                limit,
                filters,
                bucket_interval_ms,
                ..
            }) => self.execute_timeseries_scan(super::handlers::timeseries::TimeseriesScanParams {
                task,
                collection,
                time_range: *time_range,
                limit: *limit,
                filters,
                bucket_interval_ms: *bucket_interval_ms,
            }),

            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format,
            }) => self.execute_timeseries_ingest(task, collection, payload, format),

            PhysicalPlan::Spatial(SpatialOp::Scan {
                collection,
                field,
                predicate,
                query_geometry,
                distance_meters,
                attribute_filters,
                limit,
                projection,
                rls_filters,
            }) => self.execute_spatial_scan(
                task,
                tid,
                collection,
                field,
                predicate,
                query_geometry,
                *distance_meters,
                attribute_filters,
                *limit,
                projection,
                rls_filters,
            ),

            PhysicalPlan::Document(DocumentOp::Register {
                collection,
                index_paths,
                crdt_enabled,
                storage_mode,
            }) => self.execute_register_document_collection(
                task,
                tid,
                collection,
                index_paths,
                *crdt_enabled,
                storage_mode,
            ),

            PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection,
                path,
                value,
            }) => self.execute_document_index_lookup(task, tid, collection, path, value),

            PhysicalPlan::Document(DocumentOp::DropIndex { collection, field }) => {
                self.execute_drop_document_index(task, tid, collection, field)
            }

            PhysicalPlan::Kv(kv_op) => self.execute_kv(task, tid, kv_op),
        }
    }
}
