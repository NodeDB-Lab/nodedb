//! Main execute() dispatch: matches on PhysicalPlan and delegates to handlers.

use crate::bridge::envelope::{PhysicalPlan, Response};

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

impl CoreLoop {
    /// Execute a physical plan. Dispatches to the appropriate handler.
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match task.plan() {
            PhysicalPlan::PointGet {
                collection,
                document_id,
            } => self.execute_point_get(task, tid, collection, document_id),

            PhysicalPlan::PointPut {
                collection,
                document_id,
                value,
            } => self.execute_point_put(task, tid, collection, document_id, value),

            PhysicalPlan::PointDelete {
                collection,
                document_id,
            } => self.execute_point_delete(task, tid, collection, document_id),

            PhysicalPlan::PointUpdate {
                collection,
                document_id,
                updates,
            } => self.execute_point_update(task, tid, collection, document_id, updates),

            PhysicalPlan::VectorInsert {
                collection,
                vector,
                dim,
                field_name,
            } => self.execute_vector_insert(task, tid, collection, vector, *dim, field_name),

            PhysicalPlan::VectorBatchInsert {
                collection,
                vectors,
                dim,
            } => self.execute_vector_batch_insert(task, tid, collection, vectors, *dim),

            PhysicalPlan::VectorMultiSearch {
                collection,
                query_vector,
                top_k,
                ef_search,
                filter_bitmap,
            } => self.execute_vector_multi_search(
                task,
                tid,
                collection,
                query_vector,
                *top_k,
                *ef_search,
                filter_bitmap.as_ref(),
            ),

            PhysicalPlan::VectorDelete {
                collection,
                vector_id,
            } => self.execute_vector_delete(task, tid, collection, *vector_id),

            PhysicalPlan::VectorSearch {
                collection,
                query_vector,
                top_k,
                ef_search,
                filter_bitmap,
                field_name,
            } => self.execute_vector_search(
                task,
                tid,
                collection,
                query_vector,
                *top_k,
                *ef_search,
                filter_bitmap.as_ref(),
                field_name,
            ),

            PhysicalPlan::SetVectorParams {
                collection,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
            } => self.execute_set_vector_params(
                task,
                tid,
                collection,
                *m,
                *ef_construction,
                metric,
                index_type,
                *pq_m,
                *ivf_cells,
                *ivf_nprobe,
            ),

            PhysicalPlan::DocumentScan {
                collection,
                limit,
                offset,
                sort_keys,
                filters,
                distinct,
                projection,
            } => self.execute_document_scan(
                task, tid, collection, *limit, *offset, sort_keys, filters, *distinct, projection,
            ),

            PhysicalPlan::DocumentBatchInsert {
                collection,
                documents,
            } => self.execute_document_batch_insert(task, tid, collection, documents),

            PhysicalPlan::Aggregate {
                collection,
                group_by,
                aggregates,
                filters,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
            } => self.execute_aggregate(
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

            PhysicalPlan::HashJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
            } => self.execute_hash_join(
                task,
                tid,
                left_collection,
                right_collection,
                on,
                join_type,
                *limit,
            ),

            PhysicalPlan::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            } => self.execute_edge_put(task, src_id, label, dst_id, properties),

            PhysicalPlan::EdgeDelete {
                src_id,
                label,
                dst_id,
            } => self.execute_edge_delete(task, src_id, label, dst_id),

            PhysicalPlan::GraphHop {
                start_nodes,
                edge_label,
                direction,
                depth,
                options: _,
            } => self.execute_graph_hop(task, start_nodes, edge_label, *direction, *depth),

            PhysicalPlan::GraphNeighbors {
                node_id,
                edge_label,
                direction,
            } => self.execute_graph_neighbors(task, node_id, edge_label, *direction),

            PhysicalPlan::GraphPath {
                src,
                dst,
                edge_label,
                max_depth,
                options: _,
            } => self.execute_graph_path(task, src, dst, edge_label, *max_depth),

            PhysicalPlan::GraphSubgraph {
                start_nodes,
                edge_label,
                depth,
                options: _,
            } => self.execute_graph_subgraph(task, start_nodes, edge_label, *depth),

            PhysicalPlan::GraphRagFusion {
                collection,
                query_vector,
                vector_top_k,
                edge_label,
                direction,
                expansion_depth,
                final_top_k,
                rrf_k,
                options,
            } => self.execute_graph_rag_fusion(
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

            PhysicalPlan::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
            } => self.execute_range_scan(
                task,
                tid,
                collection,
                field,
                lower.as_deref(),
                upper.as_deref(),
                *limit,
            ),

            PhysicalPlan::CrdtRead {
                collection,
                document_id,
            } => self.execute_crdt_read(task, collection, document_id),

            PhysicalPlan::CrdtApply {
                collection: _,
                document_id: _,
                delta,
                peer_id: _,
                mutation_id: _,
            } => self.execute_crdt_apply(task, delta),

            PhysicalPlan::SetCollectionPolicy {
                collection,
                policy_json,
            } => self.execute_set_collection_policy(task, collection, policy_json),

            PhysicalPlan::TextSearch {
                collection,
                query,
                top_k,
                fuzzy,
            } => self.execute_text_search(task, tid, collection, query, *top_k, *fuzzy),

            PhysicalPlan::HybridSearch {
                collection,
                query_vector,
                query_text,
                top_k,
                ef_search,
                fuzzy,
                vector_weight,
                filter_bitmap,
            } => self.execute_hybrid_search(
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
            ),

            PhysicalPlan::WalAppend { payload } => self.execute_wal_append(task, payload),

            PhysicalPlan::Cancel { target_request_id } => {
                self.execute_cancel(task, *target_request_id)
            }

            PhysicalPlan::NestedLoopJoin {
                left_collection,
                right_collection,
                condition,
                join_type,
                limit,
            } => self.execute_nested_loop_join(
                task,
                tid,
                left_collection,
                right_collection,
                condition,
                join_type,
                *limit,
            ),

            PhysicalPlan::TransactionBatch { plans } => {
                self.execute_transaction_batch(task, tid, plans)
            }

            PhysicalPlan::PartialAggregate {
                collection,
                group_by,
                aggregates,
                filters,
            } => self.execute_aggregate(
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

            PhysicalPlan::BroadcastJoin {
                large_collection,
                broadcast_data,
                on,
                join_type,
                limit,
            } => self.execute_broadcast_join(
                task,
                tid,
                large_collection,
                broadcast_data,
                on,
                join_type,
                *limit,
            ),

            PhysicalPlan::ShuffleJoin { .. } => {
                // Shuffle join phase 2 (local join on repartitioned data) — not yet wired.
                self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: "shuffle join not dispatched to this core".into(),
                    },
                )
            }

            PhysicalPlan::CreateSnapshot => self.execute_create_snapshot(task),

            PhysicalPlan::Compact => self.execute_compact(task),

            PhysicalPlan::Checkpoint => self.execute_checkpoint(task),
        }
    }
}
