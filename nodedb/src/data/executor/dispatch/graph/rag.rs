// SPDX-License-Identifier: BUSL-1.1

//! `GraphOp::RagFusion` routing: the whole fusion, or one stage of a
//! cluster fusion.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::GraphOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor::dispatch) fn dispatch_rag_fusion(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &GraphOp,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let GraphOp::RagFusion {
            collection,
            query_vector,
            vector_top_k,
            edge_label,
            direction,
            expansion_depth,
            final_top_k,
            rrf_k,
            rrf_k_triple,
            vector_field,
            options,
            bm25_query,
            bm25_field,
            stage,
            scope,
        } = op
        else {
            return self.response_error(
                task,
                crate::Error::Internal {
                    detail: "rag fusion dispatch received another graph op".into(),
                },
            );
        };

        // The vector and text legs read no CSR. The expansion and
        // the bindings read the collection's graph at the cut.
        if !matches!(stage, nodedb_physical::physical_plan::RagStage::ExportLegs)
            && let Err(e) =
                self.prepare_csr_view(database_id, tid, Some(collection.as_str()), scope)
        {
            return self.response_error(task, e);
        }
        let triple_text = match (bm25_query.as_deref(), bm25_field.as_deref(), rrf_k_triple) {
            (Some(query), Some(field), Some(_)) => {
                Some(crate::data::executor::handlers::graph_rag_triple::Bm25Leg { query, field })
            }
            _ => None,
        };
        match stage {
            nodedb_physical::physical_plan::RagStage::Local => {}
            nodedb_physical::physical_plan::RagStage::ExportLegs => {
                return self.execute_rag_export_legs(
                    task,
                    crate::data::executor::handlers::graph_rag_stages::RagExportParams {
                        tenant_id: tid,
                        collection: collection.as_str(),
                        query_vector,
                        vector_top_k: *vector_top_k,
                        vector_field: vector_field.as_str(),
                        final_top_k: *final_top_k,
                        bm25: triple_text,
                    },
                );
            }
            nodedb_physical::physical_plan::RagStage::Bindings { surrogates, names } => {
                return self.execute_rag_bindings(
                    task,
                    crate::data::executor::handlers::graph_rag_stages::RagBindingsParams {
                        tenant_id: tid,
                        collection: collection.as_str(),
                        surrogates,
                        names,
                        scope,
                    },
                );
            }
        }
        if let (Some(bm25_q), Some(bm25_f), Some(triple_k)) =
            (bm25_query.as_deref(), bm25_field.as_deref(), rrf_k_triple)
        {
            self.execute_graph_rag_fusion_triple(
                task,
                crate::data::executor::handlers::graph_rag_triple::GraphRagFusionTripleParams {
                    tenant_id: tid,
                    collection: collection.as_str(),
                    query_vector,
                    vector_top_k: *vector_top_k,
                    edge_label,
                    direction: *direction,
                    expansion_depth: *expansion_depth,
                    final_top_k: *final_top_k,
                    rrf_k: *triple_k,
                    vector_field: vector_field.as_str(),
                    max_visited: options.max_visited,
                    bm25_query: bm25_q,
                    bm25_field: bm25_f,
                    scope,
                },
            )
        } else {
            self.execute_graph_rag_fusion(
                task,
                crate::data::executor::handlers::graph_rag::GraphRagFusionParams {
                    tenant_id: tid,
                    collection: collection.as_str(),
                    query_vector,
                    vector_top_k: *vector_top_k,
                    edge_label,
                    direction: *direction,
                    expansion_depth: *expansion_depth,
                    final_top_k: *final_top_k,
                    rrf_k: *rrf_k,
                    vector_field: vector_field.as_str(),
                    max_visited: options.max_visited,
                    scope,
                },
            )
        }
    }
}
