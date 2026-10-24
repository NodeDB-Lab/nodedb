// SPDX-License-Identifier: BUSL-1.1

//! The staged parts of a cluster RAG fusion (`RagStage`).
//!
//! In a cluster a coordinator runs the fusion in stages
//! (`control::server::graph_dispatch::rag_fusion`). This core answers one
//! stage:
//!
//! - `ExportLegs`: the raw vector hits, and the BM25 hits of a three-source
//!   fusion, from this core's indexes of the collection.
//! - `Bindings`: which requested surrogates name nodes in this core's graph
//!   partition, which requested names carry a surrogate, and whether the
//!   partition holds edges of the collection.

use nodedb_physical::physical_plan::{
    GraphReadScope, RagBindingRow, RagLegs, RagTextHit, RagVectorHit,
};
use nodedb_types::Surrogate;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::graph_rag_triple::Bm25Leg;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Bundled arguments for [`CoreLoop::execute_rag_export_legs`].
pub(in crate::data::executor) struct RagExportParams<'a> {
    pub tenant_id: u64,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub vector_top_k: usize,
    pub vector_field: &'a str,
    pub final_top_k: usize,
    /// The BM25 leg of a three-source fusion. `None` for two sources.
    pub bm25: Option<Bm25Leg<'a>>,
}

/// Bundled arguments for [`CoreLoop::execute_rag_bindings`].
pub(in crate::data::executor) struct RagBindingsParams<'a> {
    pub tenant_id: u64,
    pub collection: &'a str,
    pub surrogates: &'a [u32],
    pub names: &'a [String],
    /// The graph the bindings name nodes in: the fusion's bitemporal cut.
    pub scope: &'a GraphReadScope,
}

impl CoreLoop {
    /// Answer the fusion's raw legs as a one-element msgpack array of
    /// [`RagLegs`]. A missing vector index answers `NotFound`, as the
    /// single-core fusion does.
    pub(in crate::data::executor) fn execute_rag_export_legs(
        &self,
        task: &ExecutionTask,
        params: RagExportParams<'_>,
    ) -> Response {
        let RagExportParams {
            tenant_id,
            collection,
            query_vector,
            vector_top_k,
            vector_field,
            final_top_k,
            bm25,
        } = params;
        let hits = match self.vector_leg_hits(
            task,
            tenant_id,
            collection,
            query_vector,
            vector_top_k,
            vector_field,
        ) {
            Ok(hits) => hits,
            Err(response) => return response,
        };
        let text = match bm25 {
            Some(leg) => match self.bm25_leg_hits(
                task,
                TenantId::new(tenant_id),
                collection,
                leg,
                final_top_k,
            ) {
                Ok(text) => text,
                Err(response) => return response,
            },
            None => Vec::new(),
        };
        let legs = RagLegs {
            vector: hits
                .into_iter()
                .map(|(result, surrogate)| RagVectorHit {
                    surrogate: surrogate.map(|s| s.as_u32()),
                    entry_id: result.id,
                    distance: result.distance,
                })
                .collect(),
            text: text
                .into_iter()
                .map(|(surrogate, score)| RagTextHit {
                    surrogate: surrogate.as_u32(),
                    score,
                })
                .collect(),
        };
        let answer: Vec<RagLegs> = vec![legs];
        self.encode_stage_answer(task, &answer)
    }

    /// Answer this core's graph bindings as a msgpack array of
    /// [`RagBindingRow`].
    pub(in crate::data::executor) fn execute_rag_bindings(
        &self,
        task: &ExecutionTask,
        params: RagBindingsParams<'_>,
    ) -> Response {
        let RagBindingsParams {
            tenant_id,
            collection,
            surrogates,
            names,
            scope,
        } = params;
        let database_id = task.request.database_id.as_u64();
        let mut rows: Vec<RagBindingRow> = Vec::new();
        let partition = match self.csr_view(database_id, tenant_id, Some(collection), scope) {
            Ok(partition) => partition,
            Err(e) => return self.response_error(task, e),
        };
        if let Some(partition) = partition {
            if partition.collection_id(collection).is_some() {
                rows.push(RagBindingRow::KnowsCollection);
            }
            for &raw in surrogates {
                if let Some(name) = partition.node_id_for_surrogate(Surrogate::new(raw)) {
                    rows.push(RagBindingRow::Bound {
                        name: name.to_string(),
                        surrogate: raw,
                    });
                }
            }
            for name in names {
                let bound = partition
                    .local_id_for_node(name)
                    .map(|local| partition.node_surrogate_raw(local))
                    .filter(|&raw| raw != 0);
                if let Some(raw) = bound {
                    rows.push(RagBindingRow::Bound {
                        name: name.clone(),
                        surrogate: raw,
                    });
                }
            }
        }
        self.encode_stage_answer(task, &rows)
    }

    fn encode_stage_answer<T: zerompk::ToMessagePack>(
        &self,
        task: &ExecutionTask,
        answer: &T,
    ) -> Response {
        match zerompk::to_msgpack_vec(answer) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("rag fusion stage encode: {e}"),
                },
            ),
        }
    }
}
