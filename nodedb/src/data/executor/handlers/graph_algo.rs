//! Graph algorithm dispatch handler.
//!
//! Routes `PhysicalPlan::GraphAlgo` to the appropriate algorithm
//! implementation in `engine::graph::algo::*`. Each algorithm runs
//! on the in-memory CSR index and returns JSON-serialized results.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_algo(
        &self,
        task: &ExecutionTask,
        algorithm: &GraphAlgorithm,
        params: &AlgoParams,
    ) -> Response {
        debug!(
            core = self.core_id,
            algorithm = algorithm.name(),
            collection = %params.collection,
            "graph algorithm dispatch"
        );

        // Validate source_node for SSSP.
        if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "SSSP requires FROM '<source_node>'".into(),
                },
            );
        }

        use crate::engine::graph::algo;

        let result: Result<Vec<u8>, crate::Error> = match algorithm {
            // ── Graphalytics (5 core algorithms) ──
            GraphAlgorithm::PageRank => algo::pagerank::run(&self.csr, params).to_json(),
            GraphAlgorithm::Wcc => algo::wcc::run(&self.csr).to_json(),
            GraphAlgorithm::LabelPropagation => {
                algo::label_propagation::run(&self.csr, params).to_json()
            }
            GraphAlgorithm::Lcc => algo::lcc::run(&self.csr).to_json(),
            GraphAlgorithm::Sssp => {
                algo::sssp::run(&self.csr, params).and_then(|batch| batch.to_json())
            }
            // ── Centrality suite ──
            GraphAlgorithm::Betweenness => algo::betweenness::run(&self.csr, params).to_json(),
            GraphAlgorithm::Closeness => algo::closeness::run(&self.csr).to_json(),
            GraphAlgorithm::Harmonic => algo::harmonic::run(&self.csr).to_json(),
            GraphAlgorithm::Degree => algo::degree::run(&self.csr, params).to_json(),
            // ── Extended algorithms ──
            GraphAlgorithm::Louvain => algo::louvain::run(&self.csr, params).to_json(),
            GraphAlgorithm::Triangles => algo::triangles::run(&self.csr, params).to_json(),
            GraphAlgorithm::Diameter => algo::diameter::run(&self.csr, params).to_json(),
            GraphAlgorithm::KCore => algo::kcore::run(&self.csr).to_json(),
        };

        match result {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
