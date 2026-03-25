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

        let result: Result<Vec<u8>, crate::Error> = match algorithm {
            GraphAlgorithm::PageRank => {
                let batch = crate::engine::graph::algo::pagerank::run(&self.csr, params);
                batch.to_json()
            }
            GraphAlgorithm::Wcc => {
                let batch = crate::engine::graph::algo::wcc::run(&self.csr);
                batch.to_json()
            }
            GraphAlgorithm::LabelPropagation => {
                let batch = crate::engine::graph::algo::label_propagation::run(&self.csr, params);
                batch.to_json()
            }
            GraphAlgorithm::Lcc => {
                let batch = crate::engine::graph::algo::lcc::run(&self.csr);
                batch.to_json()
            }
            GraphAlgorithm::Sssp => crate::engine::graph::algo::sssp::run(&self.csr, params)
                .and_then(|batch| batch.to_json()),
            _ => Err(crate::Error::BadRequest {
                detail: format!("graph algorithm '{}' not yet implemented", algorithm.name()),
            }),
        };

        match result {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
