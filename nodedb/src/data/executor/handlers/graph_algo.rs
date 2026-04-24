//! Graph algorithm dispatch handler.
//!
//! Routes `PhysicalPlan::GraphAlgo` to the appropriate algorithm
//! implementation in `engine::graph::algo::*`. Each algorithm runs on
//! the caller's CSR partition and emits results with user-visible node
//! ids. There is no tenant prefix to strip on the way out — the
//! partition stores node names in their raw form, so algorithm output
//! is client-facing by construction.

use nodedb_graph::CsrIndex;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};
use crate::engine::graph::algo::result::AlgoResultBatch;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_algo(
        &self,
        task: &ExecutionTask,
        tid: u32,
        algorithm: &GraphAlgorithm,
        params: &AlgoParams,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            algorithm = algorithm.name(),
            collection = %params.collection,
            "graph algorithm dispatch"
        );

        if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "SSSP requires FROM '<source_node>'".into(),
                },
            );
        }

        let partition = match self.csr_partition(tid) {
            Some(p) => p,
            None => {
                // Tenant has no graph state — every algorithm returns
                // the empty batch for its schema. No error, no special
                // case at the algorithm level.
                return match AlgoResultBatch::new(*algorithm).to_msgpack() {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                };
            }
        };

        run_algorithm(partition, algorithm, params, &self.graph_tuning)
            .and_then(|batch| batch.to_msgpack())
            .map_or_else(
                |e| self.response_error(task, ErrorCode::from(e)),
                |payload| self.response_with_payload(task, payload),
            )
    }
}

/// Shared implementation used by both current-state and temporal
/// `execute_graph_algo*` handlers. Runs the algorithm and encodes the
/// response (success payload or structured error).
pub(super) fn run_algo_response(
    core: &CoreLoop,
    task: &ExecutionTask,
    csr: &CsrIndex,
    algorithm: &GraphAlgorithm,
    params: &AlgoParams,
) -> Response {
    if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
        return core.response_error(
            task,
            ErrorCode::Internal {
                detail: "SSSP requires FROM '<source_node>'".into(),
            },
        );
    }
    run_algorithm(csr, algorithm, params, &core.graph_tuning)
        .and_then(|batch| batch.to_msgpack())
        .map_or_else(
            |e| core.response_error(task, ErrorCode::from(e)),
            |payload| core.response_with_payload(task, payload),
        )
}

pub(super) fn run_algorithm(
    csr: &CsrIndex,
    algorithm: &GraphAlgorithm,
    params: &AlgoParams,
    tuning: &nodedb_types::config::tuning::GraphTuning,
) -> Result<AlgoResultBatch, crate::Error> {
    use crate::engine::graph::algo;
    match algorithm {
        GraphAlgorithm::PageRank => Ok(algo::pagerank::run(csr, params)),
        GraphAlgorithm::Wcc => Ok(algo::wcc::run(csr)),
        GraphAlgorithm::LabelPropagation => Ok(algo::label_propagation::run(csr, params)),
        GraphAlgorithm::Lcc => Ok(algo::lcc::run(
            csr,
            tuning.lcc_high_degree_threshold,
            tuning.lcc_sample_pairs,
        )),
        GraphAlgorithm::Sssp => algo::sssp::run(csr, params),
        GraphAlgorithm::Betweenness => Ok(algo::betweenness::run(csr, params)),
        GraphAlgorithm::Closeness => Ok(algo::closeness::run(csr)),
        GraphAlgorithm::Harmonic => Ok(algo::harmonic::run(csr)),
        GraphAlgorithm::Degree => Ok(algo::degree::run(csr, params)),
        GraphAlgorithm::Louvain => Ok(algo::louvain::run(csr, params)),
        GraphAlgorithm::Triangles => Ok(algo::triangles::run(csr, params)),
        GraphAlgorithm::Diameter => Ok(algo::diameter::run(csr, params)),
        GraphAlgorithm::KCore => Ok(algo::kcore::run(csr)),
    }
}
