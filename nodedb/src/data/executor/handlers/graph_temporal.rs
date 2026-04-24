//! Bitemporal graph handlers.
//!
//! Implements [`GraphOp::TemporalNeighbors`] and [`GraphOp::TemporalAlgorithm`]
//! by reading versioned edges directly from the `EdgeStore` at the requested
//! system-time cutoff, rather than the current-state CSR partition.

use nodedb_types::{TenantId, ms_to_ordinal_upper};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};
use crate::engine::graph::csr::rebuild::rebuild_sharded_from_store_as_of;
use crate::engine::graph::edge_store::Direction;

/// Parameters for [`CoreLoop::execute_graph_temporal_neighbors`]. Packed
/// so the handler signature stays within clippy's 7-arg budget — the
/// wire-level `GraphOp::TemporalNeighbors` variant carries the same
/// fields, and the dispatcher populates this struct once per call.
pub(in crate::data::executor) struct TemporalNeighborsParams<'a> {
    pub tid: u32,
    pub collection: &'a str,
    pub node_id: &'a str,
    pub edge_label: &'a Option<String>,
    pub direction: Direction,
    pub system_as_of_ms: Option<i64>,
    pub valid_at_ms: Option<i64>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_temporal_neighbors(
        &self,
        task: &ExecutionTask,
        p: TemporalNeighborsParams<'_>,
    ) -> Response {
        let TemporalNeighborsParams {
            tid,
            collection,
            node_id,
            edge_label,
            direction,
            system_as_of_ms,
            valid_at_ms,
        } = p;
        debug!(
            core = self.core_id,
            tid,
            %collection,
            %node_id,
            ?edge_label,
            ?direction,
            ?system_as_of_ms,
            ?valid_at_ms,
            "graph temporal neighbors"
        );
        let tenant = TenantId::new(tid);
        let edges_result = match direction {
            Direction::Out => self.edge_store.neighbors_out_as_of(
                tenant,
                collection,
                node_id,
                edge_label.as_deref(),
                system_as_of_ms,
                valid_at_ms,
            ),
            Direction::In => self.edge_store.neighbors_in_as_of(
                tenant,
                collection,
                node_id,
                edge_label.as_deref(),
                system_as_of_ms,
                valid_at_ms,
            ),
            Direction::Both => {
                let out = self.edge_store.neighbors_out_as_of(
                    tenant,
                    collection,
                    node_id,
                    edge_label.as_deref(),
                    system_as_of_ms,
                    valid_at_ms,
                );
                match out {
                    Ok(mut out) => match self.edge_store.neighbors_in_as_of(
                        tenant,
                        collection,
                        node_id,
                        edge_label.as_deref(),
                        system_as_of_ms,
                        valid_at_ms,
                    ) {
                        Ok(inbound) => {
                            out.extend(inbound);
                            Ok(out)
                        }
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                }
            }
        };

        let edges = match edges_result {
            Ok(e) => e,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        let entries: Vec<super::super::response_codec::NeighborEntry<'_>> = edges
            .iter()
            .map(|e| {
                let opposite = if e.src_id == node_id {
                    e.dst_id.as_str()
                } else {
                    e.src_id.as_str()
                };
                super::super::response_codec::NeighborEntry {
                    label: e.label.as_str(),
                    node: opposite,
                }
            })
            .collect();
        match super::super::response_codec::encode(&entries) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_graph_temporal_algo(
        &self,
        task: &ExecutionTask,
        tid: u32,
        algorithm: &GraphAlgorithm,
        params: &AlgoParams,
        system_as_of_ms: Option<i64>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            algorithm = algorithm.name(),
            collection = %params.collection,
            ?system_as_of_ms,
            "graph temporal algorithm dispatch"
        );
        let cutoff_ordinal = system_as_of_ms.map(ms_to_ordinal_upper);
        let sharded = match rebuild_sharded_from_store_as_of(&self.edge_store, cutoff_ordinal) {
            Ok(s) => s,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };
        let csr = match sharded.partition(TenantId::new(tid)) {
            Some(p) => p,
            None => {
                // Tenant has no graph state at this cutoff — every algorithm
                // returns the empty batch for its schema. Mirrors the
                // current-state path in `graph_algo.rs`.
                return match crate::engine::graph::algo::result::AlgoResultBatch::new(*algorithm)
                    .to_msgpack()
                {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                };
            }
        };

        super::graph_algo::run_algo_response(self, task, csr, algorithm, params)
    }
}
