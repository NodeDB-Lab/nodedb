// SPDX-License-Identifier: BUSL-1.1

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
use crate::engine::graph::edge_store::Direction;

/// Parameters for [`CoreLoop::execute_graph_temporal_neighbors`]. Packed
/// so the handler signature stays within clippy's 7-arg budget — the
/// wire-level `GraphOp::TemporalNeighbors` variant carries the same
/// fields, and the dispatcher populates this struct once per call.
pub(in crate::data::executor) struct TemporalNeighborsParams<'a> {
    pub tid: u64,
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
        let database_id = task.request.database_id.as_u64();
        let tenant = TenantId::new(tid);
        let as_of_params = crate::engine::graph::edge_store::NeighborsAsOfParams {
            db: database_id,
            tid: tenant,
            collection,
            node: node_id,
            label_filter: edge_label.as_deref(),
            system_as_of_ms,
            valid_at_ms,
        };
        let edges = match self.edge_store.node_edges_as_of(as_of_params, direction) {
            Ok(edges) => edges,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };
        // A current-state read inside a transaction sees that transaction's
        // own staged edge writes in this collection.
        let edges = match (system_as_of_ms, task.request.txn_id) {
            (None, Some(txn_id)) => {
                self.touch_overlay(txn_id);
                super::graph::graph_txn_merge::merge_graph_txn_overlay_collection_edges(
                    self.graph_txn_overlays.get(&txn_id),
                    &(task.request.database_id, tenant, collection.to_string()),
                    node_id,
                    edge_label.as_deref().as_slice(),
                    direction,
                    edges,
                )
            }
            _ => edges,
        };

        let entries: Vec<super::super::response_codec::NeighborEntry<'_>> = edges
            .iter()
            .map(|(src, label, dst)| {
                let opposite = if src == node_id {
                    dst.as_str()
                } else {
                    src.as_str()
                };
                super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
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
        tid: u64,
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
        let database_id = task.request.database_id.as_u64();
        let memory = nodedb_mem::ScopedMemory::new(
            self.governor.clone(),
            task.request.database_id,
            crate::types::TenantId::new(tid),
            nodedb_mem::EngineId::Graph,
        );

        let scoped_csr = match super::graph_algo::build_csr_for_collection(
            &self.edge_store,
            database_id,
            tid,
            &params.collection,
            params.edge_label.as_deref(),
            cutoff_ordinal,
            memory,
        ) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        if scoped_csr.node_count() == 0 {
            return match crate::engine::graph::algo::result::AlgoResultBatch::new(*algorithm)
                .to_msgpack()
            {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::from(e)),
            };
        }

        super::graph_algo::run_algo_response(self, task, &scoped_csr, algorithm, params)
    }
}
