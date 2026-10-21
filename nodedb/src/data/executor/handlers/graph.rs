// SPDX-License-Identifier: BUSL-1.1

//! Graph operation handlers: EdgePut, EdgeDelete, GraphHop, GraphNeighbors,
//! GraphPath, GraphSubgraph. The CSR index is partitioned structurally by
//! tenant. A read resolves its CSR once via `self.csr_view`, the live
//! partition or a historical CSR at the plan's cut; a write resolves the
//! live partition via `self.csr_partition_mut`. Both address node ids in
//! their raw, user-visible form throughout — no `<tid>:` prefix, no
//! `scoped_node()`.

use nodedb_types::diagnostic::DiagnosticLayer;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::Direction;
use crate::types::TenantId;
use nodedb_physical::physical_plan::GraphReadScope;

#[path = "graph_edge_resolve.rs"]
pub(in crate::data::executor) mod graph_edge_resolve;
#[path = "graph_edge_write/mod.rs"]
pub(in crate::data::executor) mod graph_edge_write;
#[path = "graph_traversal.rs"]
pub(in crate::data::executor) mod graph_traversal;
#[path = "graph_txn_merge.rs"]
pub(in crate::data::executor) mod graph_txn_merge;

pub(in crate::data::executor) use graph_edge_write::{EdgeDeleteParams, EdgePutParams};

use graph_txn_merge::merge_graph_txn_overlay_neighbors;

use super::graph_edge_predicate;

/// Bundled arguments for [`CoreLoop::execute_graph_hop`].
pub(in crate::data::executor) struct GraphHopParams<'a> {
    pub tid: u64,
    pub start_nodes: &'a [String],
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: crate::engine::graph::edge_store::Direction,
    pub depth: usize,
    /// The walk's visit cap ([`CoreLoop::walk_visit_cap`]).
    pub max_visited: usize,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    /// The collection a historical read walks. A live read walks every
    /// collection's edges.
    pub collection: Option<&'a str>,
    /// The bitemporal cut the walk reads.
    pub scope: &'a GraphReadScope,
}

/// Arguments for [`CoreLoop::execute_graph_neighbors_multi`].
pub(in crate::data::executor) struct GraphNeighborsMultiArgs<'a> {
    pub node_ids: &'a [String],
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: crate::engine::graph::edge_store::Direction,
    pub max_results: u32,
    /// Collection scope, or `None` for a label-only traversal.
    pub collection: Option<&'a str>,
    /// AND-ed edge-property predicate. Empty admits every edge.
    pub edge_predicate: &'a [nodedb_types::filter::MetadataFilter],
    /// Each row carries the crossed edge's property object.
    pub with_properties: bool,
    /// The bitemporal cut the hop reads.
    pub scope: &'a GraphReadScope,
}

/// Arguments for [`CoreLoop::execute_graph_neighbors`].
pub(in crate::data::executor) struct GraphNeighborsArgs<'a> {
    pub node_id: &'a str,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: crate::engine::graph::edge_store::Direction,
    /// Collection scope, or `None` for a label-only lookup.
    pub collection: Option<&'a str>,
    /// The bitemporal cut the lookup reads.
    pub scope: &'a GraphReadScope,
}

/// One `NeighborsMulti` row: `(frontier node, label, neighbour, properties)`.
type NeighborRow = (String, String, String, Option<nodedb_types::NativeCell>);

/// The error for a `NeighborsMulti` hop whose rows outgrow its `max_results`.
///
/// The hop answers in one terminal frame, so it has no partial result to
/// report. A cut hop is refused rather than answered short.
fn neighbor_rows_over_cap(cap: usize) -> crate::Error {
    crate::Error::ExecutionLimitExceeded {
        detail: format!(
            "graph neighbors hop produced more than max_results ({cap}) rows on one core; \
             narrow the frontier or raise the walk's visit cap"
        ),
    }
}

impl CoreLoop {
    /// The visit cap of a hop, subgraph or path walk: the plan's cap, bounded
    /// by this core's graph tuning. The cluster walk coordinators
    /// (`graph_dispatch::bfs`, `graph_dispatch::shortest_path`) bound it the
    /// same way, so a capped walk answers the same on one core and across a
    /// cluster.
    pub(in crate::data::executor) fn walk_visit_cap(
        &self,
        options: &crate::engine::graph::traversal_options::GraphTraversalOptions,
    ) -> usize {
        options.max_visited.min(self.graph_tuning.max_visited)
    }

    /// The transaction's staged edge writes, for a live read. A historical
    /// read sees committed versions only. Read-your-own-writes refreshes the
    /// overlay lease (see the overlay reaper).
    fn live_overlay(
        &self,
        task: &ExecutionTask,
        scope: &GraphReadScope,
    ) -> Option<&crate::data::executor::handlers::transaction::overlay::GraphTxnOverlay> {
        if !scope.is_live() {
            return None;
        }
        let txn_id = task.request.txn_id?;
        self.touch_overlay(txn_id);
        self.graph_txn_overlays.get(&txn_id)
    }

    pub(in crate::data::executor) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        params: GraphHopParams<'_>,
    ) -> Response {
        let GraphHopParams {
            tid,
            start_nodes,
            edge_labels,
            direction,
            depth,
            max_visited,
            frontier_bitmap,
            collection,
            scope,
        } = params;
        debug!(
            core = self.core_id,
            tid,
            ?start_nodes,
            ?edge_labels,
            ?direction,
            depth,
            "graph hop"
        );
        let database_id = task.request.database_id.as_u64();
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        // Read-your-own-writes: the staged delta joins a live walk at every
        // depth. A tenant with no CSR partition still walks its staged edges.
        let delta = self.live_overlay_delta(task, tid, scope);
        let csr = match self.csr_view(database_id, tid, collection, scope) {
            Ok(csr) => csr,
            Err(e) => return self.response_error(task, e),
        };
        let labels: Vec<&str> = edge_labels.iter().map(String::as_str).collect();
        let result: Vec<String> = CsrIndex::traverse_bfs_on(
            csr,
            nodedb_graph::BfsParams {
                start_nodes: &refs,
                label_filter: &labels,
                direction,
                max_depth: depth,
                max_visited,
                frontier_bitmap,
            },
            delta.as_ref(),
        );
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph hop serialization failed");
                self.response_error(task, ErrorCode::from(e))
            }
        }
    }

    pub(in crate::data::executor) fn execute_graph_neighbors(
        &self,
        task: &ExecutionTask,
        tid: u64,
        args: GraphNeighborsArgs<'_>,
    ) -> Response {
        let GraphNeighborsArgs {
            node_id,
            edge_labels,
            direction,
            collection,
            scope,
        } = args;
        debug!(core = self.core_id, tid, %node_id, ?edge_labels, ?direction, ?scope, "graph neighbors");
        let database_id = task.request.database_id.as_u64();
        let labels: Vec<&str> = edge_labels.iter().map(String::as_str).collect();
        // A named collection restricts the walk; unscoped `neighbors` would
        // silently span every collection's edges in the shared node space.
        let partition = match self.csr_view(database_id, tid, collection, scope) {
            Ok(partition) => partition,
            Err(e) => return self.response_error(task, e),
        };
        let durable_of = |pass: Direction| match (partition, collection) {
            (Some(partition), Some(collection)) => {
                partition.neighbors_in_collection(node_id, &labels, pass, collection)
            }
            (Some(partition), None) => partition.neighbors(node_id, &labels, pass),
            (None, _) => Vec::new(),
        };
        // Read-your-own-writes: fold staged edge writes of the same
        // collection scope into a live result (see `graph_txn_merge`), and
        // refresh the overlay lease.
        let overlay = self.live_overlay(task, scope);
        let neighbors = match overlay {
            Some(overlay) => merge_graph_txn_overlay_neighbors(
                overlay,
                &graph_txn_merge::StagedHopScope {
                    database_id: task.request.database_id,
                    tenant: TenantId::new(tid),
                    collection,
                    edge_labels: &labels,
                },
                node_id,
                direction,
                durable_of,
            ),
            None => durable_of(direction),
        };
        let result: Vec<_> = neighbors
            .iter()
            .map(
                |(label, node)| super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
                    node: node.as_str(),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph neighbors serialization failed");
                self.response_error(task, ErrorCode::from(e))
            }
        }
    }

    pub(in crate::data::executor) fn execute_graph_neighbors_multi(
        &self,
        task: &ExecutionTask,
        tid: u64,
        args: GraphNeighborsMultiArgs<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            count = args.node_ids.len(),
            edge_labels = ?args.edge_labels,
            direction = ?args.direction,
            max_results = args.max_results,
            predicate_terms = args.edge_predicate.len(),
            with_properties = args.with_properties,
            "graph neighbors multi"
        );
        let rows = match self.collect_neighbor_rows(task, tid, &args) {
            Ok(rows) => rows,
            Err(error) => return self.response_error(task, error),
        };
        let entries: Vec<super::super::response_codec::NeighborMultiEntry> = rows
            .iter()
            .map(|(src, label, node, properties)| {
                super::super::response_codec::NeighborMultiEntry {
                    src: src.as_str(),
                    label: label.as_str(),
                    node: node.as_str(),
                    properties: properties.as_ref(),
                }
            })
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        // The answer is always one terminal frame: a gather drains `Partial`
        // frames until a terminal one arrives, so a lone `Partial` frame
        // leaves it waiting until the statement's deadline.
        match super::super::response_codec::encode(&entries) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(
                    core = self.core_id,
                    layer = DiagnosticLayer::WireShape.as_str(),
                    error = %e,
                    "graph neighbors-multi serialization failed"
                );
                self.response_error(task, ErrorCode::from(e))
            }
        }
    }

    /// The rows of one `NeighborsMulti` hop. A row counts against
    /// `max_results` only once the edge predicate admits it. A hop with more
    /// rows than `max_results` is an `ExecutionLimitExceeded` error.
    ///
    /// A request with a `txn_id` merges that transaction's staged edge
    /// writes on this core: a staged tombstone drops an edge, a staged put
    /// adds one, and a staged put's property map is the map the predicate
    /// tests and the row returns.
    fn collect_neighbor_rows(
        &self,
        task: &ExecutionTask,
        tid: u64,
        args: &GraphNeighborsMultiArgs<'_>,
    ) -> crate::Result<Vec<NeighborRow>> {
        let cap: usize = if args.max_results == 0 {
            usize::MAX
        } else {
            args.max_results as usize
        };
        let database_id = task.request.database_id;
        let labels: Vec<&str> = args.edge_labels.iter().map(String::as_str).collect();
        let overlay = self.live_overlay(task, args.scope);
        let mut edge_properties = graph_edge_predicate::HopEdgeProperties::open(
            &self.edge_store,
            graph_edge_predicate::HopPropertyScope {
                database: database_id,
                tenant: TenantId::new(tid),
                collection: args.collection,
                filters: args.edge_predicate,
                with_properties: args.with_properties,
                read_scope: *args.scope,
            },
        )?;
        let mut out: Vec<NeighborRow> = Vec::with_capacity(args.node_ids.len().min(cap) * 4);
        let partition = self.csr_view(database_id.as_u64(), tid, args.collection, args.scope)?;
        let durable = |node: &str, direction| match (partition, args.collection) {
            (Some(partition), Some(collection)) => {
                partition.neighbors_in_collection(node, &labels, direction, collection)
            }
            (Some(partition), None) => partition.neighbors(node, &labels, direction),
            (None, _) => Vec::new(),
        };
        let oriented = edge_properties.is_some() || overlay.is_some();
        let passes = match graph_edge_predicate::hop_passes(args.direction, oriented) {
            graph_edge_predicate::HopPasses::Oriented(passes) => passes,
            // No predicate, no properties and no staged writes: each row is
            // the durable edge as the CSR returns it.
            graph_edge_predicate::HopPasses::Unoriented => {
                for raw_src in args.node_ids {
                    for (label, node) in durable(raw_src, args.direction) {
                        if out.len() >= cap {
                            return Err(neighbor_rows_over_cap(cap));
                        }
                        out.push((raw_src.clone(), label, node, None));
                    }
                }
                return Ok(out);
            }
        };
        let staged_scope = graph_txn_merge::StagedHopScope {
            database_id,
            tenant: TenantId::new(tid),
            collection: args.collection,
            edge_labels: &labels,
        };
        for raw_src in args.node_ids {
            for &pass in passes {
                let durable_rows = durable(raw_src, pass.direction());
                let neighbors: Vec<graph_txn_merge::StagedNeighbor<'_>> = match overlay {
                    Some(overlay) => graph_txn_merge::merge_staged_hop_pass(
                        overlay,
                        &staged_scope,
                        raw_src,
                        pass,
                        durable_rows,
                    ),
                    None => durable_rows
                        .into_iter()
                        .map(|(label, node)| (label, node, None))
                        .collect(),
                };
                for (label, node, staged) in neighbors {
                    let properties = match edge_properties.as_mut() {
                        None => None,
                        Some(hop) => {
                            let (src, dst) = pass.endpoints(raw_src, &node);
                            match hop.cross(src, &label, dst, staged)? {
                                graph_edge_predicate::EdgeCrossing::Rejected => continue,
                                graph_edge_predicate::EdgeCrossing::Admitted(properties) => {
                                    properties.map(nodedb_types::NativeCell)
                                }
                            }
                        }
                    };
                    if out.len() >= cap {
                        return Err(neighbor_rows_over_cap(cap));
                    }
                    out.push((raw_src.clone(), label, node, properties));
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
    use nodedb_types::Surrogate;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Status};
    use crate::control::server::payload_merge::extract_msgpack_elements;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TraceId, VShardId};

    /// A core and the bridge ends it needs to stay open.
    struct Harness {
        core: CoreLoop,
        _requests: Producer<BridgeRequest>,
        _responses: Consumer<BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn harness() -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let (requests, request_rx) = RingBuffer::channel::<BridgeRequest>(8);
        let (response_tx, responses) = RingBuffer::channel::<BridgeResponse>(8);
        let core = CoreLoop::open(
            0,
            request_rx,
            response_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        Harness {
            core,
            _requests: requests,
            _responses: responses,
            _dir: dir,
        }
    }

    fn task(lsn: u64) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id: "a".to_string(),
                edge_labels: Vec::new(),
                direction: Direction::Out,
                rls_filters: Vec::new(),
                collection: None,
                scope: GraphReadScope::default(),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: Some(Lsn::new(lsn)),
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    /// A core holding the edges `a -KNOWS-> b` and `a -KNOWS-> c`, each with
    /// the property map `{"w": 1}`.
    fn two_edges() -> Harness {
        let mut h = harness();
        let properties = nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({"w": 1}))
            .expect("encode properties");
        for (lsn, dst, surrogate) in [(10, "b", 2), (11, "c", 3)] {
            let put = h.core.execute_edge_put(
                &task(lsn),
                EdgePutParams {
                    tid: 1,
                    collection: "knows",
                    src_id: "a",
                    label: "KNOWS",
                    dst_id: dst,
                    properties: &properties,
                    src_surrogate: Surrogate::new(1),
                    dst_surrogate: Surrogate::new(surrogate),
                    valid_from_ms: None,
                    valid_until_ms: None,
                },
            );
            assert_eq!(put.status, Status::Ok, "{:?}", put.error_code);
        }
        h
    }

    /// One `NeighborsMulti` hop from `a`. A named collection returns each
    /// edge's properties, which takes the oriented row path. No collection
    /// takes the durable CSR path.
    fn hop(h: &Harness, collection: Option<&str>, max_results: u32) -> Response {
        let nodes = vec!["a".to_string()];
        h.core.execute_graph_neighbors_multi(
            &task(20),
            1,
            GraphNeighborsMultiArgs {
                node_ids: &nodes,
                edge_labels: &[],
                direction: Direction::Out,
                max_results,
                collection,
                edge_predicate: &[],
                with_properties: collection.is_some(),
                scope: &GraphReadScope::default(),
            },
        )
    }

    /// A hop within its cap answers every row in one terminal frame.
    #[test]
    fn a_hop_within_its_cap_answers_one_terminal_frame() {
        let h = two_edges();
        for collection in [None, Some("knows")] {
            let resp = hop(&h, collection, 10);
            assert!(!resp.partial, "a hop answer is a terminal frame");
            assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
            let rows = extract_msgpack_elements(resp.payload.as_ref()).expect("rows");
            assert_eq!(rows.len(), 2, "both edges of `a`");
        }
    }

    /// A hop that outgrows its cap is refused in one terminal frame. A
    /// lone `Partial` frame leaves the gather waiting for a terminal one.
    #[test]
    fn a_hop_past_its_cap_is_refused_in_one_terminal_frame() {
        let h = two_edges();
        for collection in [None, Some("knows")] {
            let resp = hop(&h, collection, 1);
            assert!(!resp.partial, "a cut hop still ends in a terminal frame");
            assert_eq!(resp.status, Status::Error);
            assert!(resp.payload.is_empty());
        }
    }
}
