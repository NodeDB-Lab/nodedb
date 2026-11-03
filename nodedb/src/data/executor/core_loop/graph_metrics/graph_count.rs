// SPDX-License-Identifier: BUSL-1.1

//! This core's share of the graph node and edge gauges.
//!
//! The CSR holds a dual-homed edge on both endpoint homes, and an endpoint
//! node on every core that holds one of its edges. A node that hosts only the
//! destination's home still holds the source node. A core therefore counts
//! only the homes this node hosts and that route to the core: such a node,
//! and its out-edges, which are the edges it owns.
//!
//! The Data Plane has no record of which vShards its node hosts. The Control
//! Plane sends the hosted set with each `MetaOp::RefreshGraphCount`.

use std::collections::HashSet;

use crate::bridge::envelope::Response;
use crate::control::metrics::GraphCount;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{RecordHomes, VShardId, core_for_vshard};

impl CoreLoop {
    /// `MetaOp::RefreshGraphCount`: count this core's share of the graph
    /// against `hosted_vshards` and move the gauges by the change.
    pub(in crate::data::executor) fn execute_refresh_graph_count(
        &mut self,
        task: &ExecutionTask,
        hosted_vshards: &[u32],
    ) -> Response {
        self.refresh_graph_count(hosted_vshards);
        self.response_ok(task)
    }

    /// Count this core's graph against `hosted_vshards` and move the gauges
    /// by the change since the share this core last reported.
    pub(in crate::data::executor) fn refresh_graph_count(&mut self, hosted_vshards: &[u32]) {
        let Some(metrics) = self.metrics.clone() else {
            return;
        };
        let count = self.hosted_graph_count(hosted_vshards);
        metrics.update_graph_stats(self.maintenance.graph_count, count);
        self.maintenance.graph_count = count;
    }

    /// The nodes with a live edge, and the live out-edges of those nodes,
    /// whose home vShard is in `hosted_vshards` and routes to this core.
    /// Counts across every `(database, tenant)` CSR partition.
    pub(in crate::data::executor) fn hosted_graph_count(
        &self,
        hosted_vshards: &[u32],
    ) -> GraphCount {
        let num_cores = self.redo_apply.num_cores;
        let hosted: HashSet<u32> = hosted_vshards
            .iter()
            .copied()
            .filter(|&vshard| core_for_vshard(VShardId::new(vshard), num_cores) == self.core_id)
            .collect();
        let mut count = GraphCount::default();
        if hosted.is_empty() {
            return count;
        }
        for (_, csr) in self.csr.iter() {
            let nodes = u32::try_from(csr.node_count()).unwrap_or(u32::MAX);
            for id in 0..nodes {
                let home = RecordHomes::edge_owner(csr.node_name_raw(id));
                if !hosted.contains(&home.as_u32()) {
                    continue;
                }
                let out = csr.out_degree_raw(id) as u64;
                count.edges = count.edges.saturating_add(out);
                if out > 0 || csr.in_degree_raw(id) > 0 {
                    count.nodes = count.nodes.saturating_add(1);
                }
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use nodedb_types::{RlsWriteCheck, Surrogate};

    use crate::bridge::envelope::Status;
    use crate::control::metrics::{GraphCount, SystemMetrics};
    use crate::data::executor::core_loop::tests::{make_core_with_dir, task_with_vshard};
    use crate::data::executor::handlers::graph::{EdgeDeleteParams, EdgePutParams};
    use crate::types::{RecordHomes, VShardId, core_for_vshard};

    const TID: u64 = 1;

    fn put(
        core: &mut crate::data::executor::core_loop::CoreLoop,
        vshard: VShardId,
        src: &str,
        dst: &str,
    ) {
        let response = core.execute_edge_put(
            &task_with_vshard(vshard),
            EdgePutParams {
                tid: TID,
                collection: "knows",
                src_id: src,
                label: "KNOWS",
                dst_id: dst,
                properties: &[],
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
                valid_from_ms: None,
                valid_until_ms: None,
            },
        );
        assert_eq!(response.status, Status::Ok);
    }

    /// Every vShard: the hosted set of a single node.
    fn every_vshard() -> Vec<u32> {
        (0..VShardId::COUNT).collect()
    }

    /// A dual-homed edge applied on both homes counts once, and a node
    /// counts once however many edges name it.
    #[test]
    fn a_dual_homed_edge_and_its_endpoints_count_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let metrics = Arc::new(SystemMetrics::new());
        core.set_metrics(Arc::clone(&metrics));
        let hosted = every_vshard();

        put(&mut core, RecordHomes::edge_owner("a"), "a", "b");
        put(&mut core, VShardId::from_key(b"b"), "a", "b");
        put(&mut core, RecordHomes::edge_owner("a"), "a", "c");

        assert_eq!(
            core.hosted_graph_count(&hosted),
            GraphCount { nodes: 3, edges: 2 }
        );
        core.refresh_graph_count(&hosted);
        assert_eq!(metrics.graph_nodes.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.graph_edges.load(Ordering::Relaxed), 2);

        let response = core.execute_edge_delete(
            &task_with_vshard(RecordHomes::edge_owner("a")),
            EdgeDeleteParams {
                tid: TID,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "c",
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
                rls_write_check: &RlsWriteCheck::NoPolicyApplies,
            },
        );
        assert_eq!(response.status, Status::Ok);

        // `c` keeps no live edge, so it no longer counts.
        core.refresh_graph_count(&hosted);
        assert_eq!(metrics.graph_nodes.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.graph_edges.load(Ordering::Relaxed), 1);
    }

    /// With several cores and every vShard hosted, a core counts only the
    /// nodes whose home vShard routes to it, and only those nodes' out-edges.
    #[test]
    fn a_core_counts_only_the_homes_it_hosts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let num_cores = 4;
        core.set_num_cores(num_cores);

        let names = ["a", "b", "c", "d", "e", "f", "g", "h"];
        for pair in names.windows(2) {
            put(
                &mut core,
                RecordHomes::edge_owner(pair[0]),
                pair[0],
                pair[1],
            );
        }

        let hosted =
            |name: &str| core_for_vshard(VShardId::from_key(name.as_bytes()), num_cores) == 0;
        let expected_nodes = names.iter().filter(|name| hosted(name)).count() as u64;
        let expected_edges = names[..names.len() - 1]
            .iter()
            .filter(|name| hosted(name))
            .count() as u64;
        assert_eq!(
            core.hosted_graph_count(&every_vshard()),
            GraphCount {
                nodes: expected_nodes,
                edges: expected_edges,
            }
        );
    }

    /// A node that hosts only the destination's home of a dual-homed edge
    /// counts the destination node. Neither the source node nor the edge
    /// counts until the source's home is hosted.
    #[test]
    fn an_unhosted_source_home_counts_neither_its_node_nor_its_edge() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let metrics = Arc::new(SystemMetrics::new());
        core.set_metrics(Arc::clone(&metrics));

        let src_home = RecordHomes::edge_owner("a");
        let dst_home = VShardId::from_key(b"b");
        assert_ne!(src_home, dst_home, "the edge must be dual-homed");
        put(&mut core, dst_home, "a", "b");

        let without_source: Vec<u32> = every_vshard()
            .into_iter()
            .filter(|&vshard| vshard != src_home.as_u32())
            .collect();
        assert_eq!(
            core.hosted_graph_count(&without_source),
            GraphCount { nodes: 1, edges: 0 }
        );
        core.refresh_graph_count(&without_source);
        assert_eq!(metrics.graph_nodes.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.graph_edges.load(Ordering::Relaxed), 0);

        // The source's home joins the hosted set: its node and edge count.
        core.refresh_graph_count(&every_vshard());
        assert_eq!(metrics.graph_nodes.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.graph_edges.load(Ordering::Relaxed), 1);
    }
}
