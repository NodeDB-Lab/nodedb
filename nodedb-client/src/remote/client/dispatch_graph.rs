// SPDX-License-Identifier: Apache-2.0

//! The `impl NodeDbGraph for NodeDbRemote` block.
//!
//! Each method is a one-line delegation to an inherent helper in `graph`.

use async_trait::async_trait;

use nodedb_types::document::Document;
use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::EdgeFilter;
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::protocol::EdgeWrite;
use nodedb_types::result::SubGraph;

use crate::traits::NodeDbGraph;

use super::core::NodeDbRemote;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl NodeDbGraph for NodeDbRemote {
    async fn graph_traverse(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        direction: nodedb_types::graph::Direction,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        self.graph_traverse_impl(collection, start, depth, direction, edge_filter)
            .await
    }

    async fn graph_insert_edge(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        self.graph_insert_edge_impl(collection, from, to, edge_type, properties)
            .await
    }

    async fn graph_delete_edge(&self, collection: &str, edge_id: &EdgeId) -> NodeDbResult<()> {
        self.graph_delete_edge_impl(collection, edge_id).await
    }

    async fn graph_stats(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>> {
        self.graph_stats_impl(collection, as_of).await
    }

    async fn graph_pagerank(
        &self,
        collection: &str,
        personalization: Option<std::collections::HashMap<String, f64>>,
        damping: Option<f64>,
        max_iterations: Option<u32>,
    ) -> NodeDbResult<Vec<(String, f64)>> {
        self.graph_pagerank_impl(
            collection,
            personalization.as_ref(),
            damping,
            max_iterations,
        )
        .await
    }

    async fn graph_shortest_path(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        self.graph_shortest_path_impl(collection, from, to, max_depth, edge_filter)
            .await
    }

    async fn graph_insert_edges(&self, collection: &str, edges: &[EdgeWrite]) -> NodeDbResult<u64> {
        self.graph_insert_edges_impl(collection, edges).await
    }

    async fn graph_delete_edges(&self, collection: &str, edges: &[EdgeId]) -> NodeDbResult<u64> {
        self.graph_delete_edges_impl(collection, edges).await
    }
}
