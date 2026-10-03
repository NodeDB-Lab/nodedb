// SPDX-License-Identifier: Apache-2.0

//! Single `impl NodeDb for NodeDbRemote` block.
//!
//! Each method is a one-line delegation to an inherent helper in the
//! domain-specific sibling files. The trait impl must remain a single
//! block (Rust forbids splitting it across files).

use async_trait::async_trait;

use nodedb_types::document::Document;
use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

use crate::traits::NodeDb;
use crate::traits::document::CollectionPurgedHandler;

use super::core::NodeDbRemote;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl NodeDb for NodeDbRemote {
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        allowed_ids: Option<&std::collections::HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        self.vector_search_impl(collection, query, k, filter, allowed_ids)
            .await
    }

    async fn vector_insert_field(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        self.vector_insert_field_impl(collection, field_name, id, embedding, metadata)
            .await
    }

    async fn vector_search_field(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        self.vector_search_field_impl(collection, field_name, query, k, filter)
            .await
    }

    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        self.vector_insert_impl(collection, id, embedding, metadata)
            .await
    }

    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        self.vector_delete_impl(collection, id).await
    }

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

    // CRDT movable-list operations (`list_insert` / `list_delete` /
    // `list_move`) are native-wire-only: Origin's SQL/DSL grammar has no
    // movable-list syntax, only the `OpCode::CrdtList*` native opcodes
    // exist (see `NativeClient`). Building one here would mean growing a
    // pgwire/SQL surface for these ops, which is explicitly out of scope —
    // so this stateless pgwire client reports the capability gap instead
    // of faking it through a query that doesn't exist.

    async fn list_insert(
        &self,
        _collection: &str,
        _document_id: &str,
        _list_path: &str,
        _index: usize,
        _fields: &Value,
    ) -> NodeDbResult<()> {
        Err(nodedb_types::error::NodeDbError::bad_request(
            "list_insert is not supported over pgwire — Origin's SQL/DSL grammar has no \
             movable-list syntax; use a native-wire client (NativeClient) or NodeDbLite",
        ))
    }

    async fn list_delete(
        &self,
        _collection: &str,
        _document_id: &str,
        _list_path: &str,
        _index: usize,
    ) -> NodeDbResult<()> {
        Err(nodedb_types::error::NodeDbError::bad_request(
            "list_delete is not supported over pgwire — Origin's SQL/DSL grammar has no \
             movable-list syntax; use a native-wire client (NativeClient) or NodeDbLite",
        ))
    }

    async fn list_move(
        &self,
        _collection: &str,
        _document_id: &str,
        _list_path: &str,
        _from_index: usize,
        _to_index: usize,
    ) -> NodeDbResult<()> {
        Err(nodedb_types::error::NodeDbError::bad_request(
            "list_move is not supported over pgwire — Origin's SQL/DSL grammar has no \
             movable-list syntax; use a native-wire client (NativeClient) or NodeDbLite",
        ))
    }

    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>> {
        self.document_get_impl(collection, id).await
    }

    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()> {
        self.document_put_impl(collection, doc).await
    }

    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        self.document_delete_impl(collection, id).await
    }

    async fn execute_sql(&self, query: &str, params: &[Value]) -> NodeDbResult<QueryResult> {
        self.execute_sql_impl(query, params).await
    }

    // Collection lifecycle (soft-delete / undrop / hard-delete).
    //
    // Explicit overrides of the trait defaults so the pgwire routing
    // takes the no-row `execute_raw` path for DDL (rather than
    // `query_raw`, which is shaped for row-returning statements) and so
    // the dispatch is visible and grep-able in this file.

    async fn undrop_collection(&self, name: &str) -> NodeDbResult<()> {
        self.undrop_collection_impl(name).await
    }

    async fn drop_collection_purge(&self, name: &str) -> NodeDbResult<()> {
        self.drop_collection_purge_impl(name).await
    }

    async fn list_dropped_collections(&self) -> NodeDbResult<Vec<DroppedCollection>> {
        self.list_dropped_collections_impl().await
    }

    async fn text_search(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
        allowed_ids: Option<&std::collections::HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        self.text_search_impl(collection, field, query, top_k, params, allowed_ids)
            .await
    }

    async fn on_collection_purged(&self, _handler: CollectionPurgedHandler) -> NodeDbResult<()> {
        // Stateless pgwire-only client: no push capability. The trait
        // default returns the same error; duplicated here so the impl
        // block is grep-able without following the default chain.
        Err(nodedb_types::error::NodeDbError::storage(
            "on_collection_purged is not supported on this client — \
             requires a push-capable sync connection (NodeDbLite or a \
             sync-enabled remote client)",
        ))
    }
}
