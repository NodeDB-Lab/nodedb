// SPDX-License-Identifier: Apache-2.0

//! The `NodeDb` trait: unified query interface for both Origin and Lite.
//!
//! Application code writes against this trait once. The runtime determines
//! whether queries execute locally (in-memory engines on Lite) or remotely
//! (native protocol or pgwire to Origin).
//!
//! All methods are `async`. On native targets they run on Tokio, on WASM on
//! `wasm-bindgen-futures`.
//!
//! `NodeDb` stays one trait so an implementation is one `impl` block and one
//! import. Long provided-method bodies live in `default_impls`.
//!
//! Methods appear in groups: vector, graph, document, CRDT list, named vector
//! fields, text search, batch, connection metadata, SQL, and collection
//! lifecycle. Each method states its contract first. The `On Lite` /
//! `On Remote` / `On Native` lines name what each backend runs. `Remote` is
//! the pgwire client. `Native` is the MessagePack client, the canonical typed
//! transport.

use std::collections::HashSet;

use async_trait::async_trait;

use nodedb_types::document::Document;
use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::protocol::Limits;
use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

use super::default_impls;
use super::marker::NodeDbMarker;
use crate::traits::document::CollectionPurgedHandler;

/// Unified database interface for NodeDB.
///
/// Implementations:
/// - `NodeDbLite`: in-process HNSW/CSR/Loro engines on the edge device.
///   Writes produce CRDT deltas synced to Origin in the background.
/// - `NativeClient`: the MessagePack protocol to Origin.
/// - `NodeDbRemote`: SQL over pgwire to Origin.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeDb: NodeDbMarker {
    /// Search for the `k` nearest vectors to `query` in `collection`.
    ///
    /// Results are ordered by ascending distance. `filter` constrains which
    /// vectors are considered. When `allowed_ids` is `Some`, only documents
    /// whose id is in the set are eligible.
    ///
    /// On every implementation the id restriction applies before ranking:
    /// the top-k is drawn from the allowed set only, even when the nearest
    /// vectors lie outside it. An empty set returns no result.
    ///
    /// On Lite: in-memory HNSW search with the id prefilter pushed into
    /// traversal.
    /// On Remote: `SELECT * FROM <collection> [WHERE <filter>] [AND id IN (..)]
    /// ORDER BY vector_distance(..) LIMIT <k>`. The server lowers the primary
    /// key restriction to a candidate bitmap the index search honors.
    /// On Native: the ids travel in the request; the server lowers them to
    /// the same candidate bitmap.
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>>;

    /// Insert a vector with optional metadata into `collection`.
    ///
    /// On Lite: HNSW insert + CRDT delta + persist to local storage.
    /// On Remote: `INSERT INTO collection (id, embedding, metadata) VALUES (...)`.
    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()>;

    /// Delete a vector by ID from `collection`.
    ///
    /// On Lite: marks deleted in HNSW + emits CRDT tombstone.
    /// On Remote: `DELETE FROM collection WHERE id = $1`.
    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    /// Traverse the graph from `start` up to `depth` hops within `collection`.
    ///
    /// Edges are scoped per collection, so the caller picks which graph to
    /// walk. Returns the discovered subgraph (nodes + edges). `direction`
    /// selects outgoing, incoming, or both orientations.
    ///
    /// Every label of the filter is followed. Every property filter must
    /// admit an edge before it is crossed. Each returned edge carries its
    /// properties.
    ///
    /// On Lite: CSR pointer-chasing in contiguous memory.
    /// On Remote and Native: `GRAPH TRAVERSE IN '<collection>' FROM '<start>'
    /// DEPTH <n> DIRECTION out|in|both [LABEL '<a>', '<b>' …]
    /// [EDGE WHERE <property filters>]`, one round trip.
    async fn graph_traverse(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        direction: nodedb_types::graph::Direction,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph>;

    /// Insert a directed edge `from` → `to` with label `edge_type` into
    /// `collection`. Returns the generated edge ID.
    ///
    /// On Lite: mutable adjacency buffer + CRDT delta + local storage.
    /// On Remote: `GRAPH INSERT EDGE IN '<collection>' FROM '<from>' TO '<to>' TYPE '<label>'`.
    async fn graph_insert_edge(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId>;

    /// Delete a graph edge by ID from `collection`.
    ///
    /// On Lite: marks deleted + CRDT tombstone.
    /// On Remote: `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`.
    async fn graph_delete_edge(&self, collection: &str, edge_id: &EdgeId) -> NodeDbResult<()>;

    /// Read aggregated graph statistics from the persisted edge store.
    ///
    /// `Some(name)` returns zero or one entry for that collection. `None`
    /// returns one entry per collection that has edges. Each entry holds the
    /// edge count, distinct node count, distinct label count, and per-label
    /// edge counts sorted by label.
    ///
    /// `as_of` pins the read to a past system time (ms since Unix epoch).
    /// `None` reads the live state. Lite returns an error for `Some`.
    ///
    /// On Lite: direct read of the local edge store.
    /// On Remote: `SHOW GRAPH STATS [<'collection'>] [AS OF SYSTEM TIME <ms>]`.
    async fn graph_stats(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>>;

    /// Run PageRank on a graph collection's edges.
    ///
    /// `personalization` biases the initial rank toward its seed nodes
    /// (values are normalized). `None` runs uniform-init PageRank.
    /// `damping` defaults to 0.85, `max_iterations` to 20.
    ///
    /// Returns `(node_id, rank)` pairs sorted by rank descending. Ranks sum to 1.0.
    async fn graph_pagerank(
        &self,
        collection: &str,
        personalization: Option<std::collections::HashMap<String, f64>>,
        damping: Option<f64>,
        max_iterations: Option<u32>,
    ) -> NodeDbResult<Vec<(String, f64)>> {
        default_impls::graph_pagerank_default(collection, personalization, damping, max_iterations)
    }

    /// Get a document by ID from `collection`.
    ///
    /// A missing document is `Ok(None)`. An error means the read failed.
    ///
    /// Over pgwire, field values read back as their text form. The native
    /// client returns typed values.
    ///
    /// On Lite: direct Loro state read.
    /// On Remote: `SELECT * FROM <collection> WHERE id = '<id>'` on the
    /// simple-query protocol, the id a quoted string literal.
    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>>;

    /// Put (insert or replace) a document into `collection`.
    ///
    /// The document's `id` is the key. An existing document with that id is
    /// replaced whole (last-writer-wins locally, CRDT merge on sync).
    ///
    /// Each field is a top-level column. A field named `id` must equal the
    /// document id: `id` is the identity column.
    ///
    /// Over pgwire, field values read back as their text form. The native
    /// client returns typed values.
    ///
    /// On Lite: Loro apply + CRDT delta + local storage.
    /// On Remote: `DELETE` then `INSERT INTO <collection> (id, <fields>)
    /// VALUES (...)`, every value a bound parameter. The pair is atomic.
    /// Inside the caller's transaction block it runs under a savepoint and
    /// never ends that block. A failed put rolls back to the savepoint and
    /// leaves the block usable. Outside a block it runs in its own
    /// transaction.
    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()>;

    /// Delete a document by ID from `collection`.
    ///
    /// On Lite: Loro delete + CRDT tombstone.
    /// On Remote: `DELETE FROM collection WHERE id = $1`.
    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    /// Put a document and insert its embedding under one CRDT lock acquisition.
    ///
    /// Equivalent to `document_put` then `vector_insert` into
    /// `vector_collection`. An empty `embedding` makes it `document_put`.
    /// The default makes the two calls. Implementations that can batch both
    /// under one lock override it.
    async fn document_put_with_vector(
        &self,
        doc_collection: &str,
        doc: Document,
        vector_collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> NodeDbResult<()> {
        default_impls::document_put_with_vector_default(
            self,
            doc_collection,
            doc,
            vector_collection,
            id,
            embedding,
        )
        .await
    }

    /// Read a document as of a system time, optionally at a valid time.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`; a plain
    /// collection returns an error. `as_of_ms: None` reads the live version.
    /// `Some(t)` reads the version visible at system time `t`.
    /// `valid_time_ms: Some(vt)` also requires
    /// `valid_from_ms <= vt < valid_until_ms`.
    ///
    /// Implementations without bitemporal reads return `Err`.
    async fn document_get_as_of(
        &self,
        collection: &str,
        id: &str,
        as_of_ms: Option<i64>,
        valid_time_ms: Option<i64>,
    ) -> NodeDbResult<Option<Document>> {
        default_impls::document_get_as_of_default(collection, id, as_of_ms, valid_time_ms)
    }

    /// Put a document with explicit valid-time bounds.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`.
    /// `valid_from_ms` defaults to the system time, `valid_until_ms` to
    /// open-ended. Implementations without bitemporal writes return `Err`.
    async fn document_put_with_valid_time(
        &self,
        collection: &str,
        doc: Document,
        valid_from_ms: Option<i64>,
        valid_until_ms: Option<i64>,
    ) -> NodeDbResult<()> {
        default_impls::document_put_with_valid_time_default(
            collection,
            doc,
            valid_from_ms,
            valid_until_ms,
        )
    }

    /// Insert a new block into a document's movable list at `index`.
    ///
    /// `list_path` addresses the list (dot-path for nested containers).
    /// `fields` must be `Value::Object(..)`. Each key becomes a field of the
    /// new map block. `index` is required: a defaulted position corrupts
    /// list order.
    ///
    /// On Lite: `nodedb_crdt::list_ops::list_insert_container` + CRDT delta.
    /// On Native: `OpCode::CrdtListInsert` with `list_path` / `list_index` /
    /// `list_fields_json`.
    async fn list_insert(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields: &Value,
    ) -> NodeDbResult<()>;

    /// Delete the block at `index` from a document's movable list.
    ///
    /// On Lite: removes the local entry + CRDT tombstone delta.
    /// On Native: `OpCode::CrdtListDelete` with `list_path` / `list_index`.
    async fn list_delete(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> NodeDbResult<()>;

    /// Move the block at `from_index` to `to_index` in a document's movable list.
    ///
    /// The two indexes are distinct wire fields. Swapping them reorders the
    /// list to the wrong shape.
    ///
    /// On Lite: repositions the local entry + CRDT delta.
    /// On Native: `OpCode::CrdtListMove` with `list_path` /
    /// `list_from_index` / `list_to_index`.
    async fn list_move(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> NodeDbResult<()>;

    /// Insert a vector into the named field `field_name` of `collection`.
    ///
    /// Each named field has its own HNSW index, so a collection holds several
    /// embeddings (e.g. `title_embedding`, `body_embedding`). The default
    /// returns `Err`: delegating to `vector_insert` would drop `field_name`
    /// and land the vector in the wrong field.
    async fn vector_insert_field(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        default_impls::vector_insert_field_default(collection, field_name, id, embedding, metadata)
    }

    /// Search the named vector field `field_name`.
    ///
    /// The default returns `Err`: delegating to `vector_search` would drop
    /// `field_name` and search the wrong field.
    async fn vector_search_field(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        default_impls::vector_search_field_default(collection, field_name, query, k, filter)
    }

    /// Find the shortest path between two nodes.
    ///
    /// Returns node IDs from `from` to `to`, or `None` when no path exists
    /// within `max_depth` hops. The path follows outgoing edges the filter
    /// admits.
    ///
    /// The default is a breadth-first search, one depth-1 `graph_traverse`
    /// per expanded node.
    /// On Remote and Native: `GRAPH PATH IN '<collection>' FROM '<from>'
    /// TO '<to>' MAX_DEPTH <n> [LABEL '<a>', '<b>' …]
    /// [EDGE WHERE <property filters>]`, one round trip.
    async fn graph_shortest_path(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        default_impls::graph_shortest_path_default(
            self,
            collection,
            from,
            to,
            max_depth,
            edge_filter,
        )
        .await
    }

    /// Full-text BM25 search of the indexed `field` of `collection`.
    ///
    /// Every BM25 index covers one declared field, so the caller names it.
    /// An empty `field` searches the whole-document index. Returns document
    /// ids with scores, best first, at most `top_k`. When `allowed_ids` is
    /// `Some`, only documents whose id is in the set are returned.
    ///
    /// `params` has the same meaning on every implementation: `mode` combines
    /// the query terms (`Or`: any term, `And`: all terms), and `fuzzy` lets a
    /// term with no exact match fall back to a Levenshtein match. A
    /// collection's fuzzy default also enables the fallback.
    /// [`TextSearchParams::default`] is `{ mode: Or, fuzzy: false }`, the
    /// default of SQL `text_match` / `bm25_score` with no options.
    ///
    /// On Lite: honors every `params` value, and applies `allowed_ids` after
    /// an over-fetch.
    /// On Remote: `SELECT id, bm25_score(.., opts) AS score FROM <collection>
    /// WHERE text_match(.., opts) [AND id IN (..)] ORDER BY score DESC LIMIT <k>`,
    /// where `opts` are `mode => 'and' | 'or'` and `fuzzy => true | false`.
    /// The server honors every `params` value. A query wrapped in double
    /// quotes is a phrase query there, which takes only the default `params`.
    ///
    /// The default returns `Err`. An empty `Ok` would read as a real
    /// "no matches" answer.
    async fn text_search(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        default_impls::text_search_default(collection, field, query, top_k, params, allowed_ids)
    }

    /// Batch insert vectors — amortizes CRDT delta export to O(1) per batch.
    async fn batch_vector_insert(
        &self,
        collection: &str,
        vectors: &[(&str, &[f32])],
    ) -> NodeDbResult<()> {
        default_impls::batch_vector_insert_default(self, collection, vectors).await
    }

    /// Batch insert graph edges into `collection` — amortizes CRDT
    /// delta export to O(1) per batch.
    async fn batch_graph_insert_edges(
        &self,
        collection: &str,
        edges: &[(&str, &str, &str)],
    ) -> NodeDbResult<()> {
        default_impls::batch_graph_insert_edges_default(self, collection, edges).await
    }

    /// The protocol version negotiated during the connection handshake.
    /// `0` for an implementation that performs no handshake.
    fn proto_version(&self) -> u16 {
        0
    }

    /// The raw capability bitfield advertised by the server. `0` when no
    /// handshake ran. `Capabilities::from_raw` gives named predicates.
    fn capabilities(&self) -> u64 {
        0
    }

    /// The server version string from `HelloAckFrame` (e.g. `"0.1.0-dev"`).
    /// Empty when no handshake ran.
    fn server_version(&self) -> String {
        String::new()
    }

    /// Per-operation limits announced by the server. Every field is `None`
    /// when no handshake ran. `None` means no server-side cap.
    fn limits(&self) -> Limits {
        Limits::default()
    }

    /// Execute a raw SQL query with parameters.
    ///
    /// On Lite: requires the `sql` feature. Without it, returns
    /// `NodeDbError::SqlNotEnabled`.
    /// On Remote: pass-through to Origin via pgwire.
    ///
    /// The typed methods above cover most agent workloads. Use this for BI
    /// tools, ORMs, and ad-hoc queries.
    async fn execute_sql(&self, query: &str, params: &[Value]) -> NodeDbResult<QueryResult>;

    /// Restore a soft-deleted collection within its retention window.
    ///
    /// Runs `UNDROP COLLECTION <name>` through `execute_sql`. Fails with
    /// 42P01 once the retention window has elapsed, or with 42501 when the
    /// caller is neither the preserved owner nor an admin.
    async fn undrop_collection(&self, name: &str) -> NodeDbResult<()> {
        default_impls::undrop_collection_default(self, name).await
    }

    /// Hard-delete a collection, skipping soft-delete and retention.
    ///
    /// Runs `DROP COLLECTION <name> PURGE`. Admin-only: the server rejects
    /// other callers with 42501. The data is unrecoverable.
    async fn drop_collection_purge(&self, name: &str) -> NodeDbResult<()> {
        default_impls::drop_collection_purge_default(self, name).await
    }

    /// List the current tenant's soft-deleted collections still within their
    /// retention window. Empty when there are none.
    ///
    /// Reads `tenant_id, name, owner, deactivated_at_ns,
    /// retention_expires_at_ns` from `_system.dropped_collections`.
    async fn list_dropped_collections(&self) -> NodeDbResult<Vec<DroppedCollection>> {
        default_impls::list_dropped_collections_default(self).await
    }

    /// Register a handler fired when a collection the caller synced is purged
    /// on Origin and the local copy is removed.
    ///
    /// The default returns `NodeDbError::storage` ("not supported").
    /// Implementations with a sync client (Lite) override it. A stateless
    /// pgwire client has nothing to push, so the default is correct there.
    async fn on_collection_purged(&self, _handler: CollectionPurgedHandler) -> NodeDbResult<()> {
        default_impls::on_collection_purged_default()
    }
}
