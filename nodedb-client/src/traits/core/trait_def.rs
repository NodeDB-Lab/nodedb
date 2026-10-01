// SPDX-License-Identifier: Apache-2.0

//! The `NodeDb` trait: unified query interface for both Origin and Lite.
//!
//! Application code writes against this trait once. The runtime determines
//! whether queries execute locally (in-memory engines on Lite) or remotely
//! (pgwire to Origin).
//!
//! All methods are `async` — on native this runs on Tokio, on WASM this
//! runs on `wasm-bindgen-futures`.
//!
//! This file must remain a single block for object-safety: Rust does not
//! permit a `trait` body to be split across files. Splitting `NodeDb` into
//! supertraits would break the `Arc<dyn NodeDb>` pattern all callers depend
//! on and is therefore out of scope for any mechanical refactor.

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
/// Two implementations:
/// - `NodeDbLite`: executes queries against in-memory HNSW/CSR/Loro engines
///   on the edge device. Writes produce CRDT deltas synced to Origin in background.
/// - `NodeDbRemote`: translates trait calls into parameterized SQL and sends
///   them over pgwire to the Origin cluster.
///
/// The developer writes agent logic once. Switching between local and cloud is a one-line configuration change.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeDb: NodeDbMarker {
    // ─── Vector Operations ───────────────────────────────────────────

    /// Search for the `k` nearest vectors to `query` in `collection`.
    ///
    /// Returns results ordered by ascending distance. Optional metadata filter constrains which vectors are considered. When `allowed_ids` is `Some`, only documents whose string ID appears in the set are eligible — the filter is pushed into HNSW traversal on Lite so the returned top-k is drawn exclusively from the allowed set.
    ///
    /// On Lite: direct in-memory HNSW search with optional ID prefilter. Sub-millisecond. On Remote: translated to `SELECT ... ORDER BY embedding <-> $1 LIMIT $2` (allowed_ids is ignored on the remote path — pass `None`).
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
    /// On Lite: inserts into in-memory HNSW + emits CRDT delta + persists to SQLite. On Remote: translated to `INSERT INTO collection (id, embedding, metadata) VALUES (...)`.
    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()>;

    /// Delete a vector by ID from `collection`.
    ///
    /// On Lite: marks deleted in HNSW + emits CRDT tombstone. On Remote: `DELETE FROM collection WHERE id = $1`.
    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    // ─── Graph Operations ────────────────────────────────────────────

    /// Traverse the graph from `start` up to `depth` hops within `collection`.
    ///
    /// `collection` names the graph collection holding the adjacency data. NodeDB's graph overlay scopes edges per collection, so the caller picks which graph to walk. Returns the discovered subgraph (nodes + edges). Optional edge filter constrains which edges are followed. `direction` selects outgoing, incoming, or both orientations.
    ///
    /// On Lite: direct CSR pointer-chasing in contiguous memory. Microseconds. On Remote: `GRAPH TRAVERSE IN '<collection>' FROM '<start>' DEPTH <n> DIRECTION out|in|both [LABEL '<l>']`.
    async fn graph_traverse(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        direction: nodedb_types::graph::Direction,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph>;

    /// Insert a directed edge from `from` to `to` with the given label into `collection`.
    ///
    /// Returns the generated edge ID.
    ///
    /// On Lite: appends to mutable adjacency buffer + CRDT delta + SQLite. On Remote: `GRAPH INSERT EDGE IN '<collection>' FROM '<from>' TO '<to>' TYPE '<label>'`.
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
    /// On Lite: marks deleted + CRDT tombstone. On Remote: `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`.
    async fn graph_delete_edge(&self, collection: &str, edge_id: &EdgeId) -> NodeDbResult<()>;

    /// Read aggregated graph statistics.
    ///
    /// When `collection` is `Some(name)`, returns statistics for that one collection — a vec of length 0 (no edges recorded) or 1. When `collection` is `None`, returns one `GraphStats` per collection that has edges (tenant-wide). Each entry contains the global edge count, distinct node count, distinct label count, and per-label edge counts (sorted ascending by label name). Reads what was *persisted* in the edge store, bypassing any in-memory CSR view.
    ///
    /// `as_of` pins the read to a past system-time epoch (milliseconds since Unix epoch). When `None`, the live (current) state is returned. Bitemporal reads are supported on Origin; the Lite backend returns an error when `as_of` is `Some`.
    ///
    /// On Lite: direct read of the local edge store. On Remote: `SHOW GRAPH STATS [<'collection'>] [AS OF SYSTEM TIME <ms>]`.
    async fn graph_stats(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>>;

    /// Run PageRank on a graph collection's edges.
    ///
    /// When `personalization` is `Some`, computes Personalized PageRank — initial rank is biased toward the seed nodes in the map (values are normalized). When `None`, runs standard uniform-init PageRank.
    ///
    /// `damping` defaults to 0.85 when `None`. `max_iterations` defaults to 20.
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

    // ─── Document Operations ─────────────────────────────────────────

    /// Get a document by ID from `collection`.
    ///
    /// A document that does not exist is `Ok(None)` — absence is an answer, never an error. An error means the read itself failed.
    ///
    /// On Lite: direct Loro state read. Sub-millisecond. On Remote: `SELECT * FROM collection WHERE id = $1`.
    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>>;

    /// Put (insert or update) a document into `collection`.
    ///
    /// The document's `id` field determines the key. If a document with that ID already exists, it is overwritten (last-writer-wins locally; CRDT merge on sync).
    ///
    /// On Lite: Loro apply + CRDT delta + SQLite persist. On Remote: `INSERT ... ON CONFLICT (id) DO UPDATE SET ...`.
    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()>;

    /// Delete a document by ID from `collection`.
    ///
    /// On Lite: Loro delete + CRDT tombstone. On Remote: `DELETE FROM collection WHERE id = $1`.
    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    /// Put a document and insert its embedding vector in a single CRDT lock acquisition.
    ///
    /// Equivalent to calling `document_put` then `vector_insert`, but acquires the CRDT lock only once — halving the per-insert oplog-walk cost on Lite. When `embedding` is empty this behaves identically to `document_put`.
    ///
    /// `vector_collection` names the vector index to insert into (may differ from the document `collection`).
    ///
    /// The default implementation falls back to two separate calls. Implementations that can batch the two operations under one lock should override this method.
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

    /// Read a document as-of a system time, optionally filtered by valid_time.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)` — returns an error on plain (non-bitemporal) collections.
    ///
    /// When `as_of_ms` is `None`, returns the current LIVE version (equivalent to `document_get`). When `as_of_ms` is `Some(t)`, returns the version visible at system time `t`. If `valid_time_ms` is `Some(vt)`, the returned version must additionally satisfy `valid_from_ms <= vt < valid_until_ms`.
    ///
    /// Returns `Err` on implementations that do not support bitemporal reads.
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
    /// Only valid on collections created `WITH (bitemporal=true)`. `valid_from_ms` and `valid_until_ms` specify the application-time interval for which the version is considered current.  Both default to system time / open-ended when `None`.
    ///
    /// Returns `Err` on implementations that do not support bitemporal writes.
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

    // ─── CRDT List Operations (Movable List) ───────────────────────────

    /// Insert a new block into a document's movable-list container at `index`.
    ///
    /// `list_path` addresses the movable list within the document (dot-path for nested containers). `fields` supplies the new block's contents — must be `Value::Object(..)`; each key becomes a field on the new LoroMap block inserted at `index`. `index` is unconditionally required: a missing position is a data-corruption risk, never a silent-default insert point.
    ///
    /// On Lite: appends to the local Loro movable list via `nodedb_crdt::list_ops::list_insert_container` + emits a CRDT delta. On Remote: encodes `OpCode::CrdtListInsert` (0x99) over the native wire with `list_path` / `list_index` / `list_fields_json` set.
    async fn list_insert(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields: &Value,
    ) -> NodeDbResult<()>;

    /// Delete the block at `index` from a document's movable-list container.
    ///
    /// On Lite: removes the entry from the local Loro movable list + emits a CRDT tombstone delta. On Remote: encodes `OpCode::CrdtListDelete` (0x9A) over the native wire with `list_path` / `list_index` set.
    async fn list_delete(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> NodeDbResult<()>;

    /// Move the block at `from_index` to `to_index` within a document's movable-list container.
    ///
    /// `from_index` and `to_index` are distinct wire fields — never conflate them; swapping them silently reorders the list to the wrong shape.
    ///
    /// On Lite: repositions the entry in the local Loro movable list + emits a CRDT delta. On Remote: encodes `OpCode::CrdtListMove` (0x9B) over the native wire with `list_path` / `list_from_index` / `list_to_index` set.
    async fn list_move(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> NodeDbResult<()>;

    // ─── Named Vector Fields ──────────────────────────────────────────

    /// Insert a vector into a named field within a collection.
    ///
    /// Enables multiple embeddings per collection (e.g., "title_embedding", "body_embedding") with independent HNSW indexes.
    ///
    /// Default returns `Err` — silently delegating to `vector_insert` and dropping `field_name` would land the vector in the wrong field. Implementations that route through to a server with field-aware support must override.
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

    /// Search a named vector field.
    ///
    /// Default returns `Err` — silently delegating to `vector_search` and dropping `field_name` would search the wrong field. Implementations that route through to a server with field-aware support must override.
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

    // ─── Graph Shortest Path ────────────────────────────────────────

    /// Find the shortest path between two nodes.
    ///
    /// Returns the path as a list of node IDs (`from` first, `to` last), or `None` if no path exists within `max_depth` hops.
    ///
    /// The default uses breadth-first search with `Direction::Out`.
    /// Each expanded node requires one `graph_traverse` request at depth 1.
    /// Implementations with a server-side shortest-path operator can override this default to reduce network requests.
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

    // ─── Text Search ────────────────────────────────────────────────

    /// Full-text search with BM25 scoring against the FTS-indexed `field` on `collection`.
    ///
    /// NodeDB's FTS is per-field — every BM25 index is scoped to one declared field, so the caller names which field to search. Returns document IDs with relevance scores, ordered by descending score. Pass [`TextSearchParams::default()`] for standard OR-mode non-fuzzy search.
    ///
    /// Default returns `Err` — `Ok(Vec::new())` is indistinguishable from a real "no matches" answer and would silently mask the missing implementation. Implementations must override (e.g., a `SEARCH IN '<collection>' FIELD '<field>' QUERY '<q>'` round-trip via `execute_sql`). Full-text BM25 search. When `allowed_ids` is `Some`, only documents whose ID is in the set are returned. On Lite, the filter is applied after an over-fetch; on Remote, `allowed_ids` is ignored (pass `None`).
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

    // ─── Batch Operations ───────────────────────────────────────────

    /// Batch insert vectors — amortizes CRDT delta export to O(1) per batch.
    async fn batch_vector_insert(
        &self,
        collection: &str,
        vectors: &[(&str, &[f32])],
    ) -> NodeDbResult<()> {
        default_impls::batch_vector_insert_default(self, collection, vectors).await
    }

    /// Batch insert graph edges into `collection` — amortizes CRDT delta export to O(1) per batch.
    async fn batch_graph_insert_edges(
        &self,
        collection: &str,
        edges: &[(&str, &str, &str)],
    ) -> NodeDbResult<()> {
        default_impls::batch_graph_insert_edges_default(self, collection, edges).await
    }

    // ─── Connection Metadata ─────────────────────────────────────────────

    /// The protocol version negotiated during the connection handshake.
    ///
    /// Returns `0` for implementations that do not maintain a persistent connection and therefore never perform a handshake.
    fn proto_version(&self) -> u16 {
        0
    }

    /// The raw capability bitfield advertised by the server.
    ///
    /// Returns `0` when no handshake was performed. Use `Capabilities::from_raw(self.capabilities())` for named predicates.
    fn capabilities(&self) -> u64 {
        0
    }

    /// The server version string from `HelloAckFrame` (e.g. `"0.1.0-dev"`).
    ///
    /// Returns an empty string when no handshake was performed.
    fn server_version(&self) -> String {
        String::new()
    }

    /// Per-operation limits announced by the server.
    ///
    /// All fields are `None` when no handshake was performed — the caller should treat `None` as "no server-side cap" for that dimension.
    fn limits(&self) -> Limits {
        Limits::default()
    }

    // ─── SQL Escape Hatch ────────────────────────────────────────────

    /// Execute a raw SQL query with parameters.
    ///
    /// On Lite: requires the `sql` feature flag (compiles in DataFusion parser). Returns `NodeDbError::SqlNotEnabled` if the feature is not compiled in. On Remote: pass-through to Origin via pgwire.
    ///
    /// For most AI agent workloads, the typed methods above are sufficient and faster. Use this for BI tools, existing ORMs, or ad-hoc queries.
    async fn execute_sql(&self, query: &str, params: &[Value]) -> NodeDbResult<QueryResult>;

    // ─── Collection Lifecycle (soft-delete / undrop / hard-delete) ───

    /// Restore a soft-deleted collection within its retention window.
    ///
    /// Equivalent to `UNDROP COLLECTION <name>`. Fails with 42P01 if the retention window has elapsed and the row is gone, or with 42501 if the caller is neither preserved owner nor admin.
    ///
    /// Default impl routes through `execute_sql` so any implementation that can execute SQL inherits the correct behavior for free.
    async fn undrop_collection(&self, name: &str) -> NodeDbResult<()> {
        default_impls::undrop_collection_default(self, name).await
    }

    /// Hard-delete a collection, skipping soft-delete and retention.
    ///
    /// Equivalent to `DROP COLLECTION <name> PURGE`. Admin-only on the server; the server rejects non-admin callers with 42501. Bypasses the retention safety net — data is unrecoverable.
    async fn drop_collection_purge(&self, name: &str) -> NodeDbResult<()> {
        default_impls::drop_collection_purge_default(self, name).await
    }

    /// List every soft-deleted collection in the current tenant that is still within its retention window.
    ///
    /// Equivalent to `SELECT tenant_id, name, owner, deactivated_at_ns, retention_expires_at_ns FROM _system.dropped_collections`. Returns `Vec<DroppedCollection>` — empty if no soft-deleted rows exist for the caller's tenant.
    async fn list_dropped_collections(&self) -> NodeDbResult<Vec<DroppedCollection>> {
        default_impls::list_dropped_collections_default(self).await
    }

    /// Register a handler fired when a collection the caller has synced is purged on Origin and the local copy is removed.
    ///
    /// Default impl returns `NodeDbError::storage` with a `"not supported"` detail — implementations that maintain a sync client (Lite, any future push-capable remote client) override with registration into their internal handler list. Stateless clients (pgwire-only `NodeDbRemote`) have nothing to push, so the default rejection is the correct behavior.
    async fn on_collection_purged(&self, _handler: CollectionPurgedHandler) -> NodeDbResult<()> {
        default_impls::on_collection_purged_default()
    }
}
