// SPDX-License-Identifier: Apache-2.0

//! `GraphOp`: graph engine physical operations dispatched to the Data Plane.

use nodedb_graph::{AlgoParams, Direction, GraphAlgorithm, GraphTraversalOptions};
use nodedb_types::{
    QualifiedCollection, RlsWriteCheck, Surrogate, SurrogateBitmap, SystemTimeScope,
};

use super::algo_stage::AlgoStage;
use super::batch_edge::BatchEdge;
use super::bsp::BspSuperstepPlan;
use super::rag_stage::RagStage;
use super::wcc::WccSuperstepPlan;

/// Graph engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GraphOp {
    /// Insert a graph edge with properties.
    ///
    /// `src_surrogate` / `dst_surrogate` carry global row identity; `src_id`
    /// / `dst_id` stay user-visible (CSR label interning, edge-store keying).
    /// Surrogates are the cross-engine join currency.
    EdgePut {
        collection: QualifiedCollection,
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
        src_surrogate: Surrogate,
        dst_surrogate: Surrogate,
    },

    /// Batched edge insert: many `(collection, src, label, dst)` tuples.
    /// Every edge in the batch must target the same collection — the
    /// batch is a unit of work, not a cross-collection scatter.
    EdgePutBatch { edges: Vec<BatchEdge> },

    /// Delete a graph edge.
    ///
    /// `src_surrogate` / `dst_surrogate` mirror `EdgePut` so a cross-shard
    /// delete dual-homes atomically via Calvin — same pair gives the tx
    /// class its participant shards and the lock identity that serializes
    /// against a concurrent `EdgePut` of the same edge.
    EdgeDelete {
        collection: QualifiedCollection,
        src_id: String,
        label: String,
        dst_id: String,
        src_surrogate: Surrogate,
        dst_surrogate: Surrogate,
        /// Write predicate, or the reason none is attached. The plan carries
        /// no property object to decide it against, so the Data Plane
        /// evaluates it against the pre-image it reads back — as a document
        /// DELETE does.
        rls_write_check: RlsWriteCheck,
    },

    /// Read-only resolve pass for a governed [`GraphOp::EdgeDelete`]. A
    /// follower can't judge a live `RlsWriteCheck::Predicate`, so this reads
    /// the edge's property object and decides the policy without writing.
    /// Success means the wrapped delete may be proposed with a decided check.
    ResolveEdgeDelete(Box<GraphOp>),

    /// Batched edge delete: used to revert a partial `EdgePutBatch` on
    /// failure so the DDL leaves no stranded edges.
    EdgeDeleteBatch { edges: Vec<BatchEdge> },

    /// Graph hop traversal: BFS from start nodes via label, bounded by depth.
    Hop {
        /// Collection this traversal is scoped to (the CSR partition is
        /// `(database, tenant)`-keyed with per-edge collection ids). `None`
        /// scopes by edge label alone — a tree-index BFS with no catalog
        /// mapping back to a collection, authorized via the index DDL instead.
        collection: Option<QualifiedCollection>,
        start_nodes: Vec<String>,
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        direction: Direction,
        depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to traversed nodes before returning.
        rls_filters: Vec<u8>,
        /// Optional surrogate prefilter restricting which frontier nodes are
        /// eligible as traversal targets. `None` = no restriction.
        frontier_bitmap: Option<SurrogateBitmap>,
    },

    /// Immediate 1-hop neighbors lookup.
    Neighbors {
        /// See `Hop::collection`.
        collection: Option<QualifiedCollection>,
        node_id: String,
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        direction: Direction,
        /// RLS filters applied to neighbor nodes before returning.
        rls_filters: Vec<u8>,
    },

    /// Batched 1-hop neighbors lookup: one RPC per hop of a BFS frontier
    /// instead of one RPC per frontier node. Returns
    /// `[{ src, label, node }, ...]` so the caller can attribute each
    /// neighbor to its origin (needed for shortest-path parent pointers).
    ///
    /// `max_results` is the per-RPC cap: the Data Plane handler stops
    /// emitting entries once the batch reaches this size so a single
    /// wide hop cannot allocate past the caller's budget. `0` means
    /// unbounded (use with care).
    NeighborsMulti {
        /// See `Hop::collection`.
        collection: Option<QualifiedCollection>,
        node_ids: Vec<String>,
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        direction: Direction,
        max_results: u32,
        /// RLS filters applied to neighbor nodes before returning.
        rls_filters: Vec<u8>,
        /// Edge-property predicate, AND-ed. Evaluated against each crossed
        /// edge's current property object in `collection` before the row
        /// counts against `max_results`. Empty admits every edge. Non-empty
        /// requires `collection`.
        edge_predicate: Vec<nodedb_types::filter::MetadataFilter>,
        /// Each row carries the crossed edge's current property object.
        /// Requires `collection`.
        with_properties: bool,
    },

    /// Shortest path between two nodes.
    Path {
        /// See `Hop::collection`.
        collection: Option<QualifiedCollection>,
        src: String,
        dst: String,
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        max_depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to path nodes before returning.
        rls_filters: Vec<u8>,
        /// Optional surrogate prefilter restricting which nodes may appear
        /// on the path. `None` = no restriction.
        frontier_bitmap: Option<SurrogateBitmap>,
    },

    /// Materialize a subgraph as edge tuples.
    Subgraph {
        /// See `Hop::collection`.
        collection: Option<QualifiedCollection>,
        start_nodes: Vec<String>,
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to subgraph nodes/edges before returning.
        rls_filters: Vec<u8>,
    },

    /// GraphRAG fusion: vector search → graph expansion → RRF ranking.
    ///
    /// Two-source form: vector + graph (backwards-compatible; `bm25_query` is `None`).
    /// Three-source form: vector + BM25 text + graph; activated when `bm25_query` is set.
    RagFusion {
        collection: QualifiedCollection,
        query_vector: Vec<f32>,
        vector_top_k: usize,
        edge_label: Option<String>,
        direction: Direction,
        expansion_depth: usize,
        final_top_k: usize,
        /// Two-source RRF k constants: (vector_k, graph_k).
        /// Used when `bm25_query` is absent (backwards-compatible two-source form).
        rrf_k: (f64, f64),
        /// Three-source RRF k constants: (vector_k, text_k, graph_k).
        /// Set when the FUSION DSL carries a `BM25 '...' ON '...'` clause.
        rrf_k_triple: Option<(f64, f64, f64)>,
        /// Vector index field name. Empty string selects the raw (field-less)
        /// index created via `VectorOp::Insert`; a non-empty value selects
        /// the field-backed index created when documents are inserted with an
        /// embedded vector column (e.g. `INSERT INTO col (id, embedding) VALUES …`).
        vector_field: String,
        options: GraphTraversalOptions,
        /// BM25 query for the text leg of three-source fusion. `None` = two-source.
        bm25_query: Option<String>,
        /// Document field scored by BM25. Required when `bm25_query` is set.
        bm25_field: Option<String>,
        /// Which part of the fusion the core runs.
        stage: RagStage,
    },

    /// Graph algorithm execution (PageRank, WCC, SSSP, etc.).
    Algo {
        algorithm: GraphAlgorithm,
        params: AlgoParams,
        /// Where the algorithm reads its graph from.
        stage: AlgoStage,
    },

    /// Graph pattern matching (MATCH clause execution).
    Match {
        /// Serialized `MatchQuery` (MessagePack).
        query: Vec<u8>,
        /// Surrogate prefilter restricting eligible pattern anchors.
        frontier_bitmap: Option<SurrogateBitmap>,
        /// `true` emits every bound zero-degree source as a cross-shard
        /// frontier candidate for Control-Plane filtering (cluster
        /// orchestration). `false` (default) matches non-cluster MATCH output.
        cluster_mode: bool,
    },

    /// Cross-shard MATCH continuation: resumes the SAME already-optimized
    /// pattern from `resume_triple_idx` on the shard owning `source_node`,
    /// after another shard emitted an `UnresolvedExpansion` for it. Must not
    /// be re-optimized — the index is the originating shard's triple order.
    MatchContinuation {
        /// Serialized (already-optimized) `MatchQuery` (MessagePack).
        query: Vec<u8>,
        /// Triple index to resume from (originating shard's order).
        resume_triple_idx: usize,
        /// Serialized accumulated bindings (MessagePack `HashMap<String, String>`).
        partial_row: Vec<u8>,
        /// The node name on THIS shard to resume expansion from.
        source_node: String,
        /// The binding variable bound to `source_node`.
        source_binding: String,
    },

    /// Cross-shard MATCH variable-length RESUME: continues a `[*min..max]`
    /// expansion that hit a hard cap, resuming MID-triple (unlike
    /// `MatchContinuation`, which resumes at a triple boundary). Query must
    /// not be re-optimized; may emit a fresh truncation cursor if capped again.
    MatchVarLenResume {
        /// Serialized (already-optimized) `MatchQuery` (MessagePack).
        query: Vec<u8>,
        /// Serialized `VarLenResume` resume cursor (MessagePack): the capped
        /// triple index, the source bindings, and the un-expanded frontier /
        /// resume depth.
        resume: Vec<u8>,
    },

    /// One distributed-PageRank BSP superstep on this shard's local CSR.
    /// Round-tripped once per superstep by the Control-Plane coordinator,
    /// threading the rank vector via `rank_vec` and cross-shard contributions
    /// via `incoming_contributions`; the handler is stateless across calls.
    /// Boxed: payload is large and `PhysicalPlan` clones across the SPSC bridge.
    BspSuperstep(Box<BspSuperstepPlan>),

    /// One distributed-WCC contraction round (single-round, not iterative):
    /// each shard unions its owned→owned edges and records owned→ghost edges
    /// as boundary edges; the coordinator stitches all shards' results into
    /// one global union-find over node names. Boxed to keep the enum small.
    WccSuperstep(Box<WccSuperstepPlan>),

    /// Set node labels (bitset-based, up to 64 distinct labels).
    SetNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },

    /// Remove node labels.
    RemoveNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },

    /// Bitemporal 1-hop neighbors lookup.
    ///
    /// Resolves edges whose latest version with `system_from <= system_as_of_ms`
    /// (converted to HLC ordinal) is not a sentinel, optionally also filtering
    /// by `valid_from_ms <= valid_at_ms < valid_until_ms`. The handler calls
    /// `ceiling_resolve_edge` per candidate base edge.
    TemporalNeighbors {
        /// Edge store is collection-scoped; current-state `Neighbors` reads
        /// the tenant-wide CSR, but the versioned key layout is
        /// `{collection}\x00...`, so the bitemporal path must name the
        /// collection explicitly.
        collection: QualifiedCollection,
        node_id: String,
        edge_label: Option<String>,
        direction: Direction,
        /// System-time selection. `Current` falls back to current-state
        /// semantics identical to `Neighbors`; `AsOf(ms)` is point-in-time.
        /// `AllVersions` returns a typed NotSupported error on graph.
        system_time: SystemTimeScope,
        /// Optional valid-time point. Skipped when `None`.
        valid_at_ms: Option<i64>,
        rls_filters: Vec<u8>,
    },

    /// Bitemporal graph algorithm execution.
    ///
    /// Identical to `Algo` but builds its CSR snapshot via
    /// `CsrSnapshot::from_edge_store_as_of` at the given system-time cutoff
    /// before running the algorithm.
    TemporalAlgorithm {
        algorithm: GraphAlgorithm,
        params: AlgoParams,
        /// System-time selection. `Current` means current state (equivalent to
        /// plain `Algo`); `AsOf(ms)` builds a snapshot at that cutoff.
        /// `AllVersions` returns a typed NotSupported error on graph.
        system_time: SystemTimeScope,
    },

    /// Read persistent graph-stats counters from the edge store.
    ///
    /// `collection = Some` returns stats for one `(tenant, collection)` pair.
    /// `collection = None` returns stats for every collection that has
    /// edges (or had any, per cold-start rebuild) for this tenant.
    ///
    /// `as_of = None` is the O(1) live-snapshot path (reads the cached
    /// summary row). `as_of = Some(ms)` falls back to a historical scan.
    Stats {
        collection: Option<QualifiedCollection>,
        as_of: Option<i64>,
    },

    /// Guard a node delete on the node's key home, `from_key(node_id)`,
    /// which holds every edge incident on the node.
    ///
    /// The live edges of `collection` with `node_id` as source or
    /// destination must be exactly `expected`. Any other state answers
    /// `OllpRetryRequired` and the transaction writes nothing. The planner
    /// reads the edges, deletes each one in the same transaction, and this
    /// guard proves that no edge appeared or vanished in between. The guard
    /// itself writes nothing.
    NodeEdgeGuard {
        collection: QualifiedCollection,
        node_id: String,
        expected: Vec<BatchEdge>,
    },

    /// A CRDT document delete's presence guard on the collection's vShard
    /// `vshard`. It runs only inside a Calvin transaction.
    ///
    /// It holds only when every id in `present` is a stored document of
    /// `collection` and no id in `absent` is. Any other state answers
    /// `OllpRetryRequired`, and the transaction writes nothing. The planner
    /// tombstones the edges of the present documents' nodes only, and this
    /// guard proves no document appeared or vanished since
    /// [`GraphOp::NodePresenceRead`] read them.
    NodePresenceGuard {
        collection: QualifiedCollection,
        vshard: u32,
        present: Vec<String>,
        absent: Vec<String>,
    },

    /// Tombstone every live edge of `collection` with a home on `vshard`
    /// whose newest version is below the transaction's ordinal.
    ///
    /// TRUNCATE of an edge-bearing collection is one Calvin transaction: the
    /// rows' truncate on the collection's vShard, and one copy of this on
    /// every vShard. The copy stages nothing. Its resolve runs at the
    /// transaction's turn on `vshard`, reads the collection's live edges
    /// there, and tombstones each one at the transaction's ordinal, which
    /// every participant shares. So both homes of an edge stamp one key, an
    /// edge sequenced before the TRUNCATE is gone, and one sequenced after
    /// it stays.
    TruncateEdges {
        collection: QualifiedCollection,
        vshard: u32,
    },

    /// The stored CRDT documents of `collection` among `ids`, read on the
    /// collection's vShard `vshard`. The answer is a msgpack array of the
    /// stored ids. A CRDT document delete's planner reads it before it
    /// builds the [`GraphOp::NodePresenceGuard`]. It writes nothing.
    NodePresenceRead {
        collection: QualifiedCollection,
        vshard: u32,
        ids: Vec<String>,
    },
}
