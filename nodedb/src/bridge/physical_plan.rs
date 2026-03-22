//! Physical plan types dispatched from Control Plane to Data Plane.
//!
//! Each variant carries the minimum information the Data Plane needs
//! to execute without accessing Control Plane state.

use std::sync::Arc;

use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::RequestId;

/// Physical plan dispatched to the Data Plane.
///
/// This enum will grow as engines are integrated. Each variant carries
/// the minimum information the Data Plane needs to execute without
/// accessing Control Plane state.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Vector similarity search.
    VectorSearch {
        collection: String,
        query_vector: Arc<[f32]>,
        top_k: usize,
        /// Optional search beam width override. If 0, uses default `4 * top_k`.
        /// Higher values improve recall at the cost of latency.
        ef_search: usize,
        /// Pre-computed bitmap of eligible document IDs (from filter evaluation).
        filter_bitmap: Option<Arc<[u8]>>,
        /// Named vector field to search. Empty string = default field.
        field_name: String,
    },

    /// Point lookup by document ID.
    PointGet {
        collection: String,
        document_id: String,
    },

    /// Range scan on a sparse/metadata index.
    RangeScan {
        collection: String,
        field: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        limit: usize,
    },

    /// CRDT state read for a document.
    CrdtRead {
        collection: String,
        document_id: String,
    },

    /// CRDT delta application (write path).
    CrdtApply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Per-mutation unique ID for deduplication and compensation tracking.
        /// Used by the DLQ and sync protocol to correlate rejected deltas
        /// with compensation hints.
        mutation_id: u64,
    },

    /// Insert a vector into the HNSW index (write path).
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        /// Named vector field (e.g., "title_embedding", "body_embedding").
        /// Empty string = default (unnamed) field. Each named field gets
        /// its own HNSW index with independent params and metric.
        field_name: String,
    },

    /// Batch insert vectors into the HNSW index (write path).
    ///
    /// Amortizes SPSC bridge overhead and enables single WAL group commit
    /// for the entire batch.
    VectorBatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
    },

    /// Multi-vector search: query across all named vector fields in a
    /// collection and fuse results via per-field RRF.
    ///
    /// Each named field has its own HNSW index. Results from all fields
    /// are scored independently, then merged using Reciprocal Rank Fusion.
    VectorMultiSearch {
        collection: String,
        query_vector: Arc<[f32]>,
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<Arc<[u8]>>,
    },

    /// Soft-delete a vector by internal node ID.
    VectorDelete { collection: String, vector_id: u32 },

    /// Set vector index parameters for a collection. Must be called before
    /// the first insert — parameters cannot be changed after index creation.
    ///
    /// `index_type`:
    /// - `"hnsw"` (default) — HNSW graph, FP32 vectors
    /// - `"hnsw_pq"` — HNSW graph with PQ-compressed traversal (lower memory, ~95% recall)
    /// - `"ivf_pq"` — IVF-PQ flat index (lowest memory, ~85-95% recall, best for >10M vectors)
    SetVectorParams {
        collection: String,
        m: usize,
        ef_construction: usize,
        metric: String,
        /// Index type: "hnsw" (default), "hnsw_pq", or "ivf_pq".
        #[allow(dead_code)]
        index_type: String,
        /// PQ subvectors (for hnsw_pq and ivf_pq). Must divide dim evenly. Default: 8.
        #[allow(dead_code)]
        pq_m: usize,
        /// IVF cells (for ivf_pq only). Typical: sqrt(N). Default: 256.
        #[allow(dead_code)]
        ivf_cells: usize,
        /// IVF probe count (for ivf_pq only). Higher = better recall. Default: 16.
        #[allow(dead_code)]
        ivf_nprobe: usize,
    },

    /// Batch insert documents into the sparse engine in a single redb transaction.
    DocumentBatchInsert {
        collection: String,
        /// (document_id, value_bytes) pairs.
        documents: Vec<(String, Vec<u8>)>,
    },

    /// Point write: insert/update a document in the sparse engine.
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
    },

    /// Point delete: remove a document from the sparse engine.
    PointDelete {
        collection: String,
        document_id: String,
    },

    /// Point update: read-modify-write a document with field-level changes.
    ///
    /// Reads the current document, merges the field updates, writes back.
    /// This is the UPDATE ... SET ... WHERE id = '...' path.
    PointUpdate {
        collection: String,
        document_id: String,
        /// Field name → new JSON value.
        updates: Vec<(String, Vec<u8>)>,
    },

    /// Full collection scan: reads from DOCUMENTS table with post-scan
    /// filtering, sorting, and pagination. Used for SELECT with compound
    /// WHERE predicates.
    DocumentScan {
        collection: String,
        /// Maximum documents to return (after filtering).
        limit: usize,
        /// Documents to skip before returning (OFFSET).
        offset: usize,
        /// Sort keys: `[(field_name, ascending)]`. Empty = no sort.
        /// Multiple keys give multi-column ORDER BY (first key is primary).
        sort_keys: Vec<(String, bool)>,
        /// Filter predicates serialized as JSON:
        /// `[{"field": "age", "op": "gt", "value": 25}, {"field": "city", "op": "eq", "value": "NYC"}]`
        /// Empty = no filter (return all documents up to limit).
        filters: Vec<u8>,
        /// If true, deduplicate result rows by content (SELECT DISTINCT).
        distinct: bool,
        /// Column projection: only return these fields from each document.
        /// Empty = return all fields (SELECT *).
        projection: Vec<String>,
    },

    /// Hash join: inner join two collections on matching fields.
    ///
    /// Scans both collections, builds a hash map on the right side,
    /// probes with the left side. Returns merged documents.
    HashJoin {
        left_collection: String,
        right_collection: String,
        /// Join keys: `[(left_field, right_field)]`.
        on: Vec<(String, String)>,
        /// Join type: "inner", "left", "right", "full".
        join_type: String,
        /// Maximum output rows.
        limit: usize,
    },

    /// Aggregate query: GROUP BY + aggregate functions on documents.
    ///
    /// Scans all documents in the collection, groups by the specified field,
    /// and computes aggregate functions (COUNT, SUM, AVG, MIN, MAX).
    Aggregate {
        collection: String,
        /// Fields to group by. Empty = no grouping (single aggregate over all).
        /// Multiple fields give composite group key.
        group_by: Vec<String>,
        /// Aggregate operations: `[("count", "*"), ("sum", "price"), ("avg", "age")]`
        aggregates: Vec<(String, String)>,
        /// Filter predicates (same format as DocumentScan) — applied pre-aggregation.
        filters: Vec<u8>,
        /// HAVING predicates — applied post-aggregation on computed results.
        /// Format: `[{"field": "count_all", "op": "gt", "value": 10}]`
        /// Field names use the aggregate result column names (e.g., `count_all`, `sum_price`).
        having: Vec<u8>,
        /// Maximum groups to return.
        limit: usize,
        /// Optional nested sub-aggregation: within each group, further group
        /// by these fields and compute these aggregate ops. Produces hierarchical
        /// results: `{ group_key, agg_results, sub_groups: [{...}] }`.
        /// Empty = no sub-aggregation (flat results).
        sub_group_by: Vec<String>,
        /// Sub-aggregation operations (same format as `aggregates`).
        sub_aggregates: Vec<(String, String)>,
    },

    /// Partial aggregate: each Data Plane core computes a partial aggregate
    /// locally. The Control Plane merges partial results from all cores.
    ///
    /// Partial state format: `{"group_key": {"count": N, "sum_field": S, ...}}`.
    /// The Control Plane combines partials with `count += count`, `sum += sum`,
    /// `min = min(min)`, `max = max(max)`, `avg = total_sum / total_count`.
    PartialAggregate {
        collection: String,
        group_by: Vec<String>,
        aggregates: Vec<(String, String)>,
        filters: Vec<u8>,
    },

    /// Broadcast join: the Control Plane serializes the small side (< 8 MiB)
    /// and includes it in the plan. Each core builds a local hash map from
    /// the broadcast data and probes with its local large-side data.
    BroadcastJoin {
        /// The large-side collection (scanned locally by each core).
        large_collection: String,
        /// Serialized small-side rows as MessagePack `Vec<(String, Vec<u8>)>`.
        broadcast_data: Vec<u8>,
        /// Join keys: `[(large_field, small_field)]`.
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
    },

    /// Shuffle join: each core scans its local data, hashes on the join key,
    /// and routes rows to the owning core via SPSC. The target core performs
    /// a local hash join on the repartitioned data.
    ///
    /// Phase 1 (shuffle): scan + hash + route via SPSC
    /// Phase 2 (local join): hash join on received partitions
    ShuffleJoin {
        left_collection: String,
        right_collection: String,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
        /// Target core for this partition (set by the Control Plane).
        target_core: usize,
    },

    /// Insert a graph edge with properties.
    EdgePut {
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
    },

    /// Delete a graph edge.
    EdgeDelete {
        src_id: String,
        label: String,
        dst_id: String,
    },

    /// Graph hop traversal: BFS from start nodes via label, bounded by depth.
    GraphHop {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        direction: Direction,
        depth: usize,
        options: GraphTraversalOptions,
    },

    /// Immediate 1-hop neighbors lookup.
    GraphNeighbors {
        node_id: String,
        edge_label: Option<String>,
        direction: Direction,
    },

    /// Shortest path between two nodes.
    GraphPath {
        src: String,
        dst: String,
        edge_label: Option<String>,
        max_depth: usize,
        options: GraphTraversalOptions,
    },

    /// Materialize a subgraph as edge tuples.
    GraphSubgraph {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        depth: usize,
        options: GraphTraversalOptions,
    },

    /// GraphRAG fusion: vector search → graph expansion → RRF ranking.
    ///
    /// Steps 1-3 execute entirely on the Data Plane within a single
    /// SPSC request-response cycle when all nodes are shard-local.
    GraphRagFusion {
        /// Collection for vector search.
        collection: String,
        /// Query vector for semantic similarity.
        query_vector: Arc<[f32]>,
        /// Number of initial vector results.
        vector_top_k: usize,
        /// Edge label for graph expansion.
        edge_label: Option<String>,
        /// Graph expansion direction.
        direction: Direction,
        /// Graph expansion depth (hops).
        expansion_depth: usize,
        /// Final top-N after RRF fusion.
        final_top_k: usize,
        /// RRF k constants: (vector_k, graph_k).
        rrf_k: (f64, f64),
        /// Traversal options for the graph expansion phase.
        options: GraphTraversalOptions,
    },

    /// WAL append (write path).
    WalAppend { payload: Vec<u8> },

    /// Set conflict resolution policy for a CRDT collection (DDL).
    ///
    /// Parsed from `ALTER COLLECTION <name> SET ON CONFLICT <policy>`.
    /// The Data Plane updates its per-tenant PolicyRegistry.
    SetCollectionPolicy {
        collection: String,
        /// JSON-serialized `CollectionPolicy` from nodedb-crdt.
        policy_json: String,
    },

    /// Full-text search using BM25 scoring on the inverted index.
    TextSearch {
        collection: String,
        query: String,
        top_k: usize,
        /// Enable fuzzy matching (Levenshtein) for typo tolerance.
        fuzzy: bool,
    },

    /// Hybrid search: vector similarity + BM25 text, fused via RRF.
    ///
    /// Executes vector search and text search in parallel on the Data Plane,
    /// then fuses results using Reciprocal Rank Fusion.
    HybridSearch {
        collection: String,
        query_vector: Arc<[f32]>,
        query_text: String,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        /// Weight for vector results in RRF (0.0-1.0). Text weight = 1.0 - vector_weight.
        /// Default: 0.5 (equal weight). Higher values favor vector similarity.
        vector_weight: f32,
        filter_bitmap: Option<Arc<[u8]>>,
    },

    /// Cancellation signal. Data Plane MUST stop the target request at next safe point.
    Cancel { target_request_id: RequestId },

    /// Nested loop join: fallback for non-equi joins (`a.x > b.y`),
    /// theta joins, and small cross joins where hash join can't operate.
    NestedLoopJoin {
        left_collection: String,
        right_collection: String,
        /// Join condition as serialized `Vec<ScanFilter>` (MessagePack).
        /// Each filter references fields as `{collection}.{field}`.
        /// Empty = cross join.
        condition: Vec<u8>,
        join_type: String,
        limit: usize,
    },

    /// Atomic transaction batch: execute all sub-plans atomically.
    ///
    /// On the Data Plane, all sub-plans execute within a single logical
    /// transaction. If any sub-plan fails, all previous sub-plans are
    /// rolled back (sparse engine operations via redb, CRDT deltas via
    /// scratch buffer discard). A single `RecordType::Transaction` WAL
    /// record covers the entire batch (written by Control Plane before
    /// dispatch).
    TransactionBatch { plans: Vec<PhysicalPlan> },

    /// Create a snapshot: export all engine state for this core.
    ///
    /// The Data Plane core serializes its full state (redb, HNSW, CRDT)
    /// as a `CoreSnapshot` and returns the serialized bytes as the response
    /// payload. The Control Plane collects snapshots from all cores and
    /// writes them to disk.
    CreateSnapshot,

    /// On-demand compaction: compact all vector indexes (remove tombstones),
    /// compact CSR write buffers, and sweep dangling edges.
    ///
    /// The Data Plane forces compaction regardless of tombstone ratio.
    /// Returns compaction statistics as MessagePack payload.
    Compact,

    /// Checkpoint request: flush all engine state to disk and report the
    /// core's checkpoint LSN.
    ///
    /// The Data Plane core:
    /// 1. Checkpoints all vector indexes to disk (atomic rename).
    /// 2. Exports CRDT snapshots to disk.
    /// 3. redb is already ACID-durable (no action needed).
    /// 4. Returns the core's current watermark LSN as the checkpoint LSN.
    ///
    /// The Control Plane collects checkpoint LSNs from all cores, takes
    /// the minimum, writes a WAL checkpoint marker, and truncates the WAL.
    Checkpoint,
}
