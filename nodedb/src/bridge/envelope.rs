use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use crate::engine::graph::edge_store::Direction;

/// Response payload: heap-allocated bytes behind an `Arc<[u8]>`.
///
/// The `Deref<Target=[u8]>` impl provides transparent byte access.
/// Slab-backed zero-copy transport is defined in `super::slab` and will be
/// wired in once the Data Plane slab pool is integrated.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Heap-allocated payload.
    Heap(Arc<[u8]>),
}

impl Payload {
    /// Create a heap-backed payload from a Vec.
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self::Heap(Arc::from(v.into_boxed_slice()))
    }

    /// Create an empty payload.
    pub fn empty() -> Self {
        Self::Heap(Arc::from([].as_slice()))
    }

    /// Create from Arc<[u8]> (backward compat).
    pub fn from_arc(a: Arc<[u8]>) -> Self {
        Self::Heap(a)
    }

    /// Get the payload bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Heap(a) => a,
        }
    }

    /// Whether this payload is empty.
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Convert to Vec<u8>.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Deref for Payload {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Self::from_vec(v)
    }
}

impl From<Arc<[u8]>> for Payload {
    fn from(a: Arc<[u8]>) -> Self {
        Self::Heap(a)
    }
}
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{Lsn, ReadConsistency, RequestId, TenantId, VShardId};

/// Request envelope: Control Plane -> Data Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Request {
    /// Globally unique request identifier (monotonic per connection).
    pub request_id: RequestId,

    /// Tenant scope — all data access is tenant-scoped by construction.
    pub tenant_id: TenantId,

    /// Target virtual shard.
    pub vshard_id: VShardId,

    /// Opaque plan digest identifying the physical operation to execute.
    pub plan: PhysicalPlan,

    /// Absolute deadline. Data Plane MUST stop at next safe point after expiry.
    pub deadline: Instant,

    /// Request priority for scheduling on the Data Plane.
    pub priority: Priority,

    /// Distributed trace identifier for cross-plane observability.
    pub trace_id: u64,

    /// Read consistency level for this request.
    pub consistency: ReadConsistency,

    /// Optional idempotency key for non-idempotent writes.
    /// If present, the Data Plane deduplicates by skipping execution
    /// when the same key has already been processed (returns the
    /// cached response status).
    pub idempotency_key: Option<u64>,
}

/// Response envelope: Data Plane -> Control Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Response {
    /// Echoed request identifier for correlation.
    pub request_id: RequestId,

    /// Outcome status.
    pub status: Status,

    /// Attempt number (for retry tracking).
    pub attempt: u32,

    /// Whether this is a partial result (more coming).
    pub partial: bool,

    /// Payload bytes produced by this response chunk.
    pub payload: Payload,

    /// Watermark LSN at the time of read (for snapshot consistency tracking).
    pub watermark_lsn: Lsn,

    /// Error code if status is not Ok.
    pub error_code: Option<ErrorCode>,
}

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

    /// Soft-delete a vector by internal node ID.
    VectorDelete { collection: String, vector_id: u32 },

    /// Set HNSW index parameters for a collection. Must be called before
    /// the first insert — parameters cannot be changed after index creation.
    SetVectorParams {
        collection: String,
        m: usize,
        ef_construction: usize,
        metric: String,
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
}

/// Request priority. Higher priority requests are scheduled first on the Data Plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Background tasks (compaction, GC).
    Background = 0,
    /// Normal query traffic.
    Normal = 1,
    /// Elevated (e.g., interactive queries with tight deadlines).
    High = 2,
    /// System-critical (WAL replay, leader election responses).
    Critical = 3,
}

/// Response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Success.
    Ok,
    /// Partial success — more response chunks follow.
    Partial,
    /// Request failed with error.
    Error,
}

/// Deterministic error codes returned by the Data Plane.
///
/// Final outcomes are explicit, never opaque strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    /// Request exceeded its deadline.
    DeadlineExceeded,
    /// Constraint violation at commit time.
    RejectedConstraint { constraint: String },
    /// Pre-validation fast-reject.
    RejectedPrevalidation { reason: String },
    /// Document/collection not found.
    NotFound,
    /// Authorization failure.
    RejectedAuthz,
    /// Write conflict — client should retry.
    ConflictRetry,
    /// Fan-out limit exceeded for graph/scatter queries.
    FanOutExceeded,
    /// Memory budget exhausted — DataFusion should spill.
    ResourcesExhausted,
    /// Edge creation rejected: source or destination node does not exist.
    RejectedDanglingEdge { missing_node: String },
    /// Duplicate write detected via idempotency key.
    DuplicateWrite,
    /// Internal error (io_uring failure, corruption, etc.)
    Internal { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample_request() -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::PointGet {
                collection: "users".into(),
                document_id: "doc-1".into(),
            },
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0xABCD,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
        }
    }

    #[test]
    fn request_fields_accessible() {
        let req = sample_request();
        assert_eq!(req.request_id, RequestId::new(1));
        assert_eq!(req.tenant_id, TenantId::new(1));
        assert_eq!(req.trace_id, 0xABCD);
    }

    #[test]
    fn response_ok() {
        let resp = Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_arc(Arc::from(b"result".as_slice())),
            watermark_lsn: Lsn::new(42),
            error_code: None,
        };
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.watermark_lsn, Lsn::new(42));
        assert_eq!(&*resp.payload, b"result");
    }

    #[test]
    fn response_error() {
        let resp = Response {
            request_id: RequestId::new(2),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(ErrorCode::DeadlineExceeded),
        };
        assert_eq!(resp.error_code, Some(ErrorCode::DeadlineExceeded));
    }

    #[test]
    fn priority_ordering() {
        assert!(Priority::Background < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
    }

    #[test]
    fn cancel_plan() {
        let req = Request {
            request_id: RequestId::new(99),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Cancel {
                target_request_id: RequestId::new(42),
            },
            deadline: Instant::now() + Duration::from_secs(1),
            priority: Priority::Critical,
            trace_id: 0,
            consistency: ReadConsistency::Eventual,
            idempotency_key: None,
        };
        match req.plan {
            PhysicalPlan::Cancel { target_request_id } => {
                assert_eq!(target_request_id, RequestId::new(42));
            }
            _ => panic!("expected Cancel plan"),
        }
    }
}
