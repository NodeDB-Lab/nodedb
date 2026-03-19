use std::sync::Arc;
use std::time::Instant;

use crate::engine::graph::edge_store::Direction;
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
    pub payload: Arc<[u8]>,

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
        /// Pre-computed bitmap of eligible document IDs (from filter evaluation).
        filter_bitmap: Option<Arc<[u8]>>,
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
    },

    /// Insert a vector into the HNSW index (write path).
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
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
        /// Filter predicates (same format as DocumentScan).
        filters: Vec<u8>,
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

    /// Cancellation signal. Data Plane MUST stop the target request at next safe point.
    Cancel { target_request_id: RequestId },
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
            payload: Arc::from(b"result".as_slice()),
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
            payload: Arc::from([].as_slice()),
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
        };
        match req.plan {
            PhysicalPlan::Cancel { target_request_id } => {
                assert_eq!(target_request_id, RequestId::new(42));
            }
            _ => panic!("expected Cancel plan"),
        }
    }
}
