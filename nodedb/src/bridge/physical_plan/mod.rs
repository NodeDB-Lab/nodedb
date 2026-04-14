//! Physical plan types dispatched from Control Plane to Data Plane.
//!
//! The top-level [`PhysicalPlan`] enum delegates to per-engine sub-enums,
//! each defined in its own module. This keeps each engine's operations
//! isolated.

pub mod columnar;
pub mod crdt;
pub mod document;
pub mod graph;
pub mod kv;
pub mod meta;
pub mod query;
pub mod spatial;
pub mod text;
pub mod timeseries;
pub mod vector;

pub use columnar::ColumnarOp;
pub use crdt::CrdtOp;
pub use document::{
    BalancedDef, DocumentOp, EnforcementOptions, GeneratedColumnSpec, MaterializedSumBinding,
    PeriodLockConfig, StorageMode, UpdateValue,
};
pub use graph::GraphOp;
pub use kv::KvOp;
pub use meta::MetaOp;
pub use query::{AggregateSpec, JoinProjection, QueryOp};
pub use spatial::{SpatialOp, SpatialPredicate};
pub use text::TextOp;
pub use timeseries::TimeseriesOp;
pub use vector::VectorOp;

/// Physical plan dispatched to the Data Plane.
///
/// Each variant wraps a per-engine operation enum. The Data Plane dispatcher
/// matches on the top-level variant, then delegates to engine-specific handlers.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Vector engine: HNSW search, insert, delete, params.
    Vector(VectorOp),
    /// Graph engine: edges, traversal, algorithms, pattern matching.
    Graph(GraphOp),
    /// Document engine: point CRUD, scans, indexes, bulk DML.
    Document(DocumentOp),
    /// KV engine: hash-indexed point ops, TTL, batch ops.
    Kv(KvOp),
    /// Full-text search: BM25, hybrid vector+text.
    Text(TextOp),
    /// Columnar engine (base): scan + insert for plain columnar collections.
    Columnar(ColumnarOp),
    /// Timeseries profile: extends columnar with time-range + bucketing.
    Timeseries(TimeseriesOp),
    /// Spatial profile: extends columnar with R-tree + OGC predicates.
    Spatial(SpatialOp),
    /// CRDT engine: read, apply delta, set policy.
    Crdt(CrdtOp),
    /// Query operations: joins, aggregates.
    Query(QueryOp),
    /// Meta / maintenance: WAL, cancel, snapshot, compact, checkpoint.
    Meta(MetaOp),
}

impl PhysicalPlan {
    /// Whether this plan is a read/scan operation that must broadcast to all
    /// Data Plane cores (data is distributed across cores).
    pub fn is_broadcast_scan(&self) -> bool {
        matches!(
            self,
            PhysicalPlan::Document(DocumentOp::Scan { .. })
                | PhysicalPlan::Columnar(ColumnarOp::Scan { .. })
                | PhysicalPlan::Query(QueryOp::Aggregate { .. })
                | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
                | PhysicalPlan::Graph(GraphOp::Hop { .. })
                | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
                | PhysicalPlan::Graph(GraphOp::Path { .. })
                | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
                | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
                | PhysicalPlan::Graph(GraphOp::Match { .. })
                | PhysicalPlan::Vector(VectorOp::Search { .. })
                | PhysicalPlan::Text(TextOp::Search { .. })
                | PhysicalPlan::Text(TextOp::HybridSearch { .. })
        )
    }
}
