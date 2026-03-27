//! Physical plan types dispatched from Control Plane to Data Plane.
//!
//! The top-level [`PhysicalPlan`] enum delegates to per-engine sub-enums,
//! each defined in its own module. This keeps each engine's operations
//! isolated.

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

pub use crdt::CrdtOp;
pub use document::DocumentOp;
pub use graph::GraphOp;
pub use kv::KvOp;
pub use meta::MetaOp;
pub use query::QueryOp;
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
    /// Timeseries engine: columnar scan, ILP ingest.
    Timeseries(TimeseriesOp),
    /// Spatial engine: R-tree scan with OGC predicates.
    Spatial(SpatialOp),
    /// CRDT engine: read, apply delta, set policy.
    Crdt(CrdtOp),
    /// Query operations: joins, aggregates.
    Query(QueryOp),
    /// Meta / maintenance: WAL, cancel, snapshot, compact, checkpoint.
    Meta(MetaOp),
}
