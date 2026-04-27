//! NodeDB Array Engine — shared ND sparse array primitives.
//!
//! This crate provides the type substrate, schema definitions, coordinate
//! encoding, and tile layout used by the array engine on both Origin
//! (server) and Lite (embedded) deployments. Storage, WAL integration,
//! and SQL routing live in their respective Origin-side crates.

pub mod codec;
pub mod coord;
pub mod error;
pub mod query;
pub mod schema;
pub mod segment;
pub mod sync;
pub mod tile;
pub mod types;
pub mod wal;

pub use coord::{
    bits_per_dim, decode_hilbert_prefix, decode_zorder_prefix, encode_hilbert_prefix,
    encode_zorder_prefix, normalize_coord,
};
pub use error::{ArrayError, ArrayResult};
pub use schema::{
    ArraySchema, ArraySchemaBuilder, AttrSpec, CellOrder, DimSpec, DimType, TileOrder,
};
pub use segment::{
    FOOTER_MAGIC, FORMAT_VERSION, HEADER_MAGIC, HilbertPackedRTree, MbrQueryPredicate,
    SegmentFooter, SegmentHeader, SegmentReader, SegmentWriter, TileEntry, TileKind, TilePayload,
};
pub use sync::{
    AckVector, ApplyEngine, ApplyOutcome, ApplyRejection, ArrayOp, ArrayOpHeader, ArrayOpKind,
    CoordRange, GcReport, Hlc, HlcGenerator, OpLog, ReplicaId, SchemaDoc, SnapshotChunk,
    SnapshotHeader, SnapshotSink, TileSnapshot,
};
pub use tile::{
    AttrStats, DENSE_PROMOTION_THRESHOLD, DenseTile, SparseTile, TileMBR, should_promote_to_dense,
    sparse_to_dense, tile_id_for_cell, tile_indices_for_cell,
};
pub use types::{ArrayId, CellValue, Coord, Domain, TileId};
pub use wal::ArrayWalRecord;
