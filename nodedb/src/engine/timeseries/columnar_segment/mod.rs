//! Columnar segment writer and reader for time-partitioned storage.
//!
//! Flushes columnar memtable data to per-column files within a partition
//! directory. Each column is stored as a separate file for projection
//! pushdown (only read columns the query needs).
//!
//! Layout per partition directory:
//! ```text
//! {collection}/ts-{start}_{end}/
//! ├── timestamp.col       # Codec-compressed i64 timestamps
//! ├── value.col           # Codec-compressed f64 values
//! ├── {tag}.sym           # Symbol dictionary for tag columns
//! ├── {tag}.col           # u32 symbol IDs (raw LE or codec-compressed)
//! ├── schema.json         # Column names, types, and codecs
//! └── partition.meta      # PartitionMeta + per-column statistics (JSON)
//! ```

pub mod mmap;

mod codec;
mod error;
mod reader;
mod schema;
mod util;
mod writer;

pub use error::SegmentError;
pub use mmap::{ColumnMmap, test_hooks};
pub use reader::ColumnarSegmentReader;
pub use writer::ColumnarSegmentWriter;

#[cfg(test)]
mod tests;
