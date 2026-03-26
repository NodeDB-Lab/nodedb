//! Columnar segment format and memtable for NodeDB analytical storage.
//!
//! Provides the shared segment format used by all columnar profiles
//! (plain, timeseries, spatial). Segments are self-contained files with
//! per-column compressed blocks, block statistics, and a CRC-validated footer.
//!
//! # Segment Layout
//!
//! ```text
//! [SegmentHeader: magic "NDBS" + version + endianness]
//! [Column 0 blocks][Column 1 blocks]...[Column N blocks]
//! [SegmentFooter: schema_hash, column metadata, block stats, CRC32C]
//! ```

pub mod error;
pub mod format;
pub mod memtable;
pub mod predicate;
pub mod reader;
pub mod writer;

pub use error::ColumnarError;
pub use format::{
    BLOCK_SIZE, BlockStats, ColumnMeta, MAGIC, SegmentFooter, SegmentHeader, VERSION_MAJOR,
    VERSION_MINOR,
};
pub use memtable::ColumnarMemtable;
pub use predicate::ScanPredicate;
pub use reader::SegmentReader;
pub use writer::SegmentWriter;
