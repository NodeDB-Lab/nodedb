// SPDX-License-Identifier: Apache-2.0

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

pub(crate) mod encrypt;

pub mod compaction;
pub mod delete_bitmap;
pub mod error;
pub mod filter;
pub mod format;
pub mod materialize_rows;
pub mod memtable;
pub mod mutation;
pub mod pk_index;
pub mod predicate;
pub mod reader;
#[cfg(test)]
mod test_support;
pub mod wal_record;
pub mod writer;

pub use compaction::{CompactionResult, DEFAULT_DELETE_RATIO_THRESHOLD, compact_segment};
pub use delete_bitmap::DeleteBitmap;
pub use error::ColumnarError;
pub use filter::{
    bitmask_all, bitmask_and, decoded_dict_eval_contains, decoded_dict_eval_eq,
    decoded_dict_eval_like, decoded_dict_eval_ne, dict_eval_contains, dict_eval_eq, dict_eval_like,
    dict_eval_ne, words_for,
};
pub use format::{
    BLOCK_SIZE, BlockStats, BloomFilter, ColumnMeta, MAGIC, SegmentFooter, SegmentHeader,
    VERSION_MAJOR, VERSION_MINOR,
};
pub use materialize_rows::materialize_segment_live_rows;
pub use memtable::{ColumnarMemtable, IngestValue, MemtableRowIter};
pub use mutation::{ColumnDataSnapshot, ColumnarEngineSnapshot, MutationEngine, TruncatedRows};
pub use pk_index::PkIndex;
pub use predicate::{
    BLOOM_BITS_DEFAULT, BLOOM_BYTES, BLOOM_K_DEFAULT, PredicateOp, PredicateValue, ScanPredicate,
    bloom_insert, bloom_may_contain, build_bloom, build_bloom_with_params,
};
pub use reader::OwnedSegmentReader;
pub use reader::SegmentReader;
pub use writer::SegmentWriter;
