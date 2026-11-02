// SPDX-License-Identifier: Apache-2.0

//! Columnar segment binary format definitions.
//!
//! All multi-byte integers are little-endian. The segment is self-describing:
//! the footer contains all metadata needed to read any column without
//! scanning the entire file.

use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

/// Magic bytes identifying a NodeDB columnar segment.
pub const MAGIC: [u8; 4] = *b"NDBS";

/// Format major version. Readers reject a segment of any other major version.
pub const VERSION_MAJOR: u8 = 1;

/// Format minor version. Readers reject a segment of any other minor version.
pub const VERSION_MINOR: u8 = 0;

/// Endianness marker: 0x01 = little-endian (always LE for NodeDB).
pub const ENDIANNESS_LE: u8 = 0x01;

/// Rows per block. Each block is independently compressed and decompressible.
/// 1024 balances compression ratio (larger = better) vs random access
/// granularity (smaller = less waste on filtered scans).
pub const BLOCK_SIZE: usize = 1024;

/// Size of the segment header in bytes: magic(4) + major(1) + minor(1) + endianness(1) = 7.
pub const HEADER_SIZE: usize = 7;

/// Segment header: identifies the file as a NodeDB columnar segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub magic: [u8; 4],
    pub version_major: u8,
    pub version_minor: u8,
    pub endianness: u8,
}

impl SegmentHeader {
    /// Create a header with the current format version.
    pub fn current() -> Self {
        Self {
            magic: MAGIC,
            version_major: VERSION_MAJOR,
            version_minor: VERSION_MINOR,
            endianness: ENDIANNESS_LE,
        }
    }

    /// Serialize the header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version_major;
        buf[5] = self.version_minor;
        buf[6] = self.endianness;
        buf
    }

    /// Parse a header from bytes. Returns an error if magic or version is invalid.
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::error::ColumnarError> {
        if data.len() < HEADER_SIZE {
            return Err(crate::error::ColumnarError::TruncatedSegment {
                expected: HEADER_SIZE,
                got: data.len(),
            });
        }

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&data[0..4]);
        if magic != MAGIC {
            return Err(crate::error::ColumnarError::InvalidMagic(magic));
        }

        let version_major = data[4];
        let version_minor = data[5];

        if version_major != VERSION_MAJOR || version_minor != VERSION_MINOR {
            return Err(crate::error::ColumnarError::IncompatibleVersion {
                reader_major: VERSION_MAJOR,
                reader_minor: VERSION_MINOR,
                segment_major: version_major,
                segment_minor: version_minor,
            });
        }

        Ok(Self {
            magic,
            version_major,
            version_minor,
            endianness: data[6],
        })
    }
}

/// On-disk representation of a Bloom filter, bundled with the parameters
/// that produced it.
///
/// Storing `k` and `m` alongside the bytes allows any future reader to
/// interpret the filter without relying on compile-time constants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct BloomFilter {
    /// Number of independent hash functions used when building and querying
    /// the filter.
    pub k: u8,
    /// Size of the bit array in bits. The `bytes` field holds `(m + 7) / 8`
    /// bytes.
    pub m: u32,
    /// Packed bit array.
    pub bytes: Vec<u8>,
}

/// Per-block statistics for a single column. Enables predicate pushdown:
/// skip blocks where `WHERE price > 100` and block's `max_price < 100`.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct BlockStats {
    /// Minimum value in this block (encoded as f64 for uniformity;
    /// i64 values are cast losslessly for small values; strings use NaN).
    ///
    /// For i64/timestamp columns, prefer `min_i64` when available — f64 cannot
    /// represent all i64 values exactly (values outside ±2^53 may be rounded).
    pub min: f64,
    /// Maximum value in this block (see `min` for caveats on i64 precision).
    pub max: f64,
    /// Number of null values in this block.
    pub null_count: u32,
    /// Number of rows in this block (≤ BLOCK_SIZE, last block may be smaller).
    pub row_count: u32,
    /// Lexicographic minimum for string columns (truncated to 32 bytes).
    /// `None` for numeric, bool, binary, vector, and other non-string columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub str_min: Option<String>,
    /// Lexicographic maximum for string columns (truncated to 32 bytes).
    /// `None` for non-string columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub str_max: Option<String>,
    /// Bloom filter for equality-predicate skipping on string columns.
    ///
    /// Carries the filter parameters (`k`, `m`) alongside the bytes so that
    /// readers never depend on compile-time constants to interpret the filter.
    /// `None` when there are no non-null string values in the block, or when
    /// cardinality is too low to justify the overhead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bloom: Option<BloomFilter>,
    /// Exact integer minimum for i64/timestamp columns.
    ///
    /// Set alongside `min` (which holds the lossy f64 cast) so that predicates
    /// with integral values outside ±2^53 can compare losslessly. `None` for
    /// all non-integer column types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_i64: Option<i64>,
    /// Exact integer maximum for i64/timestamp columns (see `min_i64`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_i64: Option<i64>,
}

impl BlockStats {
    /// Create stats for a numeric block with known min/max.
    pub fn numeric(min: f64, max: f64, null_count: u32, row_count: u32) -> Self {
        Self {
            min,
            max,
            null_count,
            row_count,
            str_min: None,
            str_max: None,
            bloom: None,
            min_i64: None,
            max_i64: None,
        }
    }

    /// Create stats for an i64 or timestamp column block.
    ///
    /// Populates both the lossless `min_i64`/`max_i64` fields AND the lossy
    /// `min`/`max` f64 fields so that the f64 path remains a valid fallback
    /// for non-integral predicate values.
    pub fn integer(min: i64, max: i64, null_count: u32, row_count: u32) -> Self {
        Self {
            min: min as f64,
            max: max as f64,
            null_count,
            row_count,
            str_min: None,
            str_max: None,
            bloom: None,
            min_i64: Some(min),
            max_i64: Some(max),
        }
    }

    /// Create stats for a block with no meaningful numeric min/max (non-numeric columns).
    pub fn non_numeric(null_count: u32, row_count: u32) -> Self {
        Self {
            min: f64::NAN,
            max: f64::NAN,
            null_count,
            row_count,
            str_min: None,
            str_max: None,
            bloom: None,
            min_i64: None,
            max_i64: None,
        }
    }

    /// Create stats for a string block with lexicographic bounds and bloom filter.
    pub fn string_block(
        null_count: u32,
        row_count: u32,
        str_min: Option<String>,
        str_max: Option<String>,
        bloom: Option<BloomFilter>,
    ) -> Self {
        Self {
            min: f64::NAN,
            max: f64::NAN,
            null_count,
            row_count,
            str_min,
            str_max,
            bloom,
            min_i64: None,
            max_i64: None,
        }
    }
}

/// Physical layout of every block of one column.
///
/// The writer records the layout it encodes. The reader decodes by it, so a
/// column's codec never has to stand in for its layout.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum BlockLayout {
    /// `i64` values through the integer codec pipeline.
    Int64 = 1,
    /// `f64` values through the float codec pipeline.
    Float64 = 2,
    /// One bit per row: row `i` is bit `i % 8` of byte `i / 8`.
    PackedBool = 3,
    /// A compressed `i64` offset table, then the variable-length bytes.
    VarLen = 4,
    /// Cells of one width: 16-byte decimals and identifiers, or packed `f32`
    /// vectors. Every row holds a full cell, a null row included.
    FixedWidth = 5,
    /// `i64` dictionary IDs. The strings are in [`ColumnMeta::dictionary`].
    DictIds = 6,
}

/// Metadata for a single column within the segment footer.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct ColumnMeta {
    /// Column name (matches schema definition).
    pub name: String,
    /// Byte offset of this column's first block from the start of the segment
    /// (after the header).
    pub offset: u64,
    /// Total byte length of all blocks for this column.
    pub length: u64,
    /// Codec used for this column's blocks.
    ///
    /// Always a concrete, resolved codec — never `Auto`.
    pub codec: nodedb_codec::ResolvedColumnCodec,
    /// Physical layout of this column's blocks.
    pub layout: BlockLayout,
    /// Number of blocks for this column.
    pub block_count: u32,
    /// Per-block statistics (one entry per block).
    pub block_stats: Vec<BlockStats>,
    /// Dictionary for dict-encoded columns (`None` for non-dict columns).
    ///
    /// Strings are stored in ID order: `dictionary[0]` is ID 0, etc.
    /// The actual column data is stored as i64 IDs via `DeltaFastLanesLz4`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dictionary: Option<Vec<String>>,
}

/// Segment footer: contains all metadata needed to read any column.
///
/// Serialized as MessagePack at the end of the segment, followed by
/// a 4-byte footer length (u32 LE) and a 4-byte CRC32C of the
/// serialized footer bytes.
///
/// ```text
/// [...column blocks...][footer_msgpack][footer_len: u32 LE][footer_crc: u32 LE]
/// ```
///
/// To read: seek to end - 8, read footer_len + footer_crc, seek to
/// end - 8 - footer_len, read + verify CRC, deserialize footer.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct SegmentFooter {
    /// xxHash64 of the schema definition (for schema compatibility checks).
    pub schema_hash: u64,
    /// Number of columns in this segment.
    pub column_count: u32,
    /// Total number of rows across all blocks.
    pub row_count: u64,
    /// Profile that wrote this segment (0 = plain, 1 = timeseries, 2 = spatial).
    pub profile_tag: u8,
    /// Per-column metadata (offsets, codecs, block stats).
    pub columns: Vec<ColumnMeta>,
}

impl SegmentFooter {
    /// Serialize the footer to bytes with trailing length + CRC32C.
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::error::ColumnarError> {
        let footer_msgpack = zerompk::to_msgpack_vec(self)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))?;

        let footer_len = footer_msgpack.len() as u32;
        let footer_crc = crc32c::crc32c(&footer_msgpack);

        let mut buf = Vec::with_capacity(footer_msgpack.len() + 8);
        buf.extend_from_slice(&footer_msgpack);
        buf.extend_from_slice(&footer_len.to_le_bytes());
        buf.extend_from_slice(&footer_crc.to_le_bytes());
        Ok(buf)
    }

    /// Parse a footer from the tail of a segment byte slice.
    ///
    /// Reads footer_len and CRC from the last 8 bytes, then deserializes
    /// the footer and validates the CRC.
    pub fn from_segment_tail(data: &[u8]) -> Result<Self, crate::error::ColumnarError> {
        if data.len() < 8 {
            return Err(crate::error::ColumnarError::TruncatedSegment {
                expected: 8,
                got: data.len(),
            });
        }

        let tail = &data[data.len() - 8..];
        let footer_len = u32::from_le_bytes(tail[0..4].try_into().map_err(|_| {
            crate::error::ColumnarError::Corruption {
                segment_id: None,
                reason: "footer length field: expected 4 bytes at segment tail - 8".into(),
                offset: Some((data.len() - 8) as u64),
            }
        })?) as usize;
        let stored_crc = u32::from_le_bytes(tail[4..8].try_into().map_err(|_| {
            crate::error::ColumnarError::Corruption {
                segment_id: None,
                reason: "footer CRC field: expected 4 bytes at segment tail - 4".into(),
                offset: Some((data.len() - 4) as u64),
            }
        })?);

        let footer_start = data.len().checked_sub(8 + footer_len).ok_or(
            crate::error::ColumnarError::TruncatedSegment {
                expected: 8 + footer_len,
                got: data.len(),
            },
        )?;

        let footer_bytes = &data[footer_start..footer_start + footer_len];
        let computed_crc = crc32c::crc32c(footer_bytes);

        if computed_crc != stored_crc {
            return Err(crate::error::ColumnarError::FooterCrcMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        zerompk::from_msgpack(footer_bytes)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let header = SegmentHeader::current();
        let bytes = header.to_bytes();
        let parsed = SegmentHeader::from_bytes(&bytes).expect("valid header");
        assert_eq!(parsed, header);
    }

    #[test]
    fn header_invalid_magic() {
        let mut bytes = SegmentHeader::current().to_bytes();
        bytes[0] = b'X';
        assert!(matches!(
            SegmentHeader::from_bytes(&bytes),
            Err(crate::error::ColumnarError::InvalidMagic(_))
        ));
    }

    #[test]
    fn header_incompatible_major() {
        let mut bytes = SegmentHeader::current().to_bytes();
        bytes[4] = VERSION_MAJOR + 1;
        assert!(matches!(
            SegmentHeader::from_bytes(&bytes),
            Err(crate::error::ColumnarError::IncompatibleVersion { .. })
        ));
    }

    #[test]
    fn header_incompatible_minor() {
        let mut bytes = SegmentHeader::current().to_bytes();
        bytes[5] = VERSION_MINOR + 1;
        assert!(matches!(
            SegmentHeader::from_bytes(&bytes),
            Err(crate::error::ColumnarError::IncompatibleVersion { .. })
        ));
    }

    #[test]
    fn footer_roundtrip() {
        let footer = SegmentFooter {
            schema_hash: 0xDEAD_BEEF_CAFE_1234,
            column_count: 3,
            row_count: 2048,
            profile_tag: 0,
            columns: vec![
                ColumnMeta {
                    name: "id".into(),
                    offset: 7,
                    length: 512,
                    codec: nodedb_codec::ResolvedColumnCodec::DeltaFastLanesLz4,
                    layout: BlockLayout::Int64,
                    block_count: 2,
                    block_stats: vec![
                        BlockStats::numeric(1.0, 1024.0, 0, 1024),
                        BlockStats::numeric(1025.0, 2048.0, 0, 1024),
                    ],
                    dictionary: None,
                },
                ColumnMeta {
                    name: "name".into(),
                    offset: 519,
                    length: 256,
                    codec: nodedb_codec::ResolvedColumnCodec::FsstLz4,
                    layout: BlockLayout::VarLen,
                    block_count: 2,
                    block_stats: vec![
                        BlockStats::non_numeric(0, 1024),
                        BlockStats::non_numeric(5, 1024),
                    ],
                    dictionary: None,
                },
                ColumnMeta {
                    name: "score".into(),
                    offset: 775,
                    length: 128,
                    codec: nodedb_codec::ResolvedColumnCodec::AlpFastLanesLz4,
                    layout: BlockLayout::Float64,
                    block_count: 2,
                    block_stats: vec![
                        BlockStats::numeric(0.0, 100.0, 10, 1024),
                        BlockStats::numeric(0.5, 99.5, 3, 1024),
                    ],
                    dictionary: None,
                },
            ],
        };

        // Serialize footer to bytes (with length + CRC trailer).
        let footer_bytes = footer.to_bytes().expect("serialize");

        // Simulate a full segment: header + dummy data + footer.
        let mut segment = Vec::new();
        segment.extend_from_slice(&SegmentHeader::current().to_bytes());
        segment.extend_from_slice(&vec![0u8; 896]); // Dummy column data.
        segment.extend_from_slice(&footer_bytes);

        let parsed = SegmentFooter::from_segment_tail(&segment).expect("parse footer");
        assert_eq!(parsed.schema_hash, footer.schema_hash);
        assert_eq!(parsed.column_count, 3);
        assert_eq!(parsed.row_count, 2048);
        assert_eq!(parsed.columns.len(), 3);
        assert_eq!(parsed.columns[0].name, "id");
        assert_eq!(parsed.columns[1].name, "name");
        assert_eq!(parsed.columns[2].name, "score");
        assert_eq!(parsed.columns[1].layout, BlockLayout::VarLen);
    }

    #[test]
    fn footer_crc_mismatch() {
        let footer = SegmentFooter {
            schema_hash: 0,
            column_count: 0,
            row_count: 0,
            profile_tag: 0,
            columns: vec![],
        };
        let mut bytes = footer.to_bytes().expect("serialize");
        // Corrupt the CRC.
        let len = bytes.len();
        bytes[len - 1] ^= 0xFF;

        assert!(matches!(
            SegmentFooter::from_segment_tail(&bytes),
            Err(crate::error::ColumnarError::FooterCrcMismatch { .. })
        ));
    }

    #[test]
    fn block_stats_predicate_skip() {
        let stats = BlockStats::numeric(10.0, 50.0, 0, 1024);

        use crate::predicate::ScanPredicate;

        // WHERE x > 60 → can skip (max=50 ≤ 60).
        assert!(ScanPredicate::gt(0, 60.0).can_skip_block(&stats));
        // WHERE x > 40 → cannot skip (max=50 > 40).
        assert!(!ScanPredicate::gt(0, 40.0).can_skip_block(&stats));
        // WHERE x < 5 → can skip (min=10 ≥ 5).
        assert!(ScanPredicate::lt(0, 5.0).can_skip_block(&stats));
        // WHERE x = 100 → can skip (100 > max=50).
        assert!(ScanPredicate::eq(0, 100.0).can_skip_block(&stats));
        // WHERE x = 30 → cannot skip (10 ≤ 30 ≤ 50).
        assert!(!ScanPredicate::eq(0, 30.0).can_skip_block(&stats));
    }

    #[cfg(test)]
    mod golden {
        use super::*;

        /// Asserts magic bytes, version_major == 1, version_minor == 0, and
        /// that the footer CRC round-trips cleanly.
        #[test]
        fn golden_columnar_segment_format() {
            // Header golden: magic at [0..4], major at [4], minor at [5].
            let header = SegmentHeader::current();
            let bytes = header.to_bytes();
            assert_eq!(&bytes[0..4], b"NDBS", "magic mismatch");
            assert_eq!(bytes[4], VERSION_MAJOR, "major version mismatch");
            assert_eq!(bytes[5], VERSION_MINOR, "minor version mismatch");
            assert_eq!(bytes[4], 1u8, "expected VERSION_MAJOR == 1");
            assert_eq!(bytes[5], 0u8, "expected VERSION_MINOR == 0");

            // Footer golden: serialize, then re-parse, asserting CRC consistency.
            let footer = SegmentFooter {
                schema_hash: 0xAB_CD_EF_01,
                column_count: 1,
                row_count: 128,
                profile_tag: 0,
                columns: vec![ColumnMeta {
                    name: "v".into(),
                    offset: 0,
                    length: 64,
                    codec: nodedb_codec::ResolvedColumnCodec::Lz4,
                    layout: BlockLayout::FixedWidth,
                    block_count: 1,
                    block_stats: vec![BlockStats::non_numeric(0, 128)],
                    dictionary: None,
                }],
            };
            let footer_bytes = footer.to_bytes().expect("serialize");
            // Layout: [msgpack_body][footer_len u32 LE][crc u32 LE]
            let n = footer_bytes.len();
            let stored_crc = u32::from_le_bytes([
                footer_bytes[n - 4],
                footer_bytes[n - 3],
                footer_bytes[n - 2],
                footer_bytes[n - 1],
            ]);
            let body_len = u32::from_le_bytes([
                footer_bytes[n - 8],
                footer_bytes[n - 7],
                footer_bytes[n - 6],
                footer_bytes[n - 5],
            ]) as usize;
            // CRC is computed over the msgpack body only (bytes 0..body_len).
            let recomputed = crc32c::crc32c(&footer_bytes[..body_len]);
            assert_eq!(stored_crc, recomputed, "footer CRC mismatch");

            // Round-trip via from_segment_tail.
            let mut segment = Vec::new();
            segment.extend_from_slice(&bytes);
            segment.extend_from_slice(&[0u8; 64]);
            segment.extend_from_slice(&footer_bytes);
            let parsed = SegmentFooter::from_segment_tail(&segment).expect("parse");
            assert_eq!(parsed.schema_hash, footer.schema_hash);
            assert_eq!(parsed.row_count, 128);
        }
    }
}
