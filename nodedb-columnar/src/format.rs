//! Columnar segment binary format definitions.
//!
//! All multi-byte integers are little-endian. The segment is self-describing:
//! the footer contains all metadata needed to read any column without
//! scanning the entire file.

use serde::{Deserialize, Serialize};

/// Magic bytes identifying a NodeDB columnar segment.
pub const MAGIC: [u8; 4] = *b"NDBS";

/// Current format major version. Readers reject segments with higher major.
pub const VERSION_MAJOR: u8 = 1;

/// Current format minor version. Readers tolerate segments with same major
/// but higher minor (unknown footer fields are ignored).
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

    /// Parse a header from bytes. Returns None if magic/version is invalid.
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

        // Reject incompatible major version.
        if version_major > VERSION_MAJOR {
            return Err(crate::error::ColumnarError::IncompatibleVersion {
                reader_major: VERSION_MAJOR,
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

/// Per-block statistics for a single column. Enables predicate pushdown:
/// skip blocks where `WHERE price > 100` and block's `max_price < 100`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockStats {
    /// Minimum value in this block (encoded as f64 for uniformity;
    /// i64 values are cast losslessly, strings use 0.0).
    pub min: f64,
    /// Maximum value in this block.
    pub max: f64,
    /// Number of null values in this block.
    pub null_count: u32,
    /// Number of rows in this block (≤ BLOCK_SIZE, last block may be smaller).
    pub row_count: u32,
}

impl BlockStats {
    /// Create stats for a block with no meaningful min/max (e.g., string columns).
    pub fn non_numeric(null_count: u32, row_count: u32) -> Self {
        Self {
            min: f64::NAN,
            max: f64::NAN,
            null_count,
            row_count,
        }
    }
}

/// Metadata for a single column within the segment footer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// Column name (matches schema definition).
    pub name: String,
    /// Byte offset of this column's first block from the start of the segment
    /// (after the header).
    pub offset: u64,
    /// Total byte length of all blocks for this column.
    pub length: u64,
    /// Codec used for this column's blocks.
    pub codec: nodedb_codec::ColumnCodec,
    /// Number of blocks for this column.
    pub block_count: u32,
    /// Per-block statistics (one entry per block).
    pub block_stats: Vec<BlockStats>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        let footer_msgpack = rmp_serde::to_vec_named(self)
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
        let footer_len =
            u32::from_le_bytes(tail[0..4].try_into().expect("4 bytes from slice")) as usize;
        let stored_crc = u32::from_le_bytes(tail[4..8].try_into().expect("4 bytes from slice"));

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

        rmp_serde::from_slice(footer_bytes)
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
        bytes[4] = VERSION_MAJOR + 1; // Future major version.
        assert!(matches!(
            SegmentHeader::from_bytes(&bytes),
            Err(crate::error::ColumnarError::IncompatibleVersion { .. })
        ));
    }

    #[test]
    fn header_compatible_minor() {
        let mut bytes = SegmentHeader::current().to_bytes();
        bytes[5] = VERSION_MINOR + 5; // Future minor version, same major.
        let parsed = SegmentHeader::from_bytes(&bytes).expect("compatible minor");
        assert_eq!(parsed.version_major, VERSION_MAJOR);
        assert_eq!(parsed.version_minor, VERSION_MINOR + 5);
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
                    codec: nodedb_codec::ColumnCodec::DeltaFastLanesLz4,
                    block_count: 2,
                    block_stats: vec![
                        BlockStats {
                            min: 1.0,
                            max: 1024.0,
                            null_count: 0,
                            row_count: 1024,
                        },
                        BlockStats {
                            min: 1025.0,
                            max: 2048.0,
                            null_count: 0,
                            row_count: 1024,
                        },
                    ],
                },
                ColumnMeta {
                    name: "name".into(),
                    offset: 519,
                    length: 256,
                    codec: nodedb_codec::ColumnCodec::FsstLz4,
                    block_count: 2,
                    block_stats: vec![
                        BlockStats::non_numeric(0, 1024),
                        BlockStats::non_numeric(5, 1024),
                    ],
                },
                ColumnMeta {
                    name: "score".into(),
                    offset: 775,
                    length: 128,
                    codec: nodedb_codec::ColumnCodec::AlpFastLanesLz4,
                    block_count: 2,
                    block_stats: vec![
                        BlockStats {
                            min: 0.0,
                            max: 100.0,
                            null_count: 10,
                            row_count: 1024,
                        },
                        BlockStats {
                            min: 0.5,
                            max: 99.5,
                            null_count: 3,
                            row_count: 1024,
                        },
                    ],
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
        let stats = BlockStats {
            min: 10.0,
            max: 50.0,
            null_count: 0,
            row_count: 1024,
        };

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
}
