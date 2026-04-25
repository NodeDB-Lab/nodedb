//! Segment header — 32-byte fixed prefix at file offset 0.
//!
//! Layout (little-endian):
//!
//! ```text
//! ┌────────┬─────────┬───────┬─────────────┬──────────┐
//! │ magic  │ version │ flags │ schema_hash │  crc32c  │
//! │ 8 byte │  2 byte │ 2 byte│   8 byte    │  4 byte  │
//! └────────┴─────────┴───────┴─────────────┴──────────┘
//! ```
//! Total: 20 bytes covered by CRC + 4 byte CRC = 24 bytes.
//!
//! `schema_hash` is a fingerprint of the [`crate::schema::ArraySchema`]
//! the segment was written against; readers reject segments whose hash
//! doesn't match the live schema (no implicit migrations at Tier 2).

use crate::error::{ArrayError, ArrayResult};

/// `b"NDAS\0\0\0\1"` — NodeDB Array Segment, version slot in last byte.
pub const HEADER_MAGIC: [u8; 8] = *b"NDAS\0\0\0\x01";

/// On-disk format version. Bump on layout-incompatible changes.
pub const FORMAT_VERSION: u16 = 1;

pub const HEADER_SIZE: usize = 24;

/// Fixed-size segment header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub version: u16,
    pub flags: u16,
    pub schema_hash: u64,
}

impl SegmentHeader {
    pub fn new(schema_hash: u64) -> Self {
        Self {
            version: FORMAT_VERSION,
            flags: 0,
            schema_hash,
        }
    }

    /// Write the header to `out`, appending CRC32C of the preceding 28
    /// bytes. Returns the total bytes written (always [`HEADER_SIZE`]).
    pub fn encode_to(&self, out: &mut Vec<u8>) -> usize {
        let start = out.len();
        out.extend_from_slice(&HEADER_MAGIC);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.schema_hash.to_le_bytes());
        let crc = crc32c::crc32c(&out[start..start + 20]);
        out.extend_from_slice(&crc.to_le_bytes());
        HEADER_SIZE
    }

    pub fn decode(bytes: &[u8]) -> ArrayResult<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("segment header truncated: {} bytes", bytes.len()),
            });
        }
        if bytes[..8] != HEADER_MAGIC {
            return Err(ArrayError::SegmentCorruption {
                detail: "segment header magic mismatch (not NDAS)".into(),
            });
        }
        let mut u32_buf = [0u8; 4];
        u32_buf.copy_from_slice(&bytes[20..24]);
        let crc_stored = u32::from_le_bytes(u32_buf);
        let crc_calc = crc32c::crc32c(&bytes[..20]);
        if crc_stored != crc_calc {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "segment header CRC mismatch: stored={crc_stored:08x} \
                     calc={crc_calc:08x}"
                ),
            });
        }
        let mut u16_buf = [0u8; 2];
        u16_buf.copy_from_slice(&bytes[8..10]);
        let version = u16::from_le_bytes(u16_buf);
        u16_buf.copy_from_slice(&bytes[10..12]);
        let flags = u16::from_le_bytes(u16_buf);
        let mut u64_buf = [0u8; 8];
        u64_buf.copy_from_slice(&bytes[12..20]);
        let schema_hash = u64::from_le_bytes(u64_buf);
        if version != FORMAT_VERSION {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "unsupported segment version {version} (expected {FORMAT_VERSION})"
                ),
            });
        }
        Ok(Self {
            version,
            flags,
            schema_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let h = SegmentHeader::new(0xDEAD_BEEF_CAFE_BABE);
        let mut buf = Vec::new();
        let n = h.encode_to(&mut buf);
        assert_eq!(n, HEADER_SIZE);
        assert_eq!(buf.len(), HEADER_SIZE);
        let d = SegmentHeader::decode(&buf).unwrap();
        assert_eq!(d, h);
    }

    #[test]
    fn header_rejects_bad_magic() {
        let mut buf = vec![0u8; HEADER_SIZE];
        buf[0] = b'X';
        assert!(SegmentHeader::decode(&buf).is_err());
    }

    #[test]
    fn header_rejects_bad_crc() {
        let h = SegmentHeader::new(42);
        let mut buf = Vec::new();
        h.encode_to(&mut buf);
        buf[20] ^= 0xFF;
        assert!(SegmentHeader::decode(&buf).is_err());
    }

    #[test]
    fn header_rejects_truncated() {
        assert!(SegmentHeader::decode(&[0u8; 10]).is_err());
    }
}
