//! Segment footer — trailing metadata block.
//!
//! Trailer (last 16 bytes of the file):
//!
//! ```text
//! ┌─────────────┬───────────────┬────────┐
//! │ footer_off  │ footer_length │ magic  │
//! │   8 byte    │     4 byte    │ 4 byte │
//! └─────────────┴───────────────┴────────┘
//! ```
//!
//! The footer body sits at `footer_off..footer_off+footer_length` and
//! is a single framed block (length + payload + CRC). The payload is
//! a zerompk-encoded [`SegmentFooter`].

use serde::{Deserialize, Serialize};

use super::framing::{BlockFraming, FRAMING_OVERHEAD};
use super::tile_entry::TileEntry;
use crate::error::{ArrayError, ArrayResult};

/// `b"NDFT"` — NodeDB Footer Trailer.
pub const FOOTER_MAGIC: [u8; 4] = *b"NDFT";

pub const TRAILER_SIZE: usize = 16;

/// Decoded segment footer body.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SegmentFooter {
    pub schema_hash: u64,
    pub tiles: Vec<TileEntry>,
}

impl SegmentFooter {
    pub fn new(schema_hash: u64, tiles: Vec<TileEntry>) -> Self {
        Self { schema_hash, tiles }
    }

    /// Encode footer body + trailer into `out`. Returns `(footer_off,
    /// footer_length)` so the writer can record them.
    pub fn encode_to(&self, out: &mut Vec<u8>) -> ArrayResult<(u64, u32)> {
        let body = zerompk::to_msgpack_vec(self).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("footer encode failed: {e}"),
        })?;
        let footer_off = out.len() as u64;
        let framed_len = BlockFraming::encode(&body, out);
        // Trailer
        out.extend_from_slice(&footer_off.to_le_bytes());
        out.extend_from_slice(&(framed_len as u32).to_le_bytes());
        out.extend_from_slice(&FOOTER_MAGIC);
        Ok((footer_off, framed_len as u32))
    }

    /// Decode footer from a complete segment byte slice.
    pub fn decode(segment: &[u8]) -> ArrayResult<Self> {
        if segment.len() < TRAILER_SIZE + FRAMING_OVERHEAD {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("segment too small for footer: {}", segment.len()),
            });
        }
        let trailer = &segment[segment.len() - TRAILER_SIZE..];
        if trailer[12..16] != FOOTER_MAGIC {
            return Err(ArrayError::SegmentCorruption {
                detail: "footer trailer magic mismatch (not NDFT)".into(),
            });
        }
        let mut u64_buf = [0u8; 8];
        u64_buf.copy_from_slice(&trailer[..8]);
        let footer_off = u64::from_le_bytes(u64_buf) as usize;
        let mut u32_buf = [0u8; 4];
        u32_buf.copy_from_slice(&trailer[8..12]);
        let footer_len = u32::from_le_bytes(u32_buf) as usize;
        if footer_off + footer_len > segment.len() - TRAILER_SIZE {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "footer offset/length out of bounds: off={footer_off} len={footer_len} \
                     file_size={}",
                    segment.len()
                ),
            });
        }
        let (body, _) = BlockFraming::decode(&segment[footer_off..footer_off + footer_len])?;
        zerompk::from_msgpack(body).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("footer body decode failed: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::format::tile_entry::TileKind;
    use crate::tile::mbr::TileMBR;
    use crate::types::TileId;

    #[test]
    fn footer_round_trip() {
        let f = SegmentFooter::new(
            0xABCDEF,
            vec![TileEntry::new(
                TileId::snapshot(7),
                TileKind::Sparse,
                32,
                64,
                TileMBR::new(0, 0),
            )],
        );
        // Simulate a segment: junk bytes for tile region, then footer.
        let mut buf = vec![0u8; 32];
        buf.extend_from_slice(&[0u8; 64]);
        f.encode_to(&mut buf).unwrap();
        let d = SegmentFooter::decode(&buf).unwrap();
        assert_eq!(d, f);
    }

    #[test]
    fn footer_rejects_bad_trailer_magic() {
        let f = SegmentFooter::new(0, vec![]);
        let mut buf = Vec::new();
        f.encode_to(&mut buf).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert!(SegmentFooter::decode(&buf).is_err());
    }
}
