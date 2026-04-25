//! Per-block framing: 4-byte length prefix + payload + 4-byte CRC32C.
//!
//! Used for every variable-length block in a segment (each tile
//! payload, the footer body, and the per-tile MBR table). CRC covers
//! the payload only — the length prefix is implicitly checked by the
//! length itself making sense (and by being inside the header-CRC'd
//! offset table).

use crate::error::{ArrayError, ArrayResult};

/// Bytes added to a payload by framing (4 length + 4 CRC).
pub const FRAMING_OVERHEAD: usize = 8;

pub struct BlockFraming;

impl BlockFraming {
    /// Append `[len_u32_le | payload | crc32c_u32_le]` to `out`. Returns
    /// total bytes written.
    pub fn encode(payload: &[u8], out: &mut Vec<u8>) -> usize {
        let len = payload.len() as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(payload);
        let crc = crc32c::crc32c(payload);
        out.extend_from_slice(&crc.to_le_bytes());
        FRAMING_OVERHEAD + payload.len()
    }

    /// Decode a framed block at the start of `bytes`. Returns
    /// `(payload_slice, total_bytes_consumed)`.
    pub fn decode(bytes: &[u8]) -> ArrayResult<(&[u8], usize)> {
        if bytes.len() < FRAMING_OVERHEAD {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("framed block truncated: {} bytes", bytes.len()),
            });
        }
        let len = u32::from_le_bytes(read_u32_le(bytes, 0)) as usize;
        let total = FRAMING_OVERHEAD + len;
        if bytes.len() < total {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "framed block claims len={len} but buffer has {}",
                    bytes.len() - 4
                ),
            });
        }
        let payload = &bytes[4..4 + len];
        let crc_stored = u32::from_le_bytes(read_u32_le(bytes, 4 + len));
        let crc_calc = crc32c::crc32c(payload);
        if crc_stored != crc_calc {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "framed block CRC mismatch: stored={crc_stored:08x} \
                     calc={crc_calc:08x}"
                ),
            });
        }
        Ok((payload, total))
    }
}

#[inline]
fn read_u32_le(bytes: &[u8], offset: usize) -> [u8; 4] {
    let mut out = [0u8; 4];
    out.copy_from_slice(&bytes[offset..offset + 4]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_round_trip() {
        let payload = b"hello world";
        let mut buf = Vec::new();
        let n = BlockFraming::encode(payload, &mut buf);
        assert_eq!(n, FRAMING_OVERHEAD + payload.len());
        let (got, total) = BlockFraming::decode(&buf).unwrap();
        assert_eq!(got, payload);
        assert_eq!(total, n);
    }

    #[test]
    fn framing_round_trip_empty() {
        let mut buf = Vec::new();
        BlockFraming::encode(&[], &mut buf);
        let (got, total) = BlockFraming::decode(&buf).unwrap();
        assert_eq!(got, &[] as &[u8]);
        assert_eq!(total, FRAMING_OVERHEAD);
    }

    #[test]
    fn framing_rejects_truncated_header() {
        assert!(BlockFraming::decode(&[0u8; 3]).is_err());
    }

    #[test]
    fn framing_rejects_short_payload() {
        let mut buf = Vec::new();
        BlockFraming::encode(b"hi", &mut buf);
        // Drop the trailing CRC and one payload byte
        let truncated = &buf[..buf.len() - 3];
        assert!(BlockFraming::decode(truncated).is_err());
    }

    #[test]
    fn framing_rejects_corrupt_payload() {
        let mut buf = Vec::new();
        BlockFraming::encode(b"data", &mut buf);
        // Corrupt the payload (offset 4 = first payload byte)
        buf[4] ^= 0xFF;
        assert!(BlockFraming::decode(&buf).is_err());
    }
}
