//! Authenticated frame envelope wrapping the existing RPC wire frame.
//!
//! # Layout
//!
//! ```text
//! ┌──────────┬──────────────┬────────┬───────────┬──────────────┬────────┐
//! │ env_ver  │ from_node_id │ seq    │ inner_len │ inner_frame  │ mac    │
//! │  1 byte  │   8 bytes    │ 8 B    │  4 bytes  │ inner_len B  │ 32 B   │
//! └──────────┴──────────────┴────────┴───────────┴──────────────┴────────┘
//! ```
//!
//! `inner_frame` is the legacy `header::write_frame` output (version +
//! rpc_type + payload_len + crc32c + payload) — left untouched so every
//! per-RPC-type encoder keeps working.
//!
//! The MAC covers every byte of the envelope *except* the MAC itself:
//! `[env_ver, from_node_id, seq, inner_len, inner_frame]`. Swapping any of
//! these fields invalidates the tag.
//!
//! # Bounds
//!
//! - Inner frame length is capped by the legacy
//!   [`header::MAX_RPC_PAYLOAD_SIZE`], which itself bounds the inner payload.
//! - Receiver must verify the MAC **before** trusting any declared field.
//!
//! [`header::MAX_RPC_PAYLOAD_SIZE`]: super::header::MAX_RPC_PAYLOAD_SIZE

use crate::error::{ClusterError, Result};

use super::header::MAX_RPC_PAYLOAD_SIZE;
use super::mac::{MAC_LEN, MacKey, compute_hmac, verify_hmac};

/// Envelope wire version. Bumped when the layout changes in a way that
/// cannot be negotiated on-the-fly (adding a field, moving MAC position).
pub const ENVELOPE_VERSION: u8 = 1;

/// Fixed bytes contributed by the envelope: version + from_node_id + seq +
/// inner_len + mac. Does not include the inner frame itself.
pub const ENVELOPE_OVERHEAD: usize = 1 + 8 + 8 + 4 + MAC_LEN;

/// Byte offsets within the envelope header (pre-inner-frame section).
const OFF_VERSION: usize = 0;
const OFF_FROM_NODE: usize = 1;
const OFF_SEQ: usize = 9;
const OFF_INNER_LEN: usize = 17;
const ENV_HEADER_LEN: usize = 21;

/// Metadata parsed from an envelope before MAC verification. `seq` and
/// `from_node_id` are **un-trusted** until [`parse_envelope`] has
/// returned `Ok`.
#[derive(Debug, Clone, Copy)]
pub struct EnvelopeFields {
    pub from_node_id: u64,
    pub seq: u64,
}

/// Wrap `inner_frame` in an authenticated envelope, appending to `out`.
///
/// MAC covers `[env_ver, from_node_id, seq, inner_len, inner_frame]`.
pub fn write_envelope(
    from_node_id: u64,
    seq: u64,
    inner_frame: &[u8],
    key: &MacKey,
    out: &mut Vec<u8>,
) -> Result<()> {
    let inner_len: u32 = inner_frame
        .len()
        .try_into()
        .map_err(|_| ClusterError::Codec {
            detail: format!("inner frame too large: {} bytes", inner_frame.len()),
        })?;
    if inner_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!(
                "inner frame length {inner_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"
            ),
        });
    }

    let start = out.len();
    out.reserve(ENVELOPE_OVERHEAD + inner_frame.len());
    out.push(ENVELOPE_VERSION);
    out.extend_from_slice(&from_node_id.to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&inner_len.to_le_bytes());
    out.extend_from_slice(inner_frame);
    let tag = compute_hmac(key, &out[start..]);
    out.extend_from_slice(&tag);
    Ok(())
}

/// Validate the envelope + MAC and return `(fields, inner_frame)`.
///
/// `data` must be the entire envelope — nothing before the version byte,
/// nothing after the MAC tag.
pub fn parse_envelope<'a>(data: &'a [u8], key: &MacKey) -> Result<(EnvelopeFields, &'a [u8])> {
    if data.len() < ENVELOPE_OVERHEAD {
        return Err(ClusterError::Codec {
            detail: format!(
                "envelope too short: {} bytes, need at least {ENVELOPE_OVERHEAD}",
                data.len()
            ),
        });
    }

    let version = data[OFF_VERSION];
    if version != ENVELOPE_VERSION {
        return Err(ClusterError::Codec {
            detail: format!("unsupported envelope version {version}, expected {ENVELOPE_VERSION}"),
        });
    }

    let from_node_id = u64::from_le_bytes(data[OFF_FROM_NODE..OFF_SEQ].try_into().unwrap());
    let seq = u64::from_le_bytes(data[OFF_SEQ..OFF_INNER_LEN].try_into().unwrap());
    let inner_len = u32::from_le_bytes(data[OFF_INNER_LEN..ENV_HEADER_LEN].try_into().unwrap());

    if inner_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!(
                "envelope inner length {inner_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"
            ),
        });
    }

    let inner_end = ENV_HEADER_LEN + inner_len as usize;
    let expected_total = inner_end + MAC_LEN;
    if data.len() != expected_total {
        return Err(ClusterError::Codec {
            detail: format!(
                "envelope length mismatch: got {} bytes, expected {expected_total}",
                data.len()
            ),
        });
    }

    let tag: &[u8; MAC_LEN] = data[inner_end..].try_into().unwrap();
    verify_hmac(key, &data[..inner_end], tag)?;

    let inner_frame = &data[ENV_HEADER_LEN..inner_end];
    Ok((EnvelopeFields { from_node_id, seq }, inner_frame))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_codec::header::{HEADER_SIZE, write_frame};

    fn sample_inner(rpc_type: u8) -> Vec<u8> {
        let mut out = Vec::new();
        write_frame(rpc_type, b"payload bytes", &mut out).unwrap();
        out
    }

    #[test]
    fn envelope_roundtrips() {
        let key = MacKey::from_bytes([3u8; MAC_LEN]);
        let inner = sample_inner(0x42);
        let mut buf = Vec::new();
        write_envelope(7, 12345, &inner, &key, &mut buf).unwrap();

        assert_eq!(buf.len(), ENVELOPE_OVERHEAD + inner.len());

        let (fields, parsed_inner) = parse_envelope(&buf, &key).unwrap();
        assert_eq!(fields.from_node_id, 7);
        assert_eq!(fields.seq, 12345);
        assert_eq!(parsed_inner, inner.as_slice());
        assert!(parsed_inner.len() >= HEADER_SIZE);
    }

    #[test]
    fn rejects_unknown_envelope_version() {
        let key = MacKey::from_bytes([3u8; MAC_LEN]);
        let inner = sample_inner(1);
        let mut buf = Vec::new();
        write_envelope(1, 1, &inner, &key, &mut buf).unwrap();
        buf[OFF_VERSION] = 99;
        let err = parse_envelope(&buf, &key).unwrap_err();
        assert!(err.to_string().contains("envelope version"));
    }

    #[test]
    fn rejects_tampered_from_node_id() {
        let key = MacKey::from_bytes([3u8; MAC_LEN]);
        let inner = sample_inner(1);
        let mut buf = Vec::new();
        write_envelope(7, 42, &inner, &key, &mut buf).unwrap();
        // Flip the low byte of from_node_id — original 7 becomes 6.
        buf[OFF_FROM_NODE] ^= 0x01;
        let err = parse_envelope(&buf, &key).unwrap_err();
        assert!(err.to_string().contains("MAC verification failed"));
    }

    #[test]
    fn rejects_tampered_seq() {
        let key = MacKey::from_bytes([3u8; MAC_LEN]);
        let inner = sample_inner(1);
        let mut buf = Vec::new();
        write_envelope(1, 100, &inner, &key, &mut buf).unwrap();
        buf[OFF_SEQ] ^= 0xFF;
        let err = parse_envelope(&buf, &key).unwrap_err();
        assert!(err.to_string().contains("MAC verification failed"));
    }

    #[test]
    fn rejects_tampered_inner() {
        let key = MacKey::from_bytes([3u8; MAC_LEN]);
        let inner = sample_inner(1);
        let mut buf = Vec::new();
        write_envelope(1, 1, &inner, &key, &mut buf).unwrap();
        // Flip a byte in the inner frame.
        buf[ENV_HEADER_LEN + HEADER_SIZE] ^= 0x80;
        let err = parse_envelope(&buf, &key).unwrap_err();
        assert!(err.to_string().contains("MAC verification failed"));
    }

    #[test]
    fn rejects_wrong_key() {
        let k1 = MacKey::from_bytes([1u8; MAC_LEN]);
        let k2 = MacKey::from_bytes([2u8; MAC_LEN]);
        let inner = sample_inner(1);
        let mut buf = Vec::new();
        write_envelope(1, 1, &inner, &k1, &mut buf).unwrap();
        let err = parse_envelope(&buf, &k2).unwrap_err();
        assert!(err.to_string().contains("MAC verification failed"));
    }

    #[test]
    fn rejects_truncated_envelope() {
        let key = MacKey::from_bytes([3u8; MAC_LEN]);
        let inner = sample_inner(1);
        let mut buf = Vec::new();
        write_envelope(1, 1, &inner, &key, &mut buf).unwrap();
        buf.truncate(buf.len() - 1);
        let err = parse_envelope(&buf, &key).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("envelope length mismatch") || msg.contains("envelope too short"));
    }
}
