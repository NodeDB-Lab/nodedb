//! CRC32C integrity verification for persisted checkpoints.
//!
//! All large blobs persisted to redb (Loro snapshots, HNSW checkpoints,
//! CSR checkpoints) are wrapped in a checksummed envelope:
//!
//! ```text
//! [payload: N bytes][crc32c: 4 bytes LE]
//! ```
//!
//! On write: compute CRC32C of payload, append 4 bytes.
//! On read: split last 4 bytes, recompute CRC32C, compare.
//!
//! CRC32C is hardware-accelerated on x86_64 (SSE4.2) and aarch64,
//! making it negligible cost even for multi-MB checkpoints.

/// Minimum size: at least 1 byte payload + 4 bytes checksum.
const MIN_ENVELOPE_SIZE: usize = 5;

/// Wrap payload bytes with a trailing CRC32C checksum.
///
/// Returns `[payload || crc32c_le_bytes]`.
pub fn wrap(payload: &[u8]) -> Vec<u8> {
    let checksum = crc32c::crc32c(payload);
    let mut envelope = Vec::with_capacity(payload.len() + 4);
    envelope.extend_from_slice(payload);
    envelope.extend_from_slice(&checksum.to_le_bytes());
    envelope
}

/// Unwrap a checksummed envelope, verifying integrity.
///
/// Returns `Some(payload)` if the checksum matches, `None` if:
/// - The envelope is too short (< 5 bytes)
/// - The CRC32C does not match (corruption detected)
pub fn unwrap(envelope: &[u8]) -> Option<Vec<u8>> {
    if envelope.len() < MIN_ENVELOPE_SIZE {
        return None;
    }

    let split = envelope.len() - 4;
    let payload = &envelope[..split];
    let stored_checksum = u32::from_le_bytes(
        envelope[split..]
            .try_into()
            .expect("split guarantees 4 bytes"),
    );

    let computed = crc32c::crc32c(payload);
    if computed != stored_checksum {
        return None;
    }

    Some(payload.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_payload() {
        // Empty payload is valid (edge case: 0-byte checkpoint).
        let wrapped = wrap(b"");
        assert_eq!(wrapped.len(), 4); // Just the checksum.
        // But unwrap requires MIN_ENVELOPE_SIZE (5), so empty payload fails.
        assert!(unwrap(&wrapped).is_none());
    }

    #[test]
    fn roundtrip_small_payload() {
        let data = b"hello world";
        let wrapped = wrap(data);
        assert_eq!(wrapped.len(), data.len() + 4);

        let unwrapped = unwrap(&wrapped).expect("valid checksum");
        assert_eq!(unwrapped, data);
    }

    #[test]
    fn roundtrip_large_payload() {
        let data = vec![0xABu8; 1_000_000];
        let wrapped = wrap(&data);
        let unwrapped = unwrap(&wrapped).expect("valid checksum");
        assert_eq!(unwrapped, data);
    }

    #[test]
    fn detect_bit_flip() {
        let data = b"important checkpoint data";
        let mut wrapped = wrap(data);
        // Flip a bit in the payload.
        wrapped[5] ^= 0x01;
        assert!(unwrap(&wrapped).is_none(), "should detect corruption");
    }

    #[test]
    fn detect_truncation() {
        let data = b"checkpoint";
        let wrapped = wrap(data);
        // Truncate the last byte of the checksum.
        assert!(unwrap(&wrapped[..wrapped.len() - 1]).is_none());
    }

    #[test]
    fn detect_checksum_tamper() {
        let data = b"checkpoint";
        let mut wrapped = wrap(data);
        // Tamper with the checksum bytes.
        let len = wrapped.len();
        wrapped[len - 1] ^= 0xFF;
        assert!(unwrap(&wrapped).is_none());
    }

    #[test]
    fn too_short_returns_none() {
        assert!(unwrap(&[]).is_none());
        assert!(unwrap(&[1, 2, 3, 4]).is_none()); // 4 bytes = checksum only, no payload.
    }

    #[test]
    fn deterministic() {
        let data = b"same input";
        let w1 = wrap(data);
        let w2 = wrap(data);
        assert_eq!(w1, w2, "same input must produce same output");
    }
}
