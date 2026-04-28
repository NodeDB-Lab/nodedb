//! Versioned wire envelope for cluster messages.
//!
//! # On-wire layout (v2 envelope)
//!
//! ```text
//! [0xc1] [version u16 BE] [inner_len u32 BE] [inner bytes]
//! ```
//!
//! `0xc1` is the MessagePack "reserved / never-used" marker — neither
//! `zerompk` nor the upstream `rmp` family emit it. Any byte slice
//! starting with `0xc1` is therefore guaranteed to be one of our
//! versioned envelopes and never a raw `zerompk`-encoded `T`.
//!
//! # Backward-compatibility (v1 fallback)
//!
//! Pre-versioning peers send raw `zerompk`-encoded `T` bytes (any byte
//! pattern *except* a leading `0xc1`). [`decode_versioned`] inspects the
//! first byte: if it equals `0xc1`, the payload is parsed as a v2
//! envelope; otherwise the entire slice is decoded as a raw `T` (the
//! v1 path).
//!
//! Because `0xc1` is reserved in MessagePack, this discriminator is
//! unambiguous — there is no encoding of any `T` for which the v1
//! decode path would shadow a real v2 envelope. This is the property
//! that makes the version gate trustworthy.
//!
//! An envelope with `version > WireVersion::CURRENT.0` is rejected
//! with [`WireVersionError::UnsupportedVersion`] and is *not* retried
//! as v1 — unknown future formats must never be silently misdecoded.

use super::error::WireVersionError;
use super::types::WireVersion;

/// MessagePack reserved marker. Never emitted by valid msgpack
/// encoders; used here as the unambiguous start byte of a v2 envelope.
const ENVELOPE_MARKER: u8 = 0xc1;

/// Length of the fixed envelope header (marker + version + inner_len).
const ENVELOPE_HEADER_LEN: usize = 1 + 2 + 4;

/// A versioned wrapper. Holds the version that was decoded alongside
/// the inner value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    pub version: WireVersion,
    pub inner: T,
}

/// Encode `value` into a v2 versioned envelope.
pub fn encode_versioned<T>(value: &T) -> Result<Vec<u8>, WireVersionError>
where
    T: zerompk::ToMessagePack,
{
    let inner_bytes = zerompk::to_msgpack_vec(value)
        .map_err(|e| WireVersionError::DecodeFailure(format!("encode inner: {e}")))?;

    encode_envelope(WireVersion::CURRENT.0, &inner_bytes)
}

/// Decode a versioned wire message.
///
/// **Fallback behaviour (v1 compat):** if the bytes do not begin with
/// the reserved [`ENVELOPE_MARKER`], the full byte slice is decoded as
/// a raw `T`. This preserves wire compat with pre-versioning peers.
///
/// An envelope with `version > WireVersion::CURRENT.0` is rejected
/// with [`WireVersionError::UnsupportedVersion`] and is NOT retried as
/// v1 — unknown future formats must never be silently accepted.
pub fn decode_versioned<T>(bytes: &[u8]) -> Result<T, WireVersionError>
where
    T: zerompk::FromMessagePackOwned,
{
    match parse_envelope(bytes)? {
        Some((version_raw, inner_bytes)) => {
            // Version 0 inside the envelope is malformed — reject loudly.
            if version_raw == 0 {
                return Err(WireVersionError::DecodeFailure(
                    "v2 envelope with version 0 is invalid".to_string(),
                ));
            }
            let peer_version = WireVersion(version_raw);
            if peer_version > WireVersion::CURRENT {
                return Err(WireVersionError::UnsupportedVersion {
                    peer_version,
                    supported_min: WireVersion::V1,
                    supported_max: WireVersion::CURRENT,
                });
            }
            zerompk::from_msgpack(inner_bytes).map_err(|e| {
                WireVersionError::DecodeFailure(format!("decode inner (v{peer_version}): {e}"))
            })
        }
        // No envelope marker → pre-versioning peer; decode as raw T.
        None => decode_raw_v1(bytes),
    }
}

/// Wrap arbitrary pre-encoded bytes in a v2 versioned envelope.
pub fn wrap_bytes_versioned(inner: &[u8]) -> Result<Vec<u8>, WireVersionError> {
    encode_envelope(WireVersion::CURRENT.0, inner)
}

/// Unwrap a versioned envelope and return the inner bytes, or fall
/// back to returning `bytes` as-is if it does not match the envelope
/// layout.
///
/// Same version-gate semantics as [`decode_versioned`]: future
/// versions (> CURRENT) are rejected; missing envelope falls back to
/// raw (v1).
pub fn unwrap_bytes_versioned(bytes: &[u8]) -> Result<&[u8], WireVersionError> {
    match parse_envelope(bytes)? {
        Some((version_raw, inner)) => {
            if version_raw == 0 {
                return Err(WireVersionError::DecodeFailure(
                    "v2 envelope with version 0 is invalid".to_string(),
                ));
            }
            let peer_version = WireVersion(version_raw);
            if peer_version > WireVersion::CURRENT {
                return Err(WireVersionError::UnsupportedVersion {
                    peer_version,
                    supported_min: WireVersion::V1,
                    supported_max: WireVersion::CURRENT,
                });
            }
            Ok(inner)
        }
        None => Ok(bytes),
    }
}

// ── Envelope encoding / parsing helpers ────────────────────────────────────

/// Encode a v2 envelope: `[ENVELOPE_MARKER][version u16 BE][inner_len u32 BE][inner]`.
fn encode_envelope(version: u16, inner: &[u8]) -> Result<Vec<u8>, WireVersionError> {
    if inner.len() > u32::MAX as usize {
        return Err(WireVersionError::DecodeFailure(format!(
            "inner payload {} bytes exceeds u32 length limit",
            inner.len()
        )));
    }
    let mut buf = Vec::with_capacity(ENVELOPE_HEADER_LEN + inner.len());
    buf.push(ENVELOPE_MARKER);
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&(inner.len() as u32).to_be_bytes());
    buf.extend_from_slice(inner);
    Ok(buf)
}

/// Parse a v2 envelope.
///
/// - `Ok(Some((version, inner)))`: well-formed envelope.
/// - `Ok(None)`: bytes do not begin with [`ENVELOPE_MARKER`] (treat as
///   v1 raw).
/// - `Err(...)`: bytes begin with [`ENVELOPE_MARKER`] but the header is
///   truncated or the declared length overruns the buffer. We reject
///   loudly rather than fall back to v1, because a corrupted v2
///   envelope must not be silently misinterpreted.
fn parse_envelope(bytes: &[u8]) -> Result<Option<(u16, &[u8])>, WireVersionError> {
    if bytes.is_empty() || bytes[0] != ENVELOPE_MARKER {
        return Ok(None);
    }
    if bytes.len() < ENVELOPE_HEADER_LEN {
        return Err(WireVersionError::DecodeFailure(format!(
            "v2 envelope truncated: header needs {} bytes, got {}",
            ENVELOPE_HEADER_LEN,
            bytes.len()
        )));
    }
    let version = u16::from_be_bytes([bytes[1], bytes[2]]);
    let inner_len = u32::from_be_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]) as usize;
    let inner_start = ENVELOPE_HEADER_LEN;
    let inner_end = inner_start.checked_add(inner_len).ok_or_else(|| {
        WireVersionError::DecodeFailure("v2 envelope inner length overflows usize".to_string())
    })?;
    if inner_end > bytes.len() {
        return Err(WireVersionError::DecodeFailure(format!(
            "v2 envelope truncated: declared inner_len={inner_len}, available={}",
            bytes.len() - inner_start
        )));
    }
    Ok(Some((version, &bytes[inner_start..inner_end])))
}

/// Decode bytes as a raw (v1, no envelope) `T`.
fn decode_raw_v1<T>(bytes: &[u8]) -> Result<T, WireVersionError>
where
    T: zerompk::FromMessagePackOwned,
{
    zerompk::from_msgpack(bytes)
        .map_err(|e| WireVersionError::DecodeFailure(format!("decode raw v1: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(
        Debug,
        Clone,
        PartialEq,
        Eq,
        serde::Serialize,
        serde::Deserialize,
        zerompk::ToMessagePack,
        zerompk::FromMessagePack,
    )]
    struct Payload {
        value: u32,
        label: String,
    }

    #[test]
    fn v2_roundtrip() {
        let orig = Payload {
            value: 42,
            label: "hello".to_string(),
        };
        let bytes = encode_versioned(&orig).unwrap();
        assert_eq!(bytes[0], ENVELOPE_MARKER);
        let decoded: Payload = decode_versioned(&bytes).unwrap();
        assert_eq!(orig, decoded);
    }

    #[test]
    fn v1_raw_fallback_decodes_correctly() {
        // Pre-versioning peer: encode as raw T (no envelope marker).
        let orig = Payload {
            value: 7,
            label: "raw".to_string(),
        };
        let raw_bytes = zerompk::to_msgpack_vec(&orig).unwrap();

        // Crucial invariant: zerompk never emits 0xc1 as the first byte.
        assert_ne!(raw_bytes[0], ENVELOPE_MARKER);

        let decoded: Payload = decode_versioned(&raw_bytes).unwrap();
        assert_eq!(orig, decoded);
    }

    #[test]
    fn unknown_future_version_returns_unsupported_version() {
        let fake_inner = zerompk::to_msgpack_vec(&Payload {
            value: 0,
            label: String::new(),
        })
        .unwrap();
        let bytes = encode_envelope(9999, &fake_inner).unwrap();

        let err = decode_versioned::<Payload>(&bytes).unwrap_err();
        match err {
            WireVersionError::UnsupportedVersion { peer_version, .. } => {
                assert_eq!(peer_version, WireVersion(9999));
            }
            other => panic!("expected UnsupportedVersion, got: {other}"),
        }
    }

    #[test]
    fn unknown_future_version_does_not_silently_succeed() {
        let inner = zerompk::to_msgpack_vec(&Payload {
            value: 1,
            label: "x".to_string(),
        })
        .unwrap();
        let bytes = encode_envelope(65535, &inner).unwrap();
        let err = decode_versioned::<Payload>(&bytes).unwrap_err();
        assert!(
            matches!(err, WireVersionError::UnsupportedVersion { .. }),
            "must error on future version, not silently succeed: {err}"
        );
    }

    #[test]
    fn truncated_envelope_header_is_loud_error() {
        // Marker present but header is truncated → must NOT silently fall
        // back to raw v1 (the bytes also can't decode as T, but the
        // error must explicitly name the truncated envelope).
        let bytes = vec![ENVELOPE_MARKER, 0x00, 0x02];
        let err = decode_versioned::<Payload>(&bytes).unwrap_err();
        match err {
            WireVersionError::DecodeFailure(msg) => assert!(msg.contains("truncated")),
            other => panic!("expected DecodeFailure(truncated), got {other}"),
        }
    }

    #[test]
    fn envelope_with_version_zero_is_loud_error() {
        let inner = zerompk::to_msgpack_vec(&Payload {
            value: 0,
            label: String::new(),
        })
        .unwrap();
        let bytes = encode_envelope(0, &inner).unwrap();
        let err = decode_versioned::<Payload>(&bytes).unwrap_err();
        assert!(
            matches!(err, WireVersionError::DecodeFailure(_)),
            "version=0 must be a loud error, got: {err}"
        );
    }

    #[test]
    fn raw_v1_bytes_have_distinguishable_first_byte() {
        // Document the property the design depends on: zerompk's encoder
        // never produces 0xc1 as the leading byte for any value of T.
        // If this test ever starts failing for a new T encoding, the
        // wire-format design must be revisited.
        for label in ["", "a", "longer string", "with: special chars"] {
            for value in [0u32, 1, u32::MAX] {
                let bytes = zerompk::to_msgpack_vec(&Payload {
                    value,
                    label: label.to_string(),
                })
                .unwrap();
                assert_ne!(
                    bytes[0], ENVELOPE_MARKER,
                    "Payload(value={value}, label={label:?}) leading byte 0xc1"
                );
            }
        }
    }
}
