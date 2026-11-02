// SPDX-License-Identifier: BUSL-1.1

//! Versioned wire envelope for cluster messages.
//!
//! # On-wire layout
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
//! Bytes not starting with `0xc1` are rejected. Raw frames without an
//! envelope are not accepted.
//!
//! An envelope with any version other than `WireVersion::CURRENT` is
//! rejected and is never silently misdecoded.

use super::error::WireVersionError;
use super::types::WireVersion;

/// MessagePack reserved marker. Never emitted by valid msgpack
/// encoders; used here as the unambiguous start byte of an envelope.
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

/// Encode `value` into a versioned envelope.
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
/// Bytes must begin with the reserved [`ENVELOPE_MARKER`] (`0xc1`).
/// Bytes without the marker are rejected. Raw frames are not accepted.
///
/// An envelope with version 0 is a [`WireVersionError::DecodeFailure`]. Any
/// other version than `WireVersion::CURRENT` is a
/// [`WireVersionError::UnsupportedVersion`].
pub fn decode_versioned<T>(bytes: &[u8]) -> Result<T, WireVersionError>
where
    T: zerompk::FromMessagePackOwned,
{
    let (peer_version, inner_bytes) = parse_current_envelope(bytes)?;
    zerompk::from_msgpack(inner_bytes)
        .map_err(|e| WireVersionError::DecodeFailure(format!("decode inner ({peer_version}): {e}")))
}

/// Wrap arbitrary pre-encoded bytes in a versioned envelope.
pub fn wrap_bytes_versioned(inner: &[u8]) -> Result<Vec<u8>, WireVersionError> {
    encode_envelope(WireVersion::CURRENT.0, inner)
}

/// Unwrap a versioned envelope and return the inner bytes.
///
/// Bytes must begin with the reserved [`ENVELOPE_MARKER`] (`0xc1`).
/// Missing marker, version 0, or any version other than `CURRENT` are all
/// errors.
pub fn unwrap_bytes_versioned(bytes: &[u8]) -> Result<&[u8], WireVersionError> {
    parse_current_envelope(bytes).map(|(_, inner)| inner)
}

// ── Envelope encoding / parsing helpers ────────────────────────────────────

/// Parse an envelope and refuse every version other than `WireVersion::CURRENT`.
fn parse_current_envelope(bytes: &[u8]) -> Result<(WireVersion, &[u8]), WireVersionError> {
    let Some((version_raw, inner)) = parse_envelope(bytes)? else {
        return Err(WireVersionError::DecodeFailure(
            "missing envelope marker: raw frames are not accepted".to_string(),
        ));
    };
    if version_raw == 0 {
        return Err(WireVersionError::DecodeFailure(
            "envelope with version 0 is invalid".to_string(),
        ));
    }
    let peer_version = WireVersion(version_raw);
    if peer_version != WireVersion::CURRENT {
        return Err(WireVersionError::UnsupportedVersion {
            peer_version,
            supported_min: WireVersion::CURRENT,
            supported_max: WireVersion::CURRENT,
        });
    }
    Ok((peer_version, inner))
}

/// Encode an envelope: `[ENVELOPE_MARKER][version u16 BE][inner_len u32 BE][inner]`.
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

/// Parse an envelope.
///
/// - `Ok(Some((version, inner)))`: well-formed envelope.
/// - `Ok(None)`: bytes do not begin with [`ENVELOPE_MARKER`].
/// - `Err(...)`: bytes begin with [`ENVELOPE_MARKER`] but the header is
///   truncated or the declared length overruns the buffer. A corrupted
///   envelope must not be silently misinterpreted.
fn parse_envelope(bytes: &[u8]) -> Result<Option<(u16, &[u8])>, WireVersionError> {
    if bytes.is_empty() || bytes[0] != ENVELOPE_MARKER {
        return Ok(None);
    }
    if bytes.len() < ENVELOPE_HEADER_LEN {
        return Err(WireVersionError::DecodeFailure(format!(
            "envelope truncated: header needs {} bytes, got {}",
            ENVELOPE_HEADER_LEN,
            bytes.len()
        )));
    }
    let version = u16::from_be_bytes([bytes[1], bytes[2]]);
    let inner_len = u32::from_be_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]) as usize;
    let inner_start = ENVELOPE_HEADER_LEN;
    let inner_end = inner_start.checked_add(inner_len).ok_or_else(|| {
        WireVersionError::DecodeFailure("envelope inner length overflows usize".to_string())
    })?;
    if inner_end > bytes.len() {
        return Err(WireVersionError::DecodeFailure(format!(
            "envelope truncated: declared inner_len={inner_len}, available={}",
            bytes.len() - inner_start
        )));
    }
    Ok(Some((version, &bytes[inner_start..inner_end])))
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
    fn envelope_roundtrip() {
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
    fn decode_versioned_rejects_raw_no_marker() {
        let orig = Payload {
            value: 7,
            label: "raw".to_string(),
        };
        let raw_bytes = zerompk::to_msgpack_vec(&orig).unwrap();
        assert_ne!(raw_bytes[0], ENVELOPE_MARKER);

        let err = decode_versioned::<Payload>(&raw_bytes).unwrap_err();
        match err {
            WireVersionError::DecodeFailure(msg) => {
                assert!(
                    msg.contains("missing envelope marker"),
                    "unexpected message: {msg}"
                );
            }
            other => panic!("expected DecodeFailure, got: {other}"),
        }
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
        // Marker present but header is truncated. The error must name the
        // truncated envelope.
        let bytes = vec![ENVELOPE_MARKER, 0x00, 0x02];
        let err = decode_versioned::<Payload>(&bytes).unwrap_err();
        match err {
            WireVersionError::DecodeFailure(msg) => assert!(msg.contains("truncated")),
            other => panic!("expected DecodeFailure(truncated), got {other}"),
        }
    }

    #[test]
    fn next_version_is_unsupported_on_unwrap() {
        let bytes = encode_envelope(WireVersion::CURRENT.0 + 1, b"inner").unwrap();
        let err = unwrap_bytes_versioned(&bytes).unwrap_err();
        assert!(
            matches!(err, WireVersionError::UnsupportedVersion { .. }),
            "must reject a version other than CURRENT, got: {err}"
        );
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
}
