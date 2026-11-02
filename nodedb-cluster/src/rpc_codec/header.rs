// SPDX-License-Identifier: BUSL-1.1

//! RPC frame header layout and framing helpers.
//!
//! # Current layout (18 bytes)
//!
//! ```text
//! ┌─────────┬──────────┬────────────┬──────────┬───────────────┬─────────────────────┐
//! │ version │ rpc_type │ payload_len│ crc32c   │ cluster_epoch │ rkyv payload bytes  │
//! │  1 byte │  1 byte  │  4 bytes   │ 4 bytes  │   8 bytes LE  │  payload_len bytes  │
//! └─────────┴──────────┴────────────┴──────────┴───────────────┴─────────────────────┘
//! ```
//!
//! Every frame is emitted with `RPC_FRAME_VERSION` in the version byte.
//! Frames with any other version byte are rejected immediately.

use crate::cluster_epoch::ClusterEpochState;
use crate::error::{ClusterError, Result};

/// Header size in bytes: version(1) + rpc_type(1) + payload_len(4) + crc32c(4) + cluster_epoch(8).
pub const HEADER_SIZE: usize = 18;

/// Wire-version byte stamped on every outbound frame and required on every inbound frame.
const RPC_FRAME_VERSION: u8 = 1;

/// Maximum RPC message payload size (64 MiB). Distinct from WAL's MAX_RPC_PAYLOAD_SIZE.
///
/// Prevents degenerate allocations from corrupt frames.
pub const MAX_RPC_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// Write a framed header + payload into `out`.
///
/// `rpc_type` is the discriminant byte; `payload` is the already-serialized
/// body.
///
/// The epoch field is left at zero here and filled in by [`stamp_epoch`] once,
/// at the point the message is encoded for a specific node. Per-message
/// encoders have no node to speak for, and threading one through all of them
/// to write a single fixed-offset field would put the same value in nineteen
/// places instead of one.
pub fn write_frame(rpc_type: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let payload_len: u32 = payload.len().try_into().map_err(|_| ClusterError::Codec {
        detail: format!("payload too large: {} bytes", payload.len()),
    })?;
    let crc = crc32c::crc32c(payload);
    out.push(RPC_FRAME_VERSION);
    out.push(rpc_type);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// Write the sender's APPLIED epoch into an already-framed message.
///
/// The applied epoch is the topology generation this node is actually
/// operating on. A node must never stamp an epoch it has merely overheard
/// from a peer: that would advertise a generation it has not reached, and
/// every receiver would take it at face value.
///
/// The epoch occupies a fixed 8-byte field in the header, so this rewrites it
/// in place rather than rebuilding the frame. The CRC covers the payload only,
/// so stamping does not invalidate it.
pub fn stamp_epoch(frame: &mut [u8], epoch: &ClusterEpochState) -> Result<()> {
    if frame.len() < HEADER_SIZE {
        return Err(ClusterError::Codec {
            detail: format!(
                "cannot stamp epoch: frame is {} bytes, need {HEADER_SIZE}",
                frame.len()
            ),
        });
    }
    frame[10..18].copy_from_slice(&epoch.applied().to_le_bytes());
    Ok(())
}

/// Validate the CRC32C of an inbound frame and return the payload slice.
///
/// `data` must start at byte 0 (version byte). Returns `(rpc_type, payload)`.
///
/// Side effect: records the peer's stamped epoch as an OBSERVATION. It raises
/// this node's observed mark only — a peer's stamp says where the peer is, and
/// nothing about which generation this node has applied.
pub fn parse_frame<'a>(data: &'a [u8], epoch: &ClusterEpochState) -> Result<(u8, &'a [u8])> {
    if data.is_empty() {
        return Err(ClusterError::Codec {
            detail: format!("frame too short: 0 bytes, need {HEADER_SIZE}"),
        });
    }

    let version = data[0];
    if version != RPC_FRAME_VERSION {
        return Err(ClusterError::UnsupportedWireVersion {
            got: version,
            supported_min: RPC_FRAME_VERSION,
            supported_max: RPC_FRAME_VERSION,
        });
    }

    if data.len() < HEADER_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("frame too short: {} bytes, need {HEADER_SIZE}", data.len()),
        });
    }

    let rpc_type = data[1];
    let payload_len = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
    let expected_crc = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);
    let peer_epoch = u64::from_le_bytes([
        data[10], data[11], data[12], data[13], data[14], data[15], data[16], data[17],
    ]);

    if payload_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"),
        });
    }

    let expected_total = HEADER_SIZE + payload_len as usize;
    if data.len() < expected_total {
        return Err(ClusterError::Codec {
            detail: format!(
                "frame truncated: got {} bytes, expected {expected_total}",
                data.len()
            ),
        });
    }

    let payload = &data[HEADER_SIZE..expected_total];
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != expected_crc {
        return Err(ClusterError::Codec {
            detail: format!(
                "CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
        });
    }

    if peer_epoch > 0 {
        epoch.observe(peer_epoch);
    }

    Ok((rpc_type, payload))
}

/// Return the total frame size for a buffer that starts with a valid header.
///
/// The buffer must be at least [`HEADER_SIZE`] bytes.
pub fn frame_size(header: &[u8; HEADER_SIZE]) -> Result<usize> {
    let payload_len = u32::from_le_bytes([header[2], header[3], header[4], header[5]]);
    if payload_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"),
        });
    }
    Ok(HEADER_SIZE + payload_len as usize)
}

// rkyv_deserialize and rkyv_serialize are macros in each sub-module because
// rkyv's generic bounds for Serialize and Deserialize are cumbersome to
// express generically across all types. Each sub-module calls rkyv directly.

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a frame with the given version byte and a 10-byte header with no epoch.
    /// Tests use it to check that a non-current version byte is rejected.
    fn make_short_frame(version: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + payload.len());
        let payload_len = payload.len() as u32;
        let crc = crc32c::crc32c(payload);
        out.push(version);
        out.push(0xAB); // arbitrary rpc_type
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Build a correct current-version frame (18-byte header).
    fn make_frame(payload: &[u8], epoch: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE + payload.len());
        let payload_len = payload.len() as u32;
        let crc = crc32c::crc32c(payload);
        out.push(RPC_FRAME_VERSION);
        out.push(0xAB);
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn parse_frame_accepts_current_version() {
        let payload = b"world";
        let frame = make_frame(payload, 0);
        let state = ClusterEpochState::default();
        let (_t, body) = parse_frame(&frame, &state).expect("current version must be accepted");
        assert_eq!(body, payload);
    }

    #[test]
    fn parse_frame_rejects_n_plus_one() {
        let payload = b"future";
        let frame = make_short_frame(RPC_FRAME_VERSION.saturating_add(1), payload);
        let err =
            parse_frame(&frame, &ClusterEpochState::default()).expect_err("N+1 must be rejected");
        match err {
            ClusterError::UnsupportedWireVersion {
                got,
                supported_min,
                supported_max,
            } => {
                assert_eq!(got, RPC_FRAME_VERSION.saturating_add(1));
                assert_eq!(supported_min, RPC_FRAME_VERSION);
                assert_eq!(supported_max, RPC_FRAME_VERSION);
            }
            other => panic!("expected UnsupportedWireVersion, got {other:?}"),
        }
    }

    #[test]
    fn parse_frame_rejects_version_zero() {
        let payload = b"zero";
        let frame = make_short_frame(0, payload);
        let err = parse_frame(&frame, &ClusterEpochState::default())
            .expect_err("version 0 must be rejected");
        assert!(matches!(
            err,
            ClusterError::UnsupportedWireVersion { got: 0, .. }
        ));
    }

    /// A frame carries the sender's applied generation, and the receiver
    /// records it as an observation about the sender.
    #[test]
    fn a_frame_carries_the_senders_applied_epoch() {
        let sender = ClusterEpochState::new(7);
        let payload = b"epoch-bound";
        let mut buf = Vec::new();
        write_frame(0xAB, payload, &mut buf).unwrap();
        stamp_epoch(&mut buf, &sender).unwrap();

        let receiver = ClusterEpochState::new(0);
        let (rpc_type, body) = parse_frame(&buf, &receiver).unwrap();
        assert_eq!(rpc_type, 0xAB);
        assert_eq!(body, payload);
        assert_eq!(receiver.observed(), 7, "the sender's stamp is observed");
        assert_eq!(
            receiver.applied(),
            0,
            "and it does not promote the receiver's own generation"
        );
        assert!(receiver.is_behind());
    }

    /// A node that has only overheard a newer epoch keeps stamping the one it
    /// applied. Advertising a generation it has not reached would make every
    /// receiver believe it.
    #[test]
    fn a_node_that_is_behind_still_stamps_what_it_applied() {
        let state = ClusterEpochState::new(2);
        state.observe(9);

        let mut buf = Vec::new();
        write_frame(0xAB, b"body", &mut buf).unwrap();
        stamp_epoch(&mut buf, &state).unwrap();

        let stamped = u64::from_le_bytes(buf[10..18].try_into().unwrap());
        assert_eq!(
            stamped, 2,
            "stamp the applied epoch, never the overheard one"
        );
    }

    /// Stamping rewrites a fixed header field and must leave the payload CRC
    /// intact.
    #[test]
    fn stamping_does_not_disturb_the_payload_check() {
        let mut buf = Vec::new();
        write_frame(0xAB, b"checked payload", &mut buf).unwrap();
        stamp_epoch(&mut buf, &ClusterEpochState::new(4)).unwrap();
        let receiver = ClusterEpochState::default();
        let (_t, body) = parse_frame(&buf, &receiver).expect("CRC must still verify");
        assert_eq!(body, b"checked payload");
    }

    #[test]
    fn stamping_a_runt_frame_is_an_error() {
        let mut too_short = vec![0u8; HEADER_SIZE - 1];
        assert!(stamp_epoch(&mut too_short, &ClusterEpochState::new(1)).is_err());
    }

    #[test]
    fn frame_size_current_version() {
        let frame = make_frame(b"abcde", 1);
        let mut hdr = [0u8; HEADER_SIZE];
        hdr.copy_from_slice(&frame[..HEADER_SIZE]);
        assert_eq!(frame_size(&hdr).unwrap(), HEADER_SIZE + 5);
    }
}
