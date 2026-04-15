//! RPC frame header layout and framing helpers.
//!
//! Wire layout (10-byte header + payload):
//!
//! ```text
//! ┌─────────┬──────────┬────────────┬──────────┬─────────────────────┐
//! │ version │ rpc_type │ payload_len│ crc32c   │ rkyv payload bytes  │
//! │  1 byte │  1 byte  │  4 bytes   │ 4 bytes  │  payload_len bytes  │
//! └─────────┴──────────┴────────────┴──────────┴─────────────────────┘
//! ```

use crate::error::{ClusterError, Result};
use crate::wire::WIRE_VERSION;

/// Header size in bytes: version(1) + rpc_type(1) + payload_len(4) + crc32c(4).
pub const HEADER_SIZE: usize = 10;

/// Maximum RPC message payload size (64 MiB). Distinct from WAL's MAX_RPC_PAYLOAD_SIZE.
///
/// Prevents degenerate allocations from corrupt frames.
pub const MAX_RPC_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// Write a framed header + payload into `out`.
///
/// `rpc_type` is the discriminant byte; `payload` is the already-serialized body.
pub fn write_frame(rpc_type: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let payload_len: u32 = payload.len().try_into().map_err(|_| ClusterError::Codec {
        detail: format!("payload too large: {} bytes", payload.len()),
    })?;
    let crc = crc32c::crc32c(payload);
    // Version field is 1 byte on the wire; narrowing cast is intentional.
    out.push(WIRE_VERSION as u8);
    out.push(rpc_type);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// Validate the CRC32C of an inbound frame and return the payload slice.
///
/// `data` must start at byte 0 (version byte). Returns `(rpc_type, payload)`.
pub fn parse_frame(data: &[u8]) -> Result<(u8, &[u8])> {
    if data.len() < HEADER_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("frame too short: {} bytes, need {HEADER_SIZE}", data.len()),
        });
    }

    let version = data[0];
    if version != WIRE_VERSION as u8 {
        return Err(ClusterError::Codec {
            detail: format!("unsupported wire version: {version}, expected {WIRE_VERSION}"),
        });
    }

    let rpc_type = data[1];
    let payload_len = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
    let expected_crc = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);

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

    Ok((rpc_type, payload))
}

/// Return the total frame size for a buffer that starts with a valid header.
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
