//! Versioned encode/decode wrappers for [`nodedb_types::SyncFrame`].
//!
//! `SyncFrame` lives in `nodedb-types` and cannot reference `nodedb-cluster`,
//! so versioning wrappers for the sync wire layer are provided here instead.
//!
//! # Wire compat
//!
//! Raw (v1) `SyncFrame` bytes — `[msg_type: 1B][length: 4B LE][body: N bytes]`
//! — do not start with a MessagePack fixarray marker (`0x92`) and will
//! therefore fall through to the v1 fallback path in `decode_versioned_sync_frame`.
//!
//! # PhysicalPlan bridge wire exclusion
//!
//! The `bridge::physical_plan::wire` codec is NOT versioned here. It operates
//! on the Control⟷Data SPSC boundary (within a single node, never across the
//! network) and is already round-trip tested. Cross-cluster boundary versioning
//! does not apply to it.

use nodedb_types::SyncFrame;

use crate::error::{ClusterError, Result};
use crate::wire_version::{unwrap_bytes_versioned, wrap_bytes_versioned};

/// Encode a [`SyncFrame`] and wrap its bytes in a v2 versioned envelope.
///
/// The inner bytes are `SyncFrame::to_bytes()` output. The outer envelope
/// lets the receiving peer detect and reject incompatible future versions
/// rather than silently misinterpreting the frame.
pub fn encode_versioned_sync_frame(frame: &SyncFrame) -> Result<Vec<u8>> {
    let raw = frame.to_bytes();
    wrap_bytes_versioned(&raw).map_err(|e| ClusterError::Codec {
        detail: format!("SyncFrame versioned encode: {e}"),
    })
}

/// Decode a versioned-envelope-wrapped [`SyncFrame`].
///
/// Accepts both v2 versioned envelopes and raw v1 (pre-versioning) `SyncFrame`
/// bytes; rejects envelopes with unsupported future version numbers.
///
/// The v1 fallback preserves wire compat with NodeDB-Lite and Origin peers
/// running pre-versioning sync code.
pub fn decode_versioned_sync_frame(data: &[u8]) -> Result<SyncFrame> {
    let inner = unwrap_bytes_versioned(data).map_err(|e| ClusterError::Codec {
        detail: format!("SyncFrame versioned decode: {e}"),
    })?;
    SyncFrame::from_bytes(inner).ok_or_else(|| ClusterError::Codec {
        detail: "SyncFrame::from_bytes failed: malformed or truncated frame".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{SyncFrame, SyncMessageType};

    // A minimal zerompk-serializable struct to fill a SyncFrame body.
    #[derive(
        serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
    )]
    struct PingMsg {
        seq: u64,
    }

    fn make_ping_frame() -> SyncFrame {
        let msg = PingMsg { seq: 1 };
        SyncFrame::new_msgpack(SyncMessageType::PingPong, &msg).unwrap()
    }

    #[test]
    fn sync_frame_versioned_roundtrip() {
        let frame = make_ping_frame();
        let encoded = encode_versioned_sync_frame(&frame).unwrap();
        let decoded = decode_versioned_sync_frame(&encoded).unwrap();
        assert_eq!(frame.msg_type, decoded.msg_type);
        assert_eq!(frame.body, decoded.body);
    }

    #[test]
    fn sync_frame_v1_raw_fallback() {
        // Simulate a pre-versioning peer: raw SyncFrame bytes (no outer envelope).
        let frame = make_ping_frame();
        let raw = frame.to_bytes();
        let decoded = decode_versioned_sync_frame(&raw).unwrap();
        assert_eq!(frame.msg_type, decoded.msg_type);
        assert_eq!(frame.body, decoded.body);
    }
}
