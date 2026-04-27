//! Array CRDT acknowledgment wire message.

use serde::{Deserialize, Serialize};

/// Array ack message (client → server, 0x95).
///
/// Sent periodically by Lite peers to advance Origin's GC frontier.
/// `ack_hlc_bytes` is the highest HLC the Lite peer has durably applied,
/// using the same 18-byte layout as `nodedb_array::sync::Hlc::to_bytes()`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayAckMsg {
    /// Name of the array being acked.
    pub array: String,
    /// The acking replica's numeric ID.
    pub replica_id: u64,
    /// Highest durably applied HLC on this replica (18-byte layout).
    pub ack_hlc_bytes: [u8; 18],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArrayAckMsg {
            array: "test_array".into(),
            replica_id: 99,
            ack_hlc_bytes: [0xABu8; 18],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArrayAckMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }
}
