// SPDX-License-Identifier: Apache-2.0

//! KV row push messages, and the refusal of a row push.
//!
//! `KvPushMsg` carries one KV write from a Lite client to Origin. A put
//! carries the `{key, value…}` row every KV read returns, the same shape
//! Origin sends in a `RowPushMsg`. Origin answers each push with a
//! `KvPushAckMsg`. A terminal refusal travels in that ack as
//! `AckStatus::Rejected`, the way every engine push ack carries one.
//!
//! `RowPushRejectMsg` is Lite's refusal of an Origin `RowPushMsg` it could
//! not apply.
//!
//! Wire opcodes:
//! - `0x16` — `RowPushReject` (Lite → Origin)
//! - `0xAE` — `KvPush`        (Lite → Origin)
//! - `0xAF` — `KvPushAck`     (Origin → Lite)

use serde::{Deserialize, Serialize};

use crate::sync::wire::ack_status::AckStatus;

/// The write a `KvPushMsg` carries.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvPushOp {
    /// Store `row` at the entry's key.
    Put {
        /// The `{key, value…}` row as standard MessagePack.
        row: Vec<u8>,
        /// Absolute expiry in milliseconds since the Unix epoch. `0` means
        /// the entry never expires.
        expire_at_ms: u64,
    },
    /// Remove the entry's key.
    Delete,
}

/// KV write push (Lite → Origin, 0xAE).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct KvPushMsg {
    /// Lite instance ID.
    pub lite_id: String,
    /// Target KV collection.
    pub collection: String,
    /// The entry's key bytes.
    pub key: Vec<u8>,
    /// The write.
    pub op: KvPushOp,
    /// Lite-assigned ID for ACK correlation.
    pub batch_id: u64,
    /// Stable identity of the originating producer.
    pub producer_id: u64,
    /// Producer epoch.
    pub epoch: u64,
    /// Per-stream monotonic sequence number within the epoch.
    pub seq: u64,
}

/// KV write push acknowledgment (Origin → Lite, 0xAF).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct KvPushAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Key from the originating `KvPushMsg`.
    pub key: Vec<u8>,
    /// Batch ID from the originating `KvPushMsg`.
    pub batch_id: u64,
    /// `true` unless `status` is `AckStatus::Rejected`.
    pub accepted: bool,
    /// Refusal detail when `status` is `AckStatus::Rejected`.
    pub reject_reason: Option<String>,
    /// Highest sequence from this producer's stream that Origin applied.
    pub applied_seq: u64,
    /// Idempotency outcome of the push.
    pub status: AckStatus,
}

/// Why Lite refused an Origin row push.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RowPushRefusal {
    /// The payload is not the row shape the collection's engine expects.
    Malformed { detail: String },
    /// The payload decoded, and the local write failed.
    ApplyFailed { detail: String },
}

impl std::fmt::Display for RowPushRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed { detail } => write!(f, "malformed row push: {detail}"),
            Self::ApplyFailed { detail } => write!(f, "row push apply failed: {detail}"),
        }
    }
}

/// Lite's refusal of an Origin `RowPushMsg` (Lite → Origin, 0x16).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct RowPushRejectMsg {
    /// Collection of the refused row.
    pub collection: String,
    /// Document ID of the refused row.
    pub document_id: String,
    /// `sequence` of the refused `RowPushMsg`.
    pub sequence: u64,
    /// `peer_id` of the refused `RowPushMsg`.
    pub peer_id: u64,
    /// Why Lite refused the row.
    pub refusal: RowPushRefusal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_put_push_round_trips() {
        let msg = KvPushMsg {
            lite_id: "lite-1".into(),
            collection: "cfg".into(),
            key: b"k1".to_vec(),
            op: KvPushOp::Put {
                row: vec![0x81, 0xa1, b'k', 0x01],
                expire_at_ms: 42,
            },
            batch_id: 7,
            producer_id: 3,
            epoch: 1,
            seq: 9,
        };
        let bytes = zerompk::to_msgpack_vec(&msg).expect("encode");
        let back: KvPushMsg = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back.op, msg.op);
        assert_eq!(back.key, msg.key);
        assert_eq!(back.seq, 9);
    }

    #[test]
    fn a_row_push_reject_round_trips() {
        let msg = RowPushRejectMsg {
            collection: "cfg".into(),
            document_id: "k1".into(),
            sequence: 4,
            peer_id: 2,
            refusal: RowPushRefusal::Malformed {
                detail: "not a row map".into(),
            },
        };
        let bytes = zerompk::to_msgpack_vec(&msg).expect("encode");
        let back: RowPushRejectMsg = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back.refusal, msg.refusal);
        assert_eq!(back.sequence, 4);
    }
}
