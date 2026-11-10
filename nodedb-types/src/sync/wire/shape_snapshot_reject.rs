// SPDX-License-Identifier: Apache-2.0

//! Refusal of a shape snapshot.
//!
//! Origin answers an authorized `ShapeSubscribe` or `ResyncRequest` with a
//! `ShapeSnapshot` frame. When it cannot produce that snapshot, it answers a
//! [`ShapeSnapshotRejectMsg`] instead. The client then holds no baseline for
//! the shape, and Origin has registered no subscription for it.

use serde::{Deserialize, Serialize};

/// Why Origin produced no snapshot for a shape.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
pub enum ShapeSnapshotRefusal {
    /// A read policy refused the snapshot: the shape's collection carries a
    /// policy the snapshot read cannot apply, or its predicate filters on a
    /// redacted column. A re-subscribe fails the same way.
    PolicyRefused = 1,
    /// The snapshot query failed. A re-subscribe can succeed.
    QueryFailed = 2,
    /// The snapshot rows were not in the shape the read produces. A
    /// re-subscribe fails the same way until the stored rows change.
    MalformedRows = 3,
    /// The shape predicate does not decode. A re-subscribe with the same
    /// predicate fails the same way.
    InvalidPredicate = 4,
}

impl ShapeSnapshotRefusal {
    /// Whether the same subscription can succeed once the condition clears.
    pub fn is_retryable(self) -> bool {
        match self {
            Self::QueryFailed => true,
            Self::PolicyRefused | Self::MalformedRows | Self::InvalidPredicate => false,
        }
    }
}

/// Refusal of a shape snapshot (server → client, 0x24).
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
pub struct ShapeSnapshotRejectMsg {
    /// Shape ID the refused subscription or resync named.
    pub shape_id: String,
    /// Structured reason for the refusal.
    pub refusal: ShapeSnapshotRefusal,
    /// Human-readable detail for logs. Clients never parse it.
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn reject_roundtrips_through_a_frame() {
        let msg = ShapeSnapshotRejectMsg {
            shape_id: "s1".into(),
            refusal: ShapeSnapshotRefusal::MalformedRows,
            detail: "the scan payload does not split into rows".into(),
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::ShapeSnapshotReject, &msg)
            .expect("encode reject");
        let bytes = frame.to_bytes();
        assert_eq!(bytes[1], 0x24);
        let decoded: ShapeSnapshotRejectMsg = SyncFrame::from_bytes(&bytes)
            .expect("decode frame")
            .decode_body()
            .expect("decode reject");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn only_a_failed_query_is_retryable() {
        assert!(ShapeSnapshotRefusal::QueryFailed.is_retryable());
        assert!(!ShapeSnapshotRefusal::PolicyRefused.is_retryable());
        assert!(!ShapeSnapshotRefusal::MalformedRows.is_retryable());
        assert!(!ShapeSnapshotRefusal::InvalidPredicate.is_retryable());
    }
}
