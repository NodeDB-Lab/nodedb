// SPDX-License-Identifier: Apache-2.0

//! Refusal of a client `CollectionSchema` announce.
//!
//! Origin answers every announce. A held collection answers a
//! `CollectionSchema` frame with its descriptor and incarnation. Every other
//! outcome answers a [`CollectionSchemaRejectMsg`].

use serde::{Deserialize, Serialize};

/// Why Origin refused a `CollectionSchema` announce.
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
pub enum CollectionSchemaRefusal {
    /// The session holds no authenticated identity.
    Unauthenticated = 1,
    /// The descriptor names a tenant or database the session cannot see.
    NotVisible = 2,
    /// The session lacks read permission on the existing collection, or
    /// create permission on the database for an absent one.
    PermissionDenied = 3,
    /// Origin holds no active collection under the announced name.
    NotFound = 4,
    /// The cluster gave the announce no verdict. A re-send is safe.
    Unavailable = 5,
    /// Origin refused to commit the collection. A re-send fails the same way.
    Failed = 6,
}

impl CollectionSchemaRefusal {
    /// Whether the same announce can succeed once the condition clears.
    pub fn is_retryable(self) -> bool {
        match self {
            Self::Unavailable => true,
            Self::Unauthenticated
            | Self::NotVisible
            | Self::PermissionDenied
            | Self::NotFound
            | Self::Failed => false,
        }
    }
}

/// Refusal of a `CollectionSchema` announce (server → client, 0x17).
///
/// The client learns no incarnation for `name` from this announce. Its deltas
/// for `name` carry no valid incarnation until Origin announces one.
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
pub struct CollectionSchemaRejectMsg {
    /// Collection name the refused announce carried.
    pub name: String,
    /// Structured reason for the refusal.
    pub refusal: CollectionSchemaRefusal,
    /// Human-readable detail for logs. Clients never parse it.
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn reject_roundtrips_through_a_frame() {
        let msg = CollectionSchemaRejectMsg {
            name: "orders".into(),
            refusal: CollectionSchemaRefusal::NotFound,
            detail: "no active collection".into(),
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::CollectionSchemaReject, &msg)
            .expect("encode reject");
        let bytes = frame.to_bytes();
        assert_eq!(bytes[1], 0x17);
        let decoded: CollectionSchemaRejectMsg = SyncFrame::from_bytes(&bytes)
            .expect("decode frame")
            .decode_body()
            .expect("decode reject");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn only_an_unavailable_cluster_is_retryable() {
        assert!(CollectionSchemaRefusal::Unavailable.is_retryable());
        for refusal in [
            CollectionSchemaRefusal::Unauthenticated,
            CollectionSchemaRefusal::NotVisible,
            CollectionSchemaRefusal::PermissionDenied,
            CollectionSchemaRefusal::NotFound,
            CollectionSchemaRefusal::Failed,
        ] {
            assert!(!refusal.is_retryable(), "{refusal:?}");
        }
    }
}
