// SPDX-License-Identifier: Apache-2.0

//! Shape subscribe/snapshot/delta/unsubscribe + vector clock sync messages.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::sync::shape::ShapeDefinition;

/// Shape subscribe request (client → server, 0x20).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ShapeSubscribeMsg {
    /// Shape definition to subscribe to.
    pub shape: ShapeDefinition,
}

/// Shape snapshot response (server → client, 0x21).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ShapeSnapshotMsg {
    /// Shape ID this snapshot belongs to.
    pub shape_id: String,
    /// Initial dataset: serialized document rows matching the shape.
    pub data: Vec<u8>,
    /// LSN at snapshot time — deltas after this LSN will follow.
    pub snapshot_lsn: u64,
    /// Number of documents in the snapshot.
    pub doc_count: usize,
}

/// Collection replace (server → client, 0x25).
///
/// One snapshot of a collection that covers every shape the session holds
/// on it, sent after Origin dropped pushes for the collection. The client
/// replaces the collection's rows with it: a local row missing from `data`
/// is removed, unless a local write of it still waits to reach Origin.
///
/// A definition family (`_alert_rules`, `_retention_policies`,
/// `_schedules`) resyncs the same way, with every definition of the
/// session's scope and no shape.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct CollectionResyncMsg {
    /// The collection the snapshot replaces.
    pub collection: String,
    /// The held shapes the snapshot covers. Empty for a definition family.
    pub shape_ids: Vec<String>,
    /// Every row the shapes match: a MessagePack array of `{id, data}`
    /// envelopes, the shape snapshot's row form.
    pub data: Vec<u8>,
    /// The WAL position of the serving node when the read began. The client
    /// re-bases its shapes' LSN tracking at it. `0` for a definition family,
    /// which follows no LSN.
    pub snapshot_lsn: u64,
    /// Number of rows in `data`.
    pub doc_count: usize,
}

/// Shape delta message (server → client, 0x22).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ShapeDeltaMsg {
    /// Shape ID this delta applies to.
    pub shape_id: String,
    /// Collection affected.
    pub collection: String,
    /// Document ID affected.
    pub document_id: String,
    /// Operation type: "INSERT", "UPDATE", "DELETE".
    pub operation: String,
    /// Delta payload (CRDT delta bytes or document value).
    pub delta: Vec<u8>,
    /// WAL LSN of this mutation.
    pub lsn: u64,
}

/// Shape unsubscribe request (client → server, 0x23).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ShapeUnsubscribeMsg {
    pub shape_id: String,
}

/// Vector clock sync message (bidirectional, 0x30).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorClockSyncMsg {
    /// Per-collection clock: `{ collection: max_lsn }`.
    pub clocks: HashMap<String, u64>,
    /// Sender's node/peer ID.
    pub sender_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::shape::ShapeType;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn shape_subscribe_roundtrip() {
        let msg = ShapeSubscribeMsg {
            shape: ShapeDefinition {
                shape_id: "s1".into(),
                tenant_id: 1,
                shape_type: ShapeType::Vector {
                    collection: "embeddings".into(),
                    field_name: None,
                },
                description: "all embeddings".into(),
                field_filter: vec![],
            },
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::ShapeSubscribe, &msg).unwrap();
        let decoded: ShapeSubscribeMsg = SyncFrame::from_bytes(&frame.to_bytes())
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.shape.shape_id, "s1");
    }
}
