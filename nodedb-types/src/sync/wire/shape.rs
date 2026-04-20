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
