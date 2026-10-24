// SPDX-License-Identifier: Apache-2.0

//! Spatial insert/delete sync messages (client → server / server → client).
//!
//! `SpatialInsertMsg` carries one geometry entry from a Lite client to
//! Origin for R-tree indexing. `SpatialDeleteMsg` removes a document's
//! geometry from Origin's R-tree.
//!
//! Wire opcodes:
//! - `0xAA` — `SpatialInsert`    (Lite → Origin)
//! - `0xAB` — `SpatialInsertAck` (Origin → Lite)
//! - `0xAC` — `SpatialDelete`    (Lite → Origin)
//! - `0xAD` — `SpatialDeleteAck` (Origin → Lite)

use serde::{Deserialize, Serialize};

use crate::sync::wire::ack_status::AckStatus;

/// Spatial insert request (Lite → Origin, 0xAA).
///
/// Requests that Origin index the geometry for `(collection, field, doc_id)` in
/// its per-field R-tree. Origin assigns a surrogate for `doc_id`, computes the
/// bounding box from the geometry, and inserts the entry into
/// `CoreLoop::spatial_indexes`.
///
/// `geometry_bytes` is a MessagePack-serialised
/// `nodedb_types::geometry::SridGeometry` produced via
/// `zerompk::to_msgpack_vec(&geometry)`: `[srid_code_u32, geometry, z | nil, m | nil]`.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct SpatialInsertMsg {
    /// Lite instance ID (for routing and dedup).
    pub lite_id: String,
    /// Target collection name.
    pub collection: String,
    /// Geometry field name within the collection.
    pub field: String,
    /// External document identifier.
    pub doc_id: String,
    /// MessagePack-serialised `SridGeometry` value.
    pub geometry_bytes: Vec<u8>,
    /// Monotonic batch ID (Lite-assigned, per-operation). Used for ACK correlation.
    pub batch_id: u64,
    /// Stable identity of the originating producer. 0 for legacy clients.
    #[serde(default)]
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    #[serde(default)]
    pub epoch: u64,
    /// Per-stream monotonic sequence number within the epoch.
    #[serde(default)]
    pub seq: u64,
}

/// Spatial insert acknowledgment (Origin → Lite, 0xAB).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct SpatialInsertAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Field acknowledged.
    pub field: String,
    /// Document ID from the originating `SpatialInsertMsg`.
    pub doc_id: String,
    /// Batch ID from the originating `SpatialInsertMsg`.
    pub batch_id: u64,
    /// `true` if the geometry was successfully indexed on Origin.
    pub accepted: bool,
    /// Rejection detail when `accepted == false`.
    #[serde(default)]
    pub reject_reason: Option<String>,
    /// Highest sequence number from this producer that has been durably applied.
    #[serde(default)]
    pub applied_seq: u64,
    /// Idempotency outcome of the acknowledged message.
    #[serde(default)]
    pub status: AckStatus,
}

/// Spatial delete request (Lite → Origin, 0xAC).
///
/// Removes the document identified by `doc_id` from Origin's R-tree index
/// for the given `(collection, field)`.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct SpatialDeleteMsg {
    /// Lite instance ID.
    pub lite_id: String,
    /// Target collection name.
    pub collection: String,
    /// Geometry field name within the collection.
    pub field: String,
    /// External document identifier to remove.
    pub doc_id: String,
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Stable identity of the originating producer. 0 for legacy clients.
    #[serde(default)]
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    #[serde(default)]
    pub epoch: u64,
    /// Per-stream monotonic sequence number within the epoch.
    #[serde(default)]
    pub seq: u64,
}

/// Spatial delete acknowledgment (Origin → Lite, 0xAD).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct SpatialDeleteAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Field acknowledged.
    pub field: String,
    /// Document ID from the originating `SpatialDeleteMsg`.
    pub doc_id: String,
    /// Batch ID from the originating `SpatialDeleteMsg`.
    pub batch_id: u64,
    /// `true` if the document was successfully removed from Origin's R-tree.
    pub accepted: bool,
    /// Rejection detail when `accepted == false`.
    #[serde(default)]
    pub reject_reason: Option<String>,
    /// Highest sequence number from this producer that has been durably applied.
    #[serde(default)]
    pub applied_seq: u64,
    /// Idempotency outcome of the acknowledged message.
    #[serde(default)]
    pub status: AckStatus,
}
