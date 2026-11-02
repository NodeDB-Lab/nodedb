// SPDX-License-Identifier: Apache-2.0

//! Vector insert/delete sync messages (client → server / server → client).
//!
//! `VectorInsertMsg` carries one HNSW vector from a Lite client to Origin.
//! `VectorDeleteMsg` carries a tombstone by external document ID.
//!
//! Wire opcodes:
//! - `0xA2` — `VectorInsert`    (Lite → Origin)
//! - `0xA3` — `VectorInsertAck` (Origin → Lite)
//! - `0xA4` — `VectorDelete`    (Lite → Origin)
//! - `0xA5` — `VectorDeleteAck` (Origin → Lite)

use serde::{Deserialize, Serialize};

use crate::sync::wire::ack_status::AckStatus;

/// Vector insert (Lite → Origin, 0xA2).
///
/// Inserts one vector into Origin's HNSW index for `collection`. The
/// `id` field is the external document identifier (the same string used
/// on the Lite call-site). Origin allocates a surrogate from its
/// `SurrogateAssigner` keyed on `(collection, id)` so cross-engine
/// identity is preserved.
///
/// `dim` must match the index dimension; Origin rejects mismatches.
/// `field_name` is the named vector field; empty string selects the
/// default (unnamed) field.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorInsertMsg {
    /// Lite instance ID (for routing and dedup).
    pub lite_id: String,
    /// Target collection name.
    pub collection: String,
    /// External document identifier.
    pub id: String,
    /// Raw FP32 vector components (length == `dim`).
    pub vector: Vec<f32>,
    /// Vector dimensionality.
    pub dim: usize,
    /// Named vector field; empty string = default field.
    #[serde(default)]
    pub field_name: String,
    /// Monotonic batch ID (Lite-assigned, per-insert). Used for ACK correlation.
    pub batch_id: u64,
    /// Stable identity of the originating producer.
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    pub epoch: u64,
    /// Per-stream monotonic sequence number within the epoch.
    pub seq: u64,
}

/// Vector insert acknowledgment (Origin → Lite, 0xA3).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorInsertAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Document ID from the originating `VectorInsertMsg`.
    pub id: String,
    /// Batch ID from the originating `VectorInsertMsg`.
    pub batch_id: u64,
    /// `true` if the vector was successfully inserted into Origin's HNSW index.
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

/// Vector delete (Lite → Origin, 0xA4).
///
/// Tombstones the vector identified by `id` in Origin's HNSW index.
/// Origin looks up the surrogate for `(collection, id)` and calls
/// `VectorOp::Delete` with the internal node ID.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorDeleteMsg {
    /// Lite instance ID.
    pub lite_id: String,
    /// Target collection name.
    pub collection: String,
    /// External document identifier to tombstone.
    pub id: String,
    /// Named vector field; empty string = default field.
    #[serde(default)]
    pub field_name: String,
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Stable identity of the originating producer.
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    pub epoch: u64,
    /// Per-stream monotonic sequence number within the epoch.
    pub seq: u64,
}

/// Vector delete acknowledgment (Origin → Lite, 0xA5).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorDeleteAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Document ID from the originating `VectorDeleteMsg`.
    pub id: String,
    /// Batch ID from the originating `VectorDeleteMsg`.
    pub batch_id: u64,
    /// `true` if the tombstone was successfully applied.
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
