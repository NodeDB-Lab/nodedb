// SPDX-License-Identifier: Apache-2.0

//! FTS index/delete sync messages (client → server / server → client).
//!
//! `FtsIndexMsg` carries one document's string fields from a Lite client to
//! Origin for full-text indexing. `FtsDeleteMsg` removes a document from
//! Origin's inverted index.
//!
//! Wire opcodes:
//! - `0xA6` — `FtsIndex`    (Lite → Origin)
//! - `0xA7` — `FtsIndexAck` (Origin → Lite)
//! - `0xA8` — `FtsDelete`   (Lite → Origin)
//! - `0xA9` — `FtsDeleteAck` (Origin → Lite)

use serde::{Deserialize, Serialize};

use crate::sync::wire::ack_status::AckStatus;

/// FTS index request (Lite → Origin, 0xA6).
///
/// Requests that Origin index a document's top-level string fields into its
/// inverted BM25 indexes: the whole-document index and one index per field.
/// Origin allocates a surrogate for `(collection, doc_id)` and calls
/// `InvertedIndex::index_document` on the Data Plane. Empty `fields` remove
/// the document from every index.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct FtsIndexMsg {
    /// Lite instance ID (for routing and dedup).
    pub lite_id: String,
    /// Target collection name.
    pub collection: String,
    /// External document identifier.
    pub doc_id: String,
    /// `(field, text)` per top-level string field of the document.
    pub fields: Vec<(String, String)>,
    /// Monotonic batch ID (Lite-assigned, per-document). Used for ACK correlation.
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

/// FTS index acknowledgment (Origin → Lite, 0xA7).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct FtsIndexAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Document ID from the originating `FtsIndexMsg`.
    pub doc_id: String,
    /// Batch ID from the originating `FtsIndexMsg`.
    pub batch_id: u64,
    /// `true` if the document was successfully indexed on Origin.
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

/// FTS delete request (Lite → Origin, 0xA8).
///
/// Removes the document identified by `doc_id` from Origin's inverted index.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct FtsDeleteMsg {
    /// Lite instance ID.
    pub lite_id: String,
    /// Target collection name.
    pub collection: String,
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

/// FTS delete acknowledgment (Origin → Lite, 0xA9).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct FtsDeleteAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Document ID from the originating `FtsDeleteMsg`.
    pub doc_id: String,
    /// Batch ID from the originating `FtsDeleteMsg`.
    pub batch_id: u64,
    /// `true` if the document was successfully removed from Origin's index.
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
