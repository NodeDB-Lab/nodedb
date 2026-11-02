// SPDX-License-Identifier: Apache-2.0

//! Timeseries ingest + definition-sync messages.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::sync::wire::ack_status::AckStatus;

/// Timeseries metric batch push (client → server, 0x40).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TimeseriesPushMsg {
    /// Source Lite instance ID (UUID v7).
    pub lite_id: String,
    /// Monotonic batch ID for ACK correlation, echoed verbatim on
    /// [`TimeseriesAckMsg::batch_id`].
    ///
    /// Identifies *this* batch specifically, which `seq` cannot: `applied_seq`
    /// on the ack is a cumulative producer frontier, so it can only retire
    /// batches at or below the frontier. A terminally rejected batch never
    /// advances the frontier, so without this field the sender has no way to
    /// name the one batch it must retire.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Gorilla-encoded timestamp block.
    pub ts_block: Vec<u8>,
    /// Gorilla-encoded value block.
    pub val_block: Vec<u8>,
    /// Raw LE u64 series ID block.
    pub series_block: Vec<u8>,
    /// Number of samples in this batch.
    pub sample_count: u64,
    /// Min timestamp in this batch.
    pub min_ts: i64,
    /// Max timestamp in this batch.
    pub max_ts: i64,
    /// Per-series sync watermark: highest LSN already synced for each series.
    /// Only samples after these watermarks are included.
    pub watermarks: HashMap<u64, u64>,
    /// Stable identity of the originating producer.
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    pub epoch: u64,
    /// Per-stream monotonic sequence number within the epoch.
    pub seq: u64,
}

/// Timeseries push acknowledgment (server → client, 0x41).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TimeseriesAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// The [`TimeseriesPushMsg::batch_id`] this ack answers, echoed verbatim.
    ///
    /// Every ack path sets it, including the rejection paths that never reach
    /// the Data Plane — a rejection the sender cannot correlate back to a batch
    /// is a rejection it cannot act on.
    pub batch_id: u64,
    /// Number of samples accepted.
    pub accepted: u64,
    /// Number of samples rejected (duplicates, out-of-retention, etc.)
    pub rejected: u64,
    /// Server-assigned LSN for this batch (used as sync watermark).
    pub lsn: u64,
    /// Highest sequence number from this producer that has been durably applied.
    #[serde(default)]
    pub applied_seq: u64,
    /// Idempotency outcome of the acknowledged message.
    #[serde(default)]
    pub status: AckStatus,
}

/// Definition sync message (server → client, 0x70).
///
/// Carries function/trigger/procedure definitions from Origin to Lite.
/// Sent when definitions are created, modified, or dropped on Origin.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct DefinitionSyncMsg {
    /// Authenticated tenant that owns this definition.
    pub tenant_id: u64,
    /// Database namespace that owns this definition.
    pub database_id: u64,
    /// Type of definition: "function", "trigger", "procedure".
    pub definition_type: String,
    /// The definition name.
    pub name: String,
    /// Action: "put" (create/replace) or "delete" (drop).
    pub action: String,
    /// Serialized definition body (JSON). Empty for "delete" actions.
    pub payload: Vec<u8>,
}
