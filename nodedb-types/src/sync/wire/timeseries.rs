//! Timeseries ingest + definition-sync messages.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Timeseries metric batch push (client → server, 0x40).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TimeseriesPushMsg {
    /// Source Lite instance ID (UUID v7).
    pub lite_id: String,
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
}

/// Timeseries push acknowledgment (server → client, 0x41).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TimeseriesAckMsg {
    /// Collection acknowledged.
    pub collection: String,
    /// Number of samples accepted.
    pub accepted: u64,
    /// Number of samples rejected (duplicates, out-of-retention, etc.)
    pub rejected: u64,
    /// Server-assigned LSN for this batch (used as sync watermark).
    pub lsn: u64,
}

/// Definition sync message (server → client, 0x70).
///
/// Carries function/trigger/procedure definitions from Origin to Lite.
/// Sent when definitions are created, modified, or dropped on Origin.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct DefinitionSyncMsg {
    /// Type of definition: "function", "trigger", "procedure".
    pub definition_type: String,
    /// The definition name.
    pub name: String,
    /// Action: "put" (create/replace) or "delete" (drop).
    pub action: String,
    /// Serialized definition body (JSON). Empty for "delete" actions.
    pub payload: Vec<u8>,
}
