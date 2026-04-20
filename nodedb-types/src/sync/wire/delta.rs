//! Delta push / ack / reject / collection-purged messages.

use serde::{Deserialize, Serialize};

use crate::sync::compensation::CompensationHint;

/// Delta push message (client → server, 0x10).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct DeltaPushMsg {
    /// Collection the delta applies to.
    pub collection: String,
    /// Document ID.
    pub document_id: String,
    /// Loro CRDT delta bytes.
    pub delta: Vec<u8>,
    /// Client's peer ID (for CRDT identity).
    pub peer_id: u64,
    /// Per-mutation unique ID for dedup.
    pub mutation_id: u64,
    /// CRC32C checksum of `delta` bytes for integrity verification.
    /// Computed by sender, validated by receiver. 0 for legacy clients.
    #[serde(default)]
    pub checksum: u32,
}

/// Delta acknowledgment (server → client, 0x11).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct DeltaAckMsg {
    /// Mutation ID being acknowledged.
    pub mutation_id: u64,
    /// Server-assigned LSN for this mutation.
    pub lsn: u64,
}

/// Delta rejection (server → client, 0x12).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct DeltaRejectMsg {
    /// Mutation ID being rejected.
    pub mutation_id: u64,
    /// Reason for rejection.
    pub reason: String,
    /// Compensation hints for the client.
    pub compensation: Option<CompensationHint>,
}

/// Collection purged notification (server → client, 0x14).
///
/// Emitted when Origin hard-deletes a collection (retention window
/// expired after `DROP COLLECTION` or explicit `DROP COLLECTION ... PURGE`).
/// The receiving Lite client must:
///
/// 1. Drop all local Loro CRDT state for the collection.
/// 2. Remove the collection's redb record.
/// 3. Terminate any active shape subscriptions or streaming consumers
///    sourced from the collection.
/// 4. Fire the `on_collection_purged` client-trait callback.
///
/// `purge_lsn` is the Origin WAL LSN at which the hard-delete committed.
/// Clients persist it so that on reconnect they can replay any purge
/// events that landed while they were offline by querying
/// `_system.dropped_collections` / purge event log at LSN > last_seen.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct CollectionPurgedMsg {
    /// Numeric tenant ID the collection belonged to.
    pub tenant_id: u32,
    /// Collection name.
    pub name: String,
    /// Origin WAL LSN at which the hard-delete was committed.
    pub purge_lsn: u64,
}
