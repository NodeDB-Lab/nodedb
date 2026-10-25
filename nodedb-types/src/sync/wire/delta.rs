// SPDX-License-Identifier: Apache-2.0

//! Delta push / ack / reject / collection-purged messages.

use serde::{Deserialize, Serialize};

use crate::DatabaseId;
use crate::sync::compensation::CompensationHint;
use crate::sync::wire::ack_status::AckStatus;

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
    /// Device-assigned valid-time for the mutation (ms since Unix epoch).
    ///
    /// Populated by offline-capable clients so Origin can preserve the
    /// application's notion of "when did this fact take effect" independently
    /// of the Origin-assigned `system_from_ms`. `None` means the client did
    /// not supply a valid-time — Origin will use `system_from_ms` as the
    /// default valid-from.
    #[serde(default)]
    pub device_valid_time_ms: Option<i64>,
    /// Stable identity of the originating producer (Lite peer ID or Origin node ID).
    /// 0 for legacy / pre-idempotency clients.
    #[serde(default)]
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    /// 0 for legacy / pre-idempotency clients.
    #[serde(default)]
    pub epoch: u64,
    /// Per-stream monotonic sequence number within the epoch.
    /// 0 for legacy / pre-idempotency clients.
    #[serde(default)]
    pub seq: u64,
    /// Authenticated device identifier used in delta-key derivation.
    #[serde(default)]
    pub device_id: u64,
    /// HMAC-SHA256 over the session epoch, collection, document id, exact
    /// delta bytes, and authenticated user/device/sequence tuple.
    /// All zeros denotes an unsigned legacy delta.
    #[serde(default)]
    pub delta_signature: [u8; 32],
    /// The incarnation of the collection the client built this delta
    /// against: the `creation_hlc` Origin announced with the collection's
    /// descriptor. Origin refuses a delta for an incarnation the collection
    /// no longer holds, because a drop and a same-name create replaced it.
    /// Every client the handshake admits states one: Origin refuses
    /// `Hlc::ZERO` the same way.
    pub collection_incarnation: crate::hlc::Hlc,
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
    /// Absolute clock-skew between `device_valid_time_ms` and the Origin
    /// wall clock at commit, in milliseconds. `None` when the client did
    /// not supply a device valid-time, or when skew was within tolerance
    /// (≤ 24h). Populated so clients can surface a warning UX.
    #[serde(default)]
    pub clock_skew_warning_ms: Option<i64>,
    /// Highest sequence number from this producer that has been durably applied.
    /// 0 when the server has not yet recorded a sequence for this producer.
    #[serde(default)]
    pub applied_seq: u64,
    /// Idempotency outcome of the acknowledged message.
    #[serde(default)]
    pub status: AckStatus,
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
    pub tenant_id: u64,
    /// Database containing the purged collection.
    pub database_id: DatabaseId,
    /// Collection name.
    pub name: String,
    /// Origin WAL LSN at which the hard-delete was committed.
    pub purge_lsn: u64,
}

/// Row-level operation carried by a [`RowPushMsg`].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RowOp {
    /// The row was created or updated; `payload` is its post-image.
    #[default]
    Upsert,
    /// The row was removed; `payload` is empty.
    Delete,
}

/// Row post-image push (server → client, 0x15).
///
/// Origin sends this for writes that originated on the server — SQL DML, or
/// DDL-managed system rows such as retention policies and alerts — where there
/// is no client-authored CRDT operation to replicate. The peer applies it as a
/// row upsert or delete against its local state.
///
/// Deliberately NOT [`DeltaPushMsg`]: that message is client → server and its
/// `delta` field is Loro update bytes. Carrying a MessagePack post-image in it
/// would leave the receiver unable to tell the two encodings apart, and the
/// operation (upsert vs delete) would have to be inferred from an empty
/// payload rather than stated.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct RowPushMsg {
    /// Collection the row belongs to.
    pub collection: String,
    /// Row / document ID.
    pub document_id: String,
    /// MessagePack-encoded row post-image. Empty for [`RowOp::Delete`].
    pub payload: Vec<u8>,
    /// Whether the row was written or removed.
    pub op: RowOp,
    /// Origin-assigned WAL LSN for the write, when known (`0` otherwise).
    #[serde(default)]
    pub lsn: u64,
    /// Originating peer / node ID.
    #[serde(default)]
    pub peer_id: u64,
    /// Per-collection monotonic sequence, for ordering diagnostics.
    #[serde(default)]
    pub sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn delta_reject_with_compensation() {
        let reject = DeltaRejectMsg {
            mutation_id: 42,
            reason: "unique violation".into(),
            compensation: Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "alice@example.com".into(),
            }),
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::DeltaReject, &reject).unwrap();
        let decoded: DeltaRejectMsg = SyncFrame::from_bytes(&frame.to_bytes())
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.mutation_id, 42);
        assert!(matches!(
            decoded.compensation,
            Some(CompensationHint::UniqueViolation { .. })
        ));
    }

    #[test]
    fn collection_purged_roundtrip() {
        let msg = CollectionPurgedMsg {
            tenant_id: 7,
            database_id: DatabaseId::new(42),
            name: "embeddings".into(),
            purge_lsn: 987_654_321,
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::CollectionPurged, &msg).unwrap();
        let bytes = frame.to_bytes();
        assert_eq!(bytes[0], SyncFrame::FORMAT_VERSION);
        assert_eq!(bytes[1], 0x14);
        let decoded: CollectionPurgedMsg = SyncFrame::from_bytes(&bytes)
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.tenant_id, 7);
        assert_eq!(decoded.database_id, DatabaseId::new(42));
        assert_eq!(decoded.name, "embeddings");
        assert_eq!(decoded.purge_lsn, 987_654_321);
    }
}
