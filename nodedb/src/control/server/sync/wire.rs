// SPDX-License-Identifier: BUSL-1.1

//! Sync wire protocol — re-exports from `nodedb-types`.
//!
//! All wire types are defined in `nodedb-types::sync::wire` so that both
//! Origin and NodeDB-Lite share identical serialization. This module
//! re-exports them for backwards-compatible use within the Origin codebase.

// ── Re-export all wire types from nodedb-types ──
pub use nodedb_types::sync::wire::{
    AckStatus, CollectionDescriptor, CollectionSchemaSyncMsg, ColumnarInsertAckMsg,
    ColumnarInsertMsg, DefinitionSyncMsg, DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg,
    FtsDeleteAckMsg, FtsDeleteMsg, FtsIndexAckMsg, FtsIndexMsg, HandshakeAckMsg, HandshakeMsg,
    KvPushAckMsg, KvPushMsg, KvPushOp, PeerPresence, PingPongMsg, PresenceBroadcastMsg,
    PresenceLeaveMsg, PresenceUpdateMsg, ResyncReason, ResyncRequestMsg, RowPushRefusal,
    RowPushRejectMsg, ShapeDeltaMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, ShapeUnsubscribeMsg,
    SpatialDeleteAckMsg, SpatialDeleteMsg, SpatialInsertAckMsg, SpatialInsertMsg, SyncFrame,
    SyncMessageType, SyncProvenance, ThrottleMsg, TimeseriesAckMsg, TimeseriesPushMsg,
    TokenRefreshAckMsg, TokenRefreshMsg, VectorClockSyncMsg, VectorDeleteAckMsg, VectorDeleteMsg,
    VectorInsertAckMsg, VectorInsertMsg,
};

// ── Re-export CompensationHint (used by dlq.rs and session.rs) ──
pub use nodedb_types::sync::compensation::CompensationHint;
