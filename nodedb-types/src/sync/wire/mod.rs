// SPDX-License-Identifier: Apache-2.0

//! Sync wire protocol: frame format and message types.
//!
//! Frame format: `[msg_type: 1B][length: 4B LE][rkyv/msgpack body]`
//!
//! Message types:
//! - `0x01` Handshake (client → server)
//! - `0x02` HandshakeAck (server → client)
//! - `0x10` DeltaPush (client → server)
//! - `0x11` DeltaAck (server → client)
//! - `0x12` DeltaReject (server → client)
//! - `0x13` CollectionSchema (bidirectional)
//! - `0x14` CollectionPurged (server → client)
//! - `0x15` RowPush (server → client)
//! - `0x16` RowPushReject (client → server)
//! - `0x20` ShapeSubscribe (client → server)
//! - `0x21` ShapeSnapshot (server → client)
//! - `0x22` ShapeDelta (server → client)
//! - `0x23` ShapeUnsubscribe (client → server)
//! - `0x30` VectorClockSync (bidirectional)
//! - `0x40` TimeseriesPush (client → server)
//! - `0x41` TimeseriesAck (server → client)
//! - `0x50` ResyncRequest (bidirectional)
//! - `0x52` Throttle (client → server)
//! - `0x60` TokenRefresh (client → server)
//! - `0x61` TokenRefreshAck (server → client)
//! - `0x70` DefinitionSync (server → client)
//! - `0x80` PresenceUpdate (client → server)
//! - `0x81` PresenceBroadcast (server → all subscribers)
//! - `0x82` PresenceLeave (server → all subscribers)
//! - `0x90` ArrayDelta (client → server)
//! - `0x91` ArrayDeltaBatch (client → server)
//! - `0x92` ArraySnapshot (server → client)
//! - `0x93` ArraySnapshotChunk (server → client)
//! - `0x94` ArraySchema (bidirectional)
//! - `0x95` ArrayAck (client → server)
//! - `0x96` ArrayReject (server → client)
//! - `0x97` ArrayCatchupRequest (client → server)
//! - `0xA0` ColumnarInsert (client → server)
//! - `0xA1` ColumnarInsertAck (server → client)
//! - `0xA2` VectorInsert (client → server)
//! - `0xA3` VectorInsertAck (server → client)
//! - `0xA4` VectorDelete (client → server)
//! - `0xA5` VectorDeleteAck (server → client)
//! - `0xA6` FtsIndex (client → server)
//! - `0xA7` FtsIndexAck (server → client)
//! - `0xA8` FtsDelete (client → server)
//! - `0xA9` FtsDeleteAck (server → client)
//! - `0xAA` SpatialInsert (client → server)
//! - `0xAB` SpatialInsertAck (server → client)
//! - `0xAC` SpatialDelete (client → server)
//! - `0xAD` SpatialDeleteAck (server → client)
//! - `0xAE` KvPush (client → server)
//! - `0xAF` KvPushAck (server → client)
//! - `0xFF` Ping/Pong (bidirectional)

pub mod ack_result;
pub mod ack_status;
pub mod array;
pub mod collection_schema;
pub mod columnar;
pub mod delta;
pub mod frame;
pub mod fts;
pub mod kv;
pub mod presence;
pub mod provenance;
pub mod resync;
pub mod session;
pub mod shape;
pub mod spatial;
pub mod stream_id;
pub mod timeseries;
pub mod vector;

pub use ack_result::{SyncAckResult, SyncOutcome};
pub use ack_status::AckStatus;
pub use array::{
    ArrayAckMsg, ArrayCatchupRequestMsg, ArrayDeltaBatchMsg, ArrayDeltaMsg, ArrayRejectMsg,
    ArrayRejectReason, ArraySchemaSyncMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};
pub use collection_schema::{CollectionDescriptor, CollectionSchemaSyncMsg};
pub use columnar::{ColumnarInsertAckMsg, ColumnarInsertMsg};
pub use delta::{
    CollectionPurgedMsg, DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, RowOp, RowPushMsg,
};
pub use frame::{SyncFrame, SyncMessageType};
pub use fts::{FtsDeleteAckMsg, FtsDeleteMsg, FtsIndexAckMsg, FtsIndexMsg};
pub use kv::{KvPushAckMsg, KvPushMsg, KvPushOp, RowPushRefusal, RowPushRejectMsg};
pub use presence::{PeerPresence, PresenceBroadcastMsg, PresenceLeaveMsg, PresenceUpdateMsg};
pub use provenance::SyncProvenance;
pub use resync::{ResyncReason, ResyncRequestMsg, ThrottleMsg};
pub use session::{
    HandshakeAckMsg, HandshakeMsg, PingPongMsg, TokenRefreshAckMsg, TokenRefreshMsg,
};
pub use shape::{
    ShapeDeltaMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, ShapeUnsubscribeMsg, VectorClockSyncMsg,
};
pub use spatial::{SpatialDeleteAckMsg, SpatialDeleteMsg, SpatialInsertAckMsg, SpatialInsertMsg};
pub use stream_id::{EngineKind, stream_id_for};
pub use timeseries::{DefinitionSyncMsg, TimeseriesAckMsg, TimeseriesPushMsg};
pub use vector::{VectorDeleteAckMsg, VectorDeleteMsg, VectorInsertAckMsg, VectorInsertMsg};
