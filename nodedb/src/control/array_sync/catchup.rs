// SPDX-License-Identifier: BUSL-1.1

//! [`OriginCatchupServer`] — serve array catch-up requests from Lite peers.
//!
//! Called from [`super::inbound::OriginArrayInbound::handle_catchup_request`]
//! when a Lite peer issues an `ArrayCatchupRequestMsg`. Two code paths:
//!
//! 1. **Op-stream path** (common): `from_hlc` is above the GC boundary, so
//!    the requested ops are still in the `OriginOpLog`. Stream them as one or
//!    more `ArrayDeltaBatchMsg` frames (≤256 ops per batch).
//!
//! 2. **Snapshot path** (after long disconnect or GC): `from_hlc` is below
//!    the GC boundary (i.e. below the latest `OriginSnapshotStore` snapshot
//!    for the array). Build a `TileSnapshot` from the snapshot store, split
//!    into chunks (≤256 KiB each), send `ArraySnapshotMsg` header followed
//!    by `ArraySnapshotChunkMsg` frames, then continue with the op-stream
//!    from `snapshot_hlc` onward.
//!
//! After each delivered op the subscriber cursor is advanced via
//! `SubscriberMap::mark_sent`.

use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::snapshot::split_into_chunks;
use nodedb_types::sync::wire::SyncMessageType;
use nodedb_types::sync::wire::array::{
    ArrayCatchupRequestMsg, ArrayDeltaBatchMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};
use tracing::{debug, warn};

use super::ack_registry::ArrayAckRegistry;
use super::op_log::OriginOpLog;
use super::outbound::delivery::{ArrayDeliveryRegistry, ArrayFrame};
use super::outbound::subscriber_state::SubscriberMap;
use super::schema_registry::OriginSchemaRegistry;
use super::snapshot_store::OriginSnapshotStore;

/// Max ops per `ArrayDeltaBatchMsg`.
const BATCH_SIZE: usize = 256;

/// Max bytes per `ArraySnapshotChunkMsg` payload.
const CHUNK_BYTES: usize = 256 * 1024; // 256 KiB

/// Catch-up server: turns an `ArrayCatchupRequestMsg` into a sequence of
/// outbound frames sent via `ArrayDeliveryRegistry`.
pub struct OriginCatchupServer {
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    op_log: Arc<OriginOpLog>,
    schemas: Arc<OriginSchemaRegistry>,
    snapshots: Arc<OriginSnapshotStore>,
    delivery: Arc<ArrayDeliveryRegistry>,
    cursors: Arc<SubscriberMap>,
    ack_registry: Arc<ArrayAckRegistry>,
}

impl OriginCatchupServer {
    /// Construct from shared server components.
    ///
    /// `scope` is the authenticated `(database, tenant)` this server serves;
    /// every catch-up it answers is confined to it.
    pub fn new(
        scope: super::scope::ArrayServerScope,
        op_log: Arc<OriginOpLog>,
        schemas: Arc<OriginSchemaRegistry>,
        snapshots: Arc<OriginSnapshotStore>,
        delivery: Arc<ArrayDeliveryRegistry>,
        cursors: Arc<SubscriberMap>,
        ack_registry: Arc<ArrayAckRegistry>,
    ) -> Self {
        Self {
            database_id: scope.database_id,
            tenant_id: scope.tenant_id,
            op_log,
            schemas,
            snapshots,
            delivery,
            cursors,
            ack_registry,
        }
    }

    /// Serve a catch-up request from `session_id`.
    ///
    /// Validates the array, selects the delivery path (op-stream vs snapshot +
    /// op-stream), and enqueues all resulting frames into the session's delivery
    /// channel. Delivery stops at the first encoding or enqueue failure; the
    /// subscriber cursor remains at the last successfully queued frontier.
    pub fn serve(&self, req: &ArrayCatchupRequestMsg, session_id: &str) -> crate::Result<()> {
        // 1. Validate the array exists.
        if self
            .schemas
            .schema_hlc_in_database(self.database_id, self.tenant_id, &req.array)
            .is_none()
        {
            warn!(
                session = %session_id,
                array = %req.array,
                "catchup_server: array not found in schema registry — ignoring request"
            );
            return Ok(());
        }

        let from_hlc = Hlc::from_bytes(&req.from_hlc_bytes);

        // 2. Determine whether the request falls below the GC boundary.
        let gc_boundary =
            self.ack_registry
                .min_ack_hlc_in_database(self.database_id, self.tenant_id, &req.array);
        let snapshot_hlc_opt = gc_boundary.filter(|gc| from_hlc < *gc).and_then(|_| {
            self.snapshots.latest_for_array_in_database(
                self.database_id,
                self.tenant_id,
                &req.array,
            )
        });

        if let Some(snapshot) = snapshot_hlc_opt {
            // Snapshot path: send snapshot, then op-stream from snapshot_hlc.
            let snap_hlc = snapshot.snapshot_hlc;

            // Encode the snapshot header payload.
            let (header, chunks) =
                split_into_chunks(&snapshot, CHUNK_BYTES).map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("catchup_server split_into_chunks: {e}"),
                })?;

            let header_payload =
                zerompk::to_msgpack_vec(&header).map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("catchup_server header encode: {e}"),
                })?;

            let snap_msg = ArraySnapshotMsg {
                array: req.array.clone(),
                header_payload,
            };

            if !self.send_frame(
                session_id,
                &req.array,
                SyncMessageType::ArraySnapshot,
                &snap_msg,
            ) {
                return Ok(());
            }

            for chunk in &chunks {
                let chunk_msg = ArraySnapshotChunkMsg {
                    array: req.array.clone(),
                    chunk_index: chunk.chunk_index,
                    total_chunks: chunk.total_chunks,
                    payload: chunk.payload.clone(),
                    snapshot_hlc_bytes: chunk.snapshot_hlc.to_bytes(),
                };
                if !self.send_frame(
                    session_id,
                    &req.array,
                    SyncMessageType::ArraySnapshotChunk,
                    &chunk_msg,
                ) {
                    return Ok(());
                }
            }

            // All snapshot frames are queued, so this is now the confirmed
            // frontier even if a later op batch cannot be enqueued.
            self.cursors.mark_sent_in_database(
                session_id,
                self.database_id,
                self.tenant_id,
                &req.array,
                snap_hlc,
            );

            // Continue with ops from snapshot_hlc onward (the subscriber
            // is now caught up to snap_hlc; needs ops above it).
            self.stream_ops(session_id, &req.array, snap_hlc)?;
        } else {
            // Op-stream path: subscriber still has all ops.
            self.stream_ops(session_id, &req.array, from_hlc)?;
        }

        Ok(())
    }

    /// Stream all ops for `array` with `hlc >= from_hlc` as batched delta
    /// frames, advancing the subscriber cursor after each batch.
    fn stream_ops(&self, session_id: &str, array: &str, from_hlc: Hlc) -> crate::Result<bool> {
        let ops_all = self
            .op_log
            .scan_range_in_database(
                self.database_id,
                self.tenant_id,
                array,
                from_hlc,
                Hlc {
                    physical_ms: nodedb_array::sync::hlc::MAX_PHYSICAL_MS,
                    logical: u16::MAX,
                    replica_id: nodedb_array::sync::replica_id::ReplicaId::new(u64::MAX),
                },
            )
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("catchup_server scan_range: {e}"),
            })?;

        let mut batch_payloads: Vec<Vec<u8>> = Vec::with_capacity(BATCH_SIZE);
        let mut last_hlc = from_hlc;

        for op_result in ops_all {
            let op = op_result.map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("catchup_server scan_range iter: {e}"),
            })?;
            match op_codec::encode_op(&op) {
                Ok(payload) => {
                    last_hlc = op.header.hlc;
                    batch_payloads.push(payload);
                }
                Err(e) => {
                    warn!(
                        session = %session_id,
                        array = %array,
                        error = %e,
                        "catchup_server: encode_op failed — stopping catch-up"
                    );
                    return Ok(false);
                }
            }

            if batch_payloads.len() >= BATCH_SIZE {
                let batch = ArrayDeltaBatchMsg {
                    array: array.to_owned(),
                    op_payloads: std::mem::take(&mut batch_payloads),
                };
                if !self.send_frame(session_id, array, SyncMessageType::ArrayDeltaBatch, &batch) {
                    return Ok(false);
                }
                self.cursors.mark_sent_in_database(
                    session_id,
                    self.database_id,
                    self.tenant_id,
                    array,
                    last_hlc,
                );
                debug!(
                    session = %session_id,
                    array = %array,
                    last_hlc = ?last_hlc,
                    "catchup_server: flushed batch"
                );
            }
        }

        // Flush any remaining ops.
        if !batch_payloads.is_empty() {
            let batch = ArrayDeltaBatchMsg {
                array: array.to_owned(),
                op_payloads: batch_payloads,
            };
            if !self.send_frame(session_id, array, SyncMessageType::ArrayDeltaBatch, &batch) {
                return Ok(false);
            }
            self.cursors.mark_sent_in_database(
                session_id,
                self.database_id,
                self.tenant_id,
                array,
                last_hlc,
            );
        }

        Ok(true)
    }

    /// Encode `msg` and enqueue to the session's delivery channel.
    ///
    /// Returns whether the frame encoded and was accepted by the delivery
    /// channel. Callers stop at the first failure so they never advance a
    /// cursor past a missing batch.
    fn send_frame<T: serde::Serialize + zerompk::ToMessagePack>(
        &self,
        session_id: &str,
        array: &str,
        msg_type: SyncMessageType,
        msg: &T,
    ) -> bool {
        match nodedb_types::sync::wire::SyncFrame::try_encode(msg_type, msg) {
            Some(frame) => self
                .delivery
                .enqueue(session_id, ArrayFrame::new(array, frame.to_bytes())),
            None => {
                warn!(
                    session = %session_id,
                    "catchup_server: SyncFrame encode failed for {:?}",
                    msg_type
                );
                false
            }
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::replica_id::ReplicaId;

    use crate::control::array_sync::outbound::subscriber_state::SubscriberStore;

    fn make_server() -> (
        OriginCatchupServer,
        Arc<ArrayDeliveryRegistry>,
        Arc<SubscriberMap>,
    ) {
        let op_log = Arc::new(OriginOpLog::open_in_memory().unwrap());
        let snapshots = OriginSnapshotStore::open_in_memory().unwrap();
        let delivery = Arc::new(ArrayDeliveryRegistry::new());
        let store = SubscriberStore::in_memory().unwrap();
        let cursors = Arc::new(SubscriberMap::new(store));
        let ack_registry = ArrayAckRegistry::open_in_memory().unwrap();

        // Minimal schema registry — needed only to pass the array-exists check.
        let schema_db = Arc::new(
            redb::Database::builder()
                .create_with_backend(redb::backends::InMemoryBackend::new())
                .unwrap(),
        );
        let replica_id = ReplicaId::new(0);
        let hlc_gen = Arc::new(nodedb_array::sync::HlcGenerator::new(replica_id));
        let schemas = Arc::new(
            crate::control::array_sync::OriginSchemaRegistry::open(schema_db, replica_id, hlc_gen)
                .unwrap(),
        );

        let server = OriginCatchupServer::new(
            crate::control::array_sync::ArrayServerScope::new(crate::types::DatabaseId::DEFAULT, 0),
            Arc::clone(&op_log),
            Arc::clone(&schemas),
            snapshots,
            Arc::clone(&delivery),
            Arc::clone(&cursors),
            ack_registry,
        );

        // Register a schema for "arr" so schema_hlc check passes.
        // schema_registry's import_snapshot or register will be used; here
        // we use the internal map — schemas.schema_hlc("arr") returns None
        // unless we register. For tests, just test that unknown arrays are
        // gracefully ignored and known ones stream ops.
        // We skip schema registration and verify op-stream path doesn't crash.
        (server, delivery, cursors)
    }

    #[test]
    fn unknown_array_is_ignored() {
        let (server, delivery, _) = make_server();
        let _ = delivery.register("s1".into());
        let req = ArrayCatchupRequestMsg {
            array: "nonexistent".into(),
            from_hlc_bytes: Hlc::ZERO.to_bytes(),
        };
        // Should not error — just log and return Ok.
        server.serve(&req, "s1").unwrap();
    }
}
