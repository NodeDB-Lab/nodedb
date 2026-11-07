// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard event receiver: handles inbound writes from remote Event Planes.
//!
//! When a remote node sends a `VShardEnvelope(CrossShardEvent)`, this module:
//! 1. Deserializes the `CrossShardWriteRequest` from the payload
//! 2. Drops it when its dedup key already applied here (see [`super::dedup`])
//! 3. Executes the SQL block through the local Control Plane as one
//!    transaction whose redo record carries the dedup key, so the key is
//!    recorded atomically with the writes
//! 4. Returns a `CrossShardWriteResponse` as ACK

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tracing::{debug, trace, warn};

use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType, WIRE_VERSION};

use super::dedup::{CrossShardDedup, applied_key_of};
use super::metrics::CrossShardMetrics;
use super::types::{CrossShardWriteRequest, CrossShardWriteResponse};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;
use crate::wal::CrossShardAppliedKey;

/// Decode the map encoding. Any other payload fails closed.
fn decode_write_request(payload: &[u8]) -> Result<CrossShardWriteRequest, zerompk::Error> {
    zerompk::from_msgpack(payload)
}

/// Handles incoming cross-shard event writes.
pub struct CrossShardReceiver {
    dedup: Arc<CrossShardDedup>,
    shared_state: Arc<SharedState>,
    metrics: Arc<CrossShardMetrics>,
    node_id: u64,
    /// Keys whose request is executing now. A re-send that arrives while the
    /// first delivery still runs is refused, so it cannot apply a second time.
    in_flight: Mutex<HashSet<CrossShardAppliedKey>>,
}

/// Removes a key from the in-flight set when its delivery ends.
struct InFlight<'a> {
    set: &'a Mutex<HashSet<CrossShardAppliedKey>>,
    key: CrossShardAppliedKey,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let mut set = self.set.lock().unwrap_or_else(|p| p.into_inner());
        set.remove(&self.key);
    }
}

impl CrossShardReceiver {
    pub fn new(
        dedup: Arc<CrossShardDedup>,
        shared_state: Arc<SharedState>,
        metrics: Arc<CrossShardMetrics>,
        node_id: u64,
    ) -> Self {
        Self {
            dedup,
            shared_state,
            metrics,
            node_id,
            in_flight: Mutex::new(HashSet::new()),
        }
    }

    /// Handle a raw VShardEnvelope containing a CrossShardEvent.
    ///
    /// Called by the RaftLoop's vshard_handler callback. Returns
    /// serialized VShardEnvelope bytes for the response.
    pub async fn handle_envelope(&self, envelope_bytes: Vec<u8>) -> Vec<u8> {
        let Some(envelope) = VShardEnvelope::from_bytes(&envelope_bytes) else {
            return self.error_response(0, 0, 0, "malformed VShardEnvelope");
        };

        match envelope.msg_type {
            VShardMessageType::CrossShardEvent => self.handle_write(envelope).await,
            VShardMessageType::NotifyBroadcast => self.handle_notify_broadcast(envelope),
            other => self.error_response(
                envelope.source_node,
                envelope.vshard_id,
                0,
                &format!("unexpected message type: {other:?}"),
            ),
        }
    }

    /// Process a CrossShardEvent write request.
    async fn handle_write(&self, envelope: VShardEnvelope) -> Vec<u8> {
        let request = match decode_write_request(&envelope.payload) {
            Ok(req) => req,
            Err(error) => {
                return self.error_response(
                    envelope.source_node,
                    envelope.vshard_id,
                    0,
                    &format!("deserialize request: {error}"),
                );
            }
        };

        self.metrics.record_received();

        let key = applied_key_of(&request);
        let _in_flight = {
            let mut set = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
            if !set.insert(key.clone()) {
                self.metrics.record_failure();
                let resp = CrossShardWriteResponse::error(
                    request.source_lsn,
                    "the same request is already executing; retry".into(),
                );
                return self.build_response(envelope.source_node, envelope.vshard_id, &resp);
            }
            InFlight {
                set: &self.in_flight,
                key: key.clone(),
            }
        };
        match self.dedup.is_applied(&key) {
            Ok(false) => {}
            Ok(true) => {
                self.metrics.record_duplicate();
                trace!(
                    source_vshard = request.source_vshard,
                    source_lsn = request.source_lsn,
                    source_sequence = request.source_sequence,
                    origin = %request.origin,
                    "cross-shard write dropped (already applied)"
                );
                let resp = CrossShardWriteResponse::duplicate(request.source_lsn);
                return self.build_response(envelope.source_node, envelope.vshard_id, &resp);
            }
            // Unknown whether it applied: refuse, so the sender retries.
            Err(e) => {
                self.metrics.record_failure();
                let resp = CrossShardWriteResponse::error(
                    request.source_lsn,
                    format!("dedup lookup failed: {e}"),
                );
                return self.build_response(envelope.source_node, envelope.vshard_id, &resp);
            }
        }

        // The commit's redo record carries `key`, and applying that record
        // records the key: the key and the writes are durable together.
        match self.execute_sql(&request, key.clone()).await {
            Ok(()) => {
                // A block that wrote nothing commits no redo record. Nothing
                // durable happened, so recording its key here has no window.
                if let Err(error) = self.dedup.record_applied(&key) {
                    warn!(%error, "recording a cross-shard key failed; held in memory");
                }
                debug!(
                    source_vshard = request.source_vshard,
                    source_lsn = request.source_lsn,
                    collection = %request.source_collection,
                    "cross-shard write executed successfully"
                );
                let resp = CrossShardWriteResponse::ok(request.source_lsn);
                self.build_response(envelope.source_node, envelope.vshard_id, &resp)
            }
            Err(e) => {
                self.metrics.record_failure();
                warn!(
                    source_vshard = request.source_vshard,
                    source_lsn = request.source_lsn,
                    error = %e,
                    "cross-shard write execution failed"
                );
                let resp = CrossShardWriteResponse::error(request.source_lsn, e.to_string());
                self.build_response(envelope.source_node, envelope.vshard_id, &resp)
            }
        }
    }

    /// Handle a change-stream run a partition's leader forwarded.
    ///
    /// Deserializes the `NotifyBroadcastMsg` and appends it to the local
    /// `ChangeStream` before it acks, so the leader's next run for this node
    /// lands after it.
    fn handle_notify_broadcast(&self, envelope: VShardEnvelope) -> Vec<u8> {
        let msg: super::types::NotifyBroadcastMsg = match zerompk::from_msgpack(&envelope.payload) {
            Ok(m) => m,
            Err(e) => {
                return self.error_response(
                    envelope.source_node,
                    envelope.vshard_id,
                    0,
                    &format!("deserialize NotifyBroadcast: {e}"),
                );
            }
        };

        // Don't re-deliver our own broadcasts (should not happen, but guard).
        if msg.source_node == self.node_id {
            return self.build_ack_response(envelope.source_node, envelope.vshard_id);
        }

        // Deliver to local ChangeStream subscribers.
        self.shared_state.change_stream.deliver_remote_notify(&msg);

        trace!(
            source_node = msg.source_node,
            partition_id = msg.partition_id,
            changes = msg.changes.len(),
            "appended a forwarded change-stream run"
        );

        self.build_ack_response(envelope.source_node, envelope.vshard_id)
    }

    /// Build a simple ACK response for fire-and-forget messages.
    fn build_ack_response(&self, target_node: u64, vshard_id: u32) -> Vec<u8> {
        let env = VShardEnvelope {
            version: WIRE_VERSION,
            msg_type: VShardMessageType::NotifyBroadcastAck,
            source_node: self.node_id,
            target_node,
            vshard_id,
            payload: Vec::new(),
        };
        env.to_bytes()
    }

    /// Execute a cross-shard write SQL through the Control Plane.
    ///
    /// Uses a system identity (SECURITY DEFINER) since the trigger body
    /// is database-defined code, not user-submitted queries.
    async fn execute_sql(
        &self,
        request: &CrossShardWriteRequest,
        key: CrossShardAppliedKey,
    ) -> crate::Result<()> {
        let identity = cross_shard_identity(TenantId::new(request.tenant_id));

        // Dispatch through the normal Control Plane query path.
        // The Control Plane handles parsing, planning, and Data Plane dispatch.
        crate::control::trigger::fire::fire_sql(
            &self.shared_state,
            &identity,
            crate::control::trigger::fire::ShippedBlock {
                tenant_id: TenantId::new(request.tenant_id),
                database_id: crate::types::DatabaseId::new(request.database_id),
                sql: &request.sql,
                cascade_depth: request.cascade_depth.saturating_add(1),
                target_vshard: request.target_vshard,
                applied_key: key,
            },
        )
        .await
    }

    /// Build a serialized VShardEnvelope response.
    fn build_response(
        &self,
        target_node: u64,
        vshard_id: u32,
        response: &CrossShardWriteResponse,
    ) -> Vec<u8> {
        let payload = zerompk::to_msgpack_vec(response).unwrap_or_default();
        let env = VShardEnvelope {
            version: WIRE_VERSION,
            msg_type: VShardMessageType::CrossShardEventAck,
            source_node: self.node_id,
            target_node,
            vshard_id,
            payload,
        };
        env.to_bytes()
    }

    /// Build an error response for malformed/unexpected messages.
    fn error_response(
        &self,
        target_node: u64,
        vshard_id: u32,
        source_lsn: u64,
        error: &str,
    ) -> Vec<u8> {
        let resp = CrossShardWriteResponse::error(source_lsn, error.to_string());
        self.build_response(target_node, vshard_id, &resp)
    }
}

/// Apply `request` on this node, as its receiver does: once per key. The
/// committed-message outbox delivers here when this node leads the
/// request's target vShard, since a node sends no RPC to itself.
pub(crate) async fn apply_here(
    state: &SharedState,
    request: &CrossShardWriteRequest,
) -> crate::Result<CrossShardWriteResponse> {
    let key = applied_key_of(request);
    let dedup = state
        .cross_shard
        .dedup
        .get()
        .ok_or_else(|| crate::Error::Dispatch {
            detail: "cross-shard dedup store not initialised".into(),
        })?;
    if dedup.is_applied(&key)? {
        return Ok(CrossShardWriteResponse::duplicate(request.source_lsn));
    }
    let executed = crate::control::trigger::fire::fire_sql(
        state,
        &cross_shard_identity(TenantId::new(request.tenant_id)),
        crate::control::trigger::fire::ShippedBlock {
            tenant_id: TenantId::new(request.tenant_id),
            database_id: crate::types::DatabaseId::new(request.database_id),
            sql: &request.sql,
            cascade_depth: request.cascade_depth.saturating_add(1),
            target_vshard: request.target_vshard,
            applied_key: key.clone(),
        },
    )
    .await;
    match executed {
        Ok(()) => {
            // A block that wrote nothing commits no redo record to carry it.
            if let Err(error) = dedup.record_applied(&key) {
                warn!(%error, "recording a cross-shard key failed; held in memory");
            }
            Ok(CrossShardWriteResponse::ok(request.source_lsn))
        }
        Err(error) => Ok(CrossShardWriteResponse::error(
            request.source_lsn,
            error.to_string(),
        )),
    }
}

/// Build a system identity for cross-shard execution (SECURITY DEFINER).
fn cross_shard_identity(tenant_id: TenantId) -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_internal_service(
        0,
        "_system_cross_shard",
        tenant_id,
        vec![Role::Superuser],
        true,
        None,
        crate::control::security::identity::DatabaseSet::All,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_shard_identity_is_superuser() {
        let id = cross_shard_identity(TenantId::new(5));
        assert!(id.is_superuser());
        assert_eq!(id.tenant_id, TenantId::new(5));
        assert_eq!(id.username, "_system_cross_shard");
    }

    #[test]
    fn positional_request_encoding_is_refused() {
        #[derive(zerompk::ToMessagePack)]
        struct PositionalRequest {
            sql: String,
            tenant_id: u64,
            source_vshard: u32,
            source_lsn: u64,
            source_sequence: u64,
            cascade_depth: u32,
            source_collection: String,
            target_vshard: u32,
        }
        let payload = zerompk::to_msgpack_vec(&PositionalRequest {
            sql: "INSERT INTO audit VALUES (1)".into(),
            tenant_id: 7,
            source_vshard: 3,
            source_lsn: 1500,
            source_sequence: 42,
            cascade_depth: 1,
            source_collection: "orders".into(),
            target_vshard: 9,
        })
        .unwrap();
        assert!(decode_write_request(&payload).is_err());
    }

    #[test]
    fn malformed_request_fails_closed() {
        assert!(decode_write_request(&[0xc1]).is_err());
    }

    #[test]
    fn build_response_roundtrip() {
        let resp = CrossShardWriteResponse::ok(1500);
        let payload = zerompk::to_msgpack_vec(&resp).unwrap();
        let env = VShardEnvelope::new(VShardMessageType::CrossShardEventAck, 1, 2, 7, payload);
        let bytes = env.to_bytes();
        let decoded = VShardEnvelope::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.msg_type, VShardMessageType::CrossShardEventAck);
        let decoded_resp: CrossShardWriteResponse =
            zerompk::from_msgpack(&decoded.payload).unwrap();
        assert!(decoded_resp.success);
        assert_eq!(decoded_resp.source_lsn, 1500);
    }
}
