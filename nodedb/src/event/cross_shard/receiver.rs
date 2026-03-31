//! Cross-shard event receiver: handles inbound writes from remote Event Planes.
//!
//! When a remote node sends a `VShardEnvelope(CrossShardEvent)`, this module:
//! 1. Deserializes the `CrossShardWriteRequest` from the payload
//! 2. Checks HWM dedup — drops if `source_lsn <= hwm[source_vshard]`
//! 3. Executes the SQL through the local Control Plane → Data Plane path
//! 4. Advances the HWM on success
//! 5. Returns a `CrossShardWriteResponse` as ACK

use std::sync::Arc;

use tracing::{debug, trace, warn};

use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType, WIRE_VERSION};

use super::dedup::HwmStore;
use super::metrics::CrossShardMetrics;
use super::types::{CrossShardWriteRequest, CrossShardWriteResponse};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Handles incoming cross-shard event writes.
pub struct CrossShardReceiver {
    hwm_store: Arc<HwmStore>,
    shared_state: Arc<SharedState>,
    metrics: Arc<CrossShardMetrics>,
    node_id: u64,
}

impl CrossShardReceiver {
    pub fn new(
        hwm_store: Arc<HwmStore>,
        shared_state: Arc<SharedState>,
        metrics: Arc<CrossShardMetrics>,
        node_id: u64,
    ) -> Self {
        Self {
            hwm_store,
            shared_state,
            metrics,
            node_id,
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
        let request: CrossShardWriteRequest = match rmp_serde::from_slice(&envelope.payload) {
            Ok(req) => req,
            Err(e) => {
                return self.error_response(
                    envelope.source_node,
                    envelope.vshard_id,
                    0,
                    &format!("deserialize request: {e}"),
                );
            }
        };

        self.metrics.record_received();

        // HWM dedup check.
        if self
            .hwm_store
            .is_duplicate(request.source_vshard, request.source_lsn)
        {
            self.metrics.record_duplicate();
            trace!(
                source_vshard = request.source_vshard,
                source_lsn = request.source_lsn,
                "cross-shard write dropped (HWM dedup)"
            );
            let resp = CrossShardWriteResponse::duplicate(request.source_lsn);
            return self.build_response(envelope.source_node, envelope.vshard_id, &resp);
        }

        // Execute the SQL through the Control Plane.
        let result = self.execute_sql(&request).await;

        match result {
            Ok(()) => {
                // Advance HWM after successful execution.
                self.hwm_store
                    .advance(request.source_vshard, request.source_lsn);
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

    /// Handle a NOTIFY broadcast from a remote peer.
    ///
    /// Deserializes the `NotifyBroadcastMsg` and delivers it to the local
    /// `ChangeStream` so LISTEN sessions on this node receive the event.
    fn handle_notify_broadcast(&self, envelope: VShardEnvelope) -> Vec<u8> {
        let msg: super::types::NotifyBroadcastMsg = match rmp_serde::from_slice(&envelope.payload) {
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
            collection = %msg.collection,
            "delivered remote NOTIFY to local subscribers"
        );

        self.build_ack_response(envelope.source_node, envelope.vshard_id)
    }

    /// Build a simple ACK response for fire-and-forget messages.
    fn build_ack_response(&self, target_node: u64, vshard_id: u16) -> Vec<u8> {
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
    async fn execute_sql(&self, request: &CrossShardWriteRequest) -> crate::Result<()> {
        let identity = cross_shard_identity(TenantId::new(request.tenant_id));

        // Dispatch through the normal Control Plane query path.
        // The Control Plane handles parsing, planning, and Data Plane dispatch.
        crate::control::trigger::fire::fire_sql(
            &self.shared_state,
            &identity,
            TenantId::new(request.tenant_id),
            &request.sql,
            request.cascade_depth.saturating_add(1),
        )
        .await
    }

    /// Build a serialized VShardEnvelope response.
    fn build_response(
        &self,
        target_node: u64,
        vshard_id: u16,
        response: &CrossShardWriteResponse,
    ) -> Vec<u8> {
        let payload = rmp_serde::to_vec(response).unwrap_or_default();
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
        vshard_id: u16,
        source_lsn: u64,
        error: &str,
    ) -> Vec<u8> {
        let resp = CrossShardWriteResponse::error(source_lsn, error.to_string());
        self.build_response(target_node, vshard_id, &resp)
    }
}

/// Build a system identity for cross-shard execution (SECURITY DEFINER).
fn cross_shard_identity(tenant_id: TenantId) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: "_system_cross_shard".into(),
        tenant_id,
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_shard_identity_is_superuser() {
        let id = cross_shard_identity(TenantId::new(5));
        assert!(id.is_superuser);
        assert_eq!(id.tenant_id, TenantId::new(5));
        assert_eq!(id.username, "_system_cross_shard");
    }

    #[test]
    fn build_response_roundtrip() {
        let resp = CrossShardWriteResponse::ok(1500);
        let payload = rmp_serde::to_vec(&resp).unwrap();
        let env = VShardEnvelope::new(VShardMessageType::CrossShardEventAck, 1, 2, 7, payload);
        let bytes = env.to_bytes();
        let decoded = VShardEnvelope::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.msg_type, VShardMessageType::CrossShardEventAck);
        let decoded_resp: CrossShardWriteResponse =
            rmp_serde::from_slice(&decoded.payload).unwrap();
        assert!(decoded_resp.success);
        assert_eq!(decoded_resp.source_lsn, 1500);
    }
}
