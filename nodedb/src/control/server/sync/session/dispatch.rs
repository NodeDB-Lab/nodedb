//! Frame dispatch: `process_frame` routes incoming frames to the
//! per-kind handler methods.

use std::collections::HashMap;

use tracing::{debug, info, warn};

use crate::control::security::audit::AuditLog;
use crate::control::security::jwt::JwtValidator;
use crate::control::security::rls::RlsPolicyStore;

use super::super::dlq::SyncDlq;
use super::super::wire::*;
use super::state::SyncSession;

impl SyncSession {
    /// Process an incoming frame and return a response frame (if any).
    ///
    /// Security-context parameters are optional — when provided,
    /// per-delta RLS enforcement, rate limiting, silent rejection,
    /// and DLQ persistence are active. `None` puts the session in
    /// permissive mode (testing / internal replication channels).
    pub fn process_frame(
        &mut self,
        frame: &SyncFrame,
        jwt_validator: &JwtValidator,
        rls_store: Option<&RlsPolicyStore>,
        audit_log: Option<&mut AuditLog>,
        dlq: Option<&mut SyncDlq>,
        epoch_tracker: Option<&std::sync::Mutex<HashMap<String, u64>>>,
    ) -> Option<SyncFrame> {
        match frame.msg_type {
            SyncMessageType::Handshake => {
                let msg: HandshakeMsg = frame.decode_body()?;
                Some(self.handle_handshake(
                    &msg,
                    jwt_validator,
                    self.server_clock.clone(),
                    epoch_tracker,
                ))
            }
            SyncMessageType::DeltaPush => {
                let msg: DeltaPushMsg = frame.decode_body()?;
                self.handle_delta_push(&msg, rls_store, audit_log, dlq)
            }
            SyncMessageType::VectorClockSync => {
                let msg: VectorClockSyncMsg = frame.decode_body()?;
                Some(self.handle_vector_clock_sync(&msg))
            }
            SyncMessageType::ShapeSubscribe => {
                let msg: super::super::shape::handler::ShapeSubscribeMsg = frame.decode_body()?;
                let registry = super::super::shape::registry::ShapeRegistry::new();
                let tenant_id = self.tenant_id.map(|t| t.as_u32()).unwrap_or(0);
                let current_lsn = self.server_clock.values().copied().max().unwrap_or(0);
                // Record the subscription so CollectionPurged broadcast
                // notifies this session when the shape's source
                // collection is hard-deleted. Graph shapes have no
                // single source collection; skip tracking for those.
                if let Some(coll) = msg.shape.collection() {
                    self.track_collection(tenant_id, coll);
                }
                let response = super::super::shape::handler::handle_subscribe(
                    &self.session_id,
                    tenant_id,
                    &msg,
                    &registry,
                    current_lsn,
                    |_shape, _lsn| super::super::shape::handler::ShapeSnapshotData::empty(),
                );
                Some(response)
            }
            SyncMessageType::ShapeUnsubscribe => {
                let msg: super::super::shape::handler::ShapeUnsubscribeMsg = frame.decode_body()?;
                let registry = super::super::shape::registry::ShapeRegistry::new();
                super::super::shape::handler::handle_unsubscribe(&self.session_id, &msg, &registry);
                None
            }
            SyncMessageType::TimeseriesPush => {
                let msg: TimeseriesPushMsg = frame.decode_body()?;
                let (ack, _ingest_data) = self.handle_timeseries_push(&msg);
                Some(ack)
            }
            SyncMessageType::TimeseriesAck => None,
            SyncMessageType::ResyncRequest => {
                if let Some(msg) = frame.decode_body::<ResyncRequestMsg>() {
                    warn!(
                        session = %self.session_id,
                        reason = ?msg.reason,
                        from_mutation_id = msg.from_mutation_id,
                        collection = %msg.collection,
                        "client requested re-sync"
                    );
                }
                None
            }
            SyncMessageType::TokenRefresh => {
                let msg: TokenRefreshMsg = frame.decode_body()?;
                Some(self.handle_token_refresh(&msg, jwt_validator))
            }
            SyncMessageType::Throttle => {
                if let Some(msg) = frame.decode_body::<ThrottleMsg>() {
                    info!(
                        session = %self.session_id,
                        throttle = msg.throttle,
                        queue_depth = msg.queue_depth,
                        suggested_rate = msg.suggested_rate,
                        "client throttle signal received"
                    );
                }
                None
            }
            SyncMessageType::PingPong => {
                let msg: PingPongMsg = frame.decode_body()?;
                if msg.is_pong {
                    None
                } else {
                    Some(self.handle_ping(&msg))
                }
            }
            SyncMessageType::PresenceUpdate
            | SyncMessageType::PresenceBroadcast
            | SyncMessageType::PresenceLeave => {
                debug!(
                    session = %self.session_id,
                    msg_type = frame.msg_type as u8,
                    "presence frame ignored (handled at listener level)"
                );
                None
            }
            _ => {
                warn!(
                    session = %self.session_id,
                    msg_type = frame.msg_type as u8,
                    "unhandled sync message type"
                );
                None
            }
        }
    }
}
