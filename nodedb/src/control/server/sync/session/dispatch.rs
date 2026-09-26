// SPDX-License-Identifier: BUSL-1.1

//! Frame dispatch: `process_frame` routes incoming frames to the
//! per-kind handler methods.

use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::control::security::audit::AuditLog;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::state::SharedState;

use super::super::dlq::SyncDlq;
use super::super::wire::*;
use super::state::SyncSession;

impl SyncSession {
    /// Process an incoming frame and return a response frame (if any).
    ///
    /// Security-context parameters provide rate limiting, audit, and DLQ
    /// persistence. Exact CRDT RLS is not evaluated here because raw deltas
    /// cannot describe the merged row; admission evaluates the authoritative
    /// Data-Plane preview before WAL. `rls_store` remains only for protocol
    /// compatibility while callers migrate to the admission boundary.
    ///
    /// `shared` is forwarded to `handle_handshake` so the durable
    /// `SyncProducerRegistry` can be consulted during the Lite client
    /// fencing decision, and so the handshake and token-refresh paths can
    /// authenticate a presented JWT against the configured `[auth.jwt]`
    /// providers. Without it neither can verify a credential, and both
    /// refuse.
    ///
    /// # Timeseries push
    ///
    /// In production, the listener intercepts `TimeseriesPush` before
    /// calling `process_frame` and routes it through
    /// [`SharedStateTimeseriesDispatcher`] for Data Plane ingest. If a frame of
    /// this type ever reaches `process_frame`, it means the listener
    /// interception is broken and `SharedState` is not available here;
    /// we emit a loud rejection ACK and an `error!` log so the failure
    /// is audible rather than silently dropping data after ACKing.
    ///
    /// [`SharedStateTimeseriesDispatcher`]: super::super::timeseries_handler::SharedStateTimeseriesDispatcher
    pub async fn process_frame(
        &mut self,
        frame: &SyncFrame,
        rls_store: Option<&RlsPolicyStore>,
        audit_log: Option<&mut AuditLog>,
        dlq: Option<&mut SyncDlq>,
        shared: Option<&Arc<SharedState>>,
    ) -> Option<SyncFrame> {
        match frame.msg_type {
            SyncMessageType::Handshake => match frame.decode_body::<HandshakeMsg>() {
                Some(msg) => {
                    self.handle_handshake(&msg, self.server_clock.clone(), shared)
                        .await
                }
                None => {
                    // A malformed handshake is an authentication-boundary
                    // failure. It must revoke any prior identity before the
                    // terminal failure ack is returned to the listener.
                    self.clear_handshake_binding();
                    self.malformed_handshake_reject_frame()
                }
            },
            SyncMessageType::DeltaPush => {
                let msg: DeltaPushMsg = frame.decode_body()?;
                if let Some(shared) = shared {
                    // Authorize before session bookkeeping or a provisional ACK.
                    // `authorize_delta_write` emits denial audit records through
                    // `shared.audit`, so it must run before that mutex is locked.
                    // The Data-Plane finalizer repeats this check to cover a
                    // permission revocation between admission and dispatch.
                    if super::super::async_dispatch::authorize_delta_write(
                        shared,
                        self.identity.as_ref(),
                        &msg.collection,
                    )
                    .is_err()
                    {
                        return super::super::async_dispatch::permission_denied_delta_reject(&msg);
                    }

                    // Only an authorized DeltaPush needs the handler's audit/DLQ
                    // state. Keeping these guards scoped to this synchronous call
                    // avoids the audit-emitter self-deadlock and never spans await.
                    let mut audit = shared.audit.lock().unwrap_or_else(|p| p.into_inner());
                    let mut dlq = shared.sync_dlq.lock().unwrap_or_else(|p| p.into_inner());
                    self.handle_delta_push(&msg, rls_store, Some(&mut audit), Some(&mut dlq))
                } else {
                    self.handle_delta_push(&msg, rls_store, audit_log, dlq)
                }
            }
            SyncMessageType::VectorClockSync => {
                let msg: VectorClockSyncMsg = frame.decode_body()?;
                self.handle_vector_clock_sync(&msg)
            }
            // Shape subscriptions are authorized and dispatched exclusively by
            // the asynchronous SharedState path in the session loop. This
            // synchronous fallback intentionally has no authority.
            SyncMessageType::ShapeSubscribe => None,
            SyncMessageType::ShapeUnsubscribe => {
                let msg: super::super::shape::handler::ShapeUnsubscribeMsg = frame.decode_body()?;
                if let Some(s) = shared {
                    super::super::shape::handler::handle_unsubscribe(
                        &self.session_id,
                        &msg,
                        &s.shape_registry,
                    );
                } else {
                    let registry = super::super::shape::registry::ShapeRegistry::new();
                    super::super::shape::handler::handle_unsubscribe(
                        &self.session_id,
                        &msg,
                        &registry,
                    );
                }
                None
            }
            SyncMessageType::TimeseriesPush => {
                // Production path: listener.rs intercepts TimeseriesPush
                // before this dispatch and runs it through
                // SharedStateDispatcher. Reaching this arm means the
                // listener interception is broken — emit a rejection
                // ACK and a loud error log instead of silently dropping
                // data.
                let msg: TimeseriesPushMsg = frame.decode_body()?;
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    samples = msg.sample_count,
                    "timeseries push reached generic process_frame — listener \
                     interception is broken; data NOT ingested, returning \
                     rejection ACK"
                );
                let ack = TimeseriesAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted: 0,
                    rejected: msg.sample_count,
                    lsn: 0,
                    // Nothing applied, so the producer frontier does not move.
                    applied_seq: msg.seq.saturating_sub(1),
                    // Retryable, not terminal: the data is fine and the wiring
                    // is not. Telling the sender to compensate would destroy a
                    // good batch over a server-side misconfiguration, whereas a
                    // re-send succeeds the moment interception is repaired.
                    status: AckStatus::Gap { expected: msg.seq },
                };
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
            }
            SyncMessageType::TimeseriesAck => None,
            // ColumnarInsert is intercepted in session_handler before reaching here.
            // ColumnarInsertAck is server→client; receiving it here means a mis-wired
            // client — ignore silently.
            SyncMessageType::ColumnarInsert | SyncMessageType::ColumnarInsertAck => None,
            // VectorInsert / VectorDelete are intercepted in session_handler before
            // reaching here. VectorInsertAck / VectorDeleteAck are server→client;
            // receiving them here means a mis-wired client — ignore silently.
            SyncMessageType::VectorInsert
            | SyncMessageType::VectorDelete
            | SyncMessageType::VectorInsertAck
            | SyncMessageType::VectorDeleteAck => None,
            // FtsIndex / FtsDelete are intercepted in session_handler before
            // reaching here. FtsIndexAck / FtsDeleteAck are server→client;
            // receiving them here means a mis-wired client — ignore silently.
            SyncMessageType::FtsIndex
            | SyncMessageType::FtsDelete
            | SyncMessageType::FtsIndexAck
            | SyncMessageType::FtsDeleteAck => None,
            // SpatialInsert / SpatialDelete are intercepted in session_handler
            // before reaching here. The Ack variants are server→client; receiving
            // them here means a mis-wired client — ignore silently.
            SyncMessageType::SpatialInsert
            | SyncMessageType::SpatialDelete
            | SyncMessageType::SpatialInsertAck
            | SyncMessageType::SpatialDeleteAck => None,
            // KvPush is intercepted in session_handler before reaching here.
            // KvPushAck is server→client; receiving it here means a mis-wired
            // client — ignore silently.
            SyncMessageType::KvPush | SyncMessageType::KvPushAck => None,
            SyncMessageType::RowPushReject => {
                let msg: RowPushRejectMsg = frame.decode_body()?;
                self.handle_row_push_reject(&msg, shared);
                None
            }
            SyncMessageType::ResyncRequest => {
                // In the production path (shared = Some), session_handler.rs
                // intercepts ResyncRequest before process_frame and dispatches
                // handle_resync_request_async. This arm is reached only when
                // shared is None (permissive/test path) — log and drop.
                if let Some(msg) = frame.decode_body::<ResyncRequestMsg>() {
                    warn!(
                        session = %self.session_id,
                        reason = ?msg.reason,
                        from_mutation_id = msg.from_mutation_id,
                        collection = %msg.collection,
                        shape_id = %msg.shape_id,
                        "client requested re-sync (no SharedState; dropping)"
                    );
                }
                None
            }
            SyncMessageType::TokenRefresh => {
                let msg: TokenRefreshMsg = frame.decode_body()?;
                self.handle_token_refresh(&msg, shared).await
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
                    self.handle_ping(&msg)
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
            SyncMessageType::CollectionSchema => {
                // A peer announces a collection descriptor. Materialize it
                // into the local catalog (create-only) so it becomes
                // catalog-visible and queryable cluster-wide; the handler
                // handles the permissive `shared == None` path by warn+skip.
                let msg: CollectionSchemaSyncMsg = frame.decode_body()?;
                self.handle_collection_schema(&msg, shared)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditEvent;
    use crate::control::security::identity::{AuthenticatedIdentity, Role};
    use crate::types::TenantId;
    use crate::wal::WalManager;

    fn make_session() -> SyncSession {
        SyncSession::new("test-session-1".into())
    }

    #[tokio::test]
    async fn production_process_frame_denies_unauthorized_delta_before_provisional_ack() {
        // Retain the WAL and Data-Plane dispatcher endpoints for the complete
        // SharedState lifetime, matching the production-backed test fixture.
        let _tempdir = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&_tempdir.path().join("sync-session.wal"))
                .expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("construct shared state");

        let mut session = make_session();
        session.authenticated = true;
        session.tenant_id = Some(TenantId::new(1));
        session.username = Some("collection-reader".into());
        session.identity = Some(AuthenticatedIdentity::new_regular(
            7,
            "collection-reader",
            TenantId::new(1),
            crate::control::security::identity::AuthMethod::ApiKey,
            vec![Role::Custom("sync-reader".into())],
            None,
            AuthenticatedIdentity::default_database_set(false),
        ));

        let msg = DeltaPushMsg {
            collection: "orders".into(),
            document_id: "o1".into(),
            delta: nodedb_types::json_to_msgpack(&serde_json::json!({"status": "active"}))
                .expect("encode delta"),
            peer_id: 1,
            mutation_id: 77,
            device_id: 0,
            delta_signature: [0; 32],
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg).expect("encode frame");

        // This is the production process_frame path: shared state is supplied,
        // while audit/DLQ are deliberately not pre-locked by the caller.
        let response = session
            .process_frame(&frame, Some(&shared.rls), None, None, Some(&shared))
            .await
            .expect("permission denial response");

        assert_eq!(response.msg_type, SyncMessageType::DeltaReject);
        assert_ne!(response.msg_type, SyncMessageType::DeltaAck);
        let reject: DeltaRejectMsg = response.decode_body().expect("decode rejection");
        assert_eq!(
            reject.compensation,
            Some(CompensationHint::PermissionDenied)
        );
        assert_eq!(session.mutations_processed, 0);
        assert_eq!(
            shared
                .audit
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .query_by_event(&AuditEvent::PermissionDenied)
                .len(),
            1,
            "the production authorization gate records exactly one denial"
        );
    }
}
