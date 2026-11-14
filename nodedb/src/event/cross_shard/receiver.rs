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
use super::types::{
    CrossShardWriteRequest, CrossShardWriteResponse, NotifyAnswer, NotifyBroadcastMsg,
    NotifyBroadcastReply,
};
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
            VShardMessageType::NotifyBroadcast => self.handle_notify_broadcast(envelope).await,
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

    /// Handle a change-stream run, or a probe, a partition's leader
    /// forwarded.
    ///
    /// Deserializes the `NotifyBroadcastMsg` and hands it to the local
    /// `ChangeStream` before it answers, so the leader's next run for this
    /// node lands after it. The append journals the run, so it runs on the
    /// blocking pool. The answer is [`NotifyAnswer::Taken`] only once the
    /// journal holds the run. A run the stream refused, or an append that
    /// did not finish, answers [`NotifyAnswer::Refused`], and the leader
    /// sends the stretch again. Every answer that names a partition carries
    /// this node's position in it.
    async fn handle_notify_broadcast(&self, envelope: VShardEnvelope) -> Vec<u8> {
        let (source, vshard_id) = (envelope.source_node, envelope.vshard_id);
        let rejected = |detail: String| NotifyBroadcastReply {
            answer: NotifyAnswer::Rejected { detail },
            through: None,
        };
        let msg: NotifyBroadcastMsg = match zerompk::from_msgpack(&envelope.payload) {
            Ok(m) => m,
            Err(e) => {
                let reply = rejected(format!("deserialize NotifyBroadcast: {e}"));
                return self.build_notify_reply(source, vshard_id, &reply);
            }
        };

        // A node never feeds itself.
        if msg.source_node == self.node_id {
            let reply = rejected("a node does not feed its own change runs to itself".into());
            return self.build_notify_reply(source, vshard_id, &reply);
        }

        let (source_node, partition_id, changes) =
            (msg.source_node, msg.partition_id, msg.changes.len());
        // Deliver to local ChangeStream subscribers.
        let shared = std::sync::Arc::clone(&self.shared_state);
        let delivered = tokio::task::spawn_blocking(move || {
            shared
                .change_stream
                .deliver_remote_notify(&msg, &shared.metadata_apply_wedge)
        })
        .await;
        let reply = match delivered {
            Ok(reply) => reply,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => NotifyBroadcastReply {
                answer: NotifyAnswer::Refused {
                    detail: format!("the forwarded change run did not append: {error}"),
                },
                through: None,
            },
        };
        trace!(
            source_node,
            partition_id,
            changes,
            ?reply,
            "answered a forwarded change-stream run"
        );
        self.build_notify_reply(source, vshard_id, &reply)
    }

    /// Build the `NotifyBroadcastAck` envelope that carries `reply`.
    fn build_notify_reply(
        &self,
        target_node: u64,
        vshard_id: u32,
        reply: &NotifyBroadcastReply,
    ) -> Vec<u8> {
        let payload = match zerompk::to_msgpack_vec(reply) {
            Ok(payload) => payload,
            // An empty payload does not decode as a reply, and the leader
            // sends the run again.
            Err(error) => {
                warn!(%error, "encoding a change-run reply failed; the leader sends the run again");
                Vec::new()
            }
        };
        let env = VShardEnvelope {
            version: WIRE_VERSION,
            msg_type: VShardMessageType::NotifyBroadcastAck,
            source_node: self.node_id,
            target_node,
            vshard_id,
            payload,
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

    /// A receiver over a shared state whose change stream has `journal`.
    struct NotifyHarness {
        receiver: CrossShardReceiver,
        state: Arc<SharedState>,
        journal: Arc<crate::control::change_stream::ChangeJournal>,
        _sides: Vec<crate::bridge::dispatch::CoreChannelDataSide>,
    }

    fn notify_receiver(dir: &std::path::Path) -> NotifyHarness {
        use crate::control::change_stream::ChangeJournal;

        let wal =
            Arc::new(crate::wal::WalManager::open_for_testing(&dir.join("wal")).expect("wal"));
        let (dispatcher, sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let journal =
            Arc::new(ChangeJournal::open(&dir.join("journal"), 1 << 10).expect("open journal"));
        state
            .change_stream
            .attach_journal(Arc::clone(&journal))
            .expect("attach the journal");
        let dedup = Arc::new(CrossShardDedup::open(&dir.join("dedup")).expect("open dedup"));
        let receiver = CrossShardReceiver::new(
            dedup,
            Arc::clone(&state),
            Arc::new(CrossShardMetrics::new()),
            2,
        );
        NotifyHarness {
            receiver,
            state,
            journal,
            _sides: sides,
        }
    }

    /// The envelope of a run of group 3's entry 1 with `events` changes.
    fn notify_request(partition_kind: u8, events: u64) -> Vec<u8> {
        use super::super::types::NotifyChange;
        use crate::event::cdc::CdcOffset;

        let msg = NotifyBroadcastMsg {
            source_node: 1,
            partition_kind,
            partition_id: 3,
            after: CdcOffset::whole_index(0),
            through: CdcOffset::whole_index(1),
            changes: (1..=events)
                .map(|ordinal| NotifyChange {
                    position: CdcOffset::data_event(0, 1, ordinal),
                    tenant_id: 1,
                    database_id: crate::types::DatabaseId::DEFAULT.as_u64(),
                    collection: "orders".into(),
                    document_id: format!("row-{ordinal}"),
                    operation: "INSERT".into(),
                    timestamp_ms: 1,
                    lsn: 1,
                })
                .collect(),
            probe: false,
        };
        let payload = zerompk::to_msgpack_vec(&msg).expect("encode the run");
        VShardEnvelope::new(VShardMessageType::NotifyBroadcast, 1, 2, 0, payload).to_bytes()
    }

    fn notify_reply_of(bytes: &[u8]) -> NotifyBroadcastReply {
        let envelope = VShardEnvelope::from_bytes(bytes).expect("a reply envelope");
        assert_eq!(envelope.msg_type, VShardMessageType::NotifyBroadcastAck);
        zerompk::from_msgpack(&envelope.payload).expect("a reply")
    }

    /// A run the journal does not write answers `Refused`, never `Taken`,
    /// and reports the position the node held before. Failures in a row
    /// mark the node not ready. The run sent again once the journal
    /// recovers answers `Taken` with the run's position, and publishes
    /// once. A third send, as after a lost ack, answers `Taken` and
    /// publishes nothing. A probe answers the position and takes nothing.
    #[tokio::test]
    async fn a_refused_change_run_answers_a_refusal_and_lands_once_when_resent() {
        use crate::control::change_stream::stream::journal_debt::HALT_FAILURES;
        use crate::event::cdc::CdcOffset;

        let dir = tempfile::tempdir().expect("tempdir");
        let NotifyHarness {
            receiver,
            state,
            journal,
            _sides,
        } = notify_receiver(dir.path());
        let request = notify_request(super::super::types::NOTIFY_PARTITION_GROUP, 3);

        journal.fail_next_persists_for_test(HALT_FAILURES);
        for _ in 0..HALT_FAILURES {
            let refused = notify_reply_of(&receiver.handle_envelope(request.clone()).await);
            assert!(
                matches!(refused.answer, NotifyAnswer::Refused { .. }),
                "{refused:?}"
            );
            assert_eq!(refused.through, None, "the node holds nothing of the group");
        }
        assert!(state.metadata_apply_wedge.is_wedged());
        assert_eq!(state.change_stream.events_published(), 0);

        let taken = notify_reply_of(&receiver.handle_envelope(request.clone()).await);
        assert_eq!(taken.answer, NotifyAnswer::Taken);
        assert_eq!(taken.through, Some(CdcOffset::whole_index(1)));
        assert!(!state.metadata_apply_wedge.is_wedged());
        assert_eq!(state.change_stream.events_published(), 3);

        let again = notify_reply_of(&receiver.handle_envelope(request).await);
        assert_eq!(again, taken);
        assert_eq!(
            state.change_stream.events_published(),
            3,
            "a resent run publishes once"
        );

        let probe = NotifyBroadcastMsg::probe(1, crate::control::change_stream::ChangePartition(3));
        let payload = zerompk::to_msgpack_vec(&probe).expect("encode the probe");
        let request =
            VShardEnvelope::new(VShardMessageType::NotifyBroadcast, 1, 2, 0, payload).to_bytes();
        let answered = notify_reply_of(&receiver.handle_envelope(request).await);
        assert_eq!(answered, taken, "a probe answers the position");
    }

    /// A run no node can take answers `Rejected`: an unknown partition kind,
    /// or a payload that does not decode.
    #[tokio::test]
    async fn a_change_run_no_node_can_take_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let harness = notify_receiver(dir.path());
        let (receiver, state) = (&harness.receiver, &harness.state);
        let unknown = notify_reply_of(&receiver.handle_envelope(notify_request(9, 1)).await);
        assert!(matches!(unknown.answer, NotifyAnswer::Rejected { .. }));
        let garbage =
            VShardEnvelope::new(VShardMessageType::NotifyBroadcast, 1, 2, 0, vec![0xc1]).to_bytes();
        let malformed = notify_reply_of(&receiver.handle_envelope(garbage).await);
        assert!(matches!(malformed.answer, NotifyAnswer::Rejected { .. }));
        assert_eq!(state.change_stream.events_published(), 0);
    }
}
