// SPDX-License-Identifier: BUSL-1.1

//! Timeseries push handler for sync sessions.
//!
//! Decodes Gorilla-encoded metric blocks from Lite, builds ILP-format
//! payloads with `__source` tag, and dispatches to the Data Plane via
//! a [`TimeseriesDispatcher`] implementation supplied by the caller.
//!
//! The leaky `(ack, ingest_data)` tuple is intentionally absent — the
//! handler owns both decode and dispatch so that an ACK can never be
//! returned without the corresponding ingest being attempted.

use async_trait::async_trait;
use tracing::{debug, error};

use super::session::SyncSession;
use super::wire::*;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::NO_APPLY_KEY;

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Encapsulates the async Data Plane dispatch for a decoded timeseries push.
///
/// Callers supply a concrete implementation so that the handler can complete
/// ingest atomically with ACK generation. This makes it structurally
/// impossible to ACK a push without attempting dispatch.
///
/// Returns the raw `Response.payload` bytes from the Data Plane so that the
/// handler can decode the [`SyncAckResult`] for gate status propagation.
#[async_trait]
pub trait TimeseriesDispatcher: Send + Sync {
    /// Immutable database scope selected for this connection.
    ///
    /// The default retains compatibility for external dispatcher
    /// implementations; production overrides it with the session-bound value.
    fn database_id(&self) -> DatabaseId {
        DatabaseId::DEFAULT
    }

    async fn dispatch_ingest(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        ilp_payload: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;
}

// ── SharedState adapter ──────────────────────────────────────────────────────

/// Production dispatcher: routes the ingest to the Data Plane via the SPSC
/// bridge using `EventSource::CrdtSync` so that AFTER triggers are not
/// re-fired on synced data.
pub struct SharedStateTimeseriesDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
    pub(crate) identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
    pub(crate) database_id: DatabaseId,
}

#[async_trait]
impl<'a> TimeseriesDispatcher for SharedStateTimeseriesDispatcher<'a> {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    async fn dispatch_ingest(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        ilp_payload: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use crate::control::server::wal_dispatch::{
            TimeseriesWalAppendContext, wal_append_timeseries,
        };
        use nodedb_physical::physical_plan::TimeseriesOp;

        let prov = provenance;
        let database_id = self.database_id;

        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            &collection,
        )?;
        let payload_bytes = ilp_payload.into_bytes();

        // Allocate a WAL LSN on the Control Plane before dispatching to the
        // Data Plane. This is the canonical LSN for dedup tracking.
        let appended_lsn = wal_append_timeseries(
            self.shared.wal.appender(NO_APPLY_KEY),
            TimeseriesWalAppendContext {
                tenant_id,
                vshard_id: vshard,
                database_id,
                collection: &collection,
            },
            &payload_bytes,
            Some(&prov),
            Some(&self.shared.credentials),
        )?;
        let wal_lsn = appended_lsn.map(|lsn| lsn.as_u64());

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            payload: payload_bytes,
            format: "ilp".to_string(),
            wal_lsn,
            surrogates: Vec::new(),
            provenance: Some(prov),
            // Edge-to-origin sync replays rows already decided by the policy
            // where they were written; the writing device's session is not
            // present here to resolve `$auth.*` against.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        });

        let authorized = super::raft_dispatch::authorize_sync_task(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            vshard,
            plan,
        )?;
        super::raft_dispatch::dispatch_write_replicated(
            self.shared,
            &collection,
            authorized,
            std::time::Duration::from_secs(self.shared.tuning.network.default_deadline_secs),
            crate::event::EventSource::CrdtSync,
            appended_lsn,
        )
        .await
    }
}

// ── NoOp dispatcher (loud failure) ──────────────────────────────────────────

/// Dispatcher used when `SharedState` is unavailable at a call site.
///
/// Returns a loud `Internal` error — this is intentionally NOT a silent
/// no-op. If this path is reached it means the listener wiring is wrong
/// and the push would otherwise be silently dropped after being ACKed.
pub struct NoOpTimeseriesDispatcher;

#[async_trait]
impl TimeseriesDispatcher for NoOpTimeseriesDispatcher {
    async fn dispatch_ingest(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _ilp_payload: String,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("timeseries push"))
    }
}

// ── Handler ──────────────────────────────────────────────────────────────────

impl SyncSession {
    /// Process a timeseries push: decode Gorilla blocks, dispatch to the Data
    /// Plane, and return an ACK frame.
    ///
    /// If `dispatcher.dispatch_ingest` fails the samples are reported as
    /// rejected in the returned ACK. An authentication failure also returns a
    /// rejection ACK without calling the dispatcher.
    pub async fn handle_timeseries_push<D: TimeseriesDispatcher>(
        &mut self,
        msg: &TimeseriesPushMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if !self.authenticated {
            return rejected_timeseries_ack(msg, "session is not authenticated");
        }
        let Some(identity) = self.identity.as_ref() else {
            // `authenticated` is never a substitute for a handshake-bound
            // identity: it could otherwise write under a fabricated tenant.
            return rejected_timeseries_ack(msg, "session has no handshake-bound identity");
        };
        let tenant_id = identity.tenant_id;
        let database_id = dispatcher.database_id();
        if !identity.can_access_database(database_id) {
            return rejected_timeseries_ack(msg, "identity may not access the target database");
        }

        // Decode Gorilla blocks to verify integrity.
        let timestamps = nodedb_codec::GorillaDecoder::new(&msg.ts_block).decode_all();
        let values = nodedb_codec::GorillaDecoder::new(&msg.val_block).decode_all();

        let decoded_count = timestamps.len().min(values.len());
        if decoded_count == 0 {
            // The Gorilla blocks yielded no usable sample pair. Re-sending the
            // identical bytes cannot decode any better, so this is terminal —
            // the sender must drop the batch rather than spin on it.
            return rejected_timeseries_ack(
                msg,
                "gorilla timestamp/value blocks decoded to zero samples",
            );
        }

        // Build ILP-format payload for Data Plane ingest.
        let mut ilp_lines = String::with_capacity(decoded_count * 80);
        for i in 0..decoded_count {
            let (ts, _) = timestamps[i];
            let (_, val) = values[i];
            // ILP format: measurement,__source=lite_id value=X timestamp_ns
            ilp_lines.push_str(&msg.collection);
            ilp_lines.push_str(",__source=");
            ilp_lines.push_str(&msg.lite_id);
            ilp_lines.push_str(" value=");
            ilp_lines.push_str(&val.to_string());
            ilp_lines.push(' ');
            // Convert ms to ns for ILP.
            ilp_lines.push_str(&(ts * 1_000_000).to_string());
            ilp_lines.push('\n');
        }

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            decoded = decoded_count,
            lite_id = %msg.lite_id,
            "timeseries push decoded, dispatching to Data Plane"
        );

        let vshard = VShardId::from_collection_in_database(database_id, &msg.collection);

        match dispatcher
            .dispatch_ingest(
                tenant_id,
                vshard,
                msg.collection.clone(),
                ilp_lines,
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Timeseries,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(payload_bytes) => {
                // Decode SyncAckResult from the Data Plane response payload.
                // On decode failure fall back to Applied so the client is
                // still ACKed (the ingest succeeded).
                let wire = super::ack_decode::decode_sync_ack(
                    &payload_bytes,
                    "timeseries",
                    &self.session_id,
                    &msg.collection,
                    msg.seq,
                )
                .into_wire();

                // A terminally refused batch ingested no samples, so none of it
                // may be reported as accepted.
                let accepted = if wire.accepted {
                    decoded_count as u64
                } else {
                    0
                };
                let ack = TimeseriesAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted,
                    rejected: msg.sample_count.saturating_sub(accepted),
                    // WAL LSN is not surfaced by the dispatch helper (returns
                    // payload bytes only); `applied_seq` is the real producer
                    // frontier. Don't conflate the two — leave `lsn` 0 rather
                    // than report a sequence number as a WAL LSN.
                    lsn: 0,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
            }
            Err(e) => {
                // Whether the sender should re-send or compensate is read from
                // the typed error, not assumed from the fact that dispatch
                // failed: a timeout or an unavailable leader refused nothing on
                // the merits, and reporting it as terminal would drop the batch.
                let status = super::refusal::ack_status_for_dispatch_error(&e, msg.seq);
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    batch_id = msg.batch_id,
                    error = %e,
                    retryable = matches!(status, AckStatus::Gap { .. }),
                    "timeseries ingest dispatch failed; reporting samples as rejected"
                );
                let ack = TimeseriesAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted: 0,
                    rejected: msg.sample_count,
                    lsn: 0,
                    // Nothing applied, so the producer frontier does not move.
                    applied_seq: msg.seq.saturating_sub(1),
                    status,
                };
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
            }
        }
    }
}

/// Refuse a batch terminally, before it ever reaches the Data Plane.
///
/// Every one of these refusals is a property of the batch or the session that
/// re-sending cannot change, so the sender must compensate rather than retry.
/// The status carries the reason instead of a bare `Applied`: a receiver that
/// matches on the status would otherwise read a refusal as an apply and retire
/// a write that never landed.
fn rejected_timeseries_ack(
    msg: &TimeseriesPushMsg,
    reason: impl Into<String>,
) -> Option<SyncFrame> {
    let ack = TimeseriesAckMsg {
        collection: msg.collection.clone(),
        batch_id: msg.batch_id,
        accepted: 0,
        rejected: msg.sample_count,
        lsn: 0,
        // Nothing applied, so the producer frontier does not move.
        applied_seq: msg.seq.saturating_sub(1),
        status: AckStatus::Rejected {
            reason: reason.into(),
        },
    };
    SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use std::sync::{Arc, Mutex};

    // ── Mock dispatcher ──────────────────────────────────────────────────────

    type MockCallLog = Arc<Mutex<Vec<(TenantId, DatabaseId, VShardId, String, String)>>>;

    struct MockDispatcher {
        calls: MockCallLog,
        database_id: DatabaseId,
        /// Produces the dispatch outcome on each call.
        ///
        /// A factory rather than a stored `Result` so the error's *type*
        /// survives to the handler. The handler classifies retryable-vs-terminal
        /// on that type, so a mock that flattened every failure into `Internal`
        /// could not express a retryable refusal at all — it would silently
        /// assert only the terminal half of the behavior.
        outcome: Box<dyn Fn() -> crate::Result<Vec<u8>> + Send + Sync>,
    }

    impl MockDispatcher {
        fn with(
            outcome: impl Fn() -> crate::Result<Vec<u8>> + Send + Sync + 'static,
        ) -> (Self, MockCallLog) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    calls: calls.clone(),
                    database_id: DatabaseId::DEFAULT,
                    outcome: Box::new(outcome),
                },
                calls,
            )
        }

        fn ok() -> (Self, MockCallLog) {
            // Empty payload — the handler falls back to Applied.
            Self::with(|| Ok(Vec::new()))
        }

        fn err() -> Self {
            Self::with(|| {
                Err(crate::Error::Internal {
                    detail: "mock failure".to_string(),
                })
            })
            .0
        }

        /// A dispatch that refused nothing on the merits — the batch never got
        /// a verdict and re-sending it is expected to succeed.
        fn retryable() -> Self {
            Self::with(|| {
                Err(crate::Error::RetryableRefusal {
                    reason: "shard is rebalancing".to_string(),
                })
            })
            .0
        }
    }

    #[async_trait]
    impl TimeseriesDispatcher for MockDispatcher {
        fn database_id(&self) -> DatabaseId {
            self.database_id
        }

        async fn dispatch_ingest(
            &self,
            tenant_id: TenantId,
            vshard: VShardId,
            collection: String,
            ilp_payload: String,
            _provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            self.calls.lock().unwrap().push((
                tenant_id,
                self.database_id,
                vshard,
                collection,
                ilp_payload,
            ));
            (self.outcome)()
        }
    }

    fn make_session() -> SyncSession {
        SyncSession::new("test-session".to_string())
    }

    fn authenticate(session: &mut SyncSession) {
        session.authenticated = true;
        session.identity = Some(
            crate::control::security::identity::AuthenticatedIdentity::new_regular(
                1,
                "test",
                TenantId::new(1),
                crate::control::security::identity::AuthMethod::ApiKey,
                Vec::new(),
                None,
                crate::control::security::identity::AuthenticatedIdentity::default_database_set(
                    false,
                ),
            ),
        );
    }

    /// Batch ID stamped on every test push, distinct from the sample count and
    /// the seq so an ack echoing the wrong field is visible.
    const BATCH_ID: u64 = 77;

    /// Build a minimal `TimeseriesPushMsg` with valid Gorilla-encoded blocks
    /// for a single sample (timestamp=1000 ms, value=42.0).
    fn make_push_msg(collection: &str) -> TimeseriesPushMsg {
        use nodedb_codec::GorillaEncoder;

        let mut ts_enc = GorillaEncoder::new();
        ts_enc.encode(1_000, 0.0); // timestamp=1000 ms, dummy val
        let ts_block = ts_enc.finish();

        let mut val_enc = GorillaEncoder::new();
        val_enc.encode(0, 42.0); // dummy ts, value=42.0
        let val_block = val_enc.finish();

        TimeseriesPushMsg {
            collection: collection.to_string(),
            lite_id: "lite-1".to_string(),
            batch_id: BATCH_ID,
            sample_count: 1,
            ts_block,
            val_block,
            series_block: Vec::new(),
            min_ts: 1_000,
            max_ts: 1_000,
            watermarks: std::collections::HashMap::new(),
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    // ── Test: unauthenticated session returns rejection without calling dispatcher ─

    #[tokio::test]
    async fn test_unauthenticated_rejects_without_dispatch() {
        let mut session = make_session();
        // session.authenticated == false by default
        let (mock, calls) = MockDispatcher::ok();
        let msg = make_push_msg("metrics");

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        assert!(frame.is_some(), "should return a rejection ACK frame");
        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(decoded.accepted, 0);
        assert_eq!(decoded.rejected, 1);
        assert!(
            calls.lock().unwrap().is_empty(),
            "dispatcher must not be called for unauthenticated sessions"
        );
    }

    // ── Test: every ack names the batch it answers ──────────────────────────

    #[tokio::test]
    async fn an_applied_ack_echoes_the_batch_it_answers() {
        let mut session = make_session();
        authenticate(&mut session);
        let (mock, _calls) = MockDispatcher::ok();
        let msg = make_push_msg("metrics");

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(
            decoded.batch_id, BATCH_ID,
            "the ack must name the batch it answers, not a sample count or seq"
        );
    }

    #[tokio::test]
    async fn a_terminal_refusal_names_the_batch_so_it_can_be_retired_alone() {
        // The whole point of carrying batch_id: a batch refused before it ever
        // reaches the Data Plane never advances the producer frontier, so
        // `applied_seq` cannot identify it. Without the echo the sender cannot
        // tell which batch to drop, and re-sends it forever.
        let mut session = make_session();
        let (mock, _calls) = MockDispatcher::ok();
        let msg = make_push_msg("metrics");

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(decoded.batch_id, BATCH_ID);
        assert!(
            matches!(decoded.status, AckStatus::Rejected { .. }),
            "a refused batch must not be acked as applied, got {:?}",
            decoded.status
        );
    }

    #[tokio::test]
    async fn an_undecodable_batch_is_refused_terminally_rather_than_acked() {
        // Empty Gorilla blocks decode to nothing. Re-sending the same bytes
        // cannot decode any better, so the sender must drop the batch — but it
        // must be told that, not handed an `Applied` for a write that vanished.
        let mut session = make_session();
        authenticate(&mut session);
        let (mock, calls) = MockDispatcher::ok();
        let mut msg = make_push_msg("metrics");
        msg.ts_block = Vec::new();
        msg.val_block = Vec::new();

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(decoded.batch_id, BATCH_ID);
        assert!(
            matches!(decoded.status, AckStatus::Rejected { .. }),
            "an undecodable batch must be refused, got {:?}",
            decoded.status
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "an undecodable batch must never reach the Data Plane"
        );
    }

    // ── Test: authenticated, successful dispatch → accepted ACK ─────────────

    #[tokio::test]
    async fn authenticated_without_identity_rejects_without_dispatch() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, calls) = MockDispatcher::ok();
        let msg = make_push_msg("metrics");

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        assert!(frame.is_some());
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn authenticated_default_database_is_propagated_to_dispatch() {
        let mut session = make_session();
        authenticate(&mut session);
        let database_id = DatabaseId::new(8);
        let identity = session.identity.as_mut().expect("authenticated identity");
        *identity = crate::control::security::identity::AuthenticatedIdentity::new_regular(
            1,
            "test",
            TenantId::new(1),
            crate::control::security::identity::AuthMethod::ApiKey,
            Vec::new(),
            Some(database_id),
            crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![database_id]),
        );
        let (mut mock, calls) = MockDispatcher::ok();
        mock.database_id = database_id;
        let msg = make_push_msg("metrics");

        let _ = session.handle_timeseries_push(&msg, &mock).await;

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, database_id);
        assert_eq!(
            calls[0].2,
            VShardId::from_collection_in_database(database_id, "metrics")
        );
    }

    #[tokio::test]
    async fn test_authenticated_dispatches_and_acks() {
        let mut session = make_session();
        authenticate(&mut session);
        let (mock, calls) = MockDispatcher::ok();
        let msg = make_push_msg("metrics");

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        assert!(frame.is_some());
        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(decoded.accepted, 1, "one decoded sample should be accepted");
        assert_eq!(decoded.rejected, 0);

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "dispatcher must be called exactly once");
        assert_eq!(calls[0].0, TenantId::new(1));
        assert_eq!(calls[0].1, DatabaseId::DEFAULT);
        assert_eq!(
            calls[0].2,
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, "metrics")
        );
        assert_eq!(calls[0].3, "metrics");
        // ILP payload must contain the collection name and lite_id.
        assert!(calls[0].4.contains("metrics"));
        assert!(calls[0].4.contains("lite-1"));
    }

    // ── Test: dispatcher returns Err → rejection ACK, no panic ──────────────

    #[tokio::test]
    async fn test_dispatch_failure_returns_rejection_ack() {
        let mut session = make_session();
        authenticate(&mut session);
        let mock = MockDispatcher::err();
        let msg = make_push_msg("metrics");

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        assert!(frame.is_some());
        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(
            decoded.accepted, 0,
            "on dispatch failure all samples are rejected"
        );
        assert_eq!(decoded.rejected, 1);
        assert_eq!(decoded.batch_id, BATCH_ID);
        assert!(
            matches!(decoded.status, AckStatus::Rejected { .. }),
            "a failed dispatch must never be acked as applied, got {:?}",
            decoded.status
        );
    }

    #[tokio::test]
    async fn a_dispatch_that_never_got_a_verdict_is_retryable_not_terminal() {
        // A timeout or an unavailable leader refused nothing on the merits.
        // Reporting it as terminal tells the sender to compensate, destroying a
        // batch the cluster never actually rejected.
        let mut session = make_session();
        authenticate(&mut session);
        let mock = MockDispatcher::retryable();
        let mut msg = make_push_msg("metrics");
        msg.seq = 9;

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(
            decoded.status,
            AckStatus::Gap { expected: 9 },
            "a retryable refusal must resume at the batch's own seq"
        );
        assert_eq!(decoded.batch_id, BATCH_ID);
        assert_eq!(
            decoded.applied_seq, 8,
            "nothing applied, so the producer frontier must not advance past the batch"
        );
    }
}
