// SPDX-License-Identifier: BUSL-1.1

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
    /// A factory preserves typed core outcomes across repeated dispatches.
    outcome: Box<dyn Fn() -> crate::Result<Vec<u8>> + Send + Sync>,
    /// Samples a successful dispatch reports its resolve rejected.
    rejected: u64,
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
                rejected: 0,
            },
            calls,
        )
    }

    fn ok() -> (Self, MockCallLog) {
        Self::with(|| {
            Ok(
                zerompk::to_msgpack_vec(&nodedb_types::sync::wire::SyncAckResult::acked(
                    AckStatus::Applied,
                    1,
                ))
                .expect("encode applied ack"),
            )
        })
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
    ) -> crate::Result<TimeseriesIngestResult> {
        self.calls.lock().unwrap().push((
            tenant_id,
            self.database_id,
            vshard,
            collection,
            ilp_payload,
        ));
        (self.outcome)().map(|payload| TimeseriesIngestResult {
            payload,
            rejected: self.rejected,
        })
    }
}

fn make_session() -> SyncSession {
    SyncSession::new("test-session".to_string())
}

fn authenticate(session: &mut SyncSession) {
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    session.identity = Some(
        crate::control::security::identity::AuthenticatedIdentity::new_regular(
            1,
            "test",
            TenantId::new(1),
            crate::control::security::identity::AuthMethod::ApiKey,
            Vec::new(),
            None,
            crate::control::security::identity::AuthenticatedIdentity::default_database_set(false),
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

/// A sample the resolve rejected is reported rejected in the ACK, not
/// accepted.
#[tokio::test]
async fn a_resolve_rejected_sample_is_acked_as_rejected() {
    let mut session = make_session();
    authenticate(&mut session);
    let (mut mock, _calls) = MockDispatcher::ok();
    mock.rejected = 1;
    let msg = make_push_msg("metrics");

    let frame = session.handle_timeseries_push(&msg, &mock).await;

    let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
    assert_eq!(decoded.accepted, 0);
    assert_eq!(decoded.rejected, 1);
}

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
async fn a_session_fence_echoes_the_batch_without_consumption() {
    let mut session = make_session();
    let (mock, _calls) = MockDispatcher::ok();
    let msg = make_push_msg("metrics");

    let frame = session.handle_timeseries_push(&msg, &mock).await;

    let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
    assert_eq!(decoded.batch_id, BATCH_ID);
    assert_eq!(decoded.status, AckStatus::Fenced);
    assert_eq!(decoded.applied_seq, 0);
}

#[tokio::test]
async fn undecodable_samples_close_without_consumption() {
    let mut session = make_session();
    authenticate(&mut session);
    let (mock, calls) = MockDispatcher::ok();
    let mut msg = make_push_msg("metrics");
    msg.ts_block = Vec::new();
    msg.val_block = Vec::new();

    let frame = session.handle_timeseries_push(&msg, &mock).await;

    assert!(frame.is_none());
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
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
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
        nodedb_types::CollectionKey::from_bare(database_id, "metrics").vshard()
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
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "metrics").vshard()
    );
    assert_eq!(calls[0].3, "metrics");
    // ILP payload must contain the collection name and lite_id.
    assert!(calls[0].4.contains("metrics"));
    assert!(calls[0].4.contains("lite-1"));
}

// ── Test: dispatcher returns Err → rejection ACK, no panic ──────────────

#[tokio::test]
async fn unknown_dispatch_outcome_closes_without_ack() {
    let mut session = make_session();
    authenticate(&mut session);
    let mock = MockDispatcher::err();
    let msg = make_push_msg("metrics");

    let frame = session.handle_timeseries_push(&msg, &mock).await;

    assert!(frame.is_none());
}

#[tokio::test]
async fn transient_dispatch_outcome_closes_without_ack() {
    let mut session = make_session();
    authenticate(&mut session);
    let mock = MockDispatcher::retryable();
    let mut msg = make_push_msg("metrics");
    msg.seq = 9;

    let frame = session.handle_timeseries_push(&msg, &mock).await;

    assert!(frame.is_none());
}
#[tokio::test]
async fn malformed_core_payload_closes_without_ack() {
    let mut session = make_session();
    authenticate(&mut session);
    let (mock, _) = MockDispatcher::with(|| Ok(Vec::new()));
    assert!(
        session
            .handle_timeseries_push(&make_push_msg("metrics"), &mock)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn typed_refusal_and_hold_preserve_actual_frontiers() {
    use crate::bridge::envelope::{ErrorCode, SyncHold};
    let mut session = make_session();
    authenticate(&mut session);
    let (mock, _) = MockDispatcher::with(|| {
        Err(crate::Error::DataPlane(ErrorCode::SyncRejected {
            violation: nodedb_types::sync::violation::ViolationType::PermissionDenied,
            applied_seq: 19,
            provenance: nodedb_types::sync::wire::SyncProvenance {
                producer_id: 1,
                epoch: 1,
                stream_id: 2,
                seq: 5,
            },
        }))
    });
    let mut msg = make_push_msg("metrics");
    msg.seq = 5;
    let ack: TimeseriesAckMsg = session
        .handle_timeseries_push(&msg, &mock)
        .await
        .expect("typed ack")
        .decode_body()
        .expect("ack body");
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    assert_eq!(ack.applied_seq, 19);
    assert_eq!(ack.accepted, 0);
    let (mock, _) = MockDispatcher::with(|| {
        Err(crate::Error::DataPlane(ErrorCode::SyncNotApplied {
            hold: SyncHold::Gap { expected: 3 },
            applied_seq: 2,
        }))
    });
    let ack: TimeseriesAckMsg = session
        .handle_timeseries_push(&msg, &mock)
        .await
        .expect("typed ack")
        .decode_body()
        .expect("ack body");
    assert_eq!(ack.status, AckStatus::Gap { expected: 3 });
    assert_eq!(ack.applied_seq, 2);
}

#[tokio::test]
async fn inconsistent_sample_count_closes_before_dispatch() {
    let mut session = make_session();
    authenticate(&mut session);
    let (mock, calls) = MockDispatcher::ok();
    let mut msg = make_push_msg("metrics");
    msg.sample_count = 2;
    assert!(session.handle_timeseries_push(&msg, &mock).await.is_none());
    assert!(calls.lock().expect("calls").is_empty());
}
