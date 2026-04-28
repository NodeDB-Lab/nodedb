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
use crate::types::{TenantId, VShardId};

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Encapsulates the async Data Plane dispatch for a decoded timeseries push.
///
/// Callers supply a concrete implementation so that the handler can complete
/// ingest atomically with ACK generation. This makes it structurally
/// impossible to ACK a push without attempting dispatch.
#[async_trait]
pub trait TimeseriesDispatcher: Send + Sync {
    async fn dispatch_ingest(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        ilp_payload: String,
    ) -> crate::Result<()>;
}

// ── SharedState adapter ──────────────────────────────────────────────────────

/// Production dispatcher: routes the ingest to the Data Plane via the SPSC
/// bridge using `EventSource::CrdtSync` so that AFTER triggers are not
/// re-fired on synced data.
pub struct SharedStateDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
}

#[async_trait]
impl<'a> TimeseriesDispatcher for SharedStateDispatcher<'a> {
    async fn dispatch_ingest(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        ilp_payload: String,
    ) -> crate::Result<()> {
        use crate::bridge::envelope::PhysicalPlan;
        use crate::bridge::physical_plan::TimeseriesOp;
        use crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source;
        use crate::event::EventSource;

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload: ilp_payload.into_bytes(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
        });

        dispatch_to_data_plane_with_source(
            self.shared,
            tenant_id,
            vshard,
            plan,
            0,
            EventSource::CrdtSync,
        )
        .await
        .map(|_| ())
    }
}

// ── NoOp dispatcher (loud failure) ──────────────────────────────────────────

/// Dispatcher used when `SharedState` is unavailable at a call site.
///
/// Returns a loud `Internal` error — this is intentionally NOT a silent
/// no-op. If this path is reached it means the listener wiring is wrong
/// and the push would otherwise be silently dropped after being ACKed.
pub struct NoOpDispatcher;

#[async_trait]
impl TimeseriesDispatcher for NoOpDispatcher {
    async fn dispatch_ingest(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _ilp_payload: String,
    ) -> crate::Result<()> {
        Err(crate::Error::Internal {
            detail: "timeseries push routed through path lacking SharedState; \
                     check listener wiring — push was ACKed but NOT ingested"
                .to_string(),
        })
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
            let ack = TimeseriesAckMsg {
                collection: msg.collection.clone(),
                accepted: 0,
                rejected: msg.sample_count,
                lsn: 0,
            };
            return SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack);
        }

        // Decode Gorilla blocks to verify integrity.
        let timestamps = nodedb_codec::GorillaDecoder::new(&msg.ts_block).decode_all();
        let values = nodedb_codec::GorillaDecoder::new(&msg.val_block).decode_all();

        let decoded_count = timestamps.len().min(values.len());
        if decoded_count == 0 {
            let ack = TimeseriesAckMsg {
                collection: msg.collection.clone(),
                accepted: 0,
                rejected: msg.sample_count,
                lsn: 0,
            };
            return SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack);
        }

        self.mutations_processed += decoded_count as u64;

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

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard = VShardId::from_collection(&msg.collection);

        match dispatcher
            .dispatch_ingest(tenant_id, vshard, msg.collection.clone(), ilp_lines)
            .await
        {
            Ok(()) => {
                let ack = TimeseriesAckMsg {
                    collection: msg.collection.clone(),
                    accepted: decoded_count as u64,
                    rejected: msg.sample_count.saturating_sub(decoded_count as u64),
                    lsn: self.mutations_processed,
                };
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    error = %e,
                    "timeseries ingest dispatch failed; reporting samples as rejected"
                );
                let ack = TimeseriesAckMsg {
                    collection: msg.collection.clone(),
                    accepted: 0,
                    rejected: msg.sample_count,
                    lsn: self.mutations_processed,
                };
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use std::sync::{Arc, Mutex};

    // ── Mock dispatcher ──────────────────────────────────────────────────────

    type MockCallLog = Arc<Mutex<Vec<(TenantId, String, String)>>>;

    struct MockDispatcher {
        calls: MockCallLog,
        result: crate::Result<()>,
    }

    impl MockDispatcher {
        fn ok() -> (Self, MockCallLog) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    calls: calls.clone(),
                    result: Ok(()),
                },
                calls,
            )
        }

        fn err() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                result: Err(crate::Error::Internal {
                    detail: "mock failure".to_string(),
                }),
            }
        }
    }

    #[async_trait]
    impl TimeseriesDispatcher for MockDispatcher {
        async fn dispatch_ingest(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            collection: String,
            ilp_payload: String,
        ) -> crate::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((tenant_id, collection, ilp_payload));
            // Clone the result by matching on it.
            match &self.result {
                Ok(()) => Ok(()),
                Err(e) => Err(crate::Error::Internal {
                    detail: e.to_string(),
                }),
            }
        }
    }

    fn make_session() -> SyncSession {
        SyncSession::new("test-session".to_string())
    }

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
            sample_count: 1,
            ts_block,
            val_block,
            series_block: Vec::new(),
            min_ts: 1_000,
            max_ts: 1_000,
            watermarks: std::collections::HashMap::new(),
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

    // ── Test: authenticated, successful dispatch → accepted ACK ─────────────

    #[tokio::test]
    async fn test_authenticated_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, calls) = MockDispatcher::ok();
        let msg = make_push_msg("metrics");

        let frame = session.handle_timeseries_push(&msg, &mock).await;

        assert!(frame.is_some());
        let decoded: TimeseriesAckMsg = frame.unwrap().decode_body().unwrap();
        assert_eq!(decoded.accepted, 1, "one decoded sample should be accepted");
        assert_eq!(decoded.rejected, 0);

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "dispatcher must be called exactly once");
        assert_eq!(calls[0].1, "metrics");
        // ILP payload must contain the collection name and lite_id.
        assert!(calls[0].2.contains("metrics"));
        assert!(calls[0].2.contains("lite-1"));
    }

    // ── Test: dispatcher returns Err → rejection ACK, no panic ──────────────

    #[tokio::test]
    async fn test_dispatch_failure_returns_rejection_ack() {
        let mut session = make_session();
        session.authenticated = true;
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
    }
}
