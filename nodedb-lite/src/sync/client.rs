//! WebSocket sync client for edge ↔ Origin communication.
//!
//! Runs as a background task. Handles:
//! - Connection with auto-reconnect (exponential backoff, 1s→60s cap)
//! - JWT-authenticated handshake with vector clock exchange
//! - Delta push (batched, dedup by mutation_id)
//! - Delta/shape receive from Origin
//! - Compensation handling on rejection
//! - Keepalive ping/pong

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use nodedb_types::sync::wire::{
    DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, HandshakeAckMsg, HandshakeMsg, PingPongMsg,
    ResyncReason, ResyncRequestMsg, ShapeDeltaMsg, ShapeSnapshotMsg, SyncFrame, SyncMessageType,
    VectorClockSyncMsg,
};

use super::clock::VectorClock;
use super::compensation::{CompensationEvent, CompensationRegistry};
use super::shapes::ShapeManager;
use crate::engine::crdt::engine::PendingDelta;

/// Sync client configuration.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// WebSocket URL to the Origin sync endpoint (e.g., `wss://api.nodedb.cloud/sync`).
    pub url: String,
    /// JWT bearer token for authentication.
    pub jwt_token: String,
    /// Client version string (sent in handshake).
    pub client_version: String,
    /// Minimum backoff on reconnect.
    pub min_backoff: Duration,
    /// Maximum backoff on reconnect.
    pub max_backoff: Duration,
    /// Keepalive ping interval.
    pub ping_interval: Duration,
    /// Maximum deltas to batch in a single push.
    pub max_batch_size: usize,
}

impl SyncConfig {
    pub fn new(url: impl Into<String>, jwt_token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            jwt_token: jwt_token.into(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            ping_interval: Duration::from_secs(30),
            max_batch_size: 100,
        }
    }
}

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Not connected, not trying.
    Disconnected,
    /// Attempting to connect.
    Connecting,
    /// Connected and authenticated.
    Connected,
    /// Connection lost, backing off before retry.
    Reconnecting,
}

/// Sync client — manages the WebSocket connection to Origin.
///
/// The client runs as a background Tokio task. It:
/// 1. Connects to Origin via WebSocket
/// 2. Sends handshake with JWT + vector clock + shape subscriptions
/// 3. Pushes accumulated CRDT deltas
/// 4. Receives shape snapshots and incremental deltas
/// 5. Handles rejections via CompensationRegistry
/// 6. Auto-reconnects with exponential backoff on disconnect
pub struct SyncClient {
    config: SyncConfig,
    state: Arc<Mutex<SyncState>>,
    clock: Arc<Mutex<VectorClock>>,
    shapes: Arc<Mutex<ShapeManager>>,
    compensation: Arc<CompensationRegistry>,
    /// Session ID assigned by Origin after handshake.
    session_id: Arc<Mutex<Option<String>>>,
    /// Peer ID of this Lite client (for CRDT identity).
    peer_id: u64,
    /// Lite instance identity (UUID v7) for fork detection.
    lite_id: Option<String>,
    /// Monotonic epoch counter for fork detection.
    epoch: Option<u64>,
    /// Sequence tracker: per-shape, the last LSN received from Origin.
    /// Used to detect gaps in the incoming delta stream.
    last_seen_lsn: Arc<Mutex<std::collections::HashMap<String, u64>>>,
    /// Whether a re-sync request has been sent for this connection.
    /// Prevents flooding Origin with multiple re-sync requests.
    resync_requested: Arc<Mutex<bool>>,
    /// Pending re-sync request to send to Origin (set by gap detection,
    /// consumed by the delta push loop).
    pending_resync: Arc<Mutex<Option<ResyncRequestMsg>>>,
}

impl SyncClient {
    /// Create a new sync client (does not connect yet).
    pub fn new(config: SyncConfig, peer_id: u64) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(SyncState::Disconnected)),
            clock: Arc::new(Mutex::new(VectorClock::new())),
            shapes: Arc::new(Mutex::new(ShapeManager::new())),
            compensation: Arc::new(CompensationRegistry::new()),
            session_id: Arc::new(Mutex::new(None)),
            peer_id,
            lite_id: None,
            epoch: None,
            last_seen_lsn: Arc::new(Mutex::new(std::collections::HashMap::new())),
            resync_requested: Arc::new(Mutex::new(false)),
            pending_resync: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the Lite identity for fork detection (called after LiteIdentity::load_or_create).
    pub fn set_identity(&mut self, lite_id: String, epoch: u64) {
        self.lite_id = Some(lite_id);
        self.epoch = Some(epoch);
    }

    /// Current connection state.
    pub async fn state(&self) -> SyncState {
        *self.state.lock().await
    }

    /// Register a compensation handler.
    pub fn set_compensation_handler(
        &self,
        handler: Arc<dyn super::compensation::CompensationHandler>,
    ) {
        self.compensation.set_handler(handler);
    }

    /// Access the shape manager (for subscribing/unsubscribing).
    pub fn shapes(&self) -> &Arc<Mutex<ShapeManager>> {
        &self.shapes
    }

    /// Access the vector clock.
    pub fn clock(&self) -> &Arc<Mutex<VectorClock>> {
        &self.clock
    }

    /// Build a handshake message from current state.
    pub async fn build_handshake(&self) -> HandshakeMsg {
        let clock = self.clock.lock().await;
        let shapes = self.shapes.lock().await;

        // Convert our VectorClock to the wire format expected by Origin.
        let wire_clock = clock.to_wire();
        let mut vector_clock = std::collections::HashMap::new();
        // Origin expects: { collection: { doc_id: lamport_ts } }
        // We send a simplified version: { "_global": { peer_hex: counter } }
        vector_clock.insert("_global".to_string(), wire_clock);

        HandshakeMsg {
            jwt_token: self.config.jwt_token.clone(),
            vector_clock,
            subscribed_shapes: shapes.active_shape_ids(),
            client_version: self.config.client_version.clone(),
            lite_id: self.lite_id.clone().unwrap_or_default(),
            epoch: self.epoch.unwrap_or(0),
        }
    }

    /// Process a handshake acknowledgment from Origin.
    pub async fn handle_handshake_ack(&self, ack: &HandshakeAckMsg) -> bool {
        if !ack.success {
            tracing::warn!(
                error = ack.error.as_deref().unwrap_or("unknown"),
                "handshake rejected by Origin"
            );
            return false;
        }

        // Store session ID.
        *self.session_id.lock().await = Some(ack.session_id.clone());

        // Update our clock with Origin's.
        let mut clock = self.clock.lock().await;
        for (peer_hex, &counter) in &ack.server_clock {
            if let Ok(peer_id) = u64::from_str_radix(peer_hex, 16) {
                clock.advance(peer_id, counter);
            }
        }

        *self.state.lock().await = SyncState::Connected;
        tracing::info!(session = %ack.session_id, "sync handshake accepted");
        true
    }

    /// Build DeltaPush messages from pending deltas.
    ///
    /// Each message includes a CRC32C checksum of the delta payload for
    /// integrity verification at Origin. Hardware-accelerated on x86_64/aarch64.
    pub fn build_delta_pushes(&self, pending: &[PendingDelta]) -> Vec<DeltaPushMsg> {
        pending
            .iter()
            .take(self.config.max_batch_size)
            .map(|delta| DeltaPushMsg {
                collection: delta.collection.clone(),
                document_id: delta.document_id.clone(),
                checksum: crc32c::crc32c(&delta.delta_bytes),
                delta: delta.delta_bytes.clone(),
                peer_id: self.peer_id,
                mutation_id: delta.mutation_id,
            })
            .collect()
    }

    /// Process a DeltaAck from Origin.
    pub async fn handle_delta_ack(&self, ack: &DeltaAckMsg) {
        let mut clock = self.clock.lock().await;
        // The Origin assigned an LSN — advance our view of Origin's state.
        clock.advance(0, ack.lsn); // peer 0 = Origin convention.
        tracing::debug!(
            mutation_id = ack.mutation_id,
            lsn = ack.lsn,
            "delta acknowledged"
        );
    }

    /// Process a DeltaReject from Origin.
    pub fn handle_delta_reject(&self, reject: &DeltaRejectMsg) {
        tracing::warn!(
            mutation_id = reject.mutation_id,
            reason = %reject.reason,
            "delta rejected by Origin"
        );

        if let Some(hint) = &reject.compensation {
            self.compensation.dispatch(CompensationEvent {
                mutation_id: reject.mutation_id,
                collection: String::new(), // Filled by the caller from pending deltas.
                document_id: String::new(),
                hint: hint.clone(),
            });
        }
    }

    /// Process a ShapeSnapshot from Origin.
    pub async fn handle_shape_snapshot(&self, msg: &ShapeSnapshotMsg) {
        let mut shapes = self.shapes.lock().await;
        shapes.mark_snapshot_loaded(&msg.shape_id, msg.snapshot_lsn);
        tracing::info!(
            shape_id = %msg.shape_id,
            lsn = msg.snapshot_lsn,
            doc_count = msg.doc_count,
            "shape snapshot received"
        );
    }

    /// Process a ShapeDelta from Origin.
    pub async fn handle_shape_delta(&self, msg: &ShapeDeltaMsg) {
        let mut shapes = self.shapes.lock().await;
        shapes.advance_lsn(&msg.shape_id, msg.lsn);
        tracing::debug!(
            shape_id = %msg.shape_id,
            collection = %msg.collection,
            doc_id = %msg.document_id,
            lsn = msg.lsn,
            "shape delta received"
        );
    }

    /// Process a VectorClockSync from Origin.
    pub async fn handle_clock_sync(&self, msg: &VectorClockSyncMsg) {
        let mut clock = self.clock.lock().await;
        for (peer_hex, &counter) in &msg.clocks {
            if let Ok(peer_id) = u64::from_str_radix(peer_hex, 16) {
                clock.advance(peer_id, counter);
            }
        }
    }

    /// Check an incoming ShapeDelta for sequence gaps.
    ///
    /// For each shape, we track the last LSN received. If the incoming LSN
    /// is not contiguous (gap > 1), this indicates missing deltas in the stream.
    /// Returns `Some(ResyncRequestMsg)` if a gap is detected, `None` otherwise.
    ///
    /// Note: LSNs may not be strictly +1 sequential (Origin may skip LSNs for
    /// other shapes), so we only flag a gap when the new LSN is MORE than 1
    /// ahead of the last seen LSN for the SAME shape. A gap means deltas were
    /// lost in transit.
    pub async fn check_sequence_gap(&self, shape_id: &str, lsn: u64) -> Option<ResyncRequestMsg> {
        // Don't send multiple re-sync requests per connection.
        if *self.resync_requested.lock().await {
            return None;
        }

        let mut tracker = self.last_seen_lsn.lock().await;
        if let Some(&last_lsn) = tracker.get(shape_id)
            && lsn > last_lsn + 1
        {
            // Gap detected: we expected last_lsn+1 but got lsn.
            tracing::warn!(
                shape_id,
                expected = last_lsn + 1,
                received = lsn,
                "sequence gap detected in incoming delta stream"
            );
            tracker.insert(shape_id.to_string(), lsn);

            // Mark that we've requested re-sync for this connection.
            *self.resync_requested.lock().await = true;

            return Some(ResyncRequestMsg {
                reason: ResyncReason::SequenceGap {
                    expected: last_lsn + 1,
                    received: lsn,
                },
                from_mutation_id: last_lsn + 1,
                collection: String::new(), // All collections.
            });
        }
        tracker.insert(shape_id.to_string(), lsn);
        None
    }

    /// Reset sequence tracking state on reconnect.
    pub async fn reset_sequence_tracking(&self) {
        self.last_seen_lsn.lock().await.clear();
        *self.resync_requested.lock().await = false;
        *self.pending_resync.lock().await = None;
    }

    /// Store a pending re-sync request (set by gap detection in receive loop).
    pub async fn set_pending_resync(&self, msg: ResyncRequestMsg) {
        *self.pending_resync.lock().await = Some(msg);
    }

    /// Take the pending re-sync request (consumed by delta push loop).
    pub async fn take_pending_resync(&self) -> Option<ResyncRequestMsg> {
        self.pending_resync.lock().await.take()
    }

    /// Build a ping frame.
    pub fn build_ping(&self) -> SyncFrame {
        let ping = PingPongMsg {
            timestamp_ms: crate::runtime::now_millis(),
            is_pong: false,
        };
        SyncFrame::encode_or_empty(SyncMessageType::PingPong, &ping)
    }

    /// Calculate backoff duration for reconnection attempt N.
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        let base = self.config.min_backoff.as_millis() as u64;
        let max = self.config.max_backoff.as_millis() as u64;
        let delay = (base * 2u64.saturating_pow(attempt)).min(max);
        Duration::from_millis(delay)
    }

    /// Set the connection state.
    pub async fn set_state(&self, new_state: SyncState) {
        *self.state.lock().await = new_state;
    }

    /// Access the compensation registry.
    pub fn compensation(&self) -> &Arc<CompensationRegistry> {
        &self.compensation
    }

    /// Access config.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Peer ID.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> SyncConfig {
        SyncConfig::new("wss://localhost:9090/sync", "test.jwt.token")
    }

    #[tokio::test]
    async fn initial_state_is_disconnected() {
        let client = SyncClient::new(make_config(), 1);
        assert_eq!(client.state().await, SyncState::Disconnected);
    }

    #[tokio::test]
    async fn build_handshake() {
        let client = SyncClient::new(make_config(), 1);

        {
            let mut shapes = client.shapes().lock().await;
            shapes.subscribe(nodedb_types::sync::shape::ShapeDefinition {
                shape_id: "s1".into(),
                tenant_id: 1,
                shape_type: nodedb_types::sync::shape::ShapeType::Document {
                    collection: "orders".into(),
                    predicate: Vec::new(),
                },
                description: "test".into(),
                field_filter: vec![],
            });
        }

        let hs = client.build_handshake().await;
        assert_eq!(hs.jwt_token, "test.jwt.token");
        assert_eq!(hs.subscribed_shapes, vec!["s1"]);
    }

    #[tokio::test]
    async fn handle_handshake_ack_success() {
        let client = SyncClient::new(make_config(), 1);
        let ack = HandshakeAckMsg {
            success: true,
            session_id: "sess-123".into(),
            server_clock: std::collections::HashMap::new(),
            error: None,
            fork_detected: false,
        };

        assert!(client.handle_handshake_ack(&ack).await);
        assert_eq!(client.state().await, SyncState::Connected);
    }

    #[tokio::test]
    async fn handle_handshake_ack_failure() {
        let client = SyncClient::new(make_config(), 1);
        let ack = HandshakeAckMsg {
            success: false,
            session_id: String::new(),
            server_clock: std::collections::HashMap::new(),
            error: Some("invalid token".into()),
            fork_detected: false,
        };

        assert!(!client.handle_handshake_ack(&ack).await);
        assert_eq!(client.state().await, SyncState::Disconnected);
    }

    #[tokio::test]
    async fn build_delta_pushes() {
        let client = SyncClient::new(make_config(), 42);
        let pending = vec![
            PendingDelta {
                mutation_id: 1,
                collection: "orders".into(),
                document_id: "o1".into(),
                delta_bytes: vec![1, 2, 3],
            },
            PendingDelta {
                mutation_id: 2,
                collection: "users".into(),
                document_id: "u1".into(),
                delta_bytes: vec![4, 5, 6],
            },
        ];

        let msgs = client.build_delta_pushes(&pending);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].peer_id, 42);
        assert_eq!(msgs[0].mutation_id, 1);
        assert_eq!(msgs[1].collection, "users");
    }

    #[tokio::test]
    async fn handle_delta_ack_advances_clock() {
        let client = SyncClient::new(make_config(), 1);
        client
            .handle_delta_ack(&DeltaAckMsg {
                mutation_id: 1,
                lsn: 42,
            })
            .await;

        let clock = client.clock().lock().await;
        assert_eq!(clock.get(0), 42); // peer 0 = Origin.
    }

    #[test]
    fn handle_delta_reject_dispatches_compensation() {
        let client = SyncClient::new(make_config(), 1);

        let count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let count_clone = count.clone();
        client.set_compensation_handler(Arc::new(move |_: CompensationEvent| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));

        client.handle_delta_reject(&DeltaRejectMsg {
            mutation_id: 1,
            reason: "unique violation".into(),
            compensation: Some(
                nodedb_types::sync::compensation::CompensationHint::UniqueViolation {
                    field: "email".into(),
                    conflicting_value: "a@b.com".into(),
                },
            ),
        });

        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn backoff_exponential_with_cap() {
        let client = SyncClient::new(make_config(), 1);
        assert_eq!(client.backoff_duration(0), Duration::from_secs(1));
        assert_eq!(client.backoff_duration(1), Duration::from_secs(2));
        assert_eq!(client.backoff_duration(2), Duration::from_secs(4));
        assert_eq!(client.backoff_duration(3), Duration::from_secs(8));
        // Should cap at max_backoff (60s).
        assert_eq!(client.backoff_duration(10), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn shape_snapshot_updates_manager() {
        let client = SyncClient::new(make_config(), 1);
        {
            let mut shapes = client.shapes().lock().await;
            shapes.subscribe(nodedb_types::sync::shape::ShapeDefinition {
                shape_id: "s1".into(),
                tenant_id: 1,
                shape_type: nodedb_types::sync::shape::ShapeType::Vector {
                    collection: "vecs".into(),
                    field_name: None,
                },
                description: "test".into(),
                field_filter: vec![],
            });
        }

        client
            .handle_shape_snapshot(&ShapeSnapshotMsg {
                shape_id: "s1".into(),
                data: Vec::new(),
                snapshot_lsn: 100,
                doc_count: 50,
            })
            .await;

        let shapes = client.shapes().lock().await;
        let sub = shapes.get("s1").unwrap();
        assert!(sub.snapshot_loaded);
        assert_eq!(sub.last_lsn, 100);
    }

    #[test]
    fn ping_frame_is_valid() {
        let client = SyncClient::new(make_config(), 1);
        let frame = client.build_ping();
        assert_eq!(frame.msg_type, SyncMessageType::PingPong);
        assert!(!frame.body.is_empty());
    }

    #[tokio::test]
    async fn sequence_gap_detection_no_gap() {
        let client = SyncClient::new(make_config(), 1);
        // Sequential LSNs — no gap.
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        assert!(client.check_sequence_gap("s1", 2).await.is_none());
        assert!(client.check_sequence_gap("s1", 3).await.is_none());
    }

    #[tokio::test]
    async fn sequence_gap_detection_with_gap() {
        let client = SyncClient::new(make_config(), 1);
        // First delta at LSN 1.
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        // Gap: expected 2, got 5.
        let resync = client.check_sequence_gap("s1", 5).await;
        assert!(resync.is_some());
        let msg = resync.unwrap();
        assert_eq!(msg.from_mutation_id, 2); // Resume from missing LSN.
        assert!(matches!(
            msg.reason,
            ResyncReason::SequenceGap {
                expected: 2,
                received: 5
            }
        ));
    }

    #[tokio::test]
    async fn sequence_gap_only_one_resync_per_connection() {
        let client = SyncClient::new(make_config(), 1);
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        // First gap — triggers resync.
        assert!(client.check_sequence_gap("s1", 10).await.is_some());
        // Second gap — suppressed (already requested).
        assert!(client.check_sequence_gap("s1", 20).await.is_none());
    }

    #[tokio::test]
    async fn reset_sequence_tracking_clears_state() {
        let client = SyncClient::new(make_config(), 1);
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        assert!(client.check_sequence_gap("s1", 10).await.is_some());

        // Reset (simulates reconnect).
        client.reset_sequence_tracking().await;

        // Should work again after reset.
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        assert!(client.check_sequence_gap("s1", 5).await.is_some());
    }

    #[tokio::test]
    async fn delta_push_includes_crc32c() {
        let client = SyncClient::new(make_config(), 42);
        let delta_bytes = vec![1, 2, 3, 4, 5];
        let expected_crc = crc32c::crc32c(&delta_bytes);
        let pending = vec![PendingDelta {
            mutation_id: 1,
            collection: "test".into(),
            document_id: "d1".into(),
            delta_bytes,
        }];
        let msgs = client.build_delta_pushes(&pending);
        assert_eq!(msgs[0].checksum, expected_crc);
        assert_ne!(msgs[0].checksum, 0);
    }
}
