//! WebSocket transport — actual network I/O for the sync client.
//!
//! Connects to Origin via `tokio-tungstenite`, runs a message loop that
//! dispatches incoming frames to `SyncClient` handlers, and pushes pending
//! deltas on a timer.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

use super::client::{SyncClient, SyncState};
use crate::engine::crdt::engine::PendingDelta;
use crate::error::LiteError;

/// Callback interface for the sync runner to read/write pending deltas
/// from the owning `NodeDbLite`. This avoids the runner owning the database.
pub trait SyncDelegate: Send + Sync + 'static {
    /// Get all pending CRDT deltas to push to Origin.
    fn pending_deltas(&self) -> Vec<PendingDelta>;
    /// Acknowledge deltas up to the given mutation_id.
    fn acknowledge(&self, mutation_id: u64);
    /// Reject a specific delta (rollback optimistic state).
    fn reject(&self, mutation_id: u64);
    /// Import remote deltas from Origin into local CRDT state.
    fn import_remote(&self, data: &[u8]);
}

/// Run the sync loop — connects, handshakes, pushes/receives, reconnects.
///
/// This function runs forever (until the task is cancelled). It handles:
/// 1. Connect to Origin WebSocket
/// 2. Send handshake, wait for ACK
/// 3. Push pending deltas in batches
/// 4. Receive and dispatch incoming frames
/// 5. Send periodic pings
/// 6. On disconnect: exponential backoff, then retry from step 1
pub async fn run_sync_loop(client: Arc<SyncClient>, delegate: Arc<dyn SyncDelegate>) {
    let mut attempt: u32 = 0;

    loop {
        client.set_state(SyncState::Connecting).await;
        tracing::info!(url = %client.config().url, attempt, "connecting to Origin");

        match connect_and_run(&client, &delegate).await {
            Ok(()) => {
                // Clean disconnect (server closed gracefully).
                tracing::info!("sync connection closed cleanly");
                attempt = 0;
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, "sync connection failed");
            }
        }

        client.set_state(SyncState::Reconnecting).await;
        let backoff = client.backoff_duration(attempt);
        tracing::info!(
            backoff_ms = backoff.as_millis(),
            "reconnecting after backoff"
        );
        tokio::time::sleep(backoff).await;
        attempt = attempt.saturating_add(1);
    }
}

/// Single connection attempt: connect → handshake → message loop.
async fn connect_and_run(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
) -> Result<(), LiteError> {
    // Reset sequence tracking for a fresh connection.
    client.reset_sequence_tracking().await;

    // ── Connect ──
    let (ws_stream, _response) = tokio_tungstenite::connect_async(&client.config().url)
        .await
        .map_err(|e| LiteError::Sync {
            detail: format!("WebSocket connect failed: {e}"),
        })?;

    let (mut sink, mut stream) = ws_stream.split();

    // ── Handshake ──
    let handshake = client.build_handshake().await;
    let frame = SyncFrame::encode_or_empty(SyncMessageType::Handshake, &handshake);
    sink.send(Message::Binary(frame.to_bytes().into()))
        .await
        .map_err(|e| LiteError::Sync {
            detail: format!("handshake send failed: {e}"),
        })?;

    // Wait for HandshakeAck.
    let ack_msg = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .map_err(|_| LiteError::Sync {
            detail: "handshake timeout".to_string(),
        })?
        .ok_or_else(|| LiteError::Sync {
            detail: "connection closed before handshake ack".to_string(),
        })?
        .map_err(|e| LiteError::Sync {
            detail: format!("handshake read error: {e}"),
        })?;

    let ack_bytes = match &ack_msg {
        Message::Binary(b) => b.as_ref(),
        _ => {
            return Err(LiteError::Sync {
                detail: "expected binary handshake ack".to_string(),
            });
        }
    };

    let ack_frame = SyncFrame::from_bytes(ack_bytes).ok_or_else(|| LiteError::Sync {
        detail: "invalid handshake ack frame".to_string(),
    })?;

    if ack_frame.msg_type != SyncMessageType::HandshakeAck {
        return Err(LiteError::Sync {
            detail: format!("expected HandshakeAck, got {:?}", ack_frame.msg_type),
        });
    }

    let ack: nodedb_types::sync::wire::HandshakeAckMsg =
        ack_frame.decode_body().ok_or_else(|| LiteError::Sync {
            detail: "failed to decode HandshakeAck".to_string(),
        })?;

    if !client.handle_handshake_ack(&ack).await {
        return Err(LiteError::Sync {
            detail: format!("handshake rejected: {}", ack.error.unwrap_or_default()),
        });
    }

    // ── Message loop ──
    let sink = Arc::new(Mutex::new(sink));

    // Spawn delta push task.
    let push_sink = Arc::clone(&sink);
    let push_client = Arc::clone(client);
    let push_delegate = Arc::clone(delegate);
    let push_handle = tokio::spawn(async move {
        delta_push_loop(&push_client, &push_delegate, &push_sink).await;
    });

    // Spawn ping task.
    let ping_sink = Arc::clone(&sink);
    let ping_client = Arc::clone(client);
    let ping_handle = tokio::spawn(async move {
        ping_loop(&ping_client, &ping_sink).await;
    });

    // Receive loop (runs on this task).
    let recv_result = receive_loop(client, delegate, &mut stream).await;

    // Cancel background tasks on disconnect.
    push_handle.abort();
    ping_handle.abort();

    client.set_state(SyncState::Disconnected).await;
    recv_result
}

/// Receive and dispatch incoming frames from Origin.
async fn receive_loop<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    stream: &mut S,
) -> Result<(), LiteError>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg_result) = stream.next().await {
        let msg = msg_result.map_err(|e| LiteError::Sync {
            detail: format!("WebSocket read error: {e}"),
        })?;

        let bytes = match &msg {
            Message::Binary(b) => b.as_ref(),
            Message::Close(_) => return Ok(()),
            Message::Ping(_) | Message::Pong(_) => continue,
            _ => continue,
        };

        let Some(frame) = SyncFrame::from_bytes(bytes) else {
            tracing::warn!("received malformed frame, skipping");
            continue;
        };

        dispatch_frame(client, delegate, &frame).await;
    }

    Ok(())
}

/// Dispatch a single incoming frame to the appropriate handler.
async fn dispatch_frame(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    match frame.msg_type {
        SyncMessageType::DeltaAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::DeltaAckMsg>() {
                delegate.acknowledge(ack.mutation_id);
                client.handle_delta_ack(&ack).await;
            }
        }
        SyncMessageType::ResyncRequest => {
            // Origin is requesting us to re-sync. Log and let the push loop
            // re-send from the requested mutation ID on next tick.
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::ResyncRequestMsg>() {
                tracing::warn!(
                    reason = ?msg.reason,
                    from_mutation_id = msg.from_mutation_id,
                    collection = %msg.collection,
                    "Origin requested re-sync"
                );
            }
        }
        SyncMessageType::DeltaReject => {
            if let Some(reject) = frame.decode_body::<nodedb_types::sync::wire::DeltaRejectMsg>() {
                delegate.reject(reject.mutation_id);
                client.handle_delta_reject(&reject);
            }
        }
        SyncMessageType::ShapeSnapshot => {
            if let Some(snapshot) =
                frame.decode_body::<nodedb_types::sync::wire::ShapeSnapshotMsg>()
            {
                // Import the snapshot data into local CRDT state.
                if !snapshot.data.is_empty() {
                    delegate.import_remote(&snapshot.data);
                }
                client.handle_shape_snapshot(&snapshot).await;
            }
        }
        SyncMessageType::ShapeDelta => {
            if let Some(delta) = frame.decode_body::<nodedb_types::sync::wire::ShapeDeltaMsg>() {
                // Check for sequence gaps before applying.
                if let Some(resync) = client.check_sequence_gap(&delta.shape_id, delta.lsn).await {
                    tracing::warn!(
                        shape_id = %delta.shape_id,
                        "requesting re-sync due to sequence gap"
                    );
                    // The resync request will be sent by the caller if we had
                    // access to the sink here. For now, we log and continue —
                    // the delta push loop can send it.
                    // Store for the push loop to pick up.
                    client.set_pending_resync(resync).await;
                }
                // Apply the incremental delta to local state.
                if !delta.delta.is_empty() {
                    delegate.import_remote(&delta.delta);
                }
                client.handle_shape_delta(&delta).await;
            }
        }
        SyncMessageType::VectorClockSync => {
            if let Some(clock_msg) =
                frame.decode_body::<nodedb_types::sync::wire::VectorClockSyncMsg>()
            {
                client.handle_clock_sync(&clock_msg).await;
            }
        }
        SyncMessageType::PingPong => {
            // Origin sent a ping — we could respond with pong, but our
            // ping_loop handles keepalive. Just log.
            tracing::trace!("received ping/pong from Origin");
        }
        _ => {
            tracing::debug!(msg_type = ?frame.msg_type, "unexpected frame type from Origin");
        }
    }
}

/// Periodically push pending deltas to Origin.
async fn delta_push_loop<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
) where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        interval.tick().await;

        if client.state().await != SyncState::Connected {
            continue;
        }

        // Send pending re-sync request if one was generated by gap detection.
        if let Some(resync) = client.take_pending_resync().await {
            let frame = SyncFrame::encode_or_empty(SyncMessageType::ResyncRequest, &resync);
            let mut sink_guard = sink.lock().await;
            if let Err(e) = sink_guard
                .send(Message::Binary(frame.to_bytes().into()))
                .await
            {
                tracing::warn!(error = %e, "resync request send failed");
                return;
            }
            tracing::info!(
                reason = ?resync.reason,
                from_mutation_id = resync.from_mutation_id,
                "sent ResyncRequest to Origin"
            );
        }

        let pending = delegate.pending_deltas();
        if pending.is_empty() {
            continue;
        }

        let msgs = client.build_delta_pushes(&pending);
        let mut sink_guard = sink.lock().await;

        for msg in &msgs {
            let frame = SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, msg);
            if let Err(e) = sink_guard
                .send(Message::Binary(frame.to_bytes().into()))
                .await
            {
                tracing::warn!(error = %e, "delta push send failed");
                return; // Connection lost — let reconnect handle it.
            }
        }

        tracing::debug!(count = msgs.len(), "pushed deltas to Origin");
    }
}

/// Periodically send ping frames for keepalive.
async fn ping_loop<S>(client: &Arc<SyncClient>, sink: &Arc<Mutex<S>>)
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut interval = tokio::time::interval(client.config().ping_interval);

    loop {
        interval.tick().await;

        if client.state().await != SyncState::Connected {
            continue;
        }

        let frame = client.build_ping();
        let mut sink_guard = sink.lock().await;
        if let Err(e) = sink_guard
            .send(Message::Binary(frame.to_bytes().into()))
            .await
        {
            tracing::warn!(error = %e, "ping send failed");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Mock delegate for testing (uses std::sync::Mutex, not tokio's).
    struct MockDelegate {
        acked_up_to: AtomicU64,
        rejected: std::sync::Mutex<Vec<u64>>,
        imported: std::sync::Mutex<Vec<Vec<u8>>>,
    }

    impl MockDelegate {
        fn new() -> Self {
            Self {
                acked_up_to: AtomicU64::new(0),
                rejected: std::sync::Mutex::new(Vec::new()),
                imported: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl SyncDelegate for MockDelegate {
        fn pending_deltas(&self) -> Vec<PendingDelta> {
            Vec::new()
        }

        fn acknowledge(&self, mutation_id: u64) {
            self.acked_up_to.store(mutation_id, Ordering::Relaxed);
        }

        fn reject(&self, mutation_id: u64) {
            self.rejected.lock().unwrap().push(mutation_id);
        }

        fn import_remote(&self, data: &[u8]) {
            self.imported.lock().unwrap().push(data.to_vec());
        }
    }

    fn make_client() -> Arc<SyncClient> {
        Arc::new(SyncClient::new(
            super::super::client::SyncConfig::new("wss://localhost/sync", "jwt"),
            1,
        ))
    }

    #[tokio::test]
    async fn dispatch_delta_ack() {
        let client = make_client();
        let mock = Arc::new(MockDelegate::new());
        let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

        let ack = nodedb_types::sync::wire::DeltaAckMsg {
            mutation_id: 42,
            lsn: 100,
        };
        let frame = SyncFrame::encode_or_empty(SyncMessageType::DeltaAck, &ack);

        dispatch_frame(&client, &delegate, &frame).await;
        assert_eq!(mock.acked_up_to.load(Ordering::Relaxed), 42);
    }

    #[tokio::test]
    async fn dispatch_delta_reject() {
        let client = make_client();
        let mock = Arc::new(MockDelegate::new());
        let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

        let reject = nodedb_types::sync::wire::DeltaRejectMsg {
            mutation_id: 7,
            reason: "unique violation".into(),
            compensation: None,
        };
        let frame = SyncFrame::encode_or_empty(SyncMessageType::DeltaReject, &reject);

        dispatch_frame(&client, &delegate, &frame).await;
        assert_eq!(*mock.rejected.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn dispatch_shape_delta_imports() {
        let client = make_client();
        let mock = Arc::new(MockDelegate::new());
        let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

        // Subscribe a shape so the handler can advance LSN.
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

        let delta = nodedb_types::sync::wire::ShapeDeltaMsg {
            shape_id: "s1".into(),
            collection: "orders".into(),
            document_id: "o1".into(),
            operation: "INSERT".into(),
            delta: vec![1, 2, 3],
            lsn: 50,
        };
        let frame = SyncFrame::encode_or_empty(SyncMessageType::ShapeDelta, &delta);

        dispatch_frame(&client, &delegate, &frame).await;

        // Delta bytes should have been imported.
        {
            let imported = mock.imported.lock().unwrap();
            assert_eq!(imported.len(), 1);
            assert_eq!(imported[0], vec![1, 2, 3]);
        }

        // Shape LSN should have advanced.
        let shapes = client.shapes().lock().await;
        assert_eq!(shapes.get("s1").unwrap().last_lsn, 50);
    }

    #[tokio::test]
    async fn dispatch_clock_sync() {
        let client = make_client();
        let mock = Arc::new(MockDelegate::new());
        let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

        let clock_msg = nodedb_types::sync::wire::VectorClockSyncMsg {
            clocks: {
                let mut m = std::collections::HashMap::new();
                m.insert("0000000000000001".to_string(), 99u64);
                m
            },
            sender_id: 0,
        };
        let frame = SyncFrame::encode_or_empty(SyncMessageType::VectorClockSync, &clock_msg);

        dispatch_frame(&client, &delegate, &frame).await;

        let clock = client.clock().lock().await;
        assert_eq!(clock.get(1), 99);
    }
}
