//! [`ArrayDeliveryRegistry`] — per-session bounded mpsc channels for
//! outbound array CRDT frames.
//!
//! Architecture mirror of `event::crdt_sync::CrdtSyncDelivery` for the
//! array sync outbound path. Each connected Lite session registers a
//! receiver here on authenticate; the WebSocket send loop drains it.
//!
//! Both the registry and the channels live entirely on the Control Plane
//! (Tokio, `Send + Sync`). No Data Plane or Event Bus crossing.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Capacity of each session's outbound frame channel.
///
/// Large enough to buffer a burst of ops after reconnect without OOM
/// risk. Older frames are dropped if the channel fills (Lite will
/// catch up via snapshot when it reconnects).
const CHANNEL_CAPACITY: usize = 1_024;

/// An encoded binary frame ready to write to the WebSocket.
pub type ArrayFrame = Vec<u8>;

/// Registry of connected Lite sessions for outbound array frame delivery.
///
/// Thread-safe: `register` / `unregister` from the sync listener task;
/// `enqueue` from `ArrayFanout` after an op is applied.
pub struct ArrayDeliveryRegistry {
    sessions: RwLock<HashMap<String, mpsc::Sender<ArrayFrame>>>,
    /// Monotonic count of sessions registered since startup.
    pub sessions_registered: AtomicU64,
    /// Monotonic count of frames dropped due to back-pressure.
    pub frames_dropped: AtomicU64,
}

impl Default for ArrayDeliveryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayDeliveryRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            sessions_registered: AtomicU64::new(0),
            frames_dropped: AtomicU64::new(0),
        }
    }

    /// Register a session and return the `Receiver` end of its delivery
    /// channel.  The sync listener drains this in its send loop.
    pub fn register(&self, session_id: String) -> mpsc::Receiver<ArrayFrame> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.insert(session_id.clone(), tx);
        self.sessions_registered.fetch_add(1, Ordering::Relaxed);
        info!(session = %session_id, "array_delivery: session registered");
        rx
    }

    /// Unregister a disconnected session and drop its sender.
    pub fn unregister(&self, session_id: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if sessions.remove(session_id).is_some() {
            debug!(session = %session_id, "array_delivery: session unregistered");
        }
    }

    /// Enqueue a frame for delivery to `session_id`.
    ///
    /// Uses `try_send` so callers are never blocked. If the channel is
    /// full, the frame is dropped and `frames_dropped` is incremented.
    /// The Lite device recovers via snapshot catch-up on reconnect.
    pub fn enqueue(&self, session_id: &str, frame: ArrayFrame) {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = sessions.get(session_id) {
            match tx.try_send(frame) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.frames_dropped.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        session = %session_id,
                        "array_delivery: channel full — frame dropped; Lite will catch up via snapshot"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!(session = %session_id, "array_delivery: session channel closed (disconnected)");
                }
            }
        }
    }

    /// Number of currently registered sessions.
    pub fn active_sessions(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_receive() {
        let reg = ArrayDeliveryRegistry::new();
        let mut rx = reg.register("s1".into());
        reg.enqueue("s1", vec![1, 2, 3]);
        let frame = rx.recv().await.expect("should receive frame");
        assert_eq!(frame, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn unregister_drops_sender() {
        let reg = ArrayDeliveryRegistry::new();
        let mut rx = reg.register("s1".into());
        reg.unregister("s1");
        // After unregister, the sender is dropped; channel is closed.
        reg.enqueue("s1", vec![9]); // No-op: session gone.
        // rx.recv() returns None because sender was dropped on unregister.
        assert_eq!(rx.recv().await, None);
    }

    #[test]
    fn enqueue_unknown_session_is_noop() {
        let reg = ArrayDeliveryRegistry::new();
        reg.enqueue("ghost", vec![0]); // Should not panic.
        assert_eq!(reg.frames_dropped.load(Ordering::Relaxed), 0);
    }
}
