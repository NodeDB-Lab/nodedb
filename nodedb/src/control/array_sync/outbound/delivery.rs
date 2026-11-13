// SPDX-License-Identifier: BUSL-1.1

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

/// An encoded binary frame ready to write to the WebSocket, with the array
/// it carries. The session's push-time check re-authorizes the read of
/// `array` before the bytes leave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayFrame {
    /// The bare array name the frame carries data of.
    pub array: String,
    /// The encoded `SyncFrame`.
    pub bytes: Vec<u8>,
}

impl ArrayFrame {
    pub fn new(array: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            array: array.into(),
            bytes,
        }
    }
}

/// Registry of connected Lite sessions for outbound array frame delivery.
///
/// Thread-safe: `register` / `unregister` from the sync listener task;
/// `enqueue` from `ArrayFanout` after an op is applied.
struct SessionDelivery {
    sender: mpsc::Sender<ArrayFrame>,
    /// Once a frame is rejected for back-pressure, this session cannot safely
    /// advance its cursor again until it reconnects and registers anew.
    lagged: bool,
}

pub struct ArrayDeliveryRegistry {
    sessions: RwLock<HashMap<String, SessionDelivery>>,
    channel_capacity: usize,
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
        Self::with_capacity(CHANNEL_CAPACITY)
    }

    fn with_capacity(channel_capacity: usize) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            channel_capacity,
            sessions_registered: AtomicU64::new(0),
            frames_dropped: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_capacity(channel_capacity: usize) -> Self {
        Self::with_capacity(channel_capacity)
    }

    /// Register a session and return the `Receiver` end of its delivery
    /// channel.  The sync listener drains this in its send loop.
    pub fn register(&self, session_id: String) -> mpsc::Receiver<ArrayFrame> {
        let (tx, rx) = mpsc::channel(self.channel_capacity);
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.insert(
            session_id.clone(),
            SessionDelivery {
                sender: tx,
                lagged: false,
            },
        );
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
    /// Uses `try_send` so callers are never blocked. A full channel marks the
    /// session lagged: every later frame is rejected until unregister/register
    /// creates a fresh channel. This prevents a later successful enqueue from
    /// advancing a cursor past a frame that was not queued.
    pub fn enqueue(&self, session_id: &str, frame: ArrayFrame) -> bool {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let Some(session) = sessions.get_mut(session_id) else {
            return false;
        };
        if session.lagged {
            return false;
        }

        match session.sender.try_send(frame) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                session.lagged = true;
                self.frames_dropped.fetch_add(1, Ordering::Relaxed);
                warn!(
                    session = %session_id,
                    "array_delivery: channel full — session marked lagged until reconnect"
                );
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(session = %session_id, "array_delivery: session channel closed (disconnected)");
                false
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

    fn frame(bytes: Vec<u8>) -> ArrayFrame {
        ArrayFrame::new("arr", bytes)
    }

    #[tokio::test]
    async fn register_and_receive() {
        let reg = ArrayDeliveryRegistry::new();
        let mut rx = reg.register("s1".into());
        assert!(reg.enqueue("s1", frame(vec![1, 2, 3])));
        let received = rx.recv().await.expect("should receive frame");
        assert_eq!(received, frame(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn unregister_drops_sender() {
        let reg = ArrayDeliveryRegistry::new();
        let mut rx = reg.register("s1".into());
        reg.unregister("s1");
        // After unregister, the sender is dropped; channel is closed.
        assert!(!reg.enqueue("s1", frame(vec![9]))); // Session is gone.
        // rx.recv() returns None because sender was dropped on unregister.
        assert_eq!(rx.recv().await, None);
    }

    #[test]
    fn enqueue_unknown_session_fails() {
        let reg = ArrayDeliveryRegistry::new();
        assert!(!reg.enqueue("ghost", frame(vec![0])));
        assert_eq!(reg.frames_dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn full_channel_lags_session_and_rejects_later_frames() {
        let reg = ArrayDeliveryRegistry::with_test_capacity(1);
        let mut rx = reg.register("s1".into());

        assert!(reg.enqueue("s1", frame(vec![1])));
        assert!(!reg.enqueue("s1", frame(vec![2])));
        assert_eq!(rx.recv().await, Some(frame(vec![1])));
        assert!(!reg.enqueue("s1", frame(vec![3])));
        assert!(rx.try_recv().is_err());

        reg.unregister("s1");
        let mut rx = reg.register("s1".into());
        assert!(reg.enqueue("s1", frame(vec![4])));
        assert_eq!(rx.recv().await, Some(frame(vec![4])));
    }
}
