// SPDX-License-Identifier: BUSL-1.1

//! [`DefinitionSyncFanout`] — route outbound `DefinitionSync` (0x70) frames
//! to connected Lite sessions in the definition's authenticated scope.
//!
//! Each authenticated session registers a bounded `mpsc` receiver with its
//! tenant and database. On DDL commit, the Origin DDL handler calls
//! [`DefinitionSyncFanout::broadcast`], which delivers the encoded frame only
//! to sessions whose scope exactly matches the message's durable scope.
//!
//! This lives entirely on the Control Plane (Tokio, `Send + Sync`).

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use nodedb_types::id::DatabaseId;
use nodedb_types::sync::wire::{DefinitionSyncMsg, SyncFrame, SyncMessageType};

/// Capacity of each session's outbound definition-sync channel.
const CHANNEL_CAPACITY: usize = 256;

/// A pre-encoded binary frame ready to write to the WebSocket, with the
/// tenant and database of the definition it carries. The session's push-time
/// check matches that scope against its principal before the bytes leave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionFrame {
    pub tenant_id: u64,
    pub database_id: DatabaseId,
    pub bytes: Vec<u8>,
}

/// Authenticated scope and delivery channel for one Lite session.
struct DefinitionSession {
    tenant_id: u64,
    database_id: DatabaseId,
    sender: mpsc::Sender<DefinitionFrame>,
}

/// Fan-out registry for outbound `DefinitionSync` (0x70) frames.
///
/// Thread-safe: `register` / `unregister` from the sync listener task;
/// `broadcast` from DDL handlers after a successful catalog commit.
pub struct DefinitionSyncFanout {
    sessions: RwLock<HashMap<String, DefinitionSession>>,
    /// Monotonic count of sessions registered since startup.
    pub sessions_registered: AtomicU64,
    /// Monotonic count of frames dropped due to back-pressure.
    pub frames_dropped: AtomicU64,
}

impl Default for DefinitionSyncFanout {
    fn default() -> Self {
        Self::new()
    }
}

impl DefinitionSyncFanout {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            sessions_registered: AtomicU64::new(0),
            frames_dropped: AtomicU64::new(0),
        }
    }

    /// Register an authenticated session and return the receiver end of its
    /// scoped delivery channel. The sync listener's send loop drains this on
    /// each iteration.
    pub fn register(
        &self,
        session_id: String,
        tenant_id: u64,
        database_id: DatabaseId,
    ) -> mpsc::Receiver<DefinitionFrame> {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.insert(
            session_id.clone(),
            DefinitionSession {
                tenant_id,
                database_id,
                sender,
            },
        );
        self.sessions_registered.fetch_add(1, Ordering::Relaxed);
        info!(
            session = %session_id,
            tenant_id,
            database_id = database_id.as_u64(),
            "definition_sync_fanout: session registered"
        );
        receiver
    }

    /// Unregister a disconnected session and drop its sender.
    pub fn unregister(&self, session_id: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if sessions.remove(session_id).is_some() {
            debug!(session = %session_id, "definition_sync_fanout: session unregistered");
        }
    }

    /// Encode and route a `DefinitionSyncMsg` to sessions with the exact same
    /// tenant and database scope.
    ///
    /// Uses `try_send` so callers are never blocked. If a matching session's
    /// channel is full the frame is dropped for that session
    /// (`frames_dropped` is incremented). The Lite device recovers by
    /// re-requesting the catalog on the next reconnect.
    pub fn broadcast(&self, msg: &DefinitionSyncMsg) {
        let frame = match SyncFrame::try_encode(SyncMessageType::DefinitionSync, msg) {
            Some(frame) => DefinitionFrame {
                tenant_id: msg.tenant_id,
                database_id: DatabaseId::new(msg.database_id),
                bytes: frame.to_bytes(),
            },
            None => return,
        };

        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        for (session_id, session) in sessions.iter() {
            if session.tenant_id != msg.tenant_id || session.database_id.as_u64() != msg.database_id
            {
                continue;
            }

            match session.sender.try_send(frame.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.frames_dropped.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        session = %session_id,
                        tenant_id = msg.tenant_id,
                        database_id = msg.database_id,
                        definition_type = %msg.definition_type,
                        name = %msg.name,
                        "definition_sync_fanout: channel full — frame dropped; Lite will re-sync on reconnect"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!(
                        session = %session_id,
                        "definition_sync_fanout: session channel closed (disconnected)"
                    );
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

    fn message(tenant_id: u64, database_id: u64) -> DefinitionSyncMsg {
        DefinitionSyncMsg {
            tenant_id,
            database_id,
            definition_type: "function".into(),
            name: "my_fn".into(),
            action: "put".into(),
            payload: vec![],
        }
    }

    #[tokio::test]
    async fn register_and_receive_in_matching_scope() {
        let fanout = DefinitionSyncFanout::new();
        let mut rx = fanout.register("s1".into(), 1, DatabaseId::new(7));
        let msg = message(1, 7);
        fanout.broadcast(&msg);

        let frame_bytes = rx
            .recv()
            .await
            .expect("matching scope should receive frame");
        assert_eq!(frame_bytes.tenant_id, 1);
        assert_eq!(frame_bytes.database_id, DatabaseId::new(7));
        let frame = SyncFrame::from_bytes(&frame_bytes.bytes).expect("decode frame");
        assert_eq!(frame.msg_type, SyncMessageType::DefinitionSync);
        let decoded: DefinitionSyncMsg = frame.decode_body().expect("decode body");
        assert_eq!(decoded.name, "my_fn");
        assert_eq!(decoded.tenant_id, 1);
        assert_eq!(decoded.database_id, 7);
    }

    #[tokio::test]
    async fn unregister_drops_sender() {
        let fanout = DefinitionSyncFanout::new();
        let mut rx = fanout.register("s1".into(), 1, DatabaseId::DEFAULT);
        fanout.unregister("s1");

        fanout.broadcast(&message(1, 0));
        assert_eq!(rx.recv().await, None);
    }

    #[test]
    fn broadcast_unknown_session_is_noop() {
        let fanout = DefinitionSyncFanout::new();
        fanout.broadcast(&message(1, 0));
        assert_eq!(fanout.frames_dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn broadcasts_to_every_session_in_matching_scope() {
        let fanout = DefinitionSyncFanout::new();
        let mut rx1 = fanout.register("s1".into(), 1, DatabaseId::new(7));
        let mut rx2 = fanout.register("s2".into(), 1, DatabaseId::new(7));
        fanout.broadcast(&message(1, 7));

        let f1 = rx1.recv().await.expect("s1 should receive frame");
        let f2 = rx2.recv().await.expect("s2 should receive frame");
        assert_eq!(f1, f2);
    }

    #[test]
    fn does_not_deliver_to_another_database_in_the_same_tenant() {
        let fanout = DefinitionSyncFanout::new();
        let mut matching = fanout.register("matching".into(), 1, DatabaseId::new(7));
        let mut other_database = fanout.register("other-db".into(), 1, DatabaseId::new(8));
        fanout.broadcast(&message(1, 7));

        assert!(matching.try_recv().is_ok());
        assert!(matches!(
            other_database.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn does_not_deliver_to_another_tenant() {
        let fanout = DefinitionSyncFanout::new();
        let mut matching = fanout.register("matching".into(), 1, DatabaseId::new(7));
        let mut other_tenant = fanout.register("other-tenant".into(), 2, DatabaseId::new(7));
        fanout.broadcast(&message(1, 7));

        assert!(matching.try_recv().is_ok());
        assert!(matches!(
            other_tenant.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
