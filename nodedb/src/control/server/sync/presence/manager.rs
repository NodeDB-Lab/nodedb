//! PresenceManager: channel subscription, broadcast fan-out, TTL sweeping.
//!
//! Lives entirely in the Control Plane (Tokio, `Send + Sync`). No persistence,
//! no WAL, no SPSC bridge — presence is ephemeral, in-memory only.
//!
//! **Lock strategy**: mutations (upsert/remove) return `OutboundFrames`
//! instead of broadcasting inline. The caller drops the write lock, then
//! sends frames outside the critical section. This keeps lock hold time O(1).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use nodedb_types::sync::wire::{PresenceUpdateMsg, SyncFrame, SyncMessageType};

use super::channel::ChannelState;
use super::types::{PeerState, PresenceConfig};

/// Serialized frame bytes shared across subscribers (serialize once).
type SharedBytes = Arc<Vec<u8>>;

/// Handle for sending outbound frames to a specific WebSocket session.
#[derive(Debug, Clone)]
pub struct SessionSender {
    tx: mpsc::Sender<SharedBytes>,
}

impl SessionSender {
    pub fn new(tx: mpsc::Sender<SharedBytes>) -> Self {
        Self { tx }
    }

    /// Try to send serialized frame bytes. Returns `false` if buffer full or closed.
    pub fn try_send(&self, bytes: &SharedBytes) -> bool {
        self.tx.try_send(Arc::clone(bytes)).is_ok()
    }
}

/// Frames to send after releasing the write lock.
///
/// The caller collects these from mutation methods, drops the lock,
/// then calls `send_all()` to fan out without holding the lock.
pub struct OutboundFrames {
    /// (session_id, serialized_frame_bytes) pairs to deliver.
    targets: Vec<(String, SharedBytes)>,
}

impl OutboundFrames {
    fn new() -> Self {
        Self {
            targets: Vec::new(),
        }
    }

    fn add_broadcast(
        &mut self,
        channel: &ChannelState,
        exclude_session: &str,
        frame_bytes: SharedBytes,
    ) {
        for session_id in channel.session_ids() {
            if session_id != exclude_session {
                self.targets
                    .push((session_id.to_owned(), Arc::clone(&frame_bytes)));
            }
        }
    }

    fn add_broadcast_all(&mut self, channel: &ChannelState, frame_bytes: SharedBytes) {
        for session_id in channel.session_ids() {
            self.targets
                .push((session_id.to_owned(), Arc::clone(&frame_bytes)));
        }
    }

    /// Send all queued frames via registered senders.
    /// Call this AFTER dropping the PresenceManager write lock.
    pub fn send_all(self, senders: &HashMap<String, SessionSender>) {
        for (session_id, bytes) in &self.targets {
            if let Some(sender) = senders.get(session_id)
                && !sender.try_send(bytes)
            {
                debug!(
                    session = session_id.as_str(),
                    "presence: send buffer full, dropping"
                );
            }
        }
    }

    /// Whether there are any frames to send.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

/// Central presence manager for all channels on this node.
pub struct PresenceManager {
    channels: HashMap<String, ChannelState>,
    session_channels: HashMap<String, HashSet<String>>,
    /// Senders are public so `OutboundFrames::send_all` can access them
    /// after the caller drops the write lock.
    senders: HashMap<String, SessionSender>,
    config: PresenceConfig,
}

impl PresenceManager {
    pub fn new(config: PresenceConfig) -> Self {
        Self {
            channels: HashMap::new(),
            session_channels: HashMap::new(),
            senders: HashMap::new(),
            config,
        }
    }

    /// Get the configured sweep interval (for the timer task).
    pub fn sweep_interval_ms(&self) -> u64 {
        self.config.sweep_interval_ms
    }

    /// Access senders map (for `OutboundFrames::send_all` after lock release).
    pub fn senders(&self) -> &HashMap<String, SessionSender> {
        &self.senders
    }

    /// Register a session's outbound sender.
    pub fn register_session(&mut self, session_id: String, sender: SessionSender) {
        self.senders.insert(session_id, sender);
    }

    /// Unregister a session and remove it from all channels.
    /// Returns outbound leave frames to send after releasing the lock.
    pub fn unregister_session(&mut self, session_id: &str) -> OutboundFrames {
        self.senders.remove(session_id);
        let mut outbound = OutboundFrames::new();

        let channel_names = match self.session_channels.remove(session_id) {
            Some(names) => names,
            None => return outbound,
        };

        let mut empty_channels = Vec::new();
        for channel_name in &channel_names {
            if let Some(channel) = self.channels.get_mut(channel_name) {
                if let Some(leave) = channel.remove_peer(session_id)
                    && let Some(bytes) = serialize_frame(SyncMessageType::PresenceLeave, &leave)
                {
                    outbound.add_broadcast_all(channel, bytes);
                }
                if channel.is_empty() {
                    empty_channels.push(channel_name.clone());
                }
            }
        }
        for name in &empty_channels {
            self.channels.remove(name);
        }

        outbound
    }

    /// Handle a `PresenceUpdate` from a client.
    /// Returns outbound broadcast frames to send after releasing the lock.
    pub fn handle_update(
        &mut self,
        session_id: &str,
        user_id: &str,
        msg: &PresenceUpdateMsg,
    ) -> OutboundFrames {
        let mut outbound = OutboundFrames::new();

        let session_channel_count = self.session_channels.get(session_id).map_or(0, |s| s.len());
        let is_new_channel = !self
            .session_channels
            .get(session_id)
            .is_some_and(|s| s.contains(&msg.channel));

        if is_new_channel && session_channel_count >= self.config.max_channels_per_session {
            warn!(
                session = session_id,
                channel = %msg.channel,
                limit = self.config.max_channels_per_session,
                "presence: max channels per session exceeded, ignoring"
            );
            return outbound;
        }

        if is_new_channel
            && let Some(ch) = self.channels.get(&msg.channel)
            && ch.peer_count() >= self.config.max_subscribers_per_channel
        {
            warn!(
                session = session_id,
                channel = %msg.channel,
                limit = self.config.max_subscribers_per_channel,
                "presence: max subscribers per channel exceeded, ignoring"
            );
            return outbound;
        }

        let peer = PeerState {
            user_id: user_id.to_owned(),
            session_id: session_id.to_owned(),
            state: msg.state.clone(),
            last_seen: Instant::now(),
        };

        let channel = self
            .channels
            .entry(msg.channel.clone())
            .or_insert_with(|| ChannelState::new(msg.channel.clone()));
        let broadcast = channel.upsert_peer(session_id, peer);

        self.session_channels
            .entry(session_id.to_owned())
            .or_default()
            .insert(msg.channel.clone());

        if let Some(bytes) = serialize_frame(SyncMessageType::PresenceBroadcast, &broadcast) {
            outbound.add_broadcast(channel, session_id, bytes);
        }

        debug!(
            session = session_id,
            channel = %msg.channel,
            peers = broadcast.peers.len(),
            "presence: update broadcast"
        );

        outbound
    }

    /// Subscribe a session to a presence channel without sending state.
    ///
    /// Called from ShapeSubscribe to auto-join the presence channel for the
    /// shape's collection+doc. The session will receive broadcasts from other
    /// peers but won't appear as present until it sends a PresenceUpdate.
    pub fn subscribe_to_channel(&mut self, session_id: &str, channel: &str) {
        let session_channel_count = self.session_channels.get(session_id).map_or(0, |s| s.len());
        if session_channel_count >= self.config.max_channels_per_session {
            return;
        }

        // Ensure channel exists.
        self.channels
            .entry(channel.to_owned())
            .or_insert_with(|| ChannelState::new(channel.to_owned()));

        self.session_channels
            .entry(session_id.to_owned())
            .or_default()
            .insert(channel.to_owned());
    }

    /// Sweep expired peers. Returns outbound leave frames.
    pub fn sweep_expired(&mut self) -> OutboundFrames {
        let ttl = self.config.ttl_ms;
        let mut outbound = OutboundFrames::new();
        let mut total_evicted = 0;
        let mut empty_channels = Vec::new();

        // Collect leaves and their targets during mutable iteration.
        // We have `channel` from the iterator — use it directly for targets.
        let mut pending_leaves: Vec<(Vec<String>, SharedBytes)> = Vec::new();

        for (channel_name, channel) in &mut self.channels {
            let leaves = channel.sweep_expired(ttl);
            total_evicted += leaves.len();
            for leave in &leaves {
                if let Some(bytes) = serialize_frame(SyncMessageType::PresenceLeave, leave) {
                    // Collect remaining session IDs from the channel.
                    let targets: Vec<String> =
                        channel.session_ids().map(|s| s.to_owned()).collect();
                    pending_leaves.push((targets, bytes));
                }
            }
            if channel.is_empty() {
                empty_channels.push(channel_name.clone());
            }
        }

        // Build outbound from collected targets.
        for (targets, bytes) in pending_leaves {
            for session_id in targets {
                outbound.targets.push((session_id, Arc::clone(&bytes)));
            }
        }

        for name in &empty_channels {
            self.channels.remove(name);
        }

        if total_evicted > 0 {
            let channels_ref = &self.channels;
            self.session_channels.retain(|sid, session_chs| {
                session_chs.retain(|ch_name| {
                    channels_ref
                        .get(ch_name)
                        .is_some_and(|ch| ch.has_session(sid))
                });
                !session_chs.is_empty()
            });
            info!(evicted = total_evicted, "presence: TTL sweep complete");
        }

        outbound
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn total_peers(&self) -> usize {
        self.channels.values().map(|ch| ch.peer_count()).sum()
    }
}

/// Serialize a frame once and wrap in Arc for zero-copy fan-out.
fn serialize_frame<T: serde::Serialize + zerompk::ToMessagePack>(
    msg_type: SyncMessageType,
    msg: &T,
) -> Option<SharedBytes> {
    let frame = SyncFrame::try_encode(msg_type, msg)?;
    Some(Arc::new(frame.to_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::sync::wire::{PresenceBroadcastMsg, PresenceLeaveMsg};

    fn setup() -> (
        PresenceManager,
        mpsc::Receiver<SharedBytes>,
        mpsc::Receiver<SharedBytes>,
    ) {
        let config = PresenceConfig::default();
        let mut mgr = PresenceManager::new(config);

        let (tx1, rx1) = mpsc::channel(64);
        let (tx2, rx2) = mpsc::channel(64);
        mgr.register_session("s1".into(), SessionSender::new(tx1));
        mgr.register_session("s2".into(), SessionSender::new(tx2));

        (mgr, rx1, rx2)
    }

    #[test]
    fn update_broadcasts_to_others() {
        let (mut mgr, _rx1, mut rx2) = setup();

        // s1 joins channel.
        let outbound = mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![0x01],
            },
        );
        // s2 not in channel yet, no targets.
        outbound.send_all(mgr.senders());
        assert!(rx2.try_recv().is_err());

        // s2 joins.
        let outbound = mgr.handle_update(
            "s2",
            "bob",
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![0x02],
            },
        );
        outbound.send_all(mgr.senders());

        // s1 updates — s2 receives broadcast.
        let outbound = mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![0x01],
            },
        );
        outbound.send_all(mgr.senders());
        let bytes = rx2.try_recv().unwrap();
        let frame = SyncFrame::from_bytes(&bytes).unwrap();
        assert_eq!(frame.msg_type, SyncMessageType::PresenceBroadcast);
        let broadcast: PresenceBroadcastMsg = frame.decode_body().unwrap();
        assert_eq!(broadcast.channel, "doc:d1");
        assert_eq!(broadcast.peers.len(), 2);
    }

    #[test]
    fn unregister_broadcasts_leave() {
        let (mut mgr, _rx1, mut rx2) = setup();

        let out = mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![],
            },
        );
        out.send_all(mgr.senders());
        let out = mgr.handle_update(
            "s2",
            "bob",
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![],
            },
        );
        out.send_all(mgr.senders());
        while rx2.try_recv().is_ok() {}

        let outbound = mgr.unregister_session("s1");
        outbound.send_all(mgr.senders());

        let bytes = rx2.try_recv().unwrap();
        let frame = SyncFrame::from_bytes(&bytes).unwrap();
        assert_eq!(frame.msg_type, SyncMessageType::PresenceLeave);
        let leave: PresenceLeaveMsg = frame.decode_body().unwrap();
        assert_eq!(leave.user_id, "alice");
    }

    #[test]
    fn max_channels_per_session_enforced() {
        let config = PresenceConfig {
            max_channels_per_session: 2,
            ..Default::default()
        };
        let mut mgr = PresenceManager::new(config);
        let (tx, _rx) = mpsc::channel(64);
        mgr.register_session("s1".into(), SessionSender::new(tx));

        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch1".into(),
                state: vec![],
            },
        );
        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch2".into(),
                state: vec![],
            },
        );
        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch3".into(),
                state: vec![],
            },
        );

        assert_eq!(mgr.session_channels.get("s1").unwrap().len(), 2);
    }

    #[test]
    fn sweep_expired_peers() {
        let config = PresenceConfig {
            ttl_ms: 100,
            ..Default::default()
        };
        let mut mgr = PresenceManager::new(config);
        let (tx, _rx) = mpsc::channel(64);
        mgr.register_session("s1".into(), SessionSender::new(tx));

        let channel = mgr
            .channels
            .entry("doc:d1".into())
            .or_insert_with(|| ChannelState::new("doc:d1".into()));
        channel.upsert_peer(
            "s1",
            PeerState {
                user_id: "alice".into(),
                session_id: "s1".into(),
                state: vec![],
                last_seen: Instant::now() - std::time::Duration::from_secs(1),
            },
        );
        mgr.session_channels
            .entry("s1".into())
            .or_default()
            .insert("doc:d1".into());

        assert_eq!(mgr.total_peers(), 1);
        let outbound = mgr.sweep_expired();
        outbound.send_all(mgr.senders());
        assert_eq!(mgr.total_peers(), 0);
        assert_eq!(mgr.channel_count(), 0);
    }

    #[test]
    fn subscribe_to_channel_auto_join() {
        let config = PresenceConfig::default();
        let mut mgr = PresenceManager::new(config);
        let (tx, _rx) = mpsc::channel(64);
        mgr.register_session("s1".into(), SessionSender::new(tx));

        mgr.subscribe_to_channel("s1", "doc:d1");
        assert!(mgr.channels.contains_key("doc:d1"));
        assert!(mgr.session_channels.get("s1").unwrap().contains("doc:d1"));
    }

    #[test]
    fn empty_after_all_disconnect() {
        let (mut mgr, _, _) = setup();
        let out = mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch1".into(),
                state: vec![],
            },
        );
        out.send_all(mgr.senders());
        let out = mgr.handle_update(
            "s2",
            "bob",
            &PresenceUpdateMsg {
                channel: "ch1".into(),
                state: vec![],
            },
        );
        out.send_all(mgr.senders());

        mgr.unregister_session("s1");
        mgr.unregister_session("s2");

        assert_eq!(mgr.channel_count(), 0);
        assert_eq!(mgr.total_peers(), 0);
    }
}
