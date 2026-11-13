// SPDX-License-Identifier: BUSL-1.1

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

use crate::types::{DatabaseId, TenantId};

use super::channel::ChannelState;
use super::types::{PeerState, PresenceConfig};

/// One presence frame for one subscriber: serialized once, shared across
/// every subscriber of its channel.
///
/// The frame names its channel, so the receiving session checks at push
/// time that it is still a member. A frame queued before the session left
/// the channel does not go out.
#[derive(Debug, Clone)]
pub struct PresenceFrame {
    /// The client-visible channel name the frame belongs to.
    pub channel: Arc<str>,
    /// The encoded `SyncFrame`.
    pub bytes: Arc<Vec<u8>>,
}

/// Handle for sending outbound frames to a specific WebSocket session.
#[derive(Debug, Clone)]
pub struct SessionSender {
    tx: mpsc::Sender<PresenceFrame>,
}

impl SessionSender {
    pub fn new(tx: mpsc::Sender<PresenceFrame>) -> Self {
        Self { tx }
    }

    /// Try to send one frame. Returns `false` if buffer full or closed.
    pub fn try_send(&self, frame: &PresenceFrame) -> bool {
        self.tx.try_send(frame.clone()).is_ok()
    }
}

/// Frames to send after releasing the write lock.
///
/// The caller collects these from mutation methods, drops the lock,
/// then calls `send_all()` to fan out without holding the lock.
pub struct OutboundFrames {
    /// (session_id, frame) pairs to deliver.
    targets: Vec<(String, PresenceFrame)>,
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
        frame: PresenceFrame,
    ) {
        for session_id in channel.session_ids() {
            if session_id != exclude_session {
                self.targets.push((session_id.to_owned(), frame.clone()));
            }
        }
    }

    fn add_broadcast_all(&mut self, channel: &ChannelState, frame: PresenceFrame) {
        for session_id in channel.session_ids() {
            self.targets.push((session_id.to_owned(), frame.clone()));
        }
    }

    /// Send all queued frames via registered senders.
    /// Call this AFTER dropping the PresenceManager write lock.
    pub fn send_all(self, senders: &HashMap<String, SessionSender>) {
        for (session_id, frame) in &self.targets {
            if let Some(sender) = senders.get(session_id)
                && !sender.try_send(frame)
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

/// Internal channel identity. The client-visible name stays on `ChannelState`
/// and its outbound payloads, while membership is always tenant/database scoped.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChannelKey {
    tenant_id: TenantId,
    database_id: DatabaseId,
    name: String,
}

impl ChannelKey {
    fn new(tenant_id: TenantId, database_id: DatabaseId, name: &str) -> Self {
        Self {
            tenant_id,
            database_id,
            name: name.to_owned(),
        }
    }
}

/// Central presence manager for all scoped channels on this node.
pub struct PresenceManager {
    channels: HashMap<ChannelKey, ChannelState>,
    session_channels: HashMap<String, HashSet<ChannelKey>>,
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

        let channel_keys = match self.session_channels.remove(session_id) {
            Some(keys) => keys,
            None => return outbound,
        };

        let mut empty_channels = Vec::new();
        for channel_key in &channel_keys {
            if let Some(channel) = self.channels.get_mut(channel_key) {
                channel.remove_listener(session_id);
                if let Some(leave) = channel.remove_peer(session_id)
                    && let Some(frame) =
                        serialize_frame(SyncMessageType::PresenceLeave, &leave, &leave.channel)
                {
                    outbound.add_broadcast_all(channel, frame);
                }
                if channel.is_empty() {
                    empty_channels.push(channel_key.clone());
                }
            }
        }
        for key in &empty_channels {
            self.channels.remove(key);
        }

        outbound
    }

    /// Handle a `PresenceUpdate` from a client.
    /// Returns outbound broadcast frames to send after releasing the lock.
    pub fn handle_update(
        &mut self,
        session_id: &str,
        user_id: &str,
        tenant_id: TenantId,
        database_id: DatabaseId,
        msg: &PresenceUpdateMsg,
    ) -> OutboundFrames {
        let mut outbound = OutboundFrames::new();
        let channel_key = ChannelKey::new(tenant_id, database_id, &msg.channel);

        let session_channel_count = self.session_channels.get(session_id).map_or(0, |s| s.len());
        let is_new_channel = !self
            .session_channels
            .get(session_id)
            .is_some_and(|s| s.contains(&channel_key));

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
            && let Some(ch) = self.channels.get(&channel_key)
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
            .entry(channel_key.clone())
            .or_insert_with(|| ChannelState::new(msg.channel.clone()));
        let broadcast = channel.upsert_peer(session_id, peer);

        self.session_channels
            .entry(session_id.to_owned())
            .or_default()
            .insert(channel_key);

        if let Some(frame) =
            serialize_frame(SyncMessageType::PresenceBroadcast, &broadcast, &msg.channel)
        {
            outbound.add_broadcast(channel, session_id, frame);
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
    pub fn subscribe_to_channel(
        &mut self,
        session_id: &str,
        tenant_id: TenantId,
        database_id: DatabaseId,
        channel: &str,
    ) {
        let channel_key = ChannelKey::new(tenant_id, database_id, channel);
        let session_channels = self
            .session_channels
            .entry(session_id.to_owned())
            .or_default();
        if !session_channels.contains(&channel_key)
            && session_channels.len() >= self.config.max_channels_per_session
        {
            return;
        }

        // The session listens: it receives the channel's frames, and is not
        // present until it sends an update.
        self.channels
            .entry(channel_key.clone())
            .or_insert_with(|| ChannelState::new(channel.to_owned()))
            .add_listener(session_id);

        session_channels.insert(channel_key);
    }

    /// Remove a session from one presence channel. Returns the leave frame
    /// for the channel's other peers, to send after releasing the lock.
    ///
    /// Used when the session loses the read grant a shape presence channel
    /// was joined under.
    pub fn leave_channel(
        &mut self,
        session_id: &str,
        tenant_id: TenantId,
        database_id: DatabaseId,
        channel: &str,
    ) -> OutboundFrames {
        let mut outbound = OutboundFrames::new();
        let channel_key = ChannelKey::new(tenant_id, database_id, channel);
        if let Some(joined) = self.session_channels.get_mut(session_id) {
            joined.remove(&channel_key);
            if joined.is_empty() {
                self.session_channels.remove(session_id);
            }
        }
        let Some(state) = self.channels.get_mut(&channel_key) else {
            return outbound;
        };
        state.remove_listener(session_id);
        if let Some(leave) = state.remove_peer(session_id)
            && let Some(frame) = serialize_frame(SyncMessageType::PresenceLeave, &leave, channel)
        {
            outbound.add_broadcast_all(state, frame);
        }
        if state.is_empty() {
            self.channels.remove(&channel_key);
        }
        outbound
    }

    /// Sweep expired peers. Returns outbound leave frames.
    pub fn sweep_expired(&mut self) -> OutboundFrames {
        let ttl = self.config.ttl_ms;
        let mut outbound = OutboundFrames::new();
        let mut total_evicted = 0;
        let mut empty_channels = Vec::new();

        // Collect leaves and their targets during mutable iteration.
        // We have `channel` from the iterator — use it directly for targets.
        let mut pending_leaves: Vec<(Vec<String>, PresenceFrame)> = Vec::new();

        for (channel_key, channel) in &mut self.channels {
            let leaves = channel.sweep_expired(ttl);
            total_evicted += leaves.len();
            for leave in &leaves {
                if let Some(frame) =
                    serialize_frame(SyncMessageType::PresenceLeave, leave, &leave.channel)
                {
                    // Collect remaining session IDs from the channel.
                    let targets: Vec<String> =
                        channel.session_ids().map(|s| s.to_owned()).collect();
                    pending_leaves.push((targets, frame));
                }
            }
            if channel.is_empty() {
                empty_channels.push(channel_key.clone());
            }
        }

        // Build outbound from collected targets.
        for (targets, frame) in pending_leaves {
            for session_id in targets {
                outbound.targets.push((session_id, frame.clone()));
            }
        }

        for key in &empty_channels {
            self.channels.remove(key);
        }

        if total_evicted > 0 {
            let channels_ref = &self.channels;
            self.session_channels.retain(|sid, session_chs| {
                session_chs.retain(|channel_key| {
                    channels_ref
                        .get(channel_key)
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

/// Serialize a frame of `channel` once and wrap it in `Arc` for zero-copy
/// fan-out.
fn serialize_frame<T: serde::Serialize + zerompk::ToMessagePack>(
    msg_type: SyncMessageType,
    msg: &T,
    channel: &str,
) -> Option<PresenceFrame> {
    let frame = SyncFrame::try_encode(msg_type, msg)?;
    Some(PresenceFrame {
        channel: Arc::from(channel),
        bytes: Arc::new(frame.to_bytes()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::sync::wire::{PresenceBroadcastMsg, PresenceLeaveMsg};

    fn setup() -> (
        PresenceManager,
        mpsc::Receiver<PresenceFrame>,
        mpsc::Receiver<PresenceFrame>,
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

        let outbound = mgr.handle_update(
            "s1",
            "alice",
            TenantId::new(1),
            DatabaseId::DEFAULT,
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![0x01],
            },
        );
        outbound.send_all(mgr.senders());
        assert!(rx2.try_recv().is_err());

        let outbound = mgr.handle_update(
            "s2",
            "bob",
            TenantId::new(1),
            DatabaseId::DEFAULT,
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![0x02],
            },
        );
        outbound.send_all(mgr.senders());

        let outbound = mgr.handle_update(
            "s1",
            "alice",
            TenantId::new(1),
            DatabaseId::DEFAULT,
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![0x01],
            },
        );
        outbound.send_all(mgr.senders());
        let queued = rx2.try_recv().unwrap();
        assert_eq!(&*queued.channel, "doc:d1");
        let frame = SyncFrame::from_bytes(&queued.bytes).unwrap();
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
            TenantId::new(1),
            DatabaseId::DEFAULT,
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![],
            },
        );
        out.send_all(mgr.senders());
        let out = mgr.handle_update(
            "s2",
            "bob",
            TenantId::new(1),
            DatabaseId::DEFAULT,
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![],
            },
        );
        out.send_all(mgr.senders());
        while rx2.try_recv().is_ok() {}

        let outbound = mgr.unregister_session("s1");
        outbound.send_all(mgr.senders());

        let queued = rx2.try_recv().unwrap();
        assert_eq!(&*queued.channel, "doc:d1");
        let frame = SyncFrame::from_bytes(&queued.bytes).unwrap();
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

        for channel in ["ch1", "ch2", "ch3"] {
            mgr.handle_update(
                "s1",
                "alice",
                TenantId::new(1),
                DatabaseId::DEFAULT,
                &PresenceUpdateMsg {
                    channel: channel.into(),
                    state: vec![],
                },
            );
        }

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
            .entry(ChannelKey::new(
                TenantId::new(1),
                DatabaseId::DEFAULT,
                "doc:d1",
            ))
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
            .insert(ChannelKey::new(
                TenantId::new(1),
                DatabaseId::DEFAULT,
                "doc:d1",
            ));

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

        let channel_key = ChannelKey::new(TenantId::new(1), DatabaseId::DEFAULT, "doc:d1");
        mgr.subscribe_to_channel("s1", TenantId::new(1), DatabaseId::DEFAULT, "doc:d1");
        assert!(mgr.channels.contains_key(&channel_key));
        assert!(
            mgr.session_channels
                .get("s1")
                .unwrap()
                .contains(&channel_key)
        );
    }

    #[test]
    fn empty_after_all_disconnect() {
        let (mut mgr, _, _) = setup();
        let out = mgr.handle_update(
            "s1",
            "alice",
            TenantId::new(1),
            DatabaseId::DEFAULT,
            &PresenceUpdateMsg {
                channel: "ch1".into(),
                state: vec![],
            },
        );
        out.send_all(mgr.senders());
        let out = mgr.handle_update(
            "s2",
            "bob",
            TenantId::new(1),
            DatabaseId::DEFAULT,
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

    #[test]
    fn same_channel_is_isolated_between_tenants() {
        let (mut mgr, mut rx1, mut rx2) = setup();
        let update = || PresenceUpdateMsg {
            channel: "doc:shared".into(),
            state: vec![],
        };

        let outbound = mgr.handle_update(
            "s1",
            "alice",
            TenantId::new(1),
            DatabaseId::DEFAULT,
            &update(),
        );
        outbound.send_all(mgr.senders());
        let outbound = mgr.handle_update(
            "s2",
            "bob",
            TenantId::new(2),
            DatabaseId::DEFAULT,
            &update(),
        );
        outbound.send_all(mgr.senders());

        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());
        assert_eq!(mgr.channel_count(), 2);
        assert_eq!(mgr.total_peers(), 2);
    }

    #[test]
    fn same_channel_is_isolated_between_databases() {
        let (mut mgr, mut rx1, mut rx2) = setup();
        let update = |channel: &str| PresenceUpdateMsg {
            channel: channel.into(),
            state: vec![],
        };

        let outbound = mgr.handle_update(
            "s1",
            "alice",
            TenantId::new(1),
            DatabaseId::new(7),
            &update("doc:shared"),
        );
        outbound.send_all(mgr.senders());
        let outbound = mgr.handle_update(
            "s2",
            "bob",
            TenantId::new(1),
            DatabaseId::new(8),
            &update("doc:shared"),
        );
        outbound.send_all(mgr.senders());

        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());
        assert_eq!(mgr.channel_count(), 2);
        assert_eq!(mgr.total_peers(), 2);
    }
}
