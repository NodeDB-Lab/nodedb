// SPDX-License-Identifier: BUSL-1.1

//! Per-channel presence state: subscriber tracking and broadcast generation.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use nodedb_types::sync::wire::{PeerPresence, PresenceBroadcastMsg, PresenceLeaveMsg};

use super::types::PeerState;

/// State for a single presence channel (e.g., `"doc:doc-123"`).
///
/// Tracks all peers currently present in the channel and generates
/// broadcast/leave messages for state changes.
#[derive(Debug)]
pub struct ChannelState {
    /// Channel name (e.g., `"doc:doc-123"`).
    name: String,
    /// Active peers: `session_id → PeerState`.
    /// Keyed by session_id (not user_id) because a single user may have
    /// multiple sessions (multiple tabs/devices).
    peers: HashMap<String, PeerState>,
    /// Sessions that receive the channel's frames without being present:
    /// a shape subscription joins its channel this way, before the session
    /// sends any update of its own.
    listeners: HashSet<String>,
}

impl ChannelState {
    /// Create a new empty channel.
    pub fn new(name: String) -> Self {
        Self {
            name,
            peers: HashMap::new(),
            listeners: HashSet::new(),
        }
    }

    /// Number of active peers in this channel.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Whether the channel has no peer and no listener (can be garbage
    /// collected).
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty() && self.listeners.is_empty()
    }

    /// Add a session that receives the channel's frames without being
    /// present.
    pub fn add_listener(&mut self, session_id: &str) {
        self.listeners.insert(session_id.to_owned());
    }

    /// Remove a listening session. Its leave sends nothing: it was never
    /// present.
    pub fn remove_listener(&mut self, session_id: &str) {
        self.listeners.remove(session_id);
    }

    /// Add or update a peer's presence state.
    ///
    /// Returns a `PresenceBroadcastMsg` containing all current peers
    /// (to be sent to all subscribers except the sender).
    pub fn upsert_peer(&mut self, session_id: &str, peer: PeerState) -> PresenceBroadcastMsg {
        self.peers.insert(session_id.to_owned(), peer);
        self.build_broadcast()
    }

    /// Remove a peer by session ID.
    ///
    /// Returns `Some(PresenceLeaveMsg)` if the peer existed, `None` otherwise.
    pub fn remove_peer(&mut self, session_id: &str) -> Option<PresenceLeaveMsg> {
        let removed = self.peers.remove(session_id)?;
        Some(PresenceLeaveMsg {
            channel: self.name.clone(),
            user_id: removed.user_id,
        })
    }

    /// Sweep expired peers whose last update exceeds `ttl_ms`.
    ///
    /// Returns a list of `PresenceLeaveMsg` for each expired peer.
    pub fn sweep_expired(&mut self, ttl_ms: u64) -> Vec<PresenceLeaveMsg> {
        let mut expired = Vec::new();
        self.peers.retain(|_session_id, peer| {
            if peer.elapsed_ms() > ttl_ms {
                expired.push(PresenceLeaveMsg {
                    channel: self.name.clone(),
                    user_id: peer.user_id.clone(),
                });
                false
            } else {
                true
            }
        });
        expired
    }

    /// Build a broadcast message with all current peers.
    pub fn build_broadcast(&self) -> PresenceBroadcastMsg {
        let now = Instant::now();
        let peers = self
            .peers
            .values()
            .map(|p| PeerPresence {
                user_id: p.user_id.clone(),
                state: p.state.clone(),
                last_seen_ms: now.duration_since(p.last_seen).as_millis() as u64,
            })
            .collect();
        PresenceBroadcastMsg {
            channel: self.name.clone(),
            peers,
        }
    }

    /// The session IDs a broadcast fans out to: every peer and every
    /// listener, each once.
    pub fn session_ids(&self) -> impl Iterator<Item = &str> {
        self.peers.keys().map(|s| s.as_str()).chain(
            self.listeners
                .iter()
                .filter(|s| !self.peers.contains_key(s.as_str()))
                .map(|s| s.as_str()),
        )
    }

    /// Check if a specific session is subscribed to this channel, as a
    /// peer or a listener.
    pub fn has_session(&self, session_id: &str) -> bool {
        self.peers.contains_key(session_id) || self.listeners.contains(session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peer(user_id: &str, session_id: &str) -> PeerState {
        PeerState {
            user_id: user_id.into(),
            session_id: session_id.into(),
            state: vec![0x01, 0x02],
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn upsert_and_broadcast() {
        let mut ch = ChannelState::new("doc:d1".into());
        assert!(ch.is_empty());

        let broadcast = ch.upsert_peer("s1", make_peer("alice", "s1"));
        assert_eq!(broadcast.channel, "doc:d1");
        assert_eq!(broadcast.peers.len(), 1);
        assert_eq!(broadcast.peers[0].user_id, "alice");
        assert_eq!(ch.peer_count(), 1);

        // Second peer.
        let broadcast = ch.upsert_peer("s2", make_peer("bob", "s2"));
        assert_eq!(broadcast.peers.len(), 2);
        assert_eq!(ch.peer_count(), 2);

        // Update existing peer.
        let broadcast = ch.upsert_peer("s1", make_peer("alice", "s1"));
        assert_eq!(broadcast.peers.len(), 2); // Still 2, not 3.
    }

    #[test]
    fn remove_peer() {
        let mut ch = ChannelState::new("doc:d1".into());
        ch.upsert_peer("s1", make_peer("alice", "s1"));
        ch.upsert_peer("s2", make_peer("bob", "s2"));

        let leave = ch.remove_peer("s1").unwrap();
        assert_eq!(leave.user_id, "alice");
        assert_eq!(leave.channel, "doc:d1");
        assert_eq!(ch.peer_count(), 1);

        // Removing non-existent peer returns None.
        assert!(ch.remove_peer("s1").is_none());
    }

    #[test]
    fn sweep_expired() {
        let mut ch = ChannelState::new("doc:d1".into());
        // Insert a peer with last_seen in the past.
        let mut old_peer = make_peer("alice", "s1");
        old_peer.last_seen = Instant::now() - std::time::Duration::from_secs(60);
        ch.upsert_peer("s1", old_peer);
        ch.upsert_peer("s2", make_peer("bob", "s2")); // Fresh peer.

        let leaves = ch.sweep_expired(30_000); // 30s TTL.
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].user_id, "alice");
        assert_eq!(ch.peer_count(), 1); // Only bob remains.
    }

    #[test]
    fn session_ids() {
        let mut ch = ChannelState::new("doc:d1".into());
        ch.upsert_peer("s1", make_peer("alice", "s1"));
        ch.upsert_peer("s2", make_peer("bob", "s2"));

        let ids: Vec<&str> = ch.session_ids().collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"s1"));
        assert!(ids.contains(&"s2"));
    }

    #[test]
    fn a_listener_receives_broadcasts_without_being_present() {
        let mut ch = ChannelState::new("shape:orders".into());
        ch.add_listener("watcher");
        assert!(!ch.is_empty());
        ch.upsert_peer("s1", make_peer("alice", "s1"));
        ch.add_listener("s1");

        let mut ids: Vec<&str> = ch.session_ids().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["s1", "watcher"], "each session once");
        assert_eq!(
            ch.build_broadcast().peers.len(),
            1,
            "a listener is not present"
        );

        ch.remove_peer("s1");
        ch.remove_listener("s1");
        ch.remove_listener("watcher");
        assert!(ch.is_empty());
    }
}
