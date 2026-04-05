/// Persistent state that must survive restarts.
///
/// Corresponds to Raft paper Figure 2 "Persistent state on all servers".
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct HardState {
    /// Latest term this server has seen.
    pub current_term: u64,
    /// Candidate that received vote in current term (0 = none).
    pub voted_for: u64,
}

impl HardState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: 0,
        }
    }
}

impl Default for HardState {
    fn default() -> Self {
        Self::new()
    }
}

/// Role of a Raft node within a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
    /// Non-voting member catching up : new node joins as learner first).
    Learner,
}

/// Volatile state on all servers.
#[derive(Debug, Clone)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed.
    pub commit_index: u64,
    /// Index of highest log entry applied to state machine.
    pub last_applied: u64,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

impl Default for VolatileState {
    fn default() -> Self {
        Self::new()
    }
}

/// Volatile state on leaders (reinitialized after election).
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each peer: index of next log entry to send.
    pub next_index: Vec<(u64, u64)>,
    /// For each peer: index of highest log entry known to be replicated.
    pub match_index: Vec<(u64, u64)>,
}

impl LeaderState {
    pub fn new(peers: &[u64], last_log_index: u64) -> Self {
        Self {
            next_index: peers.iter().map(|&id| (id, last_log_index + 1)).collect(),
            match_index: peers.iter().map(|&id| (id, 0)).collect(),
        }
    }

    pub fn next_index_for(&self, peer: u64) -> u64 {
        self.next_index
            .iter()
            .find(|&&(id, _)| id == peer)
            .map(|&(_, idx)| idx)
            .unwrap_or(1)
    }

    pub fn set_next_index(&mut self, peer: u64, index: u64) {
        if let Some(entry) = self.next_index.iter_mut().find(|e| e.0 == peer) {
            entry.1 = index;
        }
    }

    pub fn match_index_for(&self, peer: u64) -> u64 {
        self.match_index
            .iter()
            .find(|&&(id, _)| id == peer)
            .map(|&(_, idx)| idx)
            .unwrap_or(0)
    }

    pub fn set_match_index(&mut self, peer: u64, index: u64) {
        if let Some(entry) = self.match_index.iter_mut().find(|e| e.0 == peer) {
            entry.1 = index;
        }
    }

    /// Add a new peer to leader tracking. Initializes next_index to
    /// `last_log_index + 1` (the peer needs to catch up from the end of the log).
    pub fn add_peer(&mut self, peer: u64, last_log_index: u64) {
        if !self.next_index.iter().any(|&(id, _)| id == peer) {
            self.next_index.push((peer, last_log_index + 1));
            self.match_index.push((peer, 0));
        }
    }

    /// Remove a peer from leader tracking.
    pub fn remove_peer(&mut self, peer: u64) {
        self.next_index.retain(|&(id, _)| id != peer);
        self.match_index.retain(|&(id, _)| id != peer);
    }

    /// Current peer list tracked by this leader state.
    pub fn peers(&self) -> Vec<u64> {
        self.next_index.iter().map(|&(id, _)| id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_default() {
        let hs = HardState::new();
        assert_eq!(hs.current_term, 0);
        assert_eq!(hs.voted_for, 0);
    }

    #[test]
    fn leader_state_initialization() {
        let peers = vec![2, 3, 4];
        let ls = LeaderState::new(&peers, 10);
        assert_eq!(ls.next_index_for(2), 11);
        assert_eq!(ls.next_index_for(3), 11);
        assert_eq!(ls.match_index_for(2), 0);
    }

    #[test]
    fn leader_state_update() {
        let peers = vec![2, 3];
        let mut ls = LeaderState::new(&peers, 5);
        ls.set_next_index(2, 8);
        ls.set_match_index(2, 7);
        assert_eq!(ls.next_index_for(2), 8);
        assert_eq!(ls.match_index_for(2), 7);
        // Peer 3 unchanged.
        assert_eq!(ls.next_index_for(3), 6);
    }

    #[test]
    fn node_role_equality() {
        assert_eq!(NodeRole::Follower, NodeRole::Follower);
        assert_ne!(NodeRole::Follower, NodeRole::Leader);
    }
}
