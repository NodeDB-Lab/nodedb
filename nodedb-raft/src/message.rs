/// A single entry in the Raft log.
///
/// Each entry carries the term in which it was created and an opaque command
/// payload. The state machine interprets the payload; Raft only cares about
/// term and index for consistency.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct LogEntry {
    /// The term when this entry was received by the leader.
    pub term: u64,
    /// Log index (1-based, monotonically increasing).
    pub index: u64,
    /// Opaque command for the state machine. Empty for no-op entries
    /// (appended by newly elected leaders per Raft paper §5.4.2).
    pub data: Vec<u8>,
}

/// AppendEntries RPC (Raft paper Figure 2).
///
/// Invoked by leader to replicate log entries; also used as heartbeat
/// (entries is empty).
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct AppendEntriesRequest {
    /// Leader's term.
    pub term: u64,
    /// Leader's ID so followers can redirect clients.
    pub leader_id: u64,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: u64,
    /// Term of prev_log_index entry.
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit_index.
    pub leader_commit: u64,
    /// Raft group ID for Multi-Raft routing.
    pub group_id: u64,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself.
    pub term: u64,
    /// True if follower contained entry matching prev_log_index and prev_log_term.
    pub success: bool,
    /// Optimization: on rejection, the follower's last log index.
    /// Allows leader to skip back faster than decrementing one-by-one.
    pub last_log_index: u64,
}

/// RequestVote RPC (Raft paper Figure 2).
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RequestVoteRequest {
    /// Candidate's term.
    pub term: u64,
    /// Candidate requesting vote.
    pub candidate_id: u64,
    /// Index of candidate's last log entry.
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
    /// Raft group ID for Multi-Raft routing.
    pub group_id: u64,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself.
    pub term: u64,
    /// True means candidate received vote.
    pub vote_granted: bool,
}

/// InstallSnapshot RPC (Raft paper Figure 13).
///
/// Used when a follower is too far behind for log-based catch-up.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct InstallSnapshotRequest {
    /// Leader's term.
    pub term: u64,
    /// Leader ID.
    pub leader_id: u64,
    /// The snapshot replaces all entries up through and including this index.
    pub last_included_index: u64,
    /// Term of last_included_index.
    pub last_included_term: u64,
    /// Byte offset where chunk is positioned in the snapshot file.
    pub offset: u64,
    /// Raw bytes of the snapshot chunk.
    pub data: Vec<u8>,
    /// True if this is the last chunk.
    pub done: bool,
    /// Raft group ID for Multi-Raft routing.
    pub group_id: u64,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct InstallSnapshotResponse {
    /// Current term, for leader to update itself.
    pub term: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_entry_serde_roundtrip() {
        let entry = LogEntry {
            term: 5,
            index: 42,
            data: b"put key=val".to_vec(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn append_entries_heartbeat() {
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 1,
            prev_log_index: 10,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 8,
            group_id: 0,
        };
        assert!(req.entries.is_empty());
    }

    #[test]
    fn request_vote_serde_roundtrip() {
        let req = RequestVoteRequest {
            term: 7,
            candidate_id: 2,
            last_log_index: 100,
            last_log_term: 6,
            group_id: 5,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RequestVoteRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req.term, decoded.term);
        assert_eq!(req.candidate_id, decoded.candidate_id);
    }
}
