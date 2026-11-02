// SPDX-License-Identifier: BUSL-1.1

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
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
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
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
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
    /// Leader-lease round this request belongs to. The follower echoes it so
    /// the leader can anchor its lease at the round's send time.
    pub round: u64,
    /// The highest log index every voter's log is known to hold (see
    /// `RaftNode::replicated_floor`). The follower keeps the highest value.
    pub replicated_floor: u64,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself.
    pub term: u64,
    /// True if follower contained entry matching prev_log_index and prev_log_term.
    pub success: bool,
    /// On success, the last entry the follower shares with the leader and
    /// holds durably. The leader takes it as the follower's match index.
    /// On rejection, the follower's last log index, so the leader skips back
    /// faster than one entry at a time.
    pub last_log_index: u64,
    /// `round` of the request this answers.
    pub round: u64,
    /// What the follower needs before it takes entries. A rejection from a
    /// follower that wants a snapshot gets one instead of entries. The
    /// leader records it per peer: a peer whose data diverged cannot lead.
    pub replica: crate::state::ReplicaState,
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
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
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
    /// Set by a campaign that a `TimeoutNow` started. Only such a campaign
    /// may win a vote from a node that still hears a live leader: the leader
    /// asked for its own replacement and stopped serving lease reads first.
    pub transfer: bool,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself.
    pub term: u64,
    /// True means candidate received vote.
    pub vote_granted: bool,
}

/// PreVote RPC.
///
/// Sent before a candidate bumps its term, to ask whether peers WOULD vote
/// for it. A node partitioned from the cluster otherwise inflates its term on
/// every failed election and forces the healthy leader to step down on heal.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PreVoteRequest {
    /// The HYPOTHETICAL term the candidate would campaign in
    /// (`current_term + 1`). No node ever adopts it.
    pub term: u64,
    /// Candidate probing for support.
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
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PreVoteResponse {
    /// The responder's REAL current term — not the hypothetical one probed.
    pub term: u64,
    /// True means the responder would vote for the candidate.
    pub vote_granted: bool,
}

/// TimeoutNow RPC (Raft thesis — leadership transfer).
///
/// Sent by a leader to a caught-up target voter to make it immediately start
/// an election (bypassing its election timeout). It carries no entries and
/// grants no vote — the recipient runs a normal election at `term + 1`.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TimeoutNowRequest {
    /// Leader's term at the time the transfer was initiated.
    pub term: u64,
    /// The leader initiating the transfer.
    pub leader_id: u64,
    /// Raft group ID for Multi-Raft routing.
    pub group_id: u64,
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
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
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
    /// Total snapshot size in bytes. `0` means "unknown" (bootstrap stubs).
    /// Receivers use this only as an advisory hint; they must not reject
    /// chunks when `total_size == 0`.
    pub total_size: u64,
    /// The group's voters as the leader holds them when it sends the final
    /// chunk, the leader included. A snapshot covers the conf changes of its
    /// range, and the receiver never applies them, so it takes the
    /// membership from here. Empty on every other chunk.
    pub voters: Vec<u64>,
    /// The group's learners as the leader holds them when it sends the final
    /// chunk. Empty on every other chunk.
    pub learners: Vec<u64>,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
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
        let json = sonic_rs::to_string(&entry).unwrap();
        let decoded: LogEntry = sonic_rs::from_str(&json).unwrap();
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
            round: 1,
            replicated_floor: 0,
        };
        assert!(req.entries.is_empty());
    }

    #[test]
    fn pre_vote_serde_roundtrip() {
        let req = PreVoteRequest {
            term: 8,
            candidate_id: 3,
            last_log_index: 120,
            last_log_term: 7,
            group_id: 5,
        };
        let json = sonic_rs::to_string(&req).unwrap();
        let decoded: PreVoteRequest = sonic_rs::from_str(&json).unwrap();
        assert_eq!(req.term, decoded.term);
        assert_eq!(req.candidate_id, decoded.candidate_id);
        assert_eq!(req.last_log_index, decoded.last_log_index);
        assert_eq!(req.last_log_term, decoded.last_log_term);
        assert_eq!(req.group_id, decoded.group_id);

        let resp = PreVoteResponse {
            term: 7,
            vote_granted: true,
        };
        let json = sonic_rs::to_string(&resp).unwrap();
        let decoded: PreVoteResponse = sonic_rs::from_str(&json).unwrap();
        assert_eq!(resp.term, decoded.term);
        assert!(decoded.vote_granted);
    }

    #[test]
    fn request_vote_serde_roundtrip() {
        let req = RequestVoteRequest {
            term: 7,
            candidate_id: 2,
            last_log_index: 100,
            last_log_term: 6,
            group_id: 5,
            transfer: true,
        };
        let json = sonic_rs::to_string(&req).unwrap();
        let decoded: RequestVoteRequest = sonic_rs::from_str(&json).unwrap();
        assert_eq!(req.term, decoded.term);
        assert_eq!(req.candidate_id, decoded.candidate_id);
        assert!(decoded.transfer);
    }
}
