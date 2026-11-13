// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for chunked redo streams: an abandon that stopped
//! retrying, and a stream entry held on a replica that dropped the stream.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// The abandoner stopped retrying a session stream's abandon while the
/// stream was open on this node.
pub(in crate::diag) struct RedoAbandonGivenUp {
    /// The stream, as its `Debug` text.
    pub stream: String,
    pub vshard: u32,
    /// The data group whose log carries the stream.
    pub group_id: u64,
    /// The Raft term of the stream's first chunk entry.
    pub term: u64,
    /// The stream's declared byte length.
    pub len: u64,
    /// The last abandon proposal's error, when one ran.
    pub last_error: Option<String>,
}

impl DomainContext for RedoAbandonGivenUp {
    fn domain_kind(&self) -> &'static str {
        "nodedb.redo_abandon_given_up"
    }

    fn grouping_key(&self) -> String {
        // One bug class: an abandon that never applied before the node
        // stopped. The stream and its group are the occurrence.
        "redo_abandon_given_up".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "stream": self.stream,
            "vshard": self.vshard,
            "group_id": self.group_id,
            "term": self.term,
            "len": self.len,
            "last_error": self.last_error,
            "impact": "every replica holds the stream's chunks and a WAL floor at its \
                       first chunk record. They stay until the group applies an entry \
                       of a later term, so the WAL on each replica cannot truncate past \
                       that record meanwhile",
            "operator_action": "a leadership transfer of the named group ends the term \
                                 and drops the stream on every replica. The last error \
                                 names why the abandon proposal did not apply",
        })
    }
}

/// A chunk or final entry of a redo stream refused on a replica that owes
/// its group a snapshot install: this node dropped the group's streams when
/// it left the group, and a remount resumed the group's old log.
pub(in crate::diag) struct RedoStreamLostHere {
    /// The data group whose log carries the stream.
    pub group_id: u64,
    /// The held entry's index in the group's log.
    pub log_index: u64,
    /// The stream, as its `Debug` text.
    pub stream: String,
    pub vshard: u32,
    /// `chunk` or `final`.
    pub entry: &'static str,
    /// Why this replica's stream refused the entry.
    pub refusal: String,
}

impl DomainContext for RedoStreamLostHere {
    fn domain_kind(&self) -> &'static str {
        "nodedb.redo_stream_lost_here"
    }

    fn grouping_key(&self) -> String {
        // One bug class: a log resumed past streams this replica dropped.
        // The group, stream, and index are the occurrence.
        "redo_stream_lost_here".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "group_id": self.group_id,
            "log_index": self.log_index,
            "stream": self.stream,
            "vshard": self.vshard,
            "entry": self.entry,
            "refusal": self.refusal,
            "impact": "the other replicas can install the stream's redo, which this replica \
                       lacks. The group's apply here holds at the entry: no later entry \
                       applies, its applied index stays below the entry, and reads wait \
                       until a snapshot of the group installs",
            "operator_action": "the group's replica here refuses log entries, steps down \
                                 from leadership, and never campaigns. The group's leader \
                                 then sends a snapshot. When the group has no other \
                                 replica, no snapshot comes: restore the group from a \
                                 backup",
        })
    }
}

/// The row changes of an installed chunked redo did not read back from this
/// node's WAL after every retry, so the entry holds its group's apply.
pub(in crate::diag) struct RedoChangesUnstaged {
    /// The data group of the committed entry.
    pub group_id: u64,
    /// The entry's index in the group's log.
    pub log_index: u64,
    /// Why the read failed.
    pub error: String,
}

impl DomainContext for RedoChangesUnstaged {
    fn domain_kind(&self) -> &'static str {
        "nodedb.redo_changes_unstaged"
    }

    fn grouping_key(&self) -> String {
        // One bug class: a stream's content that does not read back after
        // its install. The group and index are the occurrence.
        "redo_changes_unstaged".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "group_id": self.group_id,
            "log_index": self.log_index,
            "error": self.error,
            "impact": "the entry's rows installed, and its change events did not stage \
                       after every retry. The group's apply holds at the entry until a \
                       snapshot of the group installs. A cursor of this node's feed below \
                       the install expires, so no consumer misses an event silently",
            "operator_action": "the error names the WAL read that failed. Check the node's \
                                 WAL directory and disk. The group's leader sends the \
                                 snapshot",
        })
    }
}

/// The membership pass failed to persist that data groups this node left
/// owe a snapshot install, so their redo streams stay.
pub(in crate::diag) struct RedoSnapshotDebtNotRecorded {
    /// The catalog error.
    pub error: String,
}

impl DomainContext for RedoSnapshotDebtNotRecorded {
    fn domain_kind(&self) -> &'static str {
        "nodedb.redo_snapshot_debt_not_recorded"
    }

    fn grouping_key(&self) -> String {
        // The pass retries on every tick: one report per root cause.
        "redo_snapshot_debt_not_recorded".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "error": self.error,
            "impact": "the left groups' redo streams and their WAL floor holds stay on this \
                       node, so the WAL cannot truncate past their first chunk records",
            "operator_action": "the error names why the system catalog refused the write. \
                                 The pass retries on every membership tick",
        })
    }
}
