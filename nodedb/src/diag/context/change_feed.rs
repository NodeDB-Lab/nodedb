// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for the Control-Plane change feed: a change-journal
//! write or read that did not succeed.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// The change journal did not take or return events of one data group's
/// feed.
pub(in crate::diag) struct ChangeJournalFailed {
    /// The data group whose feed the events belong to.
    pub group_id: u64,
    /// What the journal did not do: `staged page write`, `spilled page
    /// read`, `floor batch write`, `forwarded run write`, or `forwarded
    /// position read`.
    pub operation: &'static str,
    /// Why it failed.
    pub error: String,
}

impl DomainContext for ChangeJournalFailed {
    fn domain_kind(&self) -> &'static str {
        "nodedb.change_journal_failed"
    }

    fn grouping_key(&self) -> String {
        // One bug class per operation. The group is the occurrence.
        format!("change_journal_failed:{}", self.operation)
    }

    fn to_json(&self) -> Value {
        json!({
            "group_id": self.group_id,
            "operation": self.operation,
            "error": self.error,
            "impact": "a staged page that did not write stays in memory, so the entry's \
                       memory is no longer bounded by one page. A spilled page that did not \
                       read back leaves a hole: every cursor below it resets. A floor batch \
                       that did not write holds the group's applied floor, and the WAL keeps \
                       every entry above it. A forwarded run that did not write is refused: \
                       this node takes none of it, and the leader sends the stretch again",
            "operator_action": "the error names the journal write or read that failed. Check \
                                 the disk under the node's event-plane directory, where \
                                 change_feed.redb lives",
        })
    }
}

/// The change journal kept refusing one group's events, so the group
/// halts.
pub(in crate::diag) struct ChangeJournalHalted {
    /// The data group that halts.
    pub group_id: u64,
    /// The failure count, the events waiting, and the last error.
    pub detail: String,
}

impl DomainContext for ChangeJournalHalted {
    fn domain_kind(&self) -> &'static str {
        "nodedb.change_journal_halted"
    }

    fn grouping_key(&self) -> String {
        // One bug class: a journal that stays broken. The group is the
        // occurrence.
        "change_journal_halted".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "group_id": self.group_id,
            "detail": self.detail,
            "impact": "a replicated group settles no more entries, and a forwarded partition \
                       takes no more runs, until a journal write succeeds. The node reports \
                       not ready. A replicated group's delivered events wait in memory for \
                       the journal. A forwarded partition's events wait at its leader",
            "operator_action": "the detail names the journal error. Check the disk under the \
                                 node's event-plane directory, where change_feed.redb lives. \
                                 The group resumes on the first write that succeeds",
        })
    }
}

/// A follower rejected a partition's change feed as one no node can take.
pub(in crate::diag) struct ChangeRunRejected {
    /// The follower that rejected the feed.
    pub peer: u64,
    /// The data group whose feed it rejected.
    pub group_id: u64,
    /// The follower's reason.
    pub detail: String,
}

impl DomainContext for ChangeRunRejected {
    fn domain_kind(&self) -> &'static str {
        "nodedb.change_run_rejected"
    }

    fn grouping_key(&self) -> String {
        // One bug class: a feed message the follower cannot read. The peer
        // and the group are the occurrence.
        "change_run_rejected".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "peer": self.peer,
            "group_id": self.group_id,
            "detail": self.detail,
            "impact": "this node stops feeding the group's change events to the follower. \
                       The follower's feed of the group falls behind until its feed task \
                       starts again",
            "operator_action": "the detail names what the follower could not read. Check that \
                                 both nodes run the same build",
        })
    }
}
