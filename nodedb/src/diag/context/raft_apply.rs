// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for the data-group apply path.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// The apply loop reached a Raft log index it had already applied.
pub(in crate::diag) struct RaftEntryReapplied {
    pub group_id: u64,
    pub log_index: u64,
    /// The highest index of the group the apply loop applied before.
    pub highest_applied: u64,
}

impl DomainContext for RaftEntryReapplied {
    fn domain_kind(&self) -> &'static str {
        "nodedb.raft_entry_reapplied"
    }

    fn grouping_key(&self) -> String {
        // One bug class: a path handed an applied index to the apply loop
        // again. Group and index are the occurrence.
        "raft_entry_reapplied".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "group_id": self.group_id,
            "log_index": self.log_index,
            "highest_applied": self.highest_applied,
            "why_fatal": "a committed entry applies once per replica. A second apply of an \
                          append-shaped write (timeseries, columnar, spatial, predicated \
                          update) stores its rows twice, mints a second redo record, fires \
                          AFTER triggers twice, and records the write's mark again",
            "operator_action": "the entry reached the apply loop twice past the applier's \
                                 delivery watermark. Find the path that re-queued it: \
                                 re-delivery after a refused hand-off, snapshot install, or \
                                 a second applier feeding the same channel",
        })
    }
}
