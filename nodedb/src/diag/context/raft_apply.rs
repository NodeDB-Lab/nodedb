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

/// The catalog refused a read or write of a vShard's stored dependent-read
/// barrier log.
pub(in crate::diag) struct CalvinBarrierLogStoreFailed<'a> {
    pub vshard_id: u32,
    /// The txn's `(epoch, position)`. A removal of every row of the vShard
    /// names none.
    pub txn: Option<(u64, u32)>,
    /// What the store did: `save`, `load` or `remove`.
    pub op: &'static str,
    pub error_class: &'a str,
}

impl DomainContext for CalvinBarrierLogStoreFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.calvin_barrier_log_store_failed"
    }

    fn grouping_key(&self) -> String {
        // One report per vShard and operation: the txn changes per event.
        format!(
            "calvin_barrier_log_store_failed:{}:{}",
            self.op, self.vshard_id
        )
    }

    fn to_json(&self) -> Value {
        let effect = match self.op {
            "save" => {
                "the barrier event stays in memory, and every later event of the \
                       txn too. The entry does not count as durably applied, so a restart \
                       delivers it again from the log"
            }
            "load" => {
                "the scheduler keeps the txn's barrier open and reads the row \
                       again on its next pass"
            }
            _ => {
                "the row of a finished txn stays stored. The scheduler's next \
                  stall-tick sweep removes it"
            }
        };
        json!({
            "vshard_id": self.vshard_id,
            "txn": self.txn,
            "op": self.op,
            "error_class": self.error_class,
            "effect": effect,
            "operator_action": "check the system catalog's disk: free space, I/O errors, \
                                and the redb file's permissions",
        })
    }
}
