// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for spill-file cleanup capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A spill directory or file that cleanup could not remove.
pub(in crate::diag) struct SpillCleanupFailed<'a> {
    /// Spill owner: `txn`, `join`, `sort` or `groupby`.
    pub kind: &'static str,
    /// `dir` or `file`: what the cleanup removed.
    pub target: &'static str,
    pub path: &'a str,
}

impl DomainContext for SpillCleanupFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.spill_cleanup_failed"
    }

    fn grouping_key(&self) -> String {
        // The owner and the target name the bug. The path is the occurrence.
        format!("kind={};target={}", self.kind, self.target)
    }

    fn to_json(&self) -> Value {
        json!({
            "kind": self.kind,
            "target": self.target,
            "path": self.path,
            "effect": "spill bytes stay on disk. Spill files are never durable state, \
                       so no data is wrong, but each occurrence keeps disk space and the \
                       operation that owned the spill returns an error",
            "operator_action": "check the permissions and the free space of the data \
                                 directory's spill roots (txn-spill, join-spill, sort-spill, \
                                 groupby-spill). A restart sweeps them",
        })
    }
}
