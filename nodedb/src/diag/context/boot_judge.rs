// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for the boot judge of committed records with no
//! outcome.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A committed record with no outcome whose WAL holds no complete commit
/// boundary, so boot can not validate it.
pub(in crate::diag) struct RedoBoundaryMissing {
    pub lsn: u64,
    pub vshard_id: u32,
}

impl DomainContext for RedoBoundaryMissing {
    fn domain_kind(&self) -> &'static str {
        "nodedb.redo_boundary_missing"
    }

    fn grouping_key(&self) -> String {
        // One cause: a funnel append that lost the boundary. The LSN is
        // the occurrence.
        "redo_boundary_missing".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "lsn": self.lsn,
            "vshard_id": self.vshard_id,
            "why_fatal": "the funnel appends the boundary before the record, and the WAL is \
                          durable in LSN order, so a durable record with no boundary means \
                          the WAL lost records below it. Replaying the record without its \
                          validate can apply a write its install would refuse",
            "operator_action": "restore the node from a replica or a backup",
        })
    }
}

/// A committed record's WAL header names an event source this build does
/// not know.
pub(in crate::diag) struct RedoEventSourceUnknown {
    pub lsn: u64,
    pub code: u8,
}

impl DomainContext for RedoEventSourceUnknown {
    fn domain_kind(&self) -> &'static str {
        "nodedb.redo_event_source_unknown"
    }

    fn grouping_key(&self) -> String {
        format!("code={}", self.code)
    }

    fn to_json(&self) -> Value {
        json!({
            "lsn": self.lsn,
            "code": self.code,
            "why_fatal": "the header is covered by its checksum, so the code is what the \
                          writer stored, and this build can not rebuild the record's install",
            "operator_action": "start the node with the build that wrote the WAL",
        })
    }
}
