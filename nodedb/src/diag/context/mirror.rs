// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for a mirror's in-memory state.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A status step applied, and its row did not read back.
pub(in crate::diag) struct MirrorStatusUnread<'a> {
    pub database_id: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for MirrorStatusUnread<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.mirror_status_unread"
    }

    fn grouping_key(&self) -> String {
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "error_class": self.error_class,
            "why_fatal": "the write fence and the held-entry verdicts stay at the last status \
                          they followed. This node can refuse writes to a promoted database, or \
                          hold a mirror's entries past its final cut",
            "operator_action": "clear the catalog read error and restart this node. Boot \
                                rebuilds the fence and the verdicts from the catalog",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_database_is_the_occurrence() {
        let first = MirrorStatusUnread {
            database_id: 1,
            error_class: "storage",
        };
        let second = MirrorStatusUnread {
            database_id: 2,
            error_class: "storage",
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
    }
}
