// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for dead-letter entries that storage did not hold: an
//! entry the store refused, and a stored entry the queue refused on restore.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A rejected CRDT delta whose dead-letter entry the store refused, so the
/// rejection has no durable record.
pub(in crate::diag) struct CrdtDeadLetterNotStored<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection the delta wrote.
    pub collection: &'a str,
    /// Constraint the delta violated.
    pub constraint: &'a str,
    /// Record whose apply rejected the delta.
    pub source_lsn: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for CrdtDeadLetterNotStored<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.crdt_dead_letter_not_stored"
    }

    fn grouping_key(&self) -> String {
        // The error class names the cause. Tenant, collection, constraint and
        // record are the occurrence, so a failing store files one report.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "constraint": self.constraint,
            "source_lsn": self.source_lsn,
            "error_class": self.error_class,
            "why_reported": "the delta violated a constraint and was refused, but the store \
                             did not take its dead-letter entry. The entry is removed from \
                             the queue and the write that needed it fails: a live apply \
                             refuses with an error, a sync apply holds its high-water-mark, \
                             and restart replay stops",
            "operator_action": "check the sparse store (disk space, permissions, redb \
                                 health) on this node, then let the sender re-push or \
                                 restart the node",
        })
    }
}

/// A stored dead-letter entry the in-memory queue refused while the tenant's
/// CRDT engine opened.
pub(in crate::diag) struct CrdtDeadLetterNotRestored<'a> {
    pub tenant_id: u64,
    /// Record that produced the refused entry, when it names one.
    pub source_lsn: Option<u64>,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for CrdtDeadLetterNotRestored<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.crdt_dead_letter_not_restored"
    }

    fn grouping_key(&self) -> String {
        // The tenant and record are the occurrence, so every refused open of
        // one oversized store files one report.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "tenant_id": self.tenant_id,
            "source_lsn": self.source_lsn,
            "error_class": self.error_class,
            "why_reported": "storage holds more dead-letter entries for the tenant than its \
                             queue takes. Every stored entry was accepted by the queue when \
                             it was written, so this is a broken invariant. The tenant's \
                             CRDT engine does not open, so no CRDT write for the tenant \
                             applies and restart replay stops",
            "operator_action": "compare the tenant's stored dead-letter entries against the \
                                 queue capacity, and purge the collections whose rejected \
                                 deltas are resolved",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_store_error_groups_by_its_cause_alone() {
        let first = CrdtDeadLetterNotStored {
            database_id: 1,
            tenant_id: 2,
            collection: "users",
            constraint: "users_email_unique",
            source_lsn: 11,
            error_class: "redb",
        };
        let second = CrdtDeadLetterNotStored {
            database_id: 3,
            tenant_id: 4,
            collection: "orders",
            constraint: "orders_fk",
            source_lsn: 90,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "cause=redb");
    }

    #[test]
    fn a_refused_restore_groups_by_its_cause_alone() {
        let first = CrdtDeadLetterNotRestored {
            tenant_id: 2,
            source_lsn: Some(11),
            error_class: "dead-letter queue full",
        };
        let second = CrdtDeadLetterNotRestored {
            tenant_id: 9,
            source_lsn: None,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
    }
}
