// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for audit-log persistence capture sites.
//!
//! The audit buffer keeps every entry until the catalog commits it.
//! A catalog that keeps refusing the flush grows the buffer with no bound.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A flush whose catalog commit failed, so the entries stay in the in-memory buffer.
pub(in crate::diag) struct AuditFlushNotPersisted<'a> {
    /// Entries the failed commit carried.
    pub entry_count: usize,
    /// Highest seq the failed commit carried.
    pub through_seq: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for AuditFlushNotPersisted<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.audit_flush_not_persisted"
    }

    fn grouping_key(&self) -> String {
        // The error class names the bug; the counts are the occurrence.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "entry_count": self.entry_count,
            "through_seq": self.through_seq,
            "error_class": self.error_class,
            "why_fatal": "the entries stay readable from memory and the next flush retries \
                          them, but they are not in the system catalog. Past the buffer \
                          bound each audited action waits for a flush, up to its time \
                          bound, so a catalog that keeps failing slows every audited \
                          action and grows memory until the process stops. The audit \
                          WAL still holds the entries, so a restart replays them",
            "operator_action": "check the data directory for a full disk, a permission \
                                 error, or a damaged system.redb, and clear it. The next \
                                 flush persists the retained entries. Run VERIFY AUDIT CHAIN \
                                 afterwards to confirm the chain is whole",
        })
    }
}

/// A recorded audit entry whose WAL append failed, so it lives only in memory until a flush.
pub(in crate::diag) struct AuditWalAppendFailed<'a> {
    /// Seq of the entry that is not crash-safe.
    pub seq: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for AuditWalAppendFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.audit_wal_append_failed"
    }

    fn grouping_key(&self) -> String {
        // The error class names the bug; the seq is the occurrence.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "seq": self.seq,
            "error_class": self.error_class,
            "why_fatal": "the entry is buffered and readable, and the next flush commits it \
                          to the catalog. A crash before that flush loses it, and boot \
                          replay cannot restore it, so VERIFY AUDIT CHAIN then reports a \
                          break at the next seq",
            "operator_action": "check the audit.wal directory under the WAL directory for a \
                                 full disk or a permission error, and clear it. Entries \
                                 recorded after the fix are crash-safe again",
        })
    }
}

/// A security bus consumer that fell behind, so the bus overwrote events it had not read.
pub(in crate::diag) struct SecurityBusLagged<'a> {
    /// The bus that lost events.
    pub bus: &'a str,
    /// Events lost in this lag.
    pub dropped: u64,
}

impl DomainContext for SecurityBusLagged<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.security_bus_lagged"
    }

    fn grouping_key(&self) -> String {
        // The bus names the fault; the count is the occurrence.
        format!("bus={}", self.bus)
    }

    fn to_json(&self) -> Value {
        json!({
            "bus": self.bus,
            "dropped": self.dropped,
            "why_fatal": "each lost event costs one audit row, so the audit log misses these \
                          credential changes. The revocations themselves took effect: a hard \
                          revoke ends its sessions before it publishes. The log holds an \
                          AuditBusLagged row with this count",
            "operator_action": "check the disk under the WAL directory, because a slow audit \
                                 sync holds the consumer back, and check for a burst of \
                                 credential changes. Use the catalog to establish the current \
                                 state of users whose rows are missing",
        })
    }
}

/// A batch of security bus audit rows whose blocking task did not finish.
pub(in crate::diag) struct SecurityBusRowsNotRecorded {
    /// Rows the task carried.
    pub rows: usize,
}

impl DomainContext for SecurityBusRowsNotRecorded {
    fn domain_kind(&self) -> &'static str {
        "nodedb.security_bus_rows_not_recorded"
    }

    fn grouping_key(&self) -> String {
        // One root cause: the row count is the occurrence.
        "cause=blocking_task".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "rows": self.rows,
            "why_fatal": "the rows did not reach the audit log, or reached it without their \
                          publish, because the blocking task panicked or the runtime shut \
                          down first. The credential changes themselves took effect",
            "operator_action": "read the attached breadcrumbs for a panic. Run VERIFY AUDIT \
                                 CHAIN to confirm the chain is whole",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bus_lag_groups_by_bus_alone() {
        let first = SecurityBusLagged {
            bus: "user_change_bus",
            dropped: 1,
        };
        let second = SecurityBusLagged {
            bus: "user_change_bus",
            dropped: 9_000,
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "bus=user_change_bus");
    }

    #[test]
    fn grouping_ignores_the_counts() {
        let first = AuditFlushNotPersisted {
            entry_count: 10,
            through_seq: 40,
            error_class: "catalog",
        };
        let second = AuditFlushNotPersisted {
            entry_count: 9_000,
            through_seq: 70_000,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "cause=catalog");
    }
}
