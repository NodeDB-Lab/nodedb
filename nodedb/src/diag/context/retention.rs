// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for retention policy capture sites.
//!
//! A dropped policy whose tier aggregates survive keeps consuming storage and
//! CPU with no policy row left to explain it.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A dropped retention policy whose auto-wired continuous aggregates could
/// not be unregistered, so the tier aggregates outlive the policy.
pub(in crate::diag) struct RetentionAutowireOrphaned<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Policy the drop removed.
    pub policy: &'a str,
    /// Collection the policy targeted.
    pub collection: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for RetentionAutowireOrphaned<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.retention_autowire_orphaned"
    }

    fn grouping_key(&self) -> String {
        // The error class names the bug; the ids are the occurrence.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "policy": self.policy,
            "collection": self.collection,
            "error_class": self.error_class,
            "why_fatal": "the policy row is gone from every replica, so nothing owns the \
                          tier aggregates the policy created. They keep refreshing on every \
                          flush and seal, consuming storage and CPU, and no SHOW statement \
                          links them back to a policy an operator can drop",
            "operator_action": "drop the orphaned aggregates by name — they follow the \
                                 _policy_<name>_tier<N> pattern for the named policy — after \
                                 clearing the underlying dispatch error",
        })
    }
}

/// A committed `TRUNCATE ... RESTART IDENTITY` whose sequence kept its counter.
pub(in crate::diag) struct SequenceRestartFailed<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub detail: &'a str,
}

impl DomainContext for SequenceRestartFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.sequence_restart_failed"
    }

    fn grouping_key(&self) -> String {
        format!(
            "database={} collection={}",
            self.database_id, self.collection
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "detail": self.detail,
            "why_fatal": "the truncate committed, so the transaction reports success. The \
                          sequence keeps its counter, so it repeats no value and its next value \
                          sits above the start",
            "operator_action": "run ALTER SEQUENCE <name> RESTART WITH <start> once the cause \
                                clears",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orphan_grouping_ignores_the_policy_identity() {
        let first = RetentionAutowireOrphaned {
            database_id: 1,
            tenant_id: 2,
            policy: "sensor_policy",
            collection: "sensor_data",
            error_class: "dispatch timeout",
        };
        let second = RetentionAutowireOrphaned {
            database_id: 90,
            tenant_id: 91,
            policy: "other",
            collection: "other_data",
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "cause=dispatch timeout");
    }
}
