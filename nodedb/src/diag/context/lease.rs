// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for descriptor lease capture sites.
//!
//! A lease the renewal loop cannot refresh or release expires under a
//! holder that still plans against it, or stays in the lease map and holds
//! every DDL drain on its descriptor until expiry.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A descriptor lease the renewal loop could not refresh or release.
pub(in crate::diag) struct DescriptorLeaseNotRenewed<'a> {
    /// `"renew"` or `"release"`: the step that failed.
    pub step: &'static str,
    /// Debug form of the descriptor id.
    pub descriptor: &'a str,
    /// Version the step proposed.
    pub version: u64,
    pub node_id: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for DescriptorLeaseNotRenewed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.descriptor_lease_not_renewed"
    }

    fn grouping_key(&self) -> String {
        // The step and error class name the bug. The descriptor, version
        // and node are the occurrence, so a retry every tick files one report.
        format!("step={} cause={}", self.step, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "step": self.step,
            "descriptor": self.descriptor,
            "version": self.version,
            "node_id": self.node_id,
            "error_class": self.error_class,
            "why_fatal": "a lease that is not refreshed expires while this node still \
                          plans against its descriptor, and a lease that is not released \
                          blocks every DDL drain on the descriptor until it expires",
            "operator_action": "check metadata-group leadership and the applied index on \
                                 this node, then clear the error the step names",
        })
    }
}

/// A DDL preparation lease whose failure cleanup did not apply.
pub(in crate::diag) struct DdlCleanupNotCompleted<'a> {
    /// The cleanup step that failed.
    pub step: &'static str,
    /// The lease's fencing token.
    pub token: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for DdlCleanupNotCompleted<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.ddl_cleanup_not_completed"
    }

    fn grouping_key(&self) -> String {
        // The token is the occurrence.
        format!("step={} cause={}", self.step, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "step": self.step,
            "token": self.token,
            "error_class": self.error_class,
            "why_fatal": "until it is cancelled and released, the lease blocks every DDL in the \
                          cluster and its pending record keeps its descriptors reserved",
            "operator_action": "the background releaser retries the cleanup and the metadata \
                                 leader reclaims the lease after its expiry; check metadata-group \
                                 leadership if either report repeats",
        })
    }
}

/// A descriptor drain whose holder gave up and whose end did not apply.
pub(in crate::diag) struct DescriptorDrainNotEnded<'a> {
    /// Debug form of the descriptor id.
    pub descriptor: &'a str,
    /// Debug form of the drain owner.
    pub owner: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for DescriptorDrainNotEnded<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.descriptor_drain_not_ended"
    }

    fn grouping_key(&self) -> String {
        // The descriptor is the occurrence.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "descriptor": self.descriptor,
            "owner": self.owner,
            "error_class": self.error_class,
            "why_fatal": "a drain has no expiry: until it ends, lease acquires on the descriptor \
                          fail and plans on it are refused as a retryable schema change",
            "operator_action": "the background releaser retries the end; check metadata-group \
                                 leadership if the report repeats",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouping_ignores_the_descriptor_identity() {
        let first = DescriptorLeaseNotRenewed {
            step: "renew",
            descriptor: "a",
            version: 1,
            node_id: 1,
            error_class: "descriptor lease grant did not apply within 5s",
        };
        let second = DescriptorLeaseNotRenewed {
            descriptor: "b",
            version: 9,
            node_id: 3,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
    }
}
