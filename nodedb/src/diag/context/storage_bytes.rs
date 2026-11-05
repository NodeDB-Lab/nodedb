// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for the persisted stored-bytes counters.
//!
//! Each redb file a Data Plane core owns counts the bytes its collections
//! store. A counter that disagrees with the rows it counts misreports the
//! storage a tenant holds, and every quota or bill read from it.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A counter change that took a counter below zero, so it clamped at zero.
pub(in crate::diag) struct StorageBytesUnderflow<'a> {
    /// The counter's redb file or table.
    pub counter: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    /// The counted collection, absent on a per-tenant counter.
    pub collection: Option<&'a str>,
    /// Bytes the counter held before the change.
    pub held: u64,
    /// Bytes the change removed.
    pub removed: u64,
}

impl DomainContext for StorageBytesUnderflow<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.storage_bytes_underflow"
    }

    fn grouping_key(&self) -> String {
        // The counter names the bug. The scope and byte counts are the
        // occurrence.
        format!("counter={}", self.counter)
    }

    fn to_json(&self) -> Value {
        json!({
            "counter": self.counter,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "held": self.held,
            "removed": self.removed,
            "why_fatal": "a write path removed stored rows the counter never counted in. \
                          The counter clamps at zero and reads low, so storage totals \
                          and every quota read from them under-report the scope",
            "operator_action": "find the write path of the named counter that stores rows \
                                 without counting them. The counter stays low until the \
                                 scope's rows are dropped and rewritten",
        })
    }
}

/// A dropped scope whose counter row still held bytes after every counted
/// row of the scope was gone.
pub(in crate::diag) struct StorageBytesResidue<'a> {
    /// The counter's redb file.
    pub counter: &'static str,
    /// The dropped scope (`collection`, `tenant`).
    pub scope: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    /// Bytes the counter row held.
    pub bytes: u64,
}

impl DomainContext for StorageBytesResidue<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.storage_bytes_residue"
    }

    fn grouping_key(&self) -> String {
        // Counter and scope name the bug. The ids and bytes are the
        // occurrence.
        format!("counter={};scope={}", self.counter, self.scope)
    }

    fn to_json(&self) -> Value {
        json!({
            "counter": self.counter,
            "scope": self.scope,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "bytes": self.bytes,
            "why_fatal": "a write path removed stored rows without taking their bytes off \
                          the counter. Before the drop the counter read high, so storage \
                          totals and quotas over-reported the scope",
            "operator_action": "find the remove path of the named counter that skips the \
                                 counter. The drop removed the stale row, so the scope \
                                 starts again at zero",
        })
    }
}

/// A storage sample that did not finish, so the published share stays at
/// the last sample.
pub(in crate::diag) struct StorageBytesSampleFailed<'a> {
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for StorageBytesSampleFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.storage_bytes_sample_failed"
    }

    fn grouping_key(&self) -> String {
        // The error class names the bug. Each retry is an occurrence.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "error_class": self.error_class,
            "why_fatal": "the node's storage share, the database storage gauge and tenant \
                          storage usage keep the last sample. Storage quotas read stale \
                          totals until a sample succeeds",
            "operator_action": "clear the error this report names. The next sample retries \
                                 with no operator step",
        })
    }
}

/// A peer that did not answer a storage round, so the round skipped it.
pub(in crate::diag) struct StorageSharePeerSkipped<'a> {
    /// The peer the round skipped.
    pub peer: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for StorageSharePeerSkipped<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.storage_share_peer_skipped"
    }

    fn grouping_key(&self) -> String {
        // The peer id changes per occurrence. The error class names the cause.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "peer": self.peer,
            "error_class": self.error_class,
            "why_fatal": "the cluster storage total keeps the peer's last report until it \
                          is three sample periods old, then counts the peer's groups as zero. \
                          Storage quotas read a smaller total until the peer answers",
            "operator_action": "check that the peer is up and reachable over the cluster \
                                 transport. The next round asks again with no operator step",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn underflow_grouping_ignores_the_scope() {
        let first = StorageBytesUnderflow {
            counter: "sparse",
            database_id: 1,
            tenant_id: 2,
            collection: Some("a"),
            held: 5,
            removed: 9,
        };
        let second = StorageBytesUnderflow {
            database_id: 7,
            collection: Some("b"),
            held: 0,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "counter=sparse");
    }

    #[test]
    fn residue_grouping_keys_counter_and_scope() {
        let first = StorageBytesResidue {
            counter: "graph",
            scope: "tenant",
            database_id: 1,
            tenant_id: 2,
            collection: "a",
            bytes: 10,
        };
        let second = StorageBytesResidue {
            collection: "z",
            bytes: 99,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "counter=graph;scope=tenant");
    }

    #[test]
    fn sample_failure_grouping_keys_the_cause() {
        let ctx = StorageBytesSampleFailed {
            error_class: "deadline",
        };
        assert_eq!(ctx.grouping_key(), "cause=deadline");
    }

    #[test]
    fn peer_skip_grouping_keys_the_cause() {
        let first = StorageSharePeerSkipped {
            peer: 2,
            error_class: "dispatch",
        };
        let second = StorageSharePeerSkipped { peer: 3, ..first };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "cause=dispatch");
    }
}
