// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for a KV expiry entry the reaper cannot reap.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A due KV expiry whose stored collection name resolves to no collection.
pub(in crate::diag) struct KvReapEntryUnusable<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for KvReapEntryUnusable<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.kv_reap_entry_unusable"
    }

    fn grouping_key(&self) -> String {
        // The entry stays due, so each pass meets it again. The cause names
        // the bug, so the passes collapse into one report.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "error_class": self.error_class,
            "why_fatal": "the key never expires, and it stays due on every reap pass. Other \
                          keys in the pass still reap",
            "operator_action": "read the error chain for the stored collection name, then \
                                 drop the collection or restore the row",
        })
    }
}
