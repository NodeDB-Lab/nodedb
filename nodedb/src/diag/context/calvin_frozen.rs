// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for the stored frozen Calvin `RETURNING` replies.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// The catalog refused a read or write of the stored frozen replies.
pub(in crate::diag) struct CalvinFrozenReplyStoreFailed<'a> {
    pub vshard_id: u32,
    /// The txn's `(epoch, position)`. An operation on every row of a vShard
    /// names none.
    pub txn: Option<(u64, u32)>,
    /// What the store did: `store`, `load`, `remove` or `forget`.
    pub op: &'static str,
    pub error_class: &'a str,
}

impl DomainContext for CalvinFrozenReplyStoreFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.calvin_frozen_reply_store_failed"
    }

    fn grouping_key(&self) -> String {
        // One report per vShard and operation: the txn changes per entry,
        // and a retry loop repeats the same pair.
        format!(
            "calvin_frozen_reply_store_failed:{}:{}",
            self.op, self.vshard_id
        )
    }

    fn to_json(&self) -> Value {
        let effect = match self.op {
            "store" => {
                "the frozen reply is not stored. The entry does not count as durably \
                 applied, so a restart delivers it again from the log. The slice's leader \
                 votes once a copy is stored"
            }
            "load" => {
                "the slice's leader holds its vote, or the install answers the read \
                 failure to the statement. The writes of an installed slice stay"
            }
            "forget" => {
                "the rows of a vShard this node left stay stored. Every scheduler \
                 reconcile pass removes them again until one succeeds"
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
