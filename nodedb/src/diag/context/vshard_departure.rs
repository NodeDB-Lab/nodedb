// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for the departure purge of vShards this node left.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A step of the departure purge failed. The vShards stay pending and the
/// next pass retries them.
pub(in crate::diag) struct VShardDeparturePurgeFailed {
    /// The step that failed: `ledger`, `snapshot_debt`, `barrier`, `purge`
    /// or `settle`.
    pub step: &'static str,
    /// The vShards the step ran for.
    pub vshards: Vec<u32>,
    /// The error the step returned.
    pub error: String,
}

impl DomainContext for VShardDeparturePurgeFailed {
    fn domain_kind(&self) -> &'static str {
        "nodedb.vshard_departure_purge_failed"
    }

    fn grouping_key(&self) -> String {
        // The pass retries every tick: one report per failing step.
        format!("vshard_departure_purge_failed:{}", self.step)
    }

    fn to_json(&self) -> Value {
        json!({
            "step": self.step,
            "vshards": self.vshards,
            "error": self.error,
            "impact": "this node keeps a stale copy of the vShards' data until a later pass \
                       purges it. Their new owners serve them, so no read reaches the copy",
            "operator_action": "the error names the failing engine or catalog write. The \
                                 purge retries on every pass and after a restart",
        })
    }
}
