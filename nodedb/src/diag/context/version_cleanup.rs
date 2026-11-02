// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for the version cleanup pass.
//!
//! A pass that fails leaves superseded versions in place. History then grows
//! until a later pass succeeds, and every engine's memory and disk budget
//! carries it.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A version cleanup pass that did not finish.
pub(in crate::diag) struct VersionCleanupFailed<'a> {
    /// The pass step that failed: the horizon, the cut close, or a core's
    /// purge.
    pub step: &'a str,
    /// The horizon the pass purged below, `0` when none was computed.
    pub horizon: i64,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for VersionCleanupFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.version_cleanup_failed"
    }

    fn grouping_key(&self) -> String {
        // The step and the error class name the bug; the horizon is the
        // occurrence.
        format!("step={} cause={}", self.step, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "step": self.step,
            "horizon": self.horizon,
            "error_class": self.error_class,
            "why_fatal": "superseded row versions stay below the cleanup horizon. History \
                          grows with every overwrite until a pass succeeds, and every engine \
                          keeps its memory and disk charge for it",
            "operator_action": "clear the error this report names. The next pass retries at \
                                 a fresh horizon with no operator step",
        })
    }
}

/// A history charge that did not follow the store: the governor keeps the
/// prior charge until a later write recharges it.
pub(in crate::diag) struct HistoryChargeStale<'a> {
    /// The site whose recharge failed.
    pub site: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for HistoryChargeStale<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.history_charge_stale"
    }

    fn grouping_key(&self) -> String {
        format!("site={} cause={}", self.site, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "site": self.site,
            "error_class": self.error_class,
            "why_fatal": "the memory governor's document budget holds a stale history charge. \
                          A charge below the store lets history pass the budget unrefused, \
                          and one above it refuses writes the budget has room for",
            "operator_action": "clear the error this report names. The next document write, \
                                 purge, or restart recharges from the store's counter",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouping_ignores_the_horizon() {
        let first = VersionCleanupFailed {
            step: "core purge",
            horizon: 5,
            error_class: "storage",
        };
        let second = VersionCleanupFailed {
            horizon: 9_000,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "step=core purge cause=storage");
    }
}
