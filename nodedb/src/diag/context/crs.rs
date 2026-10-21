// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for the user-defined CRS set install.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A committed user-defined CRS set whose per-core install failed, so some
/// core of this node resolves SRIDs against an older set.
pub(in crate::diag) struct CustomCrsSetNotApplied<'a> {
    /// What reached the cores: `define`, `drop` or `image`.
    pub cause: &'static str,
    /// SRIDs in the set the cores were sent.
    pub entries: usize,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for CustomCrsSetNotApplied<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.custom_crs_set_not_applied"
    }

    fn grouping_key(&self) -> String {
        // The cause and error class name the bug. The set size is the
        // occurrence, so one broken node files one report.
        format!("cause={};error={}", self.cause, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "cause": self.cause,
            "entries": self.entries,
            "error_class": self.error_class,
            "why_fatal": "the catalog row is committed by consensus, but a core of this node \
                          still holds the previous set: it refuses a CRS every other node \
                          resolves, or resolves a dropped CRS every other node refuses. Both \
                          answer the statement differently from the rest of the cluster",
            "operator_action": "restart this node, or run any CREATE or DROP SPATIAL_REF_SYS \
                                 once the dispatch failure clears: boot and every change send \
                                 the whole set to every core",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouping_ignores_the_set_size() {
        let first = CustomCrsSetNotApplied {
            cause: "define",
            entries: 1,
            error_class: "dispatch",
        };
        let second = CustomCrsSetNotApplied {
            entries: 40,
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        let drop = CustomCrsSetNotApplied {
            cause: "drop",
            ..first
        };
        assert_ne!(first.grouping_key(), drop.grouping_key());
    }
}
