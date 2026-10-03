// SPDX-License-Identifier: Apache-2.0

//! An edge-label filter resolved against one CSR partition.
//!
//! A filter names labels as strings, and the CSR stores labels as dense ids.
//! An empty set keeps every edge. A non-empty set keeps an edge whose label is
//! any listed label. A listed label this partition has never interned carries
//! no edge here, so it adds nothing. A set whose labels are all unknown keeps
//! no edge. Widening it to "no filter" returns other labels' edges under the
//! caller's labels. In a cluster that happens whenever the labels live only on
//! other nodes.

use super::types::CsrIndex;

/// Which durable edges a label filter keeps in one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelFilter {
    /// No filter: every edge.
    Any,
    /// Edges with this dense label id.
    Only(u32),
    /// Edges with any of these dense label ids. Sorted, deduplicated, two or more.
    AnyOf(Box<[u32]>),
    /// Every listed label is unknown to this partition: no edge.
    Unknown,
}

impl LabelFilter {
    /// Whether an edge with dense label id `lid` passes the filter.
    #[inline]
    pub fn keeps(&self, lid: u32) -> bool {
        match self {
            LabelFilter::Any => true,
            LabelFilter::Only(id) => *id == lid,
            LabelFilter::AnyOf(ids) => ids.binary_search(&lid).is_ok(),
            LabelFilter::Unknown => false,
        }
    }

    /// Whether the filter keeps no edge.
    #[inline]
    pub fn keeps_none(&self) -> bool {
        matches!(self, LabelFilter::Unknown)
    }

    /// Whether a staged edge named `label` passes `labels`. Staged edges carry
    /// label names, not partition ids. An empty set keeps every edge.
    #[inline]
    pub fn keeps_name(labels: &[&str], label: &str) -> bool {
        labels.is_empty() || labels.contains(&label)
    }
}

impl CsrIndex {
    /// Resolve `labels` against this partition's interned labels.
    pub fn label_filter(&self, labels: &[&str]) -> LabelFilter {
        if labels.is_empty() {
            return LabelFilter::Any;
        }
        let mut ids: Vec<u32> = labels
            .iter()
            .filter_map(|label| self.label_to_id.get(*label).copied())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        match ids.as_slice() {
            [] => LabelFilter::Unknown,
            [id] => LabelFilter::Only(*id),
            _ => LabelFilter::AnyOf(ids.into_boxed_slice()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr::index::Direction;
    use crate::test_support::test_memory;

    fn csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_in_collection("a", "knows", "b", "people")
            .unwrap_or_else(|e| panic!("seed edge: {e}"));
        csr
    }

    fn csr_three_labels() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        for (label, dst) in [("knows", "b"), ("likes", "c"), ("hates", "d")] {
            csr.add_edge("a", label, dst)
                .unwrap_or_else(|e| panic!("seed edge: {e}"));
        }
        csr
    }

    fn id(csr: &CsrIndex, label: &str) -> u32 {
        csr.label_id(label)
            .unwrap_or_else(|| panic!("label {label} interned"))
    }

    #[test]
    fn an_unknown_label_keeps_no_edge() {
        let csr = csr();
        assert_eq!(csr.label_filter(&["likes"]), LabelFilter::Unknown);
        assert!(!csr.label_filter(&["likes"]).keeps(0));
        assert!(csr.label_filter(&["likes"]).keeps_none());
        assert!(csr.neighbors("a", &["likes"], Direction::Out).is_empty());
        assert!(
            csr.neighbors_in_collection("a", &["likes"], Direction::Out, "people")
                .is_empty()
        );
    }

    #[test]
    fn no_filter_and_a_known_label_keep_their_edges() {
        let csr = csr();
        assert_eq!(csr.neighbors("a", &[], Direction::Out).len(), 1);
        assert_eq!(csr.neighbors("a", &["knows"], Direction::Out).len(), 1);
    }

    #[test]
    fn an_empty_set_resolves_to_any() {
        let csr = csr();
        let filter = csr.label_filter(&[]);
        assert_eq!(filter, LabelFilter::Any);
        assert!(filter.keeps(0));
        assert!(!filter.keeps_none());
    }

    #[test]
    fn one_known_label_resolves_to_only() {
        let csr = csr();
        assert_eq!(
            csr.label_filter(&["knows"]),
            LabelFilter::Only(id(&csr, "knows"))
        );
    }

    #[test]
    fn a_known_and_an_unknown_label_resolve_to_the_known_one() {
        let csr = csr();
        assert_eq!(
            csr.label_filter(&["knows", "nope"]),
            LabelFilter::Only(id(&csr, "knows"))
        );
    }

    #[test]
    fn two_known_labels_keep_both_and_drop_a_third() {
        let csr = csr_three_labels();
        let filter = csr.label_filter(&["likes", "knows"]);
        assert!(matches!(filter, LabelFilter::AnyOf(_)));
        assert!(filter.keeps(id(&csr, "knows")));
        assert!(filter.keeps(id(&csr, "likes")));
        assert!(!filter.keeps(id(&csr, "hates")));
        assert!(!filter.keeps_none());
    }

    #[test]
    fn all_unknown_labels_resolve_to_unknown() {
        let csr = csr();
        assert_eq!(csr.label_filter(&["nope", "never"]), LabelFilter::Unknown);
    }

    #[test]
    fn duplicate_labels_resolve_to_only() {
        let csr = csr();
        assert_eq!(
            csr.label_filter(&["knows", "knows"]),
            LabelFilter::Only(id(&csr, "knows"))
        );
    }

    #[test]
    fn keeps_name_matches_staged_labels() {
        assert!(LabelFilter::keeps_name(&[], "knows"));
        assert!(LabelFilter::keeps_name(&["likes", "knows"], "knows"));
        assert!(!LabelFilter::keeps_name(&["likes"], "knows"));
    }

    #[test]
    fn neighbors_over_a_set_returns_the_union() {
        let csr = csr_three_labels();
        let mut got = csr.neighbors("a", &["knows", "likes"], Direction::Out);
        got.sort();
        assert_eq!(
            got,
            vec![
                ("knows".to_string(), "b".to_string()),
                ("likes".to_string(), "c".to_string()),
            ]
        );
    }
}
