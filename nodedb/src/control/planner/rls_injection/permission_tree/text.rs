// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for full-text-search operations.

use nodedb_physical::physical_plan::TextOp;

use super::context::{PermCtx, PermTreeLevel};

/// Exhaustive over [`TextOp`] so a new text operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_text(ctx: &PermCtx<'_>, op: &mut TextOp) -> crate::Result<()> {
    match op {
        // Filter: the subtree lands in the post-score / post-fusion slot the
        // handler applies to every row before returning it. A ranked result
        // may hold fewer than `top_k` rows, which is the intended effect.
        TextOp::Search {
            collection,
            rls_filters,
            ..
        }
        | TextOp::BM25ScoreScan {
            collection,
            rls_filters,
            ..
        }
        | TextOp::PhraseSearch {
            collection,
            rls_filters,
            ..
        }
        | TextOp::HybridSearch {
            collection,
            rls_filters,
            ..
        }
        | TextOp::HybridSearchTriple {
            collection,
            rls_filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Filter (write level, blanket): indexing a document names the row it
        // indexes, so there is no predicate to narrow.
        TextOp::FtsIndexDoc { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),

        // Filter (delete level, blanket): removing a document from the index
        // removes the row's searchable presence.
        TextOp::FtsDeleteDoc { collection, .. } => ctx.authorize(collection, PermTreeLevel::Delete),

        // No-op: per-collection analyzer configuration is DDL over the index,
        // not an operation on rows.
        TextOp::SetTextConfig { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TextOp;

    use super::super::plan::test_support::{
        apply, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn articles() -> nodedb_types::QualifiedCollection {
        nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "articles")
    }

    /// A BM25 score scan carries the slot, so the subtree filters every row.
    #[test]
    fn bm25_score_scan_receives_the_subtree_filter() {
        let cache = cache_with_tree("articles");
        let mut plan = PhysicalPlan::Text(TextOp::BM25ScoreScan {
            collection: articles(),
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: Vec::new(),
            bound: None,
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Text(TextOp::BM25ScoreScan { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A BM25 search does carry the slot, so the subtree is injected.
    #[test]
    fn search_receives_the_subtree_filter() {
        let cache = cache_with_tree("articles");
        let mut plan = PhysicalPlan::Text(TextOp::Search {
            collection: articles(),
            field: None,
            query: "rust".into(),
            top_k: 10,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: false,
            prefilter: None,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: Vec::new(),
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Text(TextOp::Search { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }
}
