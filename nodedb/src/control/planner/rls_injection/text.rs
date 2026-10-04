// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for full-text-search operations.

use nodedb_physical::physical_plan::TextOp;

use super::context::RlsCtx;

/// Exhaustive over [`TextOp`] so a new text operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_text(ctx: &RlsCtx<'_>, op: &mut TextOp) -> crate::Result<()> {
    match op {
        // Inject: the policy lands in the RLS slot. The handler folds it into
        // the eligible rows before ranking, so `top_k` counts only rows the
        // policy admits.
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
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: an index write carries the extracted text and a surrogate,
        // not the row body the policy names. Indexing a row the policy hides
        // makes it reachable by search, which is the disclosure the policy
        // exists to prevent, so a policy on the collection refuses the write.
        TextOp::FtsIndexDoc { collection, .. } | TextOp::FtsDeleteDoc { collection, .. } => ctx
            .refuse_if_write_policy(
                collection,
                "an index write carries extracted text and a surrogate rather than the row body \
                 the policy names, so no row image is available for it to be evaluated against",
            ),

        // No-op: the per-collection analyzer binding is configuration, not a
        // user row.
        TextOp::SetTextConfig { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TextOp;

    use super::super::plan::test_support::{
        assert_write_refused, inject, inject_without_policy, store_with_read_policy,
        store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn index_doc(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![("body".into(), "hello".into())],
            provenance: None,
        })
    }

    /// An index write carries extracted text and a surrogate, not the row body
    /// the policy names, so a write policy refuses it.
    #[test]
    fn fts_index_doc_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("articles");
        let mut plan = index_doc("articles");
        assert_write_refused(inject(&mut plan, &store), "articles");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn fts_index_doc_without_a_policy_is_untouched() {
        let mut plan = index_doc("articles");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    fn articles() -> nodedb_types::QualifiedCollection {
        nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "articles")
    }

    /// The RLS slot of a text read.
    fn rls_slot(plan: &PhysicalPlan) -> &[u8] {
        match plan {
            PhysicalPlan::Text(
                TextOp::Search { rls_filters, .. }
                | TextOp::BM25ScoreScan { rls_filters, .. }
                | TextOp::PhraseSearch { rls_filters, .. },
            ) => rls_filters,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A BM25 score scan applies the policy to every row it emits.
    #[test]
    fn bm25_score_scan_receives_the_policy_filter() {
        let store = store_with_read_policy("articles");
        let mut plan = PhysicalPlan::Text(TextOp::BM25ScoreScan {
            collection: articles(),
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: Vec::new(),
            bound: None,
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            !rls_slot(&plan).is_empty(),
            "policy filter must be injected"
        );
    }

    /// A phrase search applies the policy to its ranked hits.
    #[test]
    fn phrase_search_receives_the_policy_filter() {
        let store = store_with_read_policy("articles");
        let mut plan = PhysicalPlan::Text(TextOp::PhraseSearch {
            collection: articles(),
            field: None,
            terms: vec!["rust".into(), "lang".into()],
            top_k: 10,
            prefilter: None,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            !rls_slot(&plan).is_empty(),
            "policy filter must be injected"
        );
    }

    /// A BM25 search does carry the slot, so the policy is injected.
    #[test]
    fn search_receives_the_policy_filter() {
        let store = store_with_read_policy("articles");
        let mut plan = PhysicalPlan::Text(TextOp::Search {
            collection: articles(),
            field: Some("body".into()),
            query: "rust".into(),
            top_k: 10,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: false,
            prefilter: None,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            !rls_slot(&plan).is_empty(),
            "policy filter must be injected"
        );
    }

    /// A slot that already holds the statement's predicates keeps them: the
    /// policy joins them.
    #[test]
    fn the_policy_joins_existing_post_filters() {
        let store = store_with_read_policy("articles");
        let own = vec![nodedb_query::scan_filter::ScanFilter {
            field: "tag".into(),
            op: nodedb_query::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String("a".into()),
            clauses: Vec::new(),
            expr: None,
        }];
        let own_bytes = zerompk::to_msgpack_vec(&own).expect("encode own filters");
        let mut plan = PhysicalPlan::Text(TextOp::Search {
            collection: articles(),
            field: None,
            query: "rust".into(),
            top_k: 10,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: false,
            prefilter: None,
            filters: Vec::new(),
            rls_filters: own_bytes,
            scores: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        let merged: Vec<nodedb_query::scan_filter::ScanFilter> =
            zerompk::from_msgpack(rls_slot(&plan)).expect("decode merged filters");
        assert!(
            merged.len() > 1,
            "policy must join, not replace: {merged:?}"
        );
        assert!(merged.iter().any(|f| f.field == "tag"));
    }
}
