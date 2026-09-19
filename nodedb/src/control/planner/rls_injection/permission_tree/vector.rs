// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for vector-engine operations.

use nodedb_physical::physical_plan::{VectorOp, VectorResolvedMutation};

use super::context::{PermCtx, PermTreeLevel};
use super::plan::walk;

/// Exhaustive over [`VectorOp`] so a new vector operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_vector(ctx: &PermCtx<'_>, op: &mut VectorOp) -> crate::Result<()> {
    match op {
        // Filter, then recurse: the subtree lands in the post-candidate slot
        // the handler applies after HNSW returns candidates, and the prefilter
        // sub-plan is a full plan in its own right whose rows must be resolved
        // too.
        VectorOp::Search {
            collection,
            rls_filters,
            inline_prefilter_plan,
            ..
        } => {
            ctx.filter_into(collection, PermTreeLevel::Read, rls_filters)?;
            match inline_prefilter_plan {
                Some(child) => walk(ctx, child),
                None => Ok(()),
            }
        }

        // Filter: the fused per-field results are filtered post-candidate.
        VectorOp::MultiSearch {
            collection,
            rls_filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Refuse: both return scored document identities with no filter slot,
        // so rows outside the caller's subtree would still be ranked and
        // returned.
        VectorOp::SparseSearch { collection, .. }
        | VectorOp::MultiVectorScoreSearch { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the search returns scored document identities through a response shape that carries \
             no subtree filter",
        ),

        // Refuse: index statistics count every indexed row, including the ones
        // outside the subtree, and a statistics payload carries no resource
        // column to filter on.
        VectorOp::QueryStats { collection, .. } => ctx.refuse_if_tree(
            collection,
            "index statistics are counts over every indexed row, which the subtree filter cannot \
             be evaluated against",
        ),

        // Filter (write level, blanket): index writes attach a vector to a row
        // they name directly, so there is no predicate to narrow.
        VectorOp::Insert { collection, .. }
        | VectorOp::BatchInsert { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::DirectUpsert { collection, .. }
        | VectorOp::DirectInsert { collection, .. }
        | VectorOp::DirectInsertIfAbsent { collection, .. }
        | VectorOp::DirectUpdate { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Write)
        }

        // Filter (delete level, blanket): index deletions remove the row's
        // entry from the index.
        VectorOp::Delete { collection, .. }
        | VectorOp::DirectDelete { collection, .. }
        | VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Delete)
        }

        // Recurse: the wrapped op is the intercepted write verbatim.
        VectorOp::ResolveDirectWrite(inner) => apply_vector(ctx, inner),

        // Blanket per mutation: each names the row it writes directly, and a
        // `Delete` mutation is a removal, so it takes the delete level.
        VectorOp::ResolvedDirectWrite {
            collection,
            mutations,
            ..
        } => {
            for mutation in mutations {
                let level = match mutation {
                    VectorResolvedMutation::Update { .. }
                    | VectorResolvedMutation::Upsert { .. } => PermTreeLevel::Write,
                    VectorResolvedMutation::Delete { .. } => PermTreeLevel::Delete,
                };
                ctx.authorize(collection, level)?;
            }
            Ok(())
        }

        // No-op: index configuration and index maintenance. They act on the
        // index structure rather than on rows, and are authorized as DDL.
        VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, VectorOp};

    use super::super::plan::test_support::{
        apply, assert_refused, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn search_with_prefilter(collection: &str, prefilter: Option<PhysicalPlan>) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            query_vector: vec![0.1, 0.2],
            top_k: 4,
            ef_search: 0,
            metric: nodedb_types::vector_distance::DistanceMetric::L2,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: prefilter.map(Box::new),
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
        })
    }

    /// A vector search over a governed collection was previously unlisted and
    /// returned neighbours from the whole collection. It is now narrowed to
    /// the readable subtree.
    #[test]
    fn search_is_narrowed_to_the_readable_subtree() {
        let cache = cache_with_tree("embeddings");
        let mut plan = search_with_prefilter("embeddings", None);
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Vector(VectorOp::Search { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// The prefilter sub-plan is a real read of its own collection, so a
    /// refusable op nested there is still caught.
    #[test]
    fn inline_prefilter_plan_is_walked() {
        let cache = cache_with_tree("users");
        let mut plan = search_with_prefilter(
            "embeddings",
            Some(PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "users",
                ),
                path: "$.email".into(),
                value: "a@b.c".into(),
            })),
        );
        assert_refused(apply(&mut plan, &cache), "users");
    }

    /// A sparse search has no filter slot, so a tree refuses it.
    #[test]
    fn sparse_search_is_refused_under_a_tree() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Vector(VectorOp::SparseSearch {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            field_name: "sparse".into(),
            query_entries: vec![(1, 1.0)],
            top_k: 5,
        });
        assert_refused(apply(&mut plan, &cache), "docs");
    }
}
