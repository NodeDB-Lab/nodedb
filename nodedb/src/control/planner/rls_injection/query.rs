// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for relational query operations.

use nodedb_physical::physical_plan::{ExchangeOp, QueryOp};

use super::context::RlsCtx;
use super::plan::walk;

/// Exhaustive over [`QueryOp`] so a new relational operation forces a decision
/// about both halves: where its rows come from, and which sub-plans it holds.
pub(super) fn inject_query(ctx: &RlsCtx<'_>, op: &mut QueryOp) -> crate::Result<()> {
    match op {
        // Recurse: the coordinator wraps any sharded real-collection scan in an
        // Exchange before this pass runs, so the inner scan gets its filter
        // through the child.
        QueryOp::Exchange(ExchangeOp { child, .. }) => walk(ctx, child),

        // Recurse: the post-processor wraps a subquery body — typically a
        // sharded scan under `Exchange{Gather}` — whose rows are the ones the
        // policy restricts.
        QueryOp::PostProcess { input, .. } => walk(ctx, input),

        // Recurse: every set-operation branch is its own body whose rows the
        // policy restricts.
        QueryOp::SetOp { inputs, .. } => inputs.iter_mut().try_for_each(|input| walk(ctx, input)),

        // Inject or recurse: a catalog aggregate (`input: Some`) sources rows
        // from the embedded sub-plan, so the policy belongs in that input
        // rather than in the aggregate's own (empty) filters. A legacy
        // aggregate (`input: None`) scans the named collection and merges the
        // policy into its `filters`.
        QueryOp::Aggregate {
            collection,
            input,
            filters,
            ..
        }
        | QueryOp::PartialAggregateState {
            collection,
            input,
            filters,
            ..
        } => match input {
            Some(child) => walk(ctx, child),
            None => ctx.merge_into(collection, filters),
        },

        // Inject: the map-side producer scans the named collection directly,
        // so the policy ANDs into the same predicate the accumulation uses.
        QueryOp::PartialAggregate {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: facet counts are computed over the filtered row set, so
        // merging the policy into `filters` makes every facet count exclude
        // the rows the policy hides.
        QueryOp::FacetCounts {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: the fixed-point iteration seeds from `base_filters` and
        // re-scans the collection each round through `recursive_filters`, so
        // both predicates carry the policy or a later round reintroduces the
        // rows the first round excluded.
        QueryOp::RecursiveScan {
            collection,
            base_filters,
            recursive_filters,
            ..
        } => {
            ctx.merge_into(collection, base_filters)?;
            ctx.merge_into(collection, recursive_filters)
        }

        // Inject and recurse per side, wherever that side's rows come from.
        // A `Some` input is the actual source and receives the policy by
        // recursion; a `None` side is scanned locally by the handler, which
        // applies the per-side slot before building or probing, so an excluded
        // row neither matches a partner nor produces a null-extended outer
        // row. The inline bitmap sub-plans are full plans of their own and are
        // walked like any other child — the redaction pass walks them too.
        QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            left_rls_filters,
            right_rls_filters,
            ..
        } => {
            match left_input {
                Some(child) => walk(ctx, child)?,
                None => ctx.set_post_filters(left_collection, left_rls_filters)?,
            }
            match right_input {
                Some(child) => walk(ctx, child)?,
                None => ctx.set_post_filters(right_collection, right_rls_filters)?,
            }
            for bitmap in [left_bitmap, right_bitmap].into_iter().flatten() {
                walk(ctx, bitmap)?;
            }
            Ok(())
        }

        // Inject: neither variant takes a resolved child input, so both
        // collections are read directly by the handler and both slots are
        // always populated.
        QueryOp::NestedLoopJoin {
            left_collection,
            right_collection,
            left_rls_filters,
            right_rls_filters,
            ..
        }
        | QueryOp::SortMergeJoin {
            left_collection,
            right_collection,
            left_rls_filters,
            right_rls_filters,
            ..
        } => {
            ctx.set_post_filters(left_collection, left_rls_filters)?;
            ctx.set_post_filters(right_collection, right_rls_filters)
        }

        // Recurse and inject: `outer_plan` is a fully-formed plan producing the
        // driving rows, while `inner_collection` is scanned per outer row using
        // `inner_filters` — so both halves need the policy.
        QueryOp::LateralTopK {
            outer_plan,
            inner_collection,
            inner_filters,
            ..
        }
        | QueryOp::LateralLoop {
            outer_plan,
            inner_collection,
            inner_filters,
            ..
        } => {
            walk(ctx, outer_plan)?;
            ctx.merge_into(inner_collection, inner_filters)
        }

        // No-op: `ProviderScan` emits rows the coordinator already
        // materialized — an identity-scoped catalog table or a constant SELECT
        // — and names no collection a policy could be keyed on.
        QueryOp::ProviderScan { .. } => Ok(()),

        // No-op: pure expression evaluation over its own anchor and step
        // expressions; it reads no stored rows and names no collection.
        QueryOp::RecursiveValue { .. } => Ok(()),

        // No-op: the shuffle consumers merge partial state staged on this node
        // by producer plans, each of which went through this pass on the
        // Control Plane that dispatched it. The staged frame files name no
        // collection to key a second check on.
        QueryOp::ShuffleAggregateConsume { .. } | QueryOp::ShuffleJoinConsume { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, QueryOp};

    use super::super::plan::test_support::{assert_refused, inject, store_with_read_policy};
    use crate::bridge::envelope::PhysicalPlan;

    /// Both recursion predicates receive the policy: filtering only the seed
    /// would let a later round pull the excluded rows back in.
    #[test]
    fn recursive_scan_filters_every_round() {
        let store = store_with_read_policy("tree");
        let mut plan = PhysicalPlan::Query(QueryOp::RecursiveScan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "tree",
            ),
            base_filters: Vec::new(),
            recursive_filters: Vec::new(),
            join_link: None,
            max_iterations: 10,
            distinct: false,
            limit: 0,
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Query(QueryOp::RecursiveScan {
                base_filters,
                recursive_filters,
                ..
            }) => {
                assert!(!base_filters.is_empty(), "seed must carry the policy");
                assert!(
                    !recursive_filters.is_empty(),
                    "each recursive round must carry the policy"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// Facet counts are computed over the filtered rows, so the policy merges
    /// into the shared predicate.
    #[test]
    fn facet_counts_receive_the_policy_filter() {
        let store = store_with_read_policy("items");
        let mut plan = PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "items",
            ),
            filters: Vec::new(),
            fields: vec!["colour".into()],
            limit_per_facet: 0,
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Query(QueryOp::FacetCounts { filters, .. }) => {
                assert!(!filters.is_empty(), "policy filter must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A refusable op hidden in a join's inline bitmap sub-plan is walked into.
    #[test]
    fn hash_join_bitmap_child_is_walked() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "orders",
            ),
            right_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "items",
            ),
            left_alias: None,
            right_alias: None,
            on: Vec::new(),
            join_type: "inner".into(),
            limit: 0,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: None,
            left_bitmap: Some(Box::new(PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "users",
                ),
                path: "$.email".into(),
                value: "a@b.c".into(),
            }))),
            right_bitmap: None,
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
            left_scan_filters: Vec::new(),
            right_scan_filters: Vec::new(),
        });
        assert_refused(inject(&mut plan, &store), "users");
    }
}
