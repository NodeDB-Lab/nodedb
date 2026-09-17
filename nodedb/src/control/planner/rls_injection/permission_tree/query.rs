// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for relational query operations.

use nodedb_physical::physical_plan::{ExchangeOp, QueryOp};

use super::context::{PermCtx, PermTreeLevel};
use super::plan::walk;

/// Exhaustive over [`QueryOp`] so a new relational operation forces a decision
/// about both halves: where its rows come from, and which sub-plans it holds.
pub(super) fn apply_query(ctx: &PermCtx<'_>, op: &mut QueryOp) -> crate::Result<()> {
    match op {
        // Recurse: the coordinator wraps any sharded real-collection scan in
        // an Exchange before this pass runs, so the inner scan gets its filter
        // through the child.
        QueryOp::Exchange(ExchangeOp { child, .. }) => walk(ctx, child),

        // Recurse: the post-processor wraps a subquery body — typically a
        // sharded scan under `Exchange{Gather}` — whose rows are the ones the
        // tree restricts.
        QueryOp::PostProcess { input, .. } => walk(ctx, input),

        // Recurse: every set-operation branch is its own body whose rows the
        // policy restricts.
        QueryOp::SetOp { inputs, .. } => inputs.iter_mut().try_for_each(|input| walk(ctx, input)),

        // Filter and recurse: the aggregate handler evaluates `filters`
        // against both row sources — the per-shard collection scan and the
        // rows decoded from an embedded sub-plan — so the subtree filter goes
        // there in either shape. The sub-plan is additionally walked, since it
        // is a full plan that may read a different governed collection.
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
        } => {
            ctx.filter_into(collection, PermTreeLevel::Read, filters)?;
            match input {
                Some(child) => walk(ctx, child),
                None => Ok(()),
            }
        }

        // Filter: the map-side producer scans the named collection directly,
        // so the subtree ANDs into the same predicate the accumulation uses.
        QueryOp::PartialAggregate {
            collection,
            filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, filters),

        // Filter: facet counts are computed over the filtered row set, so
        // merging the subtree into `filters` makes every facet count exclude
        // rows the caller may not read.
        QueryOp::FacetCounts {
            collection,
            filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, filters),

        // Filter: the fixed-point iteration seeds from `base_filters` and
        // re-scans the collection each round through `recursive_filters`, so
        // both predicates carry the subtree or a later round reintroduces the
        // rows the first round excluded.
        QueryOp::RecursiveScan {
            collection,
            base_filters,
            recursive_filters,
            ..
        } => {
            ctx.filter_into(collection, PermTreeLevel::Read, base_filters)?;
            ctx.filter_into(collection, PermTreeLevel::Read, recursive_filters)
        }

        // Filter and recurse per side, wherever that side's rows come from. A
        // `Some` input is the actual source and is walked; a `None` side is
        // scanned locally by the handler, which applies the per-side slot
        // before building or probing, so an excluded row neither matches a
        // partner nor produces a null-extended outer row. The inline bitmap
        // sub-plans are full plans of their own and are walked like any other
        // child.
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
                None => ctx.filter_into(left_collection, PermTreeLevel::Read, left_rls_filters)?,
            }
            match right_input {
                Some(child) => walk(ctx, child)?,
                None => {
                    ctx.filter_into(right_collection, PermTreeLevel::Read, right_rls_filters)?
                }
            }
            for bitmap in [left_bitmap, right_bitmap].into_iter().flatten() {
                walk(ctx, bitmap)?;
            }
            Ok(())
        }

        // Filter: neither variant takes a resolved child input, so both
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
            ctx.filter_into(left_collection, PermTreeLevel::Read, left_rls_filters)?;
            ctx.filter_into(right_collection, PermTreeLevel::Read, right_rls_filters)
        }

        // Recurse and filter: `outer_plan` is a fully-formed plan producing
        // the driving rows, while `inner_collection` is scanned per outer row
        // using `inner_filters` — so both halves need the subtree.
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
            ctx.filter_into(inner_collection, PermTreeLevel::Read, inner_filters)
        }

        // No-op: `ProviderScan` emits rows the coordinator already
        // materialized — an identity-scoped catalog table or a constant SELECT
        // — and names no collection a tree could be keyed on.
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

    use super::super::plan::test_support::{
        apply, assert_refused, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// Both recursion predicates receive the subtree: filtering only the seed
    /// would let a later round pull the excluded rows back in.
    #[test]
    fn recursive_scan_filters_every_round() {
        let cache = cache_with_tree("tree");
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
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Query(QueryOp::RecursiveScan {
                base_filters,
                recursive_filters,
                ..
            }) => {
                assert_eq!(sorted(injected_resources(base_filters)), readable());
                assert_eq!(sorted(injected_resources(recursive_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A refusable op hidden in a join's inline bitmap sub-plan is walked
    /// into.
    #[test]
    fn hash_join_bitmap_child_is_walked() {
        let cache = cache_with_tree("users");
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
        assert_refused(apply(&mut plan, &cache), "users");
    }

    /// The legacy aggregate over a governed collection still merges the
    /// subtree into the predicate it always used.
    #[test]
    fn aggregate_receives_the_subtree_filter() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            input: None,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            filters: Vec::new(),
            having: Vec::new(),
            limit: 0,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            sort_keys: Vec::new(),
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Query(QueryOp::Aggregate { filters, .. }) => {
                assert_eq!(sorted(injected_resources(filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }
}
