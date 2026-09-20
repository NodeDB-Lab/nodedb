// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for the three peer engines sharing the columnar
//! storage core: plain columnar, timeseries, and spatial.

use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TimeseriesOp};

use super::context::{PermCtx, PermTreeLevel};

/// Exhaustive over [`ColumnarOp`].
pub(super) fn apply_columnar(ctx: &PermCtx<'_>, op: &mut ColumnarOp) -> crate::Result<()> {
    match op {
        // Filter: the subtree occupies the dedicated post-scan slot, applied
        // after block pruning and before rows are returned.
        ColumnarOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Refuse: the clone materializer streams raw `(surrogate, row bytes)`
        // pairs through a cursor payload that carries no subtree filter.
        ColumnarOp::MaterializeScan { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the materializing scan streams raw stored rows through a cursor payload that carries \
             no subtree filter",
        ),

        // Filter (write level, blanket): the insert carries an encoded row
        // batch rather than a predicate, so there is nothing to narrow.
        ColumnarOp::Insert { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),

        // Filter (write level): the update selects its rows through `filters`,
        // so the subtree narrows what is rewritten.
        ColumnarOp::Update {
            collection,
            filters,
            ..
        } => {
            ctx.authorize(collection, PermTreeLevel::Write)?;
            ctx.filter_into(collection, PermTreeLevel::Write, filters)
        }

        // Filter (delete level): the delete selects its rows through
        // `filters`, so the subtree narrows what is removed.
        ColumnarOp::Delete {
            collection,
            filters,
            ..
        } => {
            ctx.authorize(collection, PermTreeLevel::Delete)?;
            ctx.filter_into(collection, PermTreeLevel::Delete, filters)
        }

        // Authorize (blanket): already resolved against the write policy,
        // no predicate left to narrow. Still gates write/delete access.
        ColumnarOp::ResolvedUpdate { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Write)
        }
        // Delete level, blanket: a truncate removes rows it never
        // enumerates, so there is no predicate to narrow.
        ColumnarOp::ResolvedDelete { collection, .. } | ColumnarOp::Truncate { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Delete)
        }

        // Authorize (blanket): constructed internally from an already-
        // authorized plan, dispatched straight to the Data Plane.
        ColumnarOp::ResolveDml { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Write)
        }
    }
}

/// Exhaustive over [`TimeseriesOp`].
pub(super) fn apply_timeseries(ctx: &PermCtx<'_>, op: &mut TimeseriesOp) -> crate::Result<()> {
    match op {
        // Filter: the subtree is applied after time-range pruning, on the rows
        // the scan actually produced.
        TimeseriesOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Filter (write level, blanket): ingest carries an encoded batch of
        // samples rather than a predicate.
        TimeseriesOp::Ingest { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),

        // Recurse: the resolve pass stands in for the ingest it wraps, so it
        // needs the same write level on the same collection.
        TimeseriesOp::ResolveIngest(inner) => apply_timeseries(ctx, inner),

        // Delete level, blanket: a truncate removes rows it never
        // enumerates, so there is no predicate to narrow.
        TimeseriesOp::Truncate { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Delete)
        }
    }
}

/// Exhaustive over [`SpatialOp`].
pub(super) fn apply_spatial(ctx: &PermCtx<'_>, op: &mut SpatialOp) -> crate::Result<()> {
    match op {
        // Filter: the subtree is applied to the R-tree candidates before they
        // are returned, alongside the query's own attribute filters.
        SpatialOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Filter (write / delete level, blanket): both name the row whose
        // geometry they index or unindex.
        SpatialOp::Insert { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),
        SpatialOp::Delete { collection, .. } => ctx.authorize(collection, PermTreeLevel::Delete),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TimeseriesOp;

    use super::super::plan::test_support::{
        apply, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// A timeseries scan over a governed collection was previously unlisted
    /// and returned every series. It is now narrowed to the readable subtree.
    #[test]
    fn timeseries_scan_is_narrowed_to_the_readable_subtree() {
        let cache = cache_with_tree("metrics");
        let mut plan = PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "metrics",
            ),
            time_range: (0, 1),
            projection: Vec::new(),
            limit: 0,
            filters: Vec::new(),
            sort_keys: Vec::new(),
            bucket_interval_ms: 0,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            gap_fill: String::new(),
            computed_columns: Vec::new(),
            rls_filters: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Timeseries(TimeseriesOp::Scan { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }
}
