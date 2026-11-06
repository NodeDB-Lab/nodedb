// SPDX-License-Identifier: Apache-2.0

//! CTE inlining: a plan that reads a CTE by name, rewritten to read the
//! CTE's body. Pure `SqlPlan` → `SqlPlan`, shared by every executor that
//! lowers a `SqlPlan::Cte`.

use crate::types::{CtePlan, Filter, Projection, SortKey, SqlPlan, WindowSpec};

/// Replace scans on `cte_name` with the CTE's actual subquery plan.
///
/// Outer constraints on the CTE reference merge onto the body only where the
/// body can carry them without changing the rows it produces:
///
/// - A plain filtered `Scan` body (no computed projection, window functions,
///   LIMIT, OFFSET, DISTINCT, or ORDER BY) takes every outer constraint.
/// - A `VectorSearch` body takes filters, a column-only projection, and an
///   unordered LIMIT (as `top_k`).
/// - A column-only outer projection over any other body resolves by output
///   schema, so the body is returned as is.
///
/// Every other combination lowers into a `Subquery` post-processor over the
/// body's materialized rows. Computed projection entries and window functions
/// always take that path: the body does not evaluate them, and a name lookup
/// at the response boundary yields NULL.
pub fn inline_cte(plan: &SqlPlan, cte_name: &str, cte_plan: &SqlPlan) -> SqlPlan {
    match plan {
        // Direct scan on CTE name → replace with CTE plan.
        SqlPlan::Scan {
            collection,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            ..
        } if collection == cte_name => inline_cte_scan_ref(
            ScanRef {
                filters,
                projection,
                sort_keys,
                limit: *limit,
                offset: *offset,
                distinct: *distinct,
                window_functions,
                has_computed: has_computed_projection(projection),
            },
            cte_plan,
        ),

        // Aggregate referencing CTE → inline into the input.
        SqlPlan::Aggregate {
            input,
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            having,
            limit,
            grouping_sets,
            sort_keys,
        } => SqlPlan::Aggregate {
            input: Box::new(inline_cte(input, cte_name, cte_plan)),
            group_by: group_by.clone(),
            group_by_aliases: group_by_aliases.clone(),
            output_order: output_order.clone(),
            aggregates: aggregates.clone(),
            having: having.clone(),
            limit: *limit,
            grouping_sets: grouping_sets.clone(),
            sort_keys: sort_keys.clone(),
        },

        // JOIN referencing CTE on either side.
        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            projection,
            filters,
        } => SqlPlan::Join {
            left: Box::new(inline_cte_join_side(left, cte_name, cte_plan)),
            right: Box::new(inline_cte_join_side(right, cte_name, cte_plan)),
            on: on.clone(),
            join_type: *join_type,
            condition: condition.clone(),
            limit: *limit,
            projection: projection.clone(),
            filters: filters.clone(),
        },

        // Union referencing CTE → inline into all inputs.
        SqlPlan::Union { inputs, distinct } => SqlPlan::Union {
            inputs: inputs
                .iter()
                .map(|i| inline_cte(i, cte_name, cte_plan))
                .collect(),
            distinct: *distinct,
        },

        // Intersect referencing CTE → inline into both sides.
        SqlPlan::Intersect { left, right, all } => SqlPlan::Intersect {
            left: Box::new(inline_cte(left, cte_name, cte_plan)),
            right: Box::new(inline_cte(right, cte_name, cte_plan)),
            all: *all,
        },

        // Except referencing CTE → inline into both sides.
        SqlPlan::Except { left, right, all } => SqlPlan::Except {
            left: Box::new(inline_cte(left, cte_name, cte_plan)),
            right: Box::new(inline_cte(right, cte_name, cte_plan)),
            all: *all,
        },

        // INSERT ... SELECT referencing CTE → inline into the source subquery.
        SqlPlan::InsertSelect {
            target,
            source,
            column_map,
            source_columns,
        } => SqlPlan::InsertSelect {
            target: target.clone(),
            source: Box::new(inline_cte(source, cte_name, cte_plan)),
            column_map: column_map.clone(),
            source_columns: source_columns.clone(),
        },

        // `UPDATE ... FROM cte` and `MERGE ... USING cte` read the CTE as
        // their source relation. The source alias is carried on the plan, so
        // it survives the replacement.
        SqlPlan::UpdateFrom {
            collection,
            engine,
            source,
            source_alias,
            target_join_col,
            source_join_col,
            assignments,
            target_filters,
            returning,
        } => SqlPlan::UpdateFrom {
            collection: collection.clone(),
            engine: *engine,
            source: Box::new(inline_cte(source, cte_name, cte_plan)),
            source_alias: source_alias.clone(),
            target_join_col: target_join_col.clone(),
            source_join_col: source_join_col.clone(),
            assignments: assignments.clone(),
            target_filters: target_filters.clone(),
            returning: *returning,
        },
        SqlPlan::Merge(merge) => {
            let mut merge = merge.clone();
            merge.source = Box::new(inline_cte(&merge.source, cte_name, cte_plan));
            SqlPlan::Merge(merge)
        }

        // A post-processor produced by an earlier CTE definition: recurse into
        // its body so a later definition's references inside it still inline.
        SqlPlan::Subquery {
            input,
            filters,
            projection,
            window_functions,
            sort_keys,
            offset,
            distinct,
            limit,
        } => SqlPlan::Subquery {
            input: Box::new(inline_cte(input, cte_name, cte_plan)),
            filters: filters.clone(),
            projection: projection.clone(),
            window_functions: window_functions.clone(),
            sort_keys: sort_keys.clone(),
            offset: *offset,
            distinct: *distinct,
            limit: *limit,
        },

        // A nested CTE, such as a derived JOIN side. Its definition bodies
        // see the enclosing CTE. Its outer query sees it unless a definition
        // of the same name shadows it.
        SqlPlan::Cte(nested) => {
            let shadowed = nested.definitions.iter().any(|(name, _)| name == cte_name);
            SqlPlan::Cte(CtePlan {
                definitions: nested
                    .definitions
                    .iter()
                    .map(|(name, body)| (name.clone(), inline_cte(body, cte_name, cte_plan)))
                    .collect(),
                outer: Box::new(if shadowed {
                    nested.outer.as_ref().clone()
                } else {
                    inline_cte(&nested.outer, cte_name, cte_plan)
                }),
            })
        }

        // A LATERAL plan side recurses. A top-k inner side is a collection
        // name, and the planner refuses a CTE there.
        SqlPlan::LateralTopK(top_k) => {
            let mut top_k = top_k.clone();
            top_k.outer = Box::new(inline_cte(&top_k.outer, cte_name, cte_plan));
            SqlPlan::LateralTopK(top_k)
        }
        SqlPlan::LateralLoop(lateral_loop) => {
            let mut lateral_loop = lateral_loop.clone();
            lateral_loop.outer = Box::new(inline_cte(&lateral_loop.outer, cte_name, cte_plan));
            lateral_loop.inner = Box::new(inline_cte(&lateral_loop.inner, cte_name, cte_plan));
            SqlPlan::LateralLoop(lateral_loop)
        }

        // No nested plan to recurse into. Listed in full so a new plan
        // variant fails to compile here. A `Scan` of another name lands here.
        SqlPlan::ConstantResult { .. }
        | SqlPlan::Scan { .. }
        | SqlPlan::PointGet { .. }
        | SqlPlan::DocumentIndexLookup(_)
        | SqlPlan::RangeScan(_)
        | SqlPlan::Insert(_)
        | SqlPlan::KvInsert(_)
        | SqlPlan::Upsert(_)
        | SqlPlan::Update { .. }
        | SqlPlan::Delete { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::TimeseriesScan(_)
        | SqlPlan::TimeseriesIngest(_)
        | SqlPlan::VectorSearch { .. }
        | SqlPlan::MultiVectorSearch { .. }
        | SqlPlan::SparseSearch { .. }
        | SqlPlan::TextSearch(_)
        | SqlPlan::HybridSearch(_)
        | SqlPlan::HybridSearchTriple(_)
        | SqlPlan::SpatialScan { .. }
        | SqlPlan::RecursiveScan(_)
        | SqlPlan::RecursiveValue(_)
        | SqlPlan::CreateArray(_)
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray(_)
        | SqlPlan::InsertArray(_)
        | SqlPlan::DeleteArray(_)
        | SqlPlan::ArraySlice(_)
        | SqlPlan::ArrayAgg(_)
        | SqlPlan::ArrayElementwise(_)
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. }
        | SqlPlan::VectorPrimaryInsert(_)
        | SqlPlan::VectorPrimaryDelete(_)
        | SqlPlan::VectorPrimaryTruncate(_)
        | SqlPlan::VectorPrimaryUpdate(_)
        | SqlPlan::CreateIndex(_)
        | SqlPlan::DropIndex(_) => plan.clone(),
    }
}

/// One side of a join, with `cte_name` resolved.
///
/// A side that scans the CTE by name becomes a one-definition CTE, the shape
/// a derived JOIN side plans to. The join qualifies that side's columns by
/// the CTE name or its alias, the names the join's ON keys and projection
/// use. Replaced by the bare body, the side would be qualified by the body's
/// own collection, and every column read through the CTE name would miss.
/// Any other side resolves the CTE inside it as usual.
fn inline_cte_join_side(side: &SqlPlan, cte_name: &str, cte_plan: &SqlPlan) -> SqlPlan {
    match side {
        SqlPlan::Scan { collection, .. } if collection == cte_name => SqlPlan::Cte(CtePlan {
            definitions: vec![(cte_name.to_string(), cte_plan.clone())],
            outer: Box::new(side.clone()),
        }),
        other => inline_cte(other, cte_name, cte_plan),
    }
}

/// `true` if any projection entry is a computed expression (`price * qty AS
/// total`) rather than a bare column or star. A Control-Plane-computed entry
/// counts too: merging it into a scan body would drop the column the
/// Control Plane evaluates.
fn has_computed_projection(projection: &[Projection]) -> bool {
    projection.iter().any(|p| {
        matches!(
            p,
            Projection::Computed { .. } | Projection::CpComputed { .. }
        )
    })
}

/// The outer constraints carried on a `Scan` that references the CTE by
/// name. `has_computed` is precomputed once so the sub-cases below don't
/// each re-walk `projection`.
#[derive(Clone, Copy)]
struct ScanRef<'a> {
    filters: &'a Vec<Filter>,
    projection: &'a Vec<Projection>,
    sort_keys: &'a Vec<SortKey>,
    limit: Option<usize>,
    offset: usize,
    distinct: bool,
    window_functions: &'a Vec<WindowSpec>,
    has_computed: bool,
}

impl ScanRef<'_> {
    /// No filter, sort, limit, offset, distinct, window, or computed entry —
    /// the outer reference adds nothing the body's output schema doesn't
    /// already answer.
    fn is_unconstrained(&self) -> bool {
        self.filters.is_empty()
            && self.sort_keys.is_empty()
            && self.limit.is_none()
            && !self.distinct
            && self.offset == 0
            && self.window_functions.is_empty()
            && !self.has_computed
    }
}

/// Whether [`inline_cte`] replaces `plan` with the body of `cte_name`
/// unchanged.
///
/// That is an unconstrained `Scan` of `cte_name`. Its columns then read the
/// body's own rows, so each one resolves through the body's output schema.
pub fn reads_cte_body_unchanged(plan: &SqlPlan, cte_name: &str) -> bool {
    match plan {
        SqlPlan::Scan {
            collection,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            ..
        } if collection == cte_name => ScanRef {
            filters,
            projection,
            sort_keys,
            limit: *limit,
            offset: *offset,
            distinct: *distinct,
            window_functions,
            has_computed: has_computed_projection(projection),
        }
        .is_unconstrained(),
        _ => false,
    }
}

/// Wrap `input` in a `Subquery` post-processor carrying the outer
/// constraints that `input` has no slot for.
fn wrap_in_subquery(input: SqlPlan, filters: Vec<Filter>, outer: ScanRef<'_>) -> SqlPlan {
    SqlPlan::Subquery {
        input: Box::new(input),
        filters,
        projection: outer.projection.clone(),
        window_functions: outer.window_functions.clone(),
        sort_keys: outer.sort_keys.clone(),
        offset: outer.offset,
        distinct: outer.distinct,
        limit: outer.limit,
    }
}

/// Resolve a CTE reference for a `Scan { collection: cte_name, .. }` node,
/// merging the outer constraints onto `cte_plan` as far as its body kind
/// can carry them.
fn inline_cte_scan_ref(outer: ScanRef<'_>, cte_plan: &SqlPlan) -> SqlPlan {
    // A column-only projection resolves by the body's output schema.
    if outer.is_unconstrained() {
        return cte_plan.clone();
    }

    if let Some(merged) = merge_into_scan_body(outer, cte_plan) {
        return merged;
    }

    if let Some(merged) = merge_into_vector_search_body(outer, cte_plan) {
        return merged;
    }

    // Any other body (Aggregate, Join, TextSearch, HybridSearch,
    // SparseSearch, SpatialScan, MultiVectorSearch, a constrained Scan,
    // ...) has no slot for the outer constraints reaching this point — the
    // unconstrained case already returned at the top of the function. Apply
    // them over the body's materialized rows in a `Subquery`
    // post-processor, which evaluates computed columns and window
    // functions itself.
    wrap_in_subquery(cte_plan.clone(), outer.filters.clone(), outer)
}

/// A plain filtered `Scan` body takes every outer constraint. A body that
/// limits, offsets, dedups, orders, computes, or windows changes which rows
/// the outer constraints see if they merge into it: an outer WHERE inside an
/// inner LIMIT changes the cut, and an inner `qty AS x` alias replaced by the
/// outer projection makes `x` NULL.
fn merge_into_scan_body(outer: ScanRef<'_>, cte_plan: &SqlPlan) -> Option<SqlPlan> {
    let SqlPlan::Scan {
        collection: inner_col,
        alias: inner_alias,
        engine: inner_eng,
        filters: inner_f,
        projection: inner_p,
        sort_keys: inner_s,
        limit: inner_l,
        offset: inner_o,
        distinct: inner_d,
        window_functions: inner_w,
        temporal: inner_t,
    } = cte_plan
    else {
        return None;
    };
    if has_computed_projection(inner_p)
        || !inner_w.is_empty()
        || inner_l.is_some()
        || *inner_o != 0
        || *inner_d
        || !inner_s.is_empty()
    {
        return None;
    }

    let mut merged_filters = inner_f.clone();
    merged_filters.extend(outer.filters.iter().cloned());
    Some(SqlPlan::Scan {
        collection: inner_col.clone(),
        alias: inner_alias.clone(),
        engine: *inner_eng,
        filters: merged_filters,
        // A named outer projection overrides the inner one. An empty or
        // star-only outer projection inherits the CTE's own column list, so
        // `SELECT * FROM (SELECT a FROM t)` emits `a` alone.
        projection: if outer
            .projection
            .iter()
            .all(|p| matches!(p, Projection::Star | Projection::QualifiedStar(_)))
        {
            inner_p.clone()
        } else {
            outer.projection.clone()
        },
        sort_keys: outer.sort_keys.clone(),
        limit: outer.limit,
        offset: outer.offset,
        distinct: outer.distinct,
        window_functions: outer.window_functions.clone(),
        temporal: *inner_t,
    })
}

/// A k-NN body carries its own post-filter list and top-k. An outer `WHERE`
/// merges into the engine post-filter so the cut counts MATCHING rows. When
/// nothing reorders the result and the outer projection is column-only, an
/// unordered `LIMIT` folds into `top_k` and the projection rides along. An
/// outer `ORDER BY` / `OFFSET` / `DISTINCT` reorders the k rows, and a
/// computed projection or window function evaluates over them; the search
/// leaf has no slot for any of those, so they (and a `LIMIT` that must apply
/// after the reorder) run in a `Subquery` post-processor over the k rows.
fn merge_into_vector_search_body(outer: ScanRef<'_>, cte_plan: &SqlPlan) -> Option<SqlPlan> {
    if !matches!(cte_plan, SqlPlan::VectorSearch { .. }) {
        return None;
    }

    let needs_reorder = !outer.sort_keys.is_empty()
        || outer.offset > 0
        || outer.distinct
        || outer.has_computed
        || !outer.window_functions.is_empty();
    let mut leaf = cte_plan.clone();
    if let SqlPlan::VectorSearch {
        filters: body_filters,
        projection: body_projection,
        top_k,
        ..
    } = &mut leaf
    {
        body_filters.extend(outer.filters.iter().cloned());
        if !needs_reorder {
            if !outer.projection.is_empty() {
                body_projection.clone_from(outer.projection);
            }
            if let Some(outer_limit) = outer.limit {
                *top_k = (*top_k).min(outer_limit);
            }
        }
    }
    if !needs_reorder {
        return Some(leaf);
    }
    // Filters already run in the engine; the tail applies the
    // reorder-dependent constraints over the k rows. It sorts before
    // projecting, so ORDER BY may reference any column.
    Some(wrap_in_subquery(leaf, Vec::new(), outer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CompareOp, EngineType, Filter, FilterExpr, SortKey, SqlValue};

    fn vector_search_body() -> SqlPlan {
        SqlPlan::VectorSearch {
            collection: "docs".to_string(),
            field: "embedding".to_string(),
            query_vector: vec![0.1, 0.2],
            top_k: 3,
            ef_search: 64,
            metric: crate::types::DistanceMetric::L2,
            filters: Vec::new(),
            array_prefilter: None,
            index_prefilter: None,
            ann_options: crate::types::VectorAnnOptions::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            pk_prefilter: None,
            projection: Vec::new(),
            temporal: crate::temporal::TemporalScope::default(),
        }
    }

    fn scan_on_cte(filters: Vec<Filter>, limit: Option<usize>) -> SqlPlan {
        SqlPlan::Scan {
            collection: "knn".to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters,
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: crate::TemporalScope::default(),
        }
    }

    fn tag_filter() -> Filter {
        Filter {
            expr: FilterExpr::Comparison {
                field: "tag".to_string(),
                op: CompareOp::Eq,
                value: SqlValue::String("keep".to_string()),
            },
        }
    }

    fn expect_vector_search(plan: SqlPlan) -> (Vec<Filter>, usize) {
        match plan {
            SqlPlan::VectorSearch { filters, top_k, .. } => (filters, top_k),
            other => panic!("expected VectorSearch, got {other:?}"),
        }
    }

    #[test]
    fn outer_filter_merges_onto_vector_search_cte_body() {
        let (filters, top_k) = expect_vector_search(inline_cte(
            &scan_on_cte(vec![tag_filter()], None),
            "knn",
            &vector_search_body(),
        ));
        assert_eq!(
            filters.len(),
            1,
            "the outer WHERE must survive inlining, else the k-NN result comes back unfiltered"
        );
        assert_eq!(top_k, 3, "a filter alone must not change the requested k");
    }

    #[test]
    fn outer_limit_narrows_the_vector_search_top_k() {
        let (_, top_k) = expect_vector_search(inline_cte(
            &scan_on_cte(Vec::new(), Some(1)),
            "knn",
            &vector_search_body(),
        ));
        assert_eq!(top_k, 1, "an outer LIMIT below k must narrow the k-NN cut");
    }

    #[test]
    fn outer_limit_above_k_leaves_top_k_untouched() {
        let (_, top_k) = expect_vector_search(inline_cte(
            &scan_on_cte(Vec::new(), Some(99)),
            "knn",
            &vector_search_body(),
        ));
        assert_eq!(top_k, 3, "an outer LIMIT above k cannot widen the k-NN cut");
    }

    #[test]
    fn unconstrained_reference_returns_the_vector_search_body_verbatim() {
        let (filters, top_k) = expect_vector_search(inline_cte(
            &scan_on_cte(Vec::new(), None),
            "knn",
            &vector_search_body(),
        ));
        assert!(filters.is_empty());
        assert_eq!(top_k, 3);
    }

    /// A CTE-referencing scan carrying an outer ORDER BY / OFFSET / DISTINCT.
    fn scan_on_cte_reorder(sort_keys: Vec<SortKey>, offset: usize, distinct: bool) -> SqlPlan {
        SqlPlan::Scan {
            collection: "knn".to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys,
            limit: None,
            offset,
            distinct,
            window_functions: Vec::new(),
            temporal: crate::TemporalScope::default(),
        }
    }

    fn id_sort_key() -> SortKey {
        SortKey {
            expr: crate::types::SqlExpr::Column {
                table: Some("s".to_string()),
                name: "id".to_string(),
            },
            ascending: true,
            nulls_first: false,
        }
    }

    #[test]
    fn outer_order_by_wraps_vector_search_in_subquery() {
        // An outer ORDER BY cannot fold into the k-NN leaf; it must become a
        // post-processor over the search, and the leaf keeps its own top_k.
        match inline_cte(
            &scan_on_cte_reorder(vec![id_sort_key()], 0, false),
            "knn",
            &vector_search_body(),
        ) {
            SqlPlan::Subquery {
                input, sort_keys, ..
            } => {
                assert_eq!(
                    sort_keys.len(),
                    1,
                    "the outer ORDER BY must ride the wrapper"
                );
                assert!(
                    matches!(*input, SqlPlan::VectorSearch { top_k: 3, .. }),
                    "the search leaf keeps its own top_k under the wrapper"
                );
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    #[test]
    fn outer_distinct_and_offset_wrap_vector_search_in_subquery() {
        match inline_cte(
            &scan_on_cte_reorder(Vec::new(), 2, true),
            "knn",
            &vector_search_body(),
        ) {
            SqlPlan::Subquery {
                offset, distinct, ..
            } => {
                assert_eq!(offset, 2, "the outer OFFSET must ride the wrapper");
                assert!(distinct, "the outer DISTINCT must ride the wrapper");
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    #[test]
    fn plain_limit_does_not_wrap_vector_search() {
        // A LIMIT with no reorder still folds into top_k (fast path), NOT a
        // Subquery wrapper.
        let plan = inline_cte(
            &scan_on_cte(Vec::new(), Some(1)),
            "knn",
            &vector_search_body(),
        );
        assert!(
            matches!(plan, SqlPlan::VectorSearch { top_k: 1, .. }),
            "an unordered LIMIT must fold into top_k, not wrap: {plan:?}"
        );
    }

    fn doubled_x() -> Projection {
        Projection::Computed {
            expr: crate::types::SqlExpr::BinaryOp {
                left: Box::new(crate::types::SqlExpr::Column {
                    table: None,
                    name: "x".to_string(),
                }),
                op: crate::types::BinaryOp::Mul,
                right: Box::new(crate::types::SqlExpr::Literal(SqlValue::Int(2))),
            },
            alias: "y".to_string(),
        }
    }

    fn row_number_spec() -> crate::types::WindowSpec {
        crate::types::WindowSpec {
            function: "row_number".to_string(),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            alias: "rn".to_string(),
            frame: Default::default(),
        }
    }

    /// A CTE-referencing scan carrying an outer projection and window list.
    fn scan_on_cte_projected(
        projection: Vec<Projection>,
        window_functions: Vec<crate::types::WindowSpec>,
    ) -> SqlPlan {
        SqlPlan::Scan {
            collection: "knn".to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection,
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions,
            temporal: crate::TemporalScope::default(),
        }
    }

    /// A `SELECT 1 AS x` body: a constant result, not a scan.
    fn constant_body() -> SqlPlan {
        SqlPlan::ConstantResult {
            columns: vec!["x".to_string()],
            values: vec![SqlValue::Int(1)],
            volatile: false,
        }
    }

    #[test]
    fn computed_projection_over_vector_search_wraps_in_subquery() {
        match inline_cte(
            &scan_on_cte_projected(vec![doubled_x()], Vec::new()),
            "knn",
            &vector_search_body(),
        ) {
            SqlPlan::Subquery {
                input, projection, ..
            } => {
                assert_eq!(projection.len(), 1, "the computed entry rides the wrapper");
                assert!(
                    matches!(&*input, SqlPlan::VectorSearch { projection, .. } if projection.is_empty()),
                    "a computed projection never folds into the search leaf"
                );
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    #[test]
    fn column_only_projection_over_non_scan_body_returns_body() {
        let plan = inline_cte(
            &scan_on_cte_projected(vec![Projection::Column("x".to_string())], Vec::new()),
            "knn",
            &constant_body(),
        );
        assert!(
            matches!(plan, SqlPlan::ConstantResult { .. }),
            "a column-only projection resolves by output schema: {plan:?}"
        );
    }

    #[test]
    fn computed_projection_over_constant_body_wraps_in_subquery() {
        match inline_cte(
            &scan_on_cte_projected(vec![doubled_x()], Vec::new()),
            "knn",
            &constant_body(),
        ) {
            SqlPlan::Subquery {
                input,
                projection,
                window_functions,
                ..
            } => {
                assert!(matches!(*input, SqlPlan::ConstantResult { .. }));
                assert_eq!(projection.len(), 1);
                assert!(window_functions.is_empty());
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    #[test]
    fn window_function_over_constant_body_rides_the_subquery() {
        match inline_cte(
            &scan_on_cte_projected(Vec::new(), vec![row_number_spec()]),
            "knn",
            &constant_body(),
        ) {
            SqlPlan::Subquery {
                window_functions, ..
            } => assert_eq!(window_functions[0].alias, "rn"),
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    fn limited_scan_body() -> SqlPlan {
        SqlPlan::Scan {
            collection: "orders".to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: Some(5),
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: crate::TemporalScope::default(),
        }
    }

    #[test]
    fn outer_filter_over_limited_scan_body_wraps_instead_of_merging() {
        // Merging the WHERE under the inner LIMIT changes which rows the
        // limit sees; the filter must run over the limited rows instead.
        match inline_cte(
            &scan_on_cte(vec![tag_filter()], None),
            "knn",
            &limited_scan_body(),
        ) {
            SqlPlan::Subquery { input, filters, .. } => {
                assert_eq!(filters.len(), 1);
                assert!(matches!(*input, SqlPlan::Scan { limit: Some(5), .. }));
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    #[test]
    fn outer_filter_over_plain_scan_body_merges() {
        let body = SqlPlan::Scan {
            collection: "orders".to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: vec![Projection::Column("qty".to_string())],
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: crate::TemporalScope::default(),
        };
        match inline_cte(&scan_on_cte(vec![tag_filter()], Some(2)), "knn", &body) {
            SqlPlan::Scan {
                collection,
                filters,
                projection,
                limit,
                ..
            } => {
                assert_eq!(collection, "orders");
                assert_eq!(filters.len(), 1);
                assert_eq!(limit, Some(2));
                assert_eq!(projection.len(), 1, "the inner column list is inherited");
            }
            other => panic!("expected merged Scan, got {other:?}"),
        }
    }

    #[test]
    fn aliased_inner_scan_body_keeps_alias_under_outer_computed() {
        // `SELECT x * 2 AS y FROM (SELECT qty AS x FROM orders) s`: the inner
        // alias must be evaluated by the body before the outer expression reads it.
        let body = SqlPlan::Scan {
            collection: "orders".to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: vec![Projection::Computed {
                expr: crate::types::SqlExpr::Column {
                    table: None,
                    name: "qty".to_string(),
                },
                alias: "x".to_string(),
            }],
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: crate::TemporalScope::default(),
        };
        match inline_cte(
            &scan_on_cte_projected(vec![doubled_x()], Vec::new()),
            "knn",
            &body,
        ) {
            SqlPlan::Subquery {
                input, projection, ..
            } => {
                assert!(matches!(
                    &*input,
                    SqlPlan::Scan { projection, .. } if projection.len() == 1
                ));
                assert_eq!(projection.len(), 1);
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    /// The outer side of a top-k LATERAL reads the CTE body, never a
    /// collection named after the CTE.
    #[test]
    fn a_lateral_top_k_outer_side_reads_the_cte_body() {
        let plan = SqlPlan::LateralTopK(crate::types::LateralTopKPlan {
            outer: Box::new(scan_on_cte(Vec::new(), None)),
            outer_alias: Some("knn".to_string()),
            inner_collection: "events".to_string(),
            inner_filters: Vec::new(),
            inner_order_by: Vec::new(),
            inner_limit: 1,
            correlation_keys: vec![("id".to_string(), "user_id".to_string())],
            lateral_alias: "best".to_string(),
            projection: Vec::new(),
            left_join: false,
            temporal: crate::TemporalScope::default(),
        });
        match inline_cte(&plan, "knn", &vector_search_body()) {
            SqlPlan::LateralTopK(top_k) => {
                assert!(matches!(*top_k.outer, SqlPlan::VectorSearch { .. }));
                assert_eq!(top_k.inner_collection, "events");
            }
            other => panic!("expected LateralTopK, got {other:?}"),
        }
    }

    /// Both plans of a LATERAL loop resolve the CTE.
    #[test]
    fn a_lateral_loop_reads_the_cte_body_on_both_sides() {
        let plan = SqlPlan::LateralLoop(crate::types::LateralLoopPlan {
            outer: Box::new(scan_on_cte(Vec::new(), None)),
            outer_alias: Some("knn".to_string()),
            inner: Box::new(scan_on_cte(Vec::new(), None)),
            correlation_predicates: Vec::new(),
            lateral_alias: "recent".to_string(),
            projection: Vec::new(),
            outer_row_cap: 10,
            left_join: false,
        });
        match inline_cte(&plan, "knn", &vector_search_body()) {
            SqlPlan::LateralLoop(lateral_loop) => {
                assert!(matches!(*lateral_loop.outer, SqlPlan::VectorSearch { .. }));
                assert!(matches!(*lateral_loop.inner, SqlPlan::VectorSearch { .. }));
            }
            other => panic!("expected LateralLoop, got {other:?}"),
        }
    }

    /// A join side that scans the CTE stays a one-definition CTE over that
    /// scan, so the join still qualifies its columns by the CTE name. The
    /// other side is untouched.
    #[test]
    fn a_cte_join_side_keeps_the_cte_name() {
        let mut other_side = scan_on_cte(Vec::new(), None);
        if let SqlPlan::Scan { collection, .. } = &mut other_side {
            *collection = "orders".to_string();
        }
        let join = SqlPlan::Join {
            left: Box::new(other_side),
            right: Box::new(scan_on_cte(Vec::new(), None)),
            on: vec![("orders.id".to_string(), "knn.id".to_string())],
            join_type: crate::types::query::JoinType::Inner,
            condition: None,
            limit: None,
            projection: Vec::new(),
            filters: Vec::new(),
        };
        let SqlPlan::Join { left, right, .. } = inline_cte(&join, "knn", &vector_search_body())
        else {
            panic!("expected Join");
        };
        assert!(matches!(&*left, SqlPlan::Scan { collection, .. } if collection == "orders"));
        match *right {
            SqlPlan::Cte(CtePlan { definitions, outer }) => {
                assert_eq!(definitions.len(), 1);
                assert_eq!(definitions[0].0, "knn");
                assert!(matches!(definitions[0].1, SqlPlan::VectorSearch { .. }));
                assert!(matches!(
                    outer.as_ref(),
                    SqlPlan::Scan { collection, .. } if collection == "knn"
                ));
            }
            other => panic!("expected Cte, got {other:?}"),
        }
    }
}
