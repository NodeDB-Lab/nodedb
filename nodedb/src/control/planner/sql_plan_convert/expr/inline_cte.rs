// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::{Filter, Projection, SortKey, SqlPlan, WindowSpec};

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
pub(in crate::control::planner::sql_plan_convert) fn inline_cte(
    plan: &SqlPlan,
    cte_name: &str,
    cte_plan: &SqlPlan,
) -> SqlPlan {
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
            left: Box::new(inline_cte(left, cte_name, cte_plan)),
            right: Box::new(inline_cte(right, cte_name, cte_plan)),
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
            limit,
            column_map,
        } => SqlPlan::InsertSelect {
            target: target.clone(),
            source: Box::new(inline_cte(source, cte_name, cte_plan)),
            limit: *limit,
            column_map: column_map.clone(),
        },

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

        // No CTE reference — return as-is.
        _ => plan.clone(),
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
    use nodedb_sql::types::{CompareOp, EngineType, Filter, FilterExpr, SortKey, SqlValue};

    fn vector_search_body() -> SqlPlan {
        SqlPlan::VectorSearch {
            collection: "docs".to_string(),
            field: "embedding".to_string(),
            query_vector: vec![0.1, 0.2],
            top_k: 3,
            ef_search: 64,
            metric: nodedb_sql::types::DistanceMetric::L2,
            filters: Vec::new(),
            array_prefilter: None,
            ann_options: nodedb_sql::types::VectorAnnOptions::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            projection: Vec::new(),
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
            temporal: nodedb_sql::TemporalScope::default(),
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
            temporal: nodedb_sql::TemporalScope::default(),
        }
    }

    fn id_sort_key() -> SortKey {
        SortKey {
            expr: nodedb_sql::types::SqlExpr::Column {
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
            expr: nodedb_sql::types::SqlExpr::BinaryOp {
                left: Box::new(nodedb_sql::types::SqlExpr::Column {
                    table: None,
                    name: "x".to_string(),
                }),
                op: nodedb_sql::types::BinaryOp::Mul,
                right: Box::new(nodedb_sql::types::SqlExpr::Literal(SqlValue::Int(2))),
            },
            alias: "y".to_string(),
        }
    }

    fn row_number_spec() -> nodedb_sql::types::WindowSpec {
        nodedb_sql::types::WindowSpec {
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
        window_functions: Vec<nodedb_sql::types::WindowSpec>,
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
            temporal: nodedb_sql::TemporalScope::default(),
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
            temporal: nodedb_sql::TemporalScope::default(),
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
            temporal: nodedb_sql::TemporalScope::default(),
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
                expr: nodedb_sql::types::SqlExpr::Column {
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
            temporal: nodedb_sql::TemporalScope::default(),
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
}
