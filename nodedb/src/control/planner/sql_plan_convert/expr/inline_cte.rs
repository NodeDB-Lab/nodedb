// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::SqlPlan;

/// Replace scans on `cte_name` with the CTE's actual subquery plan.
///
/// Outer constraints on the CTE reference are merged onto the CTE body as far
/// as the body can carry them: a `Scan` body takes all of them; a
/// `VectorSearch` body takes filters, projection, and an unordered LIMIT (as
/// `top_k`). Constraints a body has no slot for — an outer `ORDER BY`, OFFSET,
/// or DISTINCT over a non-`Scan` body — are not applied.
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
            ..
        } if collection == cte_name => {
            // If the outer query adds filters/sort/limit, wrap the CTE plan.
            // For simple SELECT * FROM cte, just return the CTE plan directly.
            if filters.is_empty()
                && sort_keys.is_empty()
                && limit.is_none()
                && !distinct
                && projection.is_empty()
            {
                cte_plan.clone()
            } else {
                // Merge outer constraints onto the CTE plan if it's also a Scan.
                if let SqlPlan::Scan {
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
                {
                    let mut merged_filters = inner_f.clone();
                    merged_filters.extend(filters.iter().cloned());
                    SqlPlan::Scan {
                        collection: inner_col.clone(),
                        alias: inner_alias.clone(),
                        engine: *inner_eng,
                        filters: merged_filters,
                        // Outer projection overrides inner; empty means "inherit from CTE".
                        projection: if projection.is_empty() {
                            inner_p.clone()
                        } else {
                            projection.clone()
                        },
                        sort_keys: if sort_keys.is_empty() {
                            inner_s.clone()
                        } else {
                            sort_keys.clone()
                        },
                        limit: limit.or(*inner_l),
                        // offset 0 = unspecified → inherit CTE's offset.
                        offset: if *offset > 0 { *offset } else { *inner_o },
                        distinct: *distinct || *inner_d,
                        window_functions: inner_w.clone(),
                        temporal: *inner_t,
                    }
                } else if let SqlPlan::VectorSearch { .. } = cte_plan {
                    // A k-NN body carries its own post-filter list and top-k. An
                    // outer `WHERE` merges into the engine post-filter so the cut
                    // counts MATCHING rows, and — when nothing reorders the
                    // result — an unordered `LIMIT` folds into `top_k` and the
                    // projection rides along. An outer `ORDER BY` / `OFFSET` /
                    // `DISTINCT` reorders the k rows, which the search leaf has no
                    // slot for; those (and a `LIMIT` that must apply after the
                    // reorder) run in a `Subquery` post-processor over the k rows.
                    let needs_reorder = !sort_keys.is_empty() || *offset > 0 || *distinct;
                    let mut leaf = cte_plan.clone();
                    if let SqlPlan::VectorSearch {
                        filters: body_filters,
                        projection: body_projection,
                        top_k,
                        ..
                    } = &mut leaf
                    {
                        body_filters.extend(filters.iter().cloned());
                        if !needs_reorder {
                            if !projection.is_empty() {
                                body_projection.clone_from(projection);
                            }
                            if let Some(outer_limit) = limit {
                                *top_k = (*top_k).min(*outer_limit);
                            }
                        }
                    }
                    if needs_reorder {
                        // Filters already run in the engine; the tail applies the
                        // reorder-dependent constraints over the k rows. It sorts
                        // before projecting, so ORDER BY may reference any column.
                        SqlPlan::Subquery {
                            input: Box::new(leaf),
                            filters: Vec::new(),
                            projection: projection.clone(),
                            sort_keys: sort_keys.clone(),
                            offset: *offset,
                            distinct: *distinct,
                            limit: *limit,
                        }
                    } else {
                        leaf
                    }
                } else if filters.is_empty()
                    && sort_keys.is_empty()
                    && *offset == 0
                    && !*distinct
                    && limit.is_none()
                {
                    // Any other non-`Scan` body (Aggregate, Join, TextSearch,
                    // HybridSearch, SparseSearch, SpatialScan, MultiVectorSearch,
                    // ...) with only an outer projection: the response boundary
                    // projects by output schema, so no post-processor is needed.
                    cte_plan.clone()
                } else {
                    // The body has no slot for these outer constraints. Apply
                    // them over its materialized rows in a `Subquery`
                    // post-processor — previously they were silently dropped.
                    SqlPlan::Subquery {
                        input: Box::new(cte_plan.clone()),
                        filters: filters.clone(),
                        projection: projection.clone(),
                        sort_keys: sort_keys.clone(),
                        offset: *offset,
                        distinct: *distinct,
                        limit: *limit,
                    }
                }
            }
        }

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
            sort_keys,
            offset,
            distinct,
            limit,
        } => SqlPlan::Subquery {
            input: Box::new(inline_cte(input, cte_name, cte_plan)),
            filters: filters.clone(),
            projection: projection.clone(),
            sort_keys: sort_keys.clone(),
            offset: *offset,
            distinct: *distinct,
            limit: *limit,
        },

        // No CTE reference — return as-is.
        _ => plan.clone(),
    }
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
}
