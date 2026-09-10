// SPDX-License-Identifier: BUSL-1.1

//! Expression conversion and CTE inlining.

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_sql::types::{SortKey, SqlExpr, SqlPlan};

use super::value::sql_value_to_nodedb_value;

/// Convert a `nodedb_sql::types::SqlExpr` (parser AST) to a
/// `nodedb_query::expr::SqlExpr` (bridge evaluation type).
///
/// Column references use the **bare** name (no table qualifier) for
/// single-collection evaluation contexts (WHERE, CHECK, GENERATED).
/// For join contexts where the merged document uses qualified keys
/// (`"t1.col"`), use [`sql_expr_to_bridge_expr_qualified`] instead.
pub(super) fn sql_expr_to_bridge_expr(expr: &SqlExpr) -> crate::bridge::expr_eval::SqlExpr {
    convert_expr_inner(expr, false)
}

/// Like [`sql_expr_to_bridge_expr`] but qualifies column references
/// with their table name (`t.col` → `"t.col"`) for join merged docs.
pub(super) fn sql_expr_to_bridge_expr_qualified(
    expr: &SqlExpr,
) -> crate::bridge::expr_eval::SqlExpr {
    convert_expr_inner(expr, true)
}

fn convert_expr_inner(expr: &SqlExpr, qualify: bool) -> crate::bridge::expr_eval::SqlExpr {
    use crate::bridge::expr_eval::SqlExpr as BExpr;
    match expr {
        SqlExpr::Column { table, name } => {
            // `EXCLUDED.col` references the row proposed for insertion in
            // `INSERT ... ON CONFLICT DO UPDATE`. Emit the dedicated
            // variant so the upsert handler can resolve against the
            // incoming row via `eval_with_excluded`. The table qualifier
            // comes in already-normalized (lowercased) from the parser.
            if table
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case("excluded"))
            {
                return BExpr::ExcludedColumn(name.clone());
            }
            if qualify {
                BExpr::Column(nodedb_sql::planner::qualified_name(table.as_deref(), name))
            } else {
                BExpr::Column(name.clone())
            }
        }
        SqlExpr::Literal(v) => BExpr::Literal(sql_value_to_nodedb_value(v)),
        SqlExpr::BinaryOp { left, op, right } => BExpr::BinaryOp {
            left: Box::new(convert_expr_inner(left, qualify)),
            op: match op {
                nodedb_sql::types::BinaryOp::Add => crate::bridge::expr_eval::BinaryOp::Add,
                nodedb_sql::types::BinaryOp::Sub => crate::bridge::expr_eval::BinaryOp::Sub,
                nodedb_sql::types::BinaryOp::Mul => crate::bridge::expr_eval::BinaryOp::Mul,
                nodedb_sql::types::BinaryOp::Div => crate::bridge::expr_eval::BinaryOp::Div,
                nodedb_sql::types::BinaryOp::Mod => crate::bridge::expr_eval::BinaryOp::Mod,
                nodedb_sql::types::BinaryOp::Eq => crate::bridge::expr_eval::BinaryOp::Eq,
                nodedb_sql::types::BinaryOp::Ne => crate::bridge::expr_eval::BinaryOp::NotEq,
                nodedb_sql::types::BinaryOp::Gt => crate::bridge::expr_eval::BinaryOp::Gt,
                nodedb_sql::types::BinaryOp::Ge => crate::bridge::expr_eval::BinaryOp::GtEq,
                nodedb_sql::types::BinaryOp::Lt => crate::bridge::expr_eval::BinaryOp::Lt,
                nodedb_sql::types::BinaryOp::Le => crate::bridge::expr_eval::BinaryOp::LtEq,
                nodedb_sql::types::BinaryOp::And => crate::bridge::expr_eval::BinaryOp::And,
                nodedb_sql::types::BinaryOp::Or => crate::bridge::expr_eval::BinaryOp::Or,
                nodedb_sql::types::BinaryOp::Concat => crate::bridge::expr_eval::BinaryOp::Concat,
            },
            right: Box::new(convert_expr_inner(right, qualify)),
        },
        SqlExpr::Function { name, args, .. } => BExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| convert_expr_inner(a, qualify))
                .collect(),
        },
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => BExpr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(convert_expr_inner(e, qualify))),
            when_thens: when_then
                .iter()
                .map(|(w, t)| {
                    (
                        convert_expr_inner(w, qualify),
                        convert_expr_inner(t, qualify),
                    )
                })
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(convert_expr_inner(e, qualify))),
        },
        SqlExpr::Cast { expr, to_type } => {
            let cast_type = match to_type.to_uppercase().as_str() {
                "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => {
                    crate::bridge::expr_eval::CastType::Int
                }
                "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => {
                    crate::bridge::expr_eval::CastType::Float
                }
                "BOOL" | "BOOLEAN" => crate::bridge::expr_eval::CastType::Bool,
                _ => crate::bridge::expr_eval::CastType::String,
            };
            BExpr::Cast {
                expr: Box::new(convert_expr_inner(expr, qualify)),
                to_type: cast_type,
            }
        }
        SqlExpr::Wildcard => BExpr::Column("*".into()),

        // NOT e / -e → evaluator's Negate (handles both bool and numeric).
        SqlExpr::UnaryOp { expr, .. } => BExpr::Negate(Box::new(convert_expr_inner(expr, qualify))),

        // `e IS NULL` / `e IS NOT NULL` — direct passthrough.
        SqlExpr::IsNull { expr, negated } => BExpr::IsNull {
            expr: Box::new(convert_expr_inner(expr, qualify)),
            negated: *negated,
        },

        // `e BETWEEN low AND high` desugars to `e >= low AND e <= high`
        // (or `e < low OR e > high` when negated). The evaluator has no
        // native Between variant, so the planner must lower it here.
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let e = convert_expr_inner(expr, qualify);
            let l = convert_expr_inner(low, qualify);
            let h = convert_expr_inner(high, qualify);
            if *negated {
                let lt = BExpr::BinaryOp {
                    left: Box::new(e.clone()),
                    op: crate::bridge::expr_eval::BinaryOp::Lt,
                    right: Box::new(l),
                };
                let gt = BExpr::BinaryOp {
                    left: Box::new(e),
                    op: crate::bridge::expr_eval::BinaryOp::Gt,
                    right: Box::new(h),
                };
                BExpr::BinaryOp {
                    left: Box::new(lt),
                    op: crate::bridge::expr_eval::BinaryOp::Or,
                    right: Box::new(gt),
                }
            } else {
                let ge = BExpr::BinaryOp {
                    left: Box::new(e.clone()),
                    op: crate::bridge::expr_eval::BinaryOp::GtEq,
                    right: Box::new(l),
                };
                let le = BExpr::BinaryOp {
                    left: Box::new(e),
                    op: crate::bridge::expr_eval::BinaryOp::LtEq,
                    right: Box::new(h),
                };
                BExpr::BinaryOp {
                    left: Box::new(ge),
                    op: crate::bridge::expr_eval::BinaryOp::And,
                    right: Box::new(le),
                }
            }
        }

        // `e IN (a, b, c)` desugars to `e = a OR e = b OR e = c` — each
        // element may itself be a non-literal expression, so we must
        // recursively convert and OR the comparisons together. `NOT IN`
        // is `e <> a AND e <> b AND e <> c`.
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let target = convert_expr_inner(expr, qualify);
            if list.is_empty() {
                // Empty list: `e IN ()` = false, `e NOT IN ()` = true.
                return BExpr::Literal(nodedb_types::Value::Bool(*negated));
            }
            let (eq_op, combine_op) = if *negated {
                (
                    crate::bridge::expr_eval::BinaryOp::NotEq,
                    crate::bridge::expr_eval::BinaryOp::And,
                )
            } else {
                (
                    crate::bridge::expr_eval::BinaryOp::Eq,
                    crate::bridge::expr_eval::BinaryOp::Or,
                )
            };
            // Empty list is handled above, so `list` is guaranteed non-empty
            // here: we reduce `(target eq list[0]) op (target eq list[1]) op ...`
            // without touching `.unwrap()` or `.expect()`.
            list.iter()
                .map(|item| BExpr::BinaryOp {
                    left: Box::new(target.clone()),
                    op: eq_op,
                    right: Box::new(convert_expr_inner(item, qualify)),
                })
                .reduce(|acc, next| BExpr::BinaryOp {
                    left: Box::new(acc),
                    op: combine_op,
                    right: Box::new(next),
                })
                // Unreachable: `list.is_empty()` returns early above.
                .unwrap_or(BExpr::Literal(nodedb_types::Value::Bool(*negated)))
        }

        // `e LIKE pattern` — no direct evaluator variant; route through a
        // function call so the shared function dispatcher handles it.
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let fn_name = if *case_insensitive { "ilike" } else { "like" };
            let call = BExpr::Function {
                name: fn_name.into(),
                args: vec![
                    convert_expr_inner(expr, qualify),
                    convert_expr_inner(pattern, qualify),
                ],
            };
            if *negated {
                BExpr::Negate(Box::new(call))
            } else {
                call
            }
        }

        // `ARRAY['a', 'b', ...]` — lower each element and, when all resolve to
        // `BExpr::Literal`, fold into a single `Value::Array` literal so that
        // functions like `pg_json_has_any_key` / `pg_json_has_all_keys` receive
        // a proper `Value::Array` argument rather than `Value::Null`.
        SqlExpr::ArrayLiteral(elems) => {
            let mut values = Vec::with_capacity(elems.len());
            let mut all_literal = true;
            for elem in elems {
                match convert_expr_inner(elem, qualify) {
                    BExpr::Literal(v) => values.push(v),
                    other => {
                        all_literal = false;
                        // Non-literal element: fall back to Null for that slot.
                        let _ = other;
                        values.push(nodedb_types::Value::Null);
                    }
                }
            }
            if all_literal {
                BExpr::Literal(nodedb_types::Value::Array(values))
            } else {
                BExpr::Literal(nodedb_types::Value::Null)
            }
        }

        _ => BExpr::Literal(nodedb_types::Value::Null),
    }
}

/// Lower planner sort keys to their physical form.
///
/// Every key is carried, expression and all. Dropping a key the Data Plane
/// cannot name as a stored column would silently answer
/// `ORDER BY 100 / weight` with rows in storage order.
pub(super) fn convert_sort_keys(keys: &[SortKey]) -> Vec<SortKeySpec> {
    keys.iter()
        .map(|k| SortKeySpec {
            expr: sql_expr_to_bridge_expr(&k.expr),
            ascending: k.ascending,
            nulls_first: k.nulls_first,
        })
        .collect()
}

/// Whether a projection list carries anything beyond bare column
/// references / stars. A computed expression (`x/0`, a function call, a
/// window over a column) has no row to evaluate against once the CTE body
/// is inlined as a bare value row — the response shaper would look the
/// aliased column up, find nothing, and emit NULL. Such projections need a
/// real Subquery post-processor over the materialized rows, not a bare
/// `cte_plan.clone()`.
fn has_expression_projection(projection: &[nodedb_sql::types::query::Projection]) -> bool {
    projection.iter().any(|p| match p {
        nodedb_sql::types::query::Projection::Computed { .. } => true,
        nodedb_sql::types::query::Projection::Column(_)
        | nodedb_sql::types::query::Projection::Star
        | nodedb_sql::types::query::Projection::QualifiedStar(_) => false,
    })
}

/// Replace scans on `cte_name` with the CTE's actual subquery plan.
///
/// Outer constraints on the CTE reference are merged onto the CTE body as far
/// as the body can carry them: a `Scan` body takes all of them; a
/// `VectorSearch` body takes filters, projection, and an unordered LIMIT (as
/// `top_k`). Constraints a body has no slot for — an outer `ORDER BY`, OFFSET,
/// or DISTINCT over a non-`Scan` body — are not applied.
pub(super) fn inline_cte(plan: &SqlPlan, cte_name: &str, cte_plan: &SqlPlan) -> SqlPlan {
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
                        // Window functions: the derived body's own specs run
                        // first (they produce columns the outer may reference),
                        // then the outer's. Dropping the outer's here left
                        // `SUM(n) OVER ...` over a derived table with a Scan
                        // body silently returning NULL.
                        window_functions: {
                            let mut merged = inner_w.clone();
                            if !window_functions.is_empty() {
                                merged.extend(window_functions.iter().cloned());
                            }
                            merged
                        },
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
                            window_functions: window_functions.clone(),
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
                    && !has_expression_projection(projection)
                    && window_functions.is_empty()
                {
                    // Any other non-`Scan` body (Aggregate, Join, TextSearch,
                    // HybridSearch, SparseSearch, SpatialScan, MultiVectorSearch,
                    // ...) with only an outer projection of bare columns: the
                    // response boundary projects by output schema, so no
                    // post-processor is needed.
                    cte_plan.clone()
                } else {
                    // The body has no slot for these outer constraints. Apply
                    // them over its materialized rows in a `Subquery`
                    // post-processor — previously they were silently dropped.
                    SqlPlan::Subquery {
                        input: Box::new(cte_plan.clone()),
                        filters: filters.clone(),
                        projection: projection.clone(),
                        window_functions: window_functions.clone(),
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
