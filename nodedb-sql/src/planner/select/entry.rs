//! Top-level query entry: CTE handling, UNION dispatch, ORDER BY/LIMIT
//! application, and ORDER-BY search-trigger detection.

use sqlparser::ast::{self, Query, SetExpr};

use super::entry_ann::parse_ann_options;
use super::helpers::{
    extract_column_name, extract_float, extract_float_array, extract_func_args,
    extract_string_literal,
};
use super::select_stmt::plan_select;
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::resolver::expr::convert_expr;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Default `ef_search` multiplier applied when the user has not supplied
/// `ef_search_override` in the `vector_distance` options. Wider beams trade
/// extra distance computations for higher recall; `2 * top_k` is a standard
/// HNSW heuristic that keeps planning predictable while leaving room for
/// the executor's own recall-based escalation.
const DEFAULT_EF_SEARCH_MULTIPLIER: usize = 2;

/// Plan a SELECT query.
pub fn plan_query(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    // Handle CTEs (WITH clause).
    if let Some(with) = &query.with
        && with.recursive
    {
        return crate::planner::cte::plan_recursive_cte(query, catalog, functions, temporal);
    }
    // Non-recursive CTEs: plan each CTE subquery and the outer query.
    if let Some(with) = &query.with
        && !with.cte_tables.is_empty()
    {
        let inner_query = Query {
            with: None,
            body: query.body.clone(),
            order_by: query.order_by.clone(),
            limit_clause: query.limit_clause.clone(),
            fetch: query.fetch.clone(),
            locks: query.locks.clone(),
            for_clause: query.for_clause.clone(),
            settings: query.settings.clone(),
            format_clause: query.format_clause.clone(),
            pipe_operators: query.pipe_operators.clone(),
        };

        // Plan each CTE subquery.
        let mut definitions = Vec::new();
        let mut cte_names = Vec::new();
        for cte in &with.cte_tables {
            let name = normalize_ident(&cte.alias.name);
            let cte_plan = plan_query(&cte.query, catalog, functions, temporal)?;
            definitions.push((name.clone(), cte_plan));
            cte_names.push(name);
        }

        // Build CTE-aware catalog so the outer query can reference CTE names.
        let cte_catalog = CteCatalog {
            inner: catalog,
            cte_names,
        };
        let outer = plan_query(&inner_query, &cte_catalog, functions, temporal)?;

        return Ok(SqlPlan::Cte {
            definitions,
            outer: Box::new(outer),
        });
    }

    // Handle UNION.
    match &*query.body {
        SetExpr::Select(select) => {
            let mut plan = plan_select(select, catalog, functions, temporal)?;
            if let Some(order_by) = &query.order_by {
                plan = apply_order_by(&plan, order_by, functions)?;
            }
            plan = apply_limit(plan, &query.limit_clause);
            Ok(plan)
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => crate::planner::union::plan_set_operation(
            op,
            left,
            right,
            set_quantifier,
            catalog,
            functions,
            temporal,
        ),
        _ => Err(SqlError::Unsupported {
            detail: format!("query body type: {}", query.body),
        }),
    }
}

/// Apply ORDER BY, detecting search-triggering sort expressions.
fn apply_order_by(
    plan: &SqlPlan,
    order_by: &ast::OrderBy,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    let exprs = match &order_by.kind {
        ast::OrderByKind::Expressions(exprs) => exprs,
        ast::OrderByKind::All(_) => return Ok(plan.clone()),
    };

    if exprs.is_empty() {
        return Ok(plan.clone());
    }

    // Check first ORDER BY expression for search triggers.
    let first = &exprs[0];
    if let Some(search_plan) = try_extract_sort_search(&first.expr, plan, functions)? {
        return Ok(search_plan);
    }

    // Normal sort keys.
    let sort_keys: Vec<SortKey> = exprs
        .iter()
        .map(|o| {
            Ok(SortKey {
                expr: convert_expr(&o.expr)?,
                ascending: o.options.asc.unwrap_or(true),
                nulls_first: o.options.nulls_first.unwrap_or(false),
            })
        })
        .collect::<Result<_>>()?;

    match plan {
        SqlPlan::Scan {
            collection,
            alias,
            engine,
            filters,
            projection,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
            ..
        } => Ok(SqlPlan::Scan {
            collection: collection.clone(),
            alias: alias.clone(),
            engine: *engine,
            filters: filters.clone(),
            projection: projection.clone(),
            sort_keys,
            limit: *limit,
            offset: *offset,
            distinct: *distinct,
            window_functions: window_functions.clone(),
            temporal: *temporal,
        }),
        _ => Ok(plan.clone()),
    }
}

/// Try to detect search-triggering ORDER BY expressions.
fn try_extract_sort_search(
    expr: &ast::Expr,
    plan: &SqlPlan,
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    if let ast::Expr::Function(func) = expr {
        let name = func
            .name
            .0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".");
        let (collection, array_prefilter) = match plan {
            SqlPlan::Scan { collection, .. } => (collection.clone(), None),
            SqlPlan::Join { left, right, .. } => match extract_vector_join_target(left, right) {
                Some(t) => (t.vector_collection, t.array_prefilter),
                None => return Ok(None),
            },
            _ => return Ok(None),
        };
        let args = extract_func_args(func)?;

        match functions.search_trigger(&name) {
            SearchTrigger::VectorSearch => {
                if args.len() < 2 {
                    return Ok(None);
                }
                let field = extract_column_name(&args[0])?;
                let vector = extract_float_array(&args[1])?;
                let ann_options = parse_ann_options(args.get(2))?;
                let limit = match plan {
                    SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
                    SqlPlan::Join { limit, .. } => *limit,
                    _ => 10,
                };
                let ef_search = ann_options
                    .ef_search_override
                    .unwrap_or(limit * DEFAULT_EF_SEARCH_MULTIPLIER);
                return Ok(Some(SqlPlan::VectorSearch {
                    collection,
                    field,
                    query_vector: vector,
                    top_k: limit,
                    ef_search,
                    filters: match plan {
                        SqlPlan::Scan { filters, .. } => filters.clone(),
                        _ => Vec::new(),
                    },
                    array_prefilter,
                    ann_options,
                }));
            }
            SearchTrigger::TextSearch if args.len() >= 2 => {
                let query_text = extract_string_literal(&args[1])?;
                let limit = match plan {
                    SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
                    _ => 10,
                };
                return Ok(Some(SqlPlan::TextSearch {
                    collection,
                    query: query_text,
                    top_k: limit,
                    fuzzy: true,
                    filters: match plan {
                        SqlPlan::Scan { filters, .. } => filters.clone(),
                        _ => Vec::new(),
                    },
                }));
            }
            SearchTrigger::TextSearch => {}
            SearchTrigger::HybridSearch => {
                return plan_hybrid_from_sort(&args, &collection, plan, functions);
            }
            _ => {}
        }
    }
    Ok(None)
}

fn plan_hybrid_from_sort(
    args: &[ast::Expr],
    collection: &str,
    plan: &SqlPlan,
    _functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    // rrf_score(vector_distance(...), bm25_score(...), k1, k2)
    if args.len() < 2 {
        return Ok(None);
    }
    let vector = match &args[0] {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_float_array(&inner_args[1]).unwrap_or_default()
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    };
    let text = match &args[1] {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_string_literal(&inner_args[1]).unwrap_or_default()
            } else {
                String::new()
            }
        }
        _ => String::new(),
    };
    let k1 = args
        .get(2)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let k2 = args
        .get(3)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let limit = match plan {
        SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
        _ => 10,
    };
    let vector_weight = k2 as f32 / (k1 as f32 + k2 as f32);

    Ok(Some(SqlPlan::HybridSearch {
        collection: collection.into(),
        query_vector: vector,
        query_text: text,
        top_k: limit,
        ef_search: limit * 2,
        vector_weight,
        fuzzy: true,
    }))
}

/// Apply LIMIT and OFFSET to a plan.
fn apply_limit(mut plan: SqlPlan, limit_clause: &Option<ast::LimitClause>) -> SqlPlan {
    let (limit_val, offset_val) = match limit_clause {
        None => (None, 0usize),
        Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
            let lv = limit
                .as_ref()
                .and_then(crate::coerce::expr_as_usize_literal);
            let ov = offset
                .as_ref()
                .and_then(|o| crate::coerce::expr_as_usize_literal(&o.value))
                .unwrap_or(0);
            (lv, ov)
        }
        Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
            let lv = crate::coerce::expr_as_usize_literal(limit);
            let ov = crate::coerce::expr_as_usize_literal(offset).unwrap_or(0);
            (lv, ov)
        }
    };

    match plan {
        SqlPlan::Scan {
            ref mut limit,
            ref mut offset,
            ..
        } => {
            *limit = limit_val;
            *offset = offset_val;
        }
        SqlPlan::Aggregate {
            limit: ref mut l, ..
        } => {
            if let Some(lv) = limit_val {
                *l = lv;
            }
        }
        SqlPlan::VectorSearch {
            top_k: ref mut k,
            ef_search: ref mut ef,
            ann_options: ref opts,
            ..
        } => {
            // Fused VectorSearch (e.g. ORDER BY vector_distance + JOIN
            // NDARRAY_SLICE) inherits its outer LIMIT here. Without this,
            // a join-derived VectorSearch carries the join's default
            // 10000 limit instead of the user's `LIMIT N`.
            if let Some(lv) = limit_val {
                *k = lv;
                *ef = opts
                    .ef_search_override
                    .unwrap_or(lv * DEFAULT_EF_SEARCH_MULTIPLIER);
            }
        }
        _ => {}
    }
    plan
}

/// Catalog wrapper that resolves CTE names as schemaless document collections.
struct CteCatalog<'a> {
    inner: &'a dyn SqlCatalog,
    cte_names: Vec<String>,
}

impl SqlCatalog for CteCatalog<'_> {
    fn get_collection(
        &self,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        // Check CTE names first.
        if self.cte_names.iter().any(|n| n == name) {
            return Ok(Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
            }));
        }
        self.inner.get_collection(name)
    }
}

/// Result of inspecting a `SqlPlan::Join` for the
/// `ORDER BY vector_distance(...) + JOIN NDARRAY_SLICE(...)` fusion shape.
struct VectorJoinTarget {
    /// Vector collection backing the search (left or right of the join).
    vector_collection: String,
    /// Array slice that materializes into a surrogate prefilter.
    array_prefilter: Option<crate::types::NdArrayPrefilter>,
}

/// If the join has exactly one `SqlPlan::NdArraySlice` side and the other
/// side is a vector-collection scan, return the fused target. Returns
/// `None` for any other shape — the caller falls through to non-fused
/// join planning.
fn extract_vector_join_target(left: &SqlPlan, right: &SqlPlan) -> Option<VectorJoinTarget> {
    match (left, right) {
        (SqlPlan::Scan { collection, .. }, SqlPlan::NdArraySlice { name, slice, .. }) => {
            Some(VectorJoinTarget {
                vector_collection: collection.clone(),
                array_prefilter: Some(crate::types::NdArrayPrefilter {
                    array_name: name.clone(),
                    slice: slice.clone(),
                }),
            })
        }
        (SqlPlan::NdArraySlice { name, slice, .. }, SqlPlan::Scan { collection, .. }) => {
            Some(VectorJoinTarget {
                vector_collection: collection.clone(),
                array_prefilter: Some(crate::types::NdArrayPrefilter {
                    array_name: name.clone(),
                    slice: slice.clone(),
                }),
            })
        }
        _ => None,
    }
}
