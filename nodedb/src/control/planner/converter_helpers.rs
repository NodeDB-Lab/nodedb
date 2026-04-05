//! Converter helpers: sort, aggregate, and timeseries plan conversion.
//!

use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{DocumentOp, QueryOp, TextOp, TimeseriesOp, VectorOp};
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::converter::PlanConverter;
use super::extract::expr_to_scan_filters;
use super::search::{
    extract_table_name, try_extract_hybrid_search, try_extract_multi_vector_search,
    try_extract_text_search, try_extract_vector_search,
};

impl PlanConverter {
    /// Convert a Sort logical plan into physical tasks.
    ///
    /// Handles: vector search, text search, hybrid search, index-ordered scan,
    /// and generic sort-key propagation.
    pub(super) fn convert_sort(
        &self,
        sort: &datafusion::logical_expr::Sort,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        // Check for vector_distance() function in sort expression.
        if let Some((collection, query_vector, top_k)) =
            try_extract_vector_search(&sort.expr, &sort.input, sort.fetch)?
        {
            let vshard = VShardId::from_collection(&collection);
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Vector(VectorOp::Search {
                    collection,
                    query_vector: std::sync::Arc::from(query_vector.as_slice()),
                    top_k,
                    ef_search: 0,
                    filter_bitmap: None,
                    field_name: String::new(),
                    rls_filters: Vec::new(),
                }),
            }]);
        }

        // Check for multi_vector_search() in sort expression → MultiSearch.
        if let Some((collection, query_vector, top_k)) =
            try_extract_multi_vector_search(&sort.expr, &sort.input, sort.fetch)?
        {
            let vshard = VShardId::from_collection(&collection);
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Vector(VectorOp::MultiSearch {
                    collection,
                    query_vector: std::sync::Arc::from(query_vector.as_slice()),
                    top_k,
                    ef_search: 0,
                    filter_bitmap: None,
                    rls_filters: Vec::new(),
                }),
            }]);
        }

        // Check for bm25_score() in sort expression → TextSearch.
        if let Some((collection, query_text, top_k)) =
            try_extract_text_search(&sort.expr, &sort.input, sort.fetch)
        {
            let vshard = VShardId::from_collection(&collection);
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::Search {
                    collection,
                    query: query_text,
                    top_k,
                    fuzzy: true,
                    rls_filters: Vec::new(),
                }),
            }]);
        }

        // Check for rrf_score(vector_distance(...), bm25_score(...), k1, k2) → HybridSearch.
        if let Some((collection, query_vector, query_text, top_k, vector_k, text_k)) =
            try_extract_hybrid_search(&sort.expr, &sort.input, sort.fetch)?
        {
            let vshard = VShardId::from_collection(&collection);
            let vector_weight = (text_k / (vector_k + text_k).max(0.001)) as f32;
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::HybridSearch {
                    collection,
                    query_vector: std::sync::Arc::from(query_vector.as_slice()),
                    query_text,
                    top_k,
                    ef_search: 0,
                    fuzzy: true,
                    vector_weight,
                    filter_bitmap: None,
                    rls_filters: Vec::new(),
                }),
            }]);
        }

        // Index-ordered scan optimization (single ASC field with LIMIT).
        if sort.expr.len() == 1
            && sort.expr[0].asc
            && let Expr::Column(col) = &sort.expr[0].expr
        {
            let sort_field = &col.name;
            if sort_field != "id"
                && sort_field != "document_id"
                && let Some(collection) = extract_table_name(&sort.input)
            {
                let limit = sort.fetch.unwrap_or(1000);
                let vshard = VShardId::from_collection(&collection);
                return Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::RangeScan {
                        collection,
                        field: sort_field.clone(),
                        lower: None,
                        upper: None,
                        limit,
                    }),
                }]);
            }
        }

        let mut tasks = self.convert(&sort.input, tenant_id)?;

        // Extract sort expressions as (field, ascending) pairs.
        let mut extracted_keys = Vec::new();
        for sort_expr in &sort.expr {
            if let Expr::Column(col) = &sort_expr.expr {
                extracted_keys.push((col.name.clone(), sort_expr.asc));
            }
        }
        if !extracted_keys.is_empty() {
            for task in &mut tasks {
                if let PhysicalPlan::Document(DocumentOp::Scan { sort_keys, .. }) = &mut task.plan {
                    *sort_keys = extracted_keys.clone();
                }
            }
        }

        // Propagate sort's fetch as additional limit.
        if let Some(fetch) = sort.fetch {
            for task in &mut tasks {
                if let PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) = &mut task.plan {
                    *limit = fetch;
                }
            }
        }

        Ok(tasks)
    }
}

impl PlanConverter {
    /// Convert a DataFusion `Aggregate` logical plan into physical tasks.
    ///
    /// For timeseries collections, ALL aggregates route through
    /// `TimeseriesOp::Scan` — the universal timeseries query path that
    /// reads from both the active memtable and sealed disk partitions.
    /// Non-timeseries collections use `QueryOp::Aggregate`.
    pub(super) fn convert_aggregate(
        &self,
        agg: &datafusion::logical_expr::Aggregate,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        let collection = extract_table_name(&agg.input).ok_or_else(|| crate::Error::PlanError {
            detail: "GROUP BY requires a table scan input".into(),
        })?;
        let vshard = VShardId::from_collection(&collection);

        // Extract GROUP BY columns (filtering out time_bucket expressions
        // which are handled separately via bucket_interval_ms).
        let group_by: Vec<String> = agg
            .group_expr
            .iter()
            .filter_map(|e| {
                if let Expr::Column(col) = e {
                    Some(col.name.clone())
                } else {
                    None
                }
            })
            .collect();

        // Extract aggregate expressions: (op, field).
        let mut aggregates = Vec::new();
        for expr in &agg.aggr_expr {
            if let Expr::AggregateFunction(func) = expr {
                let mut op = func.func.name().to_lowercase();
                let field = func
                    .params
                    .args
                    .first()
                    .map(|a| match a {
                        Expr::Column(col) => col.name.clone(),
                        Expr::Literal(..) => "*".into(),
                        _ => format!("{a}"),
                    })
                    .unwrap_or_else(|| "*".into());

                if func.params.distinct {
                    op = format!("{op}_distinct");
                }

                aggregates.push((op, field));
            }
        }

        // Route ALL timeseries aggregates through TimeseriesOp::Scan.
        // This is the universal path that reads from both memtable and
        // sealed disk partitions, avoiding the QueryOp::Aggregate path
        // which only sees memtable data.
        if matches!(
            self.collection_type(tenant_id, &collection),
            Some(nodedb_types::CollectionType::Columnar(
                nodedb_types::columnar::ColumnarProfile::Timeseries { .. }
            ))
        ) {
            let bucket_interval_ms = try_extract_time_bucket_interval(&agg.group_expr).unwrap_or(0);

            let (time_range, filter_bytes) = extract_aggregate_filters(&agg.input)?;

            // AUTO_TIER: if the collection has an enabled auto_tier policy,
            // split the query across tiers instead of scanning raw only.
            if let Some(policy) = self.auto_tier_policy(tenant_id, &collection) {
                return Ok(super::auto_tier::plan_tiered_scan(
                    &policy,
                    tenant_id,
                    time_range,
                    filter_bytes,
                    group_by,
                    aggregates,
                    String::new(),
                ));
            }

            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                    collection,
                    time_range,
                    projection: Vec::new(),
                    limit: usize::MAX,
                    filters: filter_bytes,
                    bucket_interval_ms,
                    group_by,
                    aggregates,
                    gap_fill: String::new(),
                    rls_filters: Vec::new(),
                }),
            }]);
        }

        // Non-timeseries: standard document-style aggregation.
        let filter_bytes =
            if let datafusion::logical_expr::LogicalPlan::Filter(filter) = agg.input.as_ref() {
                let filters = expr_to_scan_filters(&filter.predicate);
                zerompk::to_msgpack_vec(&filters).unwrap_or_default()
            } else {
                Vec::new()
            };

        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Query(QueryOp::Aggregate {
                collection,
                group_by,
                aggregates,
                filters: filter_bytes,
                having: Vec::new(),
                limit: usize::MAX,
                sub_group_by: Vec::new(),
                sub_aggregates: Vec::new(),
            }),
        }])
    }
}

/// Extract filters from an aggregate's input plan.
///
/// Handles all shapes DataFusion may produce:
/// - `Filter(TableScan)` — explicit filter node
/// - `TableScan{filters:[...]}` — pushed-down predicates
/// - `Filter(TableScan{filters:[...]})` — both
/// - `SubqueryAlias(Filter(...))` — alias wrappers
///
/// Returns `((min_ts, max_ts), serialized_non_timestamp_filters)`.
fn extract_aggregate_filters(
    input: &datafusion::logical_expr::LogicalPlan,
) -> crate::Result<((i64, i64), Vec<u8>)> {
    let mut all_filter_exprs: Vec<Expr> = Vec::new();

    collect_filter_exprs(input, &mut all_filter_exprs);

    if all_filter_exprs.is_empty() {
        return Ok(((0i64, i64::MAX), Vec::new()));
    }

    extract_timeseries_filters(&all_filter_exprs)
}

/// Recursively collect filter expressions from a plan tree.
fn collect_filter_exprs(plan: &datafusion::logical_expr::LogicalPlan, out: &mut Vec<Expr>) {
    use datafusion::logical_expr::LogicalPlan;
    match plan {
        LogicalPlan::Filter(filter) => {
            out.push(filter.predicate.clone());
            collect_filter_exprs(&filter.input, out);
        }
        LogicalPlan::TableScan(scan) => {
            out.extend(scan.filters.iter().cloned());
        }
        LogicalPlan::SubqueryAlias(alias) => {
            collect_filter_exprs(&alias.input, out);
        }
        LogicalPlan::Projection(proj) => {
            collect_filter_exprs(&proj.input, out);
        }
        _ => {}
    }
}

/// Extract `time_bucket(interval, ...)` from GROUP BY expressions.
///
/// Returns `Some(bucket_interval_ms)` if any GROUP BY expression is a
/// `time_bucket()` call with a parseable interval string.
fn try_extract_time_bucket_interval(group_exprs: &[Expr]) -> Option<i64> {
    for expr in group_exprs {
        let func = match expr {
            Expr::ScalarFunction(f) => f,
            Expr::Alias(alias) => match alias.expr.as_ref() {
                Expr::ScalarFunction(f) => f,
                _ => continue,
            },
            _ => continue,
        };

        if func.name() != "time_bucket" || func.args.is_empty() {
            continue;
        }

        // First arg is the interval string literal.
        if let Expr::Literal(lit, _) = &func.args[0] {
            let interval_str = lit.to_string();
            let trimmed = interval_str.trim_matches('\'').trim_matches('"');
            if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(trimmed) {
                return Some(ms as i64);
            }
        }
    }
    None
}

/// Extract time-range predicates from filter expressions for timeseries routing.
///
/// Looks for patterns like `timestamp >= X AND timestamp <= Y` and extracts
/// the range bounds. Non-timestamp filters are serialized as generic filters.
/// Returns `((min_ts, max_ts), filter_bytes)`.
pub(super) fn extract_timeseries_filters(filters: &[Expr]) -> crate::Result<((i64, i64), Vec<u8>)> {
    let mut min_ts = 0i64;
    let mut max_ts = i64::MAX;
    let mut remaining = Vec::new();

    for f in filters {
        if let Some((bound, is_lower)) = try_extract_timestamp_bound(f) {
            if is_lower {
                min_ts = min_ts.max(bound);
            } else {
                max_ts = max_ts.min(bound);
            }
        } else {
            remaining.extend(expr_to_scan_filters(f));
        }
    }

    let filter_bytes = if remaining.is_empty() {
        Vec::new()
    } else {
        zerompk::to_msgpack_vec(&remaining).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("ts filter serialization: {e}"),
        })?
    };

    Ok(((min_ts, max_ts), filter_bytes))
}

/// Try to extract a timestamp bound from a comparison expression.
///
/// Returns `Some((bound_value_ms, is_lower_bound))` if the expression
/// is `timestamp >= X` or `timestamp <= X` (or `>`, `<`).
fn try_extract_timestamp_bound(expr: &Expr) -> Option<(i64, bool)> {
    match expr {
        Expr::BinaryExpr(bin) => {
            // Check if left side is a column named "timestamp" or "ts".
            let col_name = match bin.left.as_ref() {
                Expr::Column(col) => col.name.to_lowercase(),
                _ => return None,
            };
            if col_name != "timestamp" && col_name != "ts" && col_name != "time" {
                return None;
            }

            // Extract literal value.
            let val = match bin.right.as_ref() {
                Expr::Literal(datafusion::scalar::ScalarValue::Int64(Some(v)), _) => *v,
                Expr::Literal(datafusion::scalar::ScalarValue::Float64(Some(v)), _) => *v as i64,
                _ => return None,
            };

            match bin.op {
                datafusion::logical_expr::Operator::GtEq
                | datafusion::logical_expr::Operator::Gt => {
                    Some((val, true)) // lower bound
                }
                datafusion::logical_expr::Operator::LtEq
                | datafusion::logical_expr::Operator::Lt => {
                    Some((val, false)) // upper bound
                }
                _ => None,
            }
        }
        _ => None,
    }
}
