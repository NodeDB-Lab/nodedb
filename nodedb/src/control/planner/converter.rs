use datafusion::logical_expr::{FetchType, LogicalPlan};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{DocumentOp, QueryOp, TextOp, TimeseriesOp, VectorOp};
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::credential::CredentialStore;
use crate::types::{TenantId, VShardId};

use super::extract::{expr_to_scan_filters, expr_to_usize, try_range_scan_from_predicate};
use super::search::{
    extract_table_name, try_extract_hybrid_search, try_extract_text_match_predicate,
    try_extract_text_search, try_extract_vector_search,
};
use super::sql_expr_convert::try_convert_projection;

/// Converts DataFusion logical plans into NodeDB physical tasks.
///
/// This is the bridge between DataFusion's `Send` logical plans and
/// our `!Send` Data Plane execution. The converter walks the logical
/// plan tree and produces `PhysicalPlan` variants that the CoreLoop
/// can execute.
///
/// Lives on the Control Plane (Send + Sync).
#[derive(Default)]
pub struct PlanConverter {
    /// Optional catalog reference for collection-type-aware routing.
    credentials: Option<std::sync::Arc<CredentialStore>>,
}

impl PlanConverter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a converter with catalog access for timeseries detection.
    pub fn with_credentials(credentials: std::sync::Arc<CredentialStore>) -> Self {
        Self {
            credentials: Some(credentials),
        }
    }

    /// Check if a collection is a timeseries collection.
    fn is_timeseries(&self, tenant_id: TenantId, collection: &str) -> bool {
        if let Some(ref creds) = self.credentials
            && let Some(catalog) = creds.catalog()
            && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), collection)
        {
            return coll.collection_type.is_timeseries();
        }
        false
    }

    /// Convert a DataFusion logical plan into one or more physical tasks.
    ///
    /// Simple queries (point gets, scans) produce a single task.
    /// Complex queries (joins, aggregations) may produce multiple tasks
    /// targeting different vShards.
    pub fn convert(
        &self,
        plan: &LogicalPlan,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        match plan {
            LogicalPlan::Projection(proj) => {
                let mut tasks = self.convert(&proj.input, tenant_id)?;

                // Try to convert computed expressions (e.g., price * qty AS total).
                if let Some(computed) = try_convert_projection(&proj.expr) {
                    let computed_bytes = rmp_serde::to_vec_named(&computed).unwrap_or_default();
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan {
                            computed_columns, ..
                        }) = &mut task.plan
                        {
                            *computed_columns = computed_bytes.clone();
                        }
                    }
                    return Ok(tasks);
                }

                // Simple column projection (no computed expressions).
                let columns: Vec<String> = proj
                    .expr
                    .iter()
                    .filter_map(|e| {
                        if let Expr::Column(col) = e {
                            Some(col.name.clone())
                        } else if let Expr::Alias(a) = e
                            && let Expr::Column(col) = &*a.expr
                        {
                            Some(col.name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                if !columns.is_empty() {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { projection, .. }) =
                            &mut task.plan
                        {
                            *projection = columns.clone();
                        }
                    }
                }

                Ok(tasks)
            }

            LogicalPlan::Filter(filter) => {
                // Check for text_match() UDF in filter → TextSearch plan.
                if let Some((collection, query_text)) =
                    try_extract_text_match_predicate(&filter.predicate, &filter.input)
                {
                    let vshard = VShardId::from_collection(&collection);
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Text(TextOp::Search {
                            collection,
                            query: query_text,
                            top_k: 1000,
                            fuzzy: true,
                        }),
                    }]);
                }

                // Check if the filter predicate can be converted to a point get
                // before recursing into the input.
                if let LogicalPlan::TableScan(scan) = filter.input.as_ref() {
                    let collection = scan.table_name.to_string().to_lowercase();
                    let vshard = VShardId::from_collection(&collection);

                    if let Some(task) = self.try_point_get(
                        &collection,
                        std::slice::from_ref(&filter.predicate),
                        tenant_id,
                        vshard,
                    )? {
                        return Ok(vec![task]);
                    }

                    // Try secondary index: equality or range on a non-id field → RangeScan.
                    if let Some(task) = try_range_scan_from_predicate(
                        &collection,
                        &filter.predicate,
                        tenant_id,
                        vshard,
                    ) {
                        return Ok(vec![task]);
                    }

                    // Not a point get or indexed scan — emit DocumentScan with filters.
                    let filters = expr_to_scan_filters(&filter.predicate);
                    let filter_bytes = rmp_serde::to_vec_named(&filters).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("filter serialization: {e}"),
                        }
                    })?;
                    let limit = scan.fetch.unwrap_or(1000);

                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Document(DocumentOp::Scan {
                            collection,
                            limit,
                            offset: 0,
                            sort_keys: Vec::new(),
                            filters: filter_bytes,
                            distinct: false,
                            projection: Vec::new(),
                            computed_columns: Vec::new(),
                            window_functions: Vec::new(),
                        }),
                    }]);
                }
                // Filter wrapping Aggregate = HAVING clause.
                if matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_)) {
                    let mut tasks = self.convert(&filter.input, tenant_id)?;
                    let having_filters = expr_to_scan_filters(&filter.predicate);
                    let having_bytes = rmp_serde::to_vec_named(&having_filters).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("having serialization: {e}"),
                        }
                    })?;
                    for task in &mut tasks {
                        if let PhysicalPlan::Query(QueryOp::Aggregate { having, .. }) =
                            &mut task.plan
                        {
                            *having = having_bytes.clone();
                        }
                    }
                    return Ok(tasks);
                }

                // Filter wrapping something else — recurse and apply filters.
                let mut tasks = self.convert(&filter.input, tenant_id)?;
                let filters = expr_to_scan_filters(&filter.predicate);
                let filter_bytes =
                    rmp_serde::to_vec_named(&filters).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("filter serialization: {e}"),
                    })?;
                for task in &mut tasks {
                    if let PhysicalPlan::Document(DocumentOp::Scan { filters, .. }) =
                        &mut task.plan
                    {
                        *filters = filter_bytes.clone();
                    }
                }
                Ok(tasks)
            }

            LogicalPlan::TableScan(scan) => {
                let collection = scan.table_name.to_string().to_lowercase();
                let vshard = VShardId::from_collection(&collection);

                // Timeseries routing: if collection is timeseries, emit TimeseriesScan.
                if self.is_timeseries(tenant_id, &collection) {
                    let limit = scan.fetch.unwrap_or(10_000);
                    let (time_range, filter_bytes) =
                        extract_timeseries_filters(&scan.filters)?;
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                            collection,
                            time_range,
                            projection: Vec::new(),
                            limit,
                            filters: filter_bytes,
                            bucket_interval_ms: 0,
                        }),
                    }]);
                }

                // Check for filter pushdown: equality on id → point get.
                if let Some(task) =
                    self.try_point_get(&collection, &scan.filters, tenant_id, vshard)?
                {
                    return Ok(vec![task]);
                }

                // Default: full document scan.
                let limit = scan.fetch.unwrap_or(1000);

                // Convert any TableScan filters to scan filters.
                let filter_bytes = if !scan.filters.is_empty() {
                    let mut all_filters = Vec::new();
                    for f in &scan.filters {
                        all_filters.extend(expr_to_scan_filters(f));
                    }
                    rmp_serde::to_vec_named(&all_filters).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("filter serialization: {e}"),
                        }
                    })?
                } else {
                    Vec::new()
                };

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::Scan {
                        collection,
                        limit,
                        offset: 0,
                        sort_keys: Vec::new(),
                        filters: filter_bytes,
                        distinct: false,
                        projection: Vec::new(),
                        computed_columns: Vec::new(),
                        window_functions: Vec::new(),
                    }),
                }])
            }

            LogicalPlan::Limit(limit_plan) => {
                let mut tasks = self.convert(&limit_plan.input, tenant_id)?;

                // Extract LIMIT (fetch) value.
                if let Ok(FetchType::Literal(Some(n))) = limit_plan.get_fetch_type() {
                    for task in &mut tasks {
                        match &mut task.plan {
                            PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) => *limit = n,
                            PhysicalPlan::Document(DocumentOp::RangeScan { limit, .. }) => {
                                *limit = n
                            }
                            _ => {}
                        }
                    }
                }

                // Extract OFFSET (skip) value.
                if let Some(skip) = &limit_plan.skip
                    && let Ok(skip_n) = expr_to_usize(skip)
                {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { offset, .. }) =
                            &mut task.plan
                        {
                            *offset = skip_n;
                        }
                    }
                }

                Ok(tasks)
            }

            LogicalPlan::SubqueryAlias(alias) => self.convert(&alias.input, tenant_id),

            LogicalPlan::Sort(sort) => {
                // Check for vector_distance() function in sort expression.
                // Pattern: ORDER BY vector_distance(embedding, ARRAY[...]) LIMIT k
                {
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
                            }),
                        }]);
                    }
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
                        }),
                    }]);
                }

                // Check for rrf_score(vector_distance(...), bm25_score(...), k1, k2) → HybridSearch.
                if let Some((collection, query_vector, query_text, top_k, vector_k, text_k)) =
                    try_extract_hybrid_search(&sort.expr, &sort.input, sort.fetch)?
                {
                    let vshard = VShardId::from_collection(&collection);
                    // Convert per-source k-constants to a vector_weight:
                    // higher vector_k relative to text_k = more weight on text (inverse).
                    // weight = text_k / (vector_k + text_k) so equal k → 0.5.
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
                        }),
                    }]);
                }

                // Index-ordered scan optimization:
                // If sorting by a single ASC field on a plain TableScan with a LIMIT
                // (no OFFSET), use RangeScan — results come back in index order,
                // eliminating the sort. Only works for ASC because B-tree indexes
                // are ascending. DESC or OFFSET queries fall through to DocumentScan.
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

                // Extract all sort expressions as (field, ascending) pairs.
                let mut extracted_keys = Vec::new();
                for sort_expr in &sort.expr {
                    if let Expr::Column(col) = &sort_expr.expr {
                        extracted_keys.push((col.name.clone(), sort_expr.asc));
                    }
                }
                if !extracted_keys.is_empty() {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { sort_keys, .. }) =
                            &mut task.plan
                        {
                            *sort_keys = extracted_keys.clone();
                        }
                    }
                }

                // Propagate sort's fetch as additional limit.
                if let Some(fetch) = sort.fetch {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) =
                            &mut task.plan
                        {
                            *limit = fetch;
                        }
                    }
                }

                Ok(tasks)
            }

            LogicalPlan::Aggregate(agg) => {
                // Extract collection from input (TableScan).
                let collection =
                    extract_table_name(&agg.input).ok_or_else(|| crate::Error::PlanError {
                        detail: "GROUP BY requires a table scan input".into(),
                    })?;
                let vshard = VShardId::from_collection(&collection);

                // Extract GROUP BY fields (all group expressions).
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

                // Extract aggregate functions.
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

                        // Handle DISTINCT modifier: COUNT(DISTINCT col) → "count_distinct".
                        if func.params.distinct {
                            op = format!("{op}_distinct");
                        }

                        aggregates.push((op, field));
                    }
                }

                // Extract filters from input if it's a Filter plan.
                let filter_bytes = if let LogicalPlan::Filter(filter) = agg.input.as_ref() {
                    let filters = expr_to_scan_filters(&filter.predicate);
                    rmp_serde::to_vec_named(&filters).unwrap_or_default()
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
                        limit: 10000,
                        sub_group_by: Vec::new(),
                        sub_aggregates: Vec::new(),
                    }),
                }])
            }

            LogicalPlan::Dml(dml) => self.convert_dml(dml, tenant_id),

            LogicalPlan::Join(join) => super::join::convert_join(join, tenant_id),

            // DISTINCT: recurse into input, then mark DocumentScan for dedup.
            LogicalPlan::Distinct(distinct) => {
                let mut tasks = self.convert(distinct.input(), tenant_id)?;
                for task in &mut tasks {
                    if let PhysicalPlan::Document(DocumentOp::Scan { distinct, .. }) =
                        &mut task.plan
                    {
                        *distinct = true;
                    }
                }
                Ok(tasks)
            }

            // Window functions: convert inner plan + extract window specs.
            LogicalPlan::Window(window) => {
                let mut tasks = self.convert(&window.input, tenant_id)?;

                // Convert window expressions to WindowFuncSpec.
                let specs = super::sql_expr_convert::convert_window_exprs(&window.window_expr);
                if !specs.is_empty() {
                    let spec_bytes = rmp_serde::to_vec_named(&specs).unwrap_or_default();
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan {
                            window_functions, ..
                        }) = &mut task.plan
                        {
                            *window_functions = spec_bytes.clone();
                        }
                    }
                }

                Ok(tasks)
            }

            // Subqueries: DataFusion's optimizer decorrelates many subqueries
            // into joins before they reach the converter. If a Subquery node
            // arrives here, it means the optimizer couldn't decorrelate it.
            // We handle it by converting the subquery's inner plan — this
            // works for uncorrelated subqueries (IN, EXISTS, scalar).
            // Correlated subqueries that reference outer columns will fail
            // at execution time if the Data Plane can't resolve them.
            LogicalPlan::Subquery(subquery) => self.convert(&subquery.subquery, tenant_id),

            // UNION / UNION ALL: convert each input plan and concatenate tasks.
            // The Data Plane executes each sub-plan independently; the Control
            // Plane concatenates the result sets (UNION ALL) or deduplicates
            // (UNION DISTINCT — handled by wrapping in Distinct if needed).
            LogicalPlan::Union(union) => {
                let mut all_tasks = Vec::new();
                for input in &union.inputs {
                    let tasks = self.convert(input, tenant_id)?;
                    all_tasks.extend(tasks);
                }
                Ok(all_tasks)
            }

            LogicalPlan::RecursiveQuery(_) => Err(crate::Error::PlanError {
                detail: "WITH RECURSIVE is not yet supported. Use iterative queries or graph traversal (GRAPH TRAVERSE) instead.".into(),
            }),

            _ => Err(crate::Error::PlanError {
                detail: format!("unsupported logical plan type: {}", plan.display()),
            }),
        }
    }
}

/// Extract time-range predicates from filter expressions for timeseries routing.
///
/// Looks for patterns like `timestamp >= X AND timestamp <= Y` and extracts
/// the range bounds. Non-timestamp filters are serialized as generic filters.
/// Returns `((min_ts, max_ts), filter_bytes)`.
fn extract_timeseries_filters(filters: &[Expr]) -> crate::Result<((i64, i64), Vec<u8>)> {
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
        rmp_serde::to_vec_named(&remaining).map_err(|e| crate::Error::Serialization {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    async fn make_context_with_table() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE users (id VARCHAR, name VARCHAR, email VARCHAR) AS VALUES ('u1', 'alice', 'a@b.com'), ('u2', 'bob', 'b@c.com')")
            .await
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn converts_point_get() {
        let ctx = make_context_with_table().await;
        let df = ctx
            .sql("SELECT * FROM users WHERE id = 'u1'")
            .await
            .unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);

        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::PointGet { document_id, .. }) => {
                assert_eq!(document_id, "u1");
            }
            other => panic!("expected PointGet, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn converts_table_scan() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT name FROM users").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::Document(DocumentOp::Scan { .. })
        ));
    }

    #[tokio::test]
    async fn limit_propagates() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT * FROM users LIMIT 5").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) => assert_eq!(*limit, 5),
            other => panic!("expected DocumentScan, got {other:?}"),
        }
    }
}
