use datafusion::logical_expr::{FetchType, LogicalPlan, Operator};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::extract::{
    expr_to_scan_filters, expr_to_usize, extract_delete_targets, extract_insert_values,
    extract_update_assignments,
};
use super::search::{extract_table_name, try_extract_vector_search};

/// Converts DataFusion logical plans into NodeDB physical tasks.
///
/// This is the bridge between DataFusion's `Send` logical plans and
/// our `!Send` Data Plane execution. The converter walks the logical
/// plan tree and produces `PhysicalPlan` variants that the CoreLoop
/// can execute.
///
/// Lives on the Control Plane (Send + Sync).
#[derive(Default)]
pub struct PlanConverter;

impl PlanConverter {
    pub fn new() -> Self {
        Self
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
            LogicalPlan::Projection(proj) => self.convert(&proj.input, tenant_id),

            LogicalPlan::Filter(filter) => {
                // Check if the filter predicate can be converted to a point get
                // before recursing into the input.
                if let LogicalPlan::TableScan(scan) = filter.input.as_ref() {
                    let collection = scan.table_name.to_string();
                    let vshard = VShardId::from_collection(&collection);

                    if let Some(task) = self.try_point_get(
                        &collection,
                        std::slice::from_ref(&filter.predicate),
                        tenant_id,
                        vshard,
                    )? {
                        return Ok(vec![task]);
                    }

                    // Not a point get — emit DocumentScan with filters.
                    let filters = expr_to_scan_filters(&filter.predicate);
                    let filter_bytes =
                        serde_json::to_vec(&filters).map_err(|e| crate::Error::Serialization {
                            format: "json".into(),
                            detail: format!("filter serialization: {e}"),
                        })?;
                    let limit = scan.fetch.unwrap_or(1000);

                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::DocumentScan {
                            collection,
                            limit,
                            offset: 0,
                            sort_field: String::new(),
                            sort_asc: true,
                            filters: filter_bytes,
                        },
                    }]);
                }
                // Filter wrapping something else — recurse and apply filters.
                let mut tasks = self.convert(&filter.input, tenant_id)?;
                let filters = expr_to_scan_filters(&filter.predicate);
                let filter_bytes =
                    serde_json::to_vec(&filters).map_err(|e| crate::Error::Serialization {
                        format: "json".into(),
                        detail: format!("filter serialization: {e}"),
                    })?;
                for task in &mut tasks {
                    if let PhysicalPlan::DocumentScan { filters, .. } = &mut task.plan {
                        *filters = filter_bytes.clone();
                    }
                }
                Ok(tasks)
            }

            LogicalPlan::TableScan(scan) => {
                let collection = scan.table_name.to_string();
                let vshard = VShardId::from_collection(&collection);

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
                    serde_json::to_vec(&all_filters).map_err(|e| crate::Error::Serialization {
                        format: "json".into(),
                        detail: format!("filter serialization: {e}"),
                    })?
                } else {
                    Vec::new()
                };

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::DocumentScan {
                        collection,
                        limit,
                        offset: 0,
                        sort_field: String::new(),
                        sort_asc: true,
                        filters: filter_bytes,
                    },
                }])
            }

            LogicalPlan::Limit(limit_plan) => {
                let mut tasks = self.convert(&limit_plan.input, tenant_id)?;

                // Extract LIMIT (fetch) value.
                if let Ok(FetchType::Literal(Some(n))) = limit_plan.get_fetch_type() {
                    for task in &mut tasks {
                        match &mut task.plan {
                            PhysicalPlan::DocumentScan { limit, .. } => *limit = n,
                            PhysicalPlan::RangeScan { limit, .. } => *limit = n,
                            _ => {}
                        }
                    }
                }

                // Extract OFFSET (skip) value.
                if let Some(skip) = &limit_plan.skip {
                    if let Ok(skip_n) = expr_to_usize(skip) {
                        for task in &mut tasks {
                            if let PhysicalPlan::DocumentScan { offset, .. } = &mut task.plan {
                                *offset = skip_n;
                            }
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
                            plan: PhysicalPlan::VectorSearch {
                                collection,
                                query_vector: std::sync::Arc::from(query_vector.as_slice()),
                                top_k,
                                filter_bitmap: None,
                            },
                        }]);
                    }
                }

                let mut tasks = self.convert(&sort.input, tenant_id)?;

                // Extract sort field and direction from the first sort expression.
                if let Some(sort_expr) = sort.expr.first() {
                    if let Expr::Column(col) = &sort_expr.expr {
                        let field = col.name.clone();
                        let asc = sort_expr.asc;
                        for task in &mut tasks {
                            if let PhysicalPlan::DocumentScan {
                                sort_field,
                                sort_asc,
                                ..
                            } = &mut task.plan
                            {
                                *sort_field = field.clone();
                                *sort_asc = asc;
                            }
                        }
                    }
                }

                // Propagate sort's fetch as additional limit.
                if let Some(fetch) = sort.fetch {
                    for task in &mut tasks {
                        if let PhysicalPlan::DocumentScan { limit, .. } = &mut task.plan {
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

                // Extract GROUP BY field (first group expression).
                let group_by = agg
                    .group_expr
                    .first()
                    .and_then(|e| {
                        if let Expr::Column(col) = e {
                            Some(col.name.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();

                // Extract aggregate functions.
                let mut aggregates = Vec::new();
                for expr in &agg.aggr_expr {
                    if let Expr::AggregateFunction(func) = expr {
                        let op = func.func.name().to_lowercase();
                        let field = func
                            .args
                            .first()
                            .map(|a| match a {
                                Expr::Column(col) => col.name.clone(),
                                Expr::Literal(_) => "*".into(),
                                Expr::Wildcard { .. } => "*".into(),
                                _ => format!("{a}"),
                            })
                            .unwrap_or_else(|| "*".into());
                        aggregates.push((op, field));
                    }
                }

                // Extract filters from input if it's a Filter plan.
                let filter_bytes = if let LogicalPlan::Filter(filter) = agg.input.as_ref() {
                    let filters = expr_to_scan_filters(&filter.predicate);
                    serde_json::to_vec(&filters).unwrap_or_default()
                } else {
                    Vec::new()
                };

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Aggregate {
                        collection,
                        group_by,
                        aggregates,
                        filters: filter_bytes,
                        limit: 10000,
                    },
                }])
            }

            LogicalPlan::Dml(dml) => self.convert_dml(dml, tenant_id),

            LogicalPlan::Join(join) => super::join::convert_join(join, tenant_id),

            // Pass through — deduplication happens post-scan.
            LogicalPlan::Distinct(distinct) => self.convert(distinct.input(), tenant_id),

            // Scalar subqueries are handled by DataFusion's optimizer.
            // If we reach here, the subquery wasn't optimized away.
            LogicalPlan::Subquery(_) => Err(crate::Error::PlanError {
                detail: "correlated subqueries are not yet supported. Use JOIN instead.".into(),
            }),

            _ => Err(crate::Error::PlanError {
                detail: format!("unsupported logical plan type: {}", plan.display()),
            }),
        }
    }

    /// Convert DML operations (INSERT, UPDATE, DELETE) to physical plans.
    fn convert_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        let collection = dml.table_name.to_string();
        let vshard = VShardId::from_collection(&collection);

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                // Extract values from the input plan.
                // DataFusion represents INSERT VALUES as a projection of literals.
                let values = extract_insert_values(&dml.input)?;

                let mut tasks = Vec::with_capacity(values.len());
                for (doc_id, value_bytes) in values {
                    tasks.push(PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::PointPut {
                            collection: collection.clone(),
                            document_id: doc_id,
                            value: value_bytes,
                        },
                    });
                }

                if tasks.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "INSERT requires at least one row".into(),
                    });
                }

                Ok(tasks)
            }
            WriteOp::Delete => {
                // DELETE FROM <collection> WHERE id = '<value>'
                let doc_ids = extract_delete_targets(&dml.input, &collection)?;

                if doc_ids.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "DELETE requires a WHERE clause with id = '<value>'".into(),
                    });
                }

                Ok(doc_ids
                    .into_iter()
                    .map(|doc_id| PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::PointDelete {
                            collection: collection.clone(),
                            document_id: doc_id,
                        },
                    })
                    .collect())
            }

            WriteOp::Update => {
                // UPDATE <collection> SET field = value WHERE id = '<value>'
                // Extract SET assignments and WHERE targets.
                let doc_ids = extract_delete_targets(&dml.input, &collection)?;

                if doc_ids.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "UPDATE requires a WHERE clause with id = '<value>'".into(),
                    });
                }

                // Extract SET field assignments from the DML input plan.
                let updates = extract_update_assignments(&dml.input)?;

                if updates.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "UPDATE requires at least one SET assignment".into(),
                    });
                }

                Ok(doc_ids
                    .into_iter()
                    .map(|doc_id| PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::PointUpdate {
                            collection: collection.clone(),
                            document_id: doc_id,
                            updates: updates.clone(),
                        },
                    })
                    .collect())
            }
        }
    }

    // extract_insert_values and extract_delete_targets live in extract.rs

    /// Try to convert equality filters into a point get.
    ///
    /// Matches patterns like `WHERE id = 'value'` or `WHERE document_id = 'value'`.
    fn try_point_get(
        &self,
        collection: &str,
        filters: &[Expr],
        tenant_id: TenantId,
        vshard: VShardId,
    ) -> crate::Result<Option<PhysicalTask>> {
        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter {
                if binary.op == Operator::Eq {
                    let (col_name, value) = match (&*binary.left, &*binary.right) {
                        (Expr::Column(col), Expr::Literal(lit)) => {
                            (col.name.as_str(), lit.to_string())
                        }
                        (Expr::Literal(lit), Expr::Column(col)) => {
                            (col.name.as_str(), lit.to_string())
                        }
                        _ => continue,
                    };

                    if col_name == "id" || col_name == "document_id" {
                        let doc_id = value.trim_matches('\'').trim_matches('"').to_string();

                        return Ok(Some(PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::PointGet {
                                collection: collection.to_string(),
                                document_id: doc_id,
                            },
                        }));
                    }
                }
            }
        }
        Ok(None)
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
            PhysicalPlan::PointGet { document_id, .. } => {
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
        assert!(matches!(&tasks[0].plan, PhysicalPlan::DocumentScan { .. }));
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
            PhysicalPlan::DocumentScan { limit, .. } => assert_eq!(*limit, 5),
            other => panic!("expected DocumentScan, got {other:?}"),
        }
    }
}
