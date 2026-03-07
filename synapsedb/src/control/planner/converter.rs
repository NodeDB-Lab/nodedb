use datafusion::logical_expr::{FetchType, LogicalPlan, Operator};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

/// Converts DataFusion logical plans into SynapseDB physical tasks.
///
/// This is the bridge between DataFusion's `Send` logical plans and
/// our `!Send` Data Plane execution. The converter walks the logical
/// plan tree and produces `PhysicalPlan` variants that the CoreLoop
/// can execute.
///
/// Lives on the Control Plane (Send + Sync).
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
                    let hash = collection
                        .as_bytes()
                        .iter()
                        .fold(0u16, |h, &b| h.wrapping_mul(31).wrapping_add(b as u16));
                    let vshard = VShardId::new(hash % VShardId::COUNT);

                    if let Some(task) = self.try_point_get(
                        &collection,
                        std::slice::from_ref(&filter.predicate),
                        tenant_id,
                        vshard,
                    )? {
                        return Ok(vec![task]);
                    }
                }
                self.convert(&filter.input, tenant_id)
            }

            LogicalPlan::TableScan(scan) => {
                let collection = scan.table_name.to_string();
                // Hash the collection name to determine the target vShard.
                let hash = collection
                    .as_bytes()
                    .iter()
                    .fold(0u16, |h, &b| h.wrapping_mul(31).wrapping_add(b as u16));
                let vshard = VShardId::new(hash % VShardId::COUNT);

                // Check for filter pushdown: equality on id → point get.
                if let Some(task) =
                    self.try_point_get(&collection, &scan.filters, tenant_id, vshard)?
                {
                    return Ok(vec![task]);
                }

                // Default: range scan with optional fetch limit.
                let limit = scan.fetch.unwrap_or(1000);
                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::RangeScan {
                        collection,
                        field: "id".into(),
                        lower: None,
                        upper: None,
                        limit,
                    },
                }])
            }

            LogicalPlan::Limit(limit) => {
                let mut tasks = self.convert(&limit.input, tenant_id)?;
                if let Ok(FetchType::Literal(Some(n))) = limit.get_fetch_type() {
                    for task in &mut tasks {
                        if let PhysicalPlan::RangeScan { limit, .. } = &mut task.plan {
                            *limit = n;
                        }
                    }
                }
                Ok(tasks)
            }

            LogicalPlan::SubqueryAlias(alias) => self.convert(&alias.input, tenant_id),

            _ => Err(crate::Error::Bridge(format!(
                "unsupported logical plan type: {}",
                plan.display()
            ))),
        }
    }

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

impl Default for PlanConverter {
    fn default() -> Self {
        Self
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
        assert!(matches!(&tasks[0].plan, PhysicalPlan::RangeScan { .. }));
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
            PhysicalPlan::RangeScan { limit, .. } => assert_eq!(*limit, 5),
            other => panic!("expected RangeScan, got {other:?}"),
        }
    }
}
