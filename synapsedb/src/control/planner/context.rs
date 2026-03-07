use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

use crate::control::planner::converter::PlanConverter;

/// DataFusion session context for the Control Plane.
///
/// Wraps a DataFusion `SessionContext` configured for SynapseDB's hybrid
/// execution model. SQL/DSL queries are parsed and logically planned here,
/// then converted to `PhysicalPlan` variants for dispatch to the Data Plane.
///
/// This type is `Send + Sync` — lives on the Control Plane (Tokio).
pub struct QueryContext {
    session: SessionContext,
    converter: PlanConverter,
}

impl QueryContext {
    /// Create a new query context with default DataFusion configuration.
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("synapsedb", "public");

        let session = SessionContext::new_with_config(config);

        Self {
            session,
            converter: PlanConverter::new(),
        }
    }

    /// Parse a SQL string into a DataFusion logical plan.
    pub async fn sql_to_logical(
        &self,
        sql: &str,
    ) -> crate::Result<datafusion::logical_expr::LogicalPlan> {
        let df = self
            .session
            .sql(sql)
            .await
            .map_err(|e| crate::Error::Bridge(format!("SQL parse error: {e}")))?;
        let plan = df
            .into_optimized_plan()
            .map_err(|e| crate::Error::Bridge(format!("plan optimization error: {e}")))?;
        Ok(plan)
    }

    /// Parse SQL and convert to SynapseDB physical plan(s).
    ///
    /// Returns one or more `PhysicalTask` for dispatch to the Data Plane.
    pub async fn plan_sql(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        let logical = self.sql_to_logical(sql).await?;
        self.converter.convert(&logical, tenant_id)
    }

    /// Access the underlying DataFusion session for advanced configuration
    /// (e.g., registering UDFs, table providers).
    pub fn session(&self) -> &SessionContext {
        &self.session
    }

    /// Register a custom scalar UDF with the DataFusion context.
    pub fn register_udf(&self, udf: datafusion::logical_expr::ScalarUDF) {
        self.session.register_udf(udf);
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parse_simple_select() {
        let ctx = QueryContext::new();

        // Register a dummy table so the SQL parses.
        ctx.session()
            .sql("CREATE TABLE users (id INT, name VARCHAR, email VARCHAR) AS VALUES (1, 'alice', 'a@b.com')")
            .await
            .unwrap();

        let plan = ctx
            .sql_to_logical("SELECT id, name FROM users WHERE id = 1")
            .await;
        assert!(plan.is_ok(), "failed: {:?}", plan.err());
    }

    #[tokio::test]
    async fn invalid_sql_returns_error() {
        let ctx = QueryContext::new();
        let result = ctx.sql_to_logical("SELECTT * FROMM nowhere").await;
        assert!(result.is_err());
    }
}
