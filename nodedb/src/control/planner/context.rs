//! Query planning context for the Control Plane.
//!
//! Uses nodedb-sql for SQL parsing and planning. The DataFusion session
//! is retained only for the function body validator (CREATE FUNCTION)
//! and procedural executor (PL/pgSQL expression evaluation).

use std::sync::Arc;

use crate::control::security::credential::CredentialStore;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;

/// Security context for query planning — bundles identity + permission stores.
///
/// Used by `plan_sql_with_rls` to check EXECUTE permissions on user UDFs
/// and inject RLS predicates.
pub struct PlanSecurityContext<'a> {
    pub identity: &'a AuthenticatedIdentity,
    pub auth: &'a crate::control::security::auth_context::AuthContext,
    pub rls_store: &'a crate::control::security::rls::RlsPolicyStore,
    pub permissions: &'a PermissionStore,
    pub roles: &'a RoleStore,
    /// Permission tree cache for hierarchical ACL injection.
    /// `None` = skip permission tree filtering (e.g., internal queries).
    pub permission_cache: Option<&'a crate::control::security::permission_tree::PermissionCache>,
}

/// Query context for the Control Plane.
///
/// SQL queries are parsed and planned via nodedb-sql, then converted
/// to `PhysicalPlan` variants for dispatch to the Data Plane.
///
/// This type is `Send + Sync` — lives on the Control Plane (Tokio).
pub struct QueryContext {
    /// nodedb-sql catalog adapter for planning.
    sql_catalog: Option<super::catalog_adapter::OriginCatalog>,
    /// Retention policy registry for auto-tier routing.
    retention_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
}

impl QueryContext {
    /// Create a new query context without catalog integration.
    pub fn new() -> Self {
        Self {
            sql_catalog: None,
            retention_registry: None,
        }
    }

    /// Create a query context from SharedState.
    ///
    /// This is the standard constructor — all query paths should use this.
    pub fn for_state(state: &crate::control::state::SharedState, tenant_id: u32) -> Self {
        Self::with_catalog(
            Arc::clone(&state.credentials),
            tenant_id,
            Some(Arc::clone(&state.retention_policy_registry)),
        )
    }

    /// Override the default rounding mode for `ROUND()`.
    ///
    /// No-op: rounding mode is handled at execution time, not planning.
    pub fn set_rounding_mode(&self, _mode: &str) {}

    /// Create a query context with catalog integration.
    pub fn with_catalog(
        credentials: Arc<CredentialStore>,
        tenant_id: u32,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        let sql_catalog = super::catalog_adapter::OriginCatalog::new(
            credentials,
            tenant_id,
            retention_policy_registry.clone(),
        );

        Self {
            sql_catalog: Some(sql_catalog),
            retention_registry: retention_policy_registry,
        }
    }

    /// Parse SQL and convert to NodeDB physical plan(s).
    ///
    /// Uses nodedb-sql for parsing and planning, then converts to PhysicalTasks.
    pub async fn plan_sql(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        self.plan_with_nodedb_sql(sql, tenant_id)
    }

    /// Core planning via nodedb-sql: parse → plan → optimize → convert.
    fn plan_with_nodedb_sql(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        let catalog: &dyn nodedb_sql::SqlCatalog = match &self.sql_catalog {
            Some(c) => c,
            None => {
                return Err(crate::Error::PlanError {
                    detail: "no catalog available for SQL planning".into(),
                });
            }
        };
        let plans = nodedb_sql::plan_sql(sql, catalog).map_err(|e| crate::Error::PlanError {
            detail: format!("{e}"),
        })?;
        let ctx = super::sql_plan_convert::ConvertContext {
            retention_registry: self.retention_registry.clone(),
        };
        super::sql_plan_convert::convert(&plans, tenant_id, &ctx)
    }

    /// Parse SQL, inject RLS predicates, convert to physical plan.
    ///
    /// This is the primary query entry point.
    #[allow(clippy::too_many_arguments)]
    pub async fn plan_sql_with_rls(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        sec: &PlanSecurityContext<'_>,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        self.plan_sql_with_rls_returning(sql, tenant_id, sec, false)
            .await
    }

    /// Plan SQL with RLS injection, optionally propagating a RETURNING flag.
    pub async fn plan_sql_with_rls_returning(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        sec: &PlanSecurityContext<'_>,
        _returning: bool,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        let mut tasks = self.plan_with_nodedb_sql(sql, tenant_id)?;

        // Inject RLS predicates.
        super::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;

        // Inject permission tree filters (hierarchical ACL).
        if let Some(cache) = sec.permission_cache {
            super::rls_injection::inject_permission_tree(&mut tasks, cache, sec.auth)?;
        }

        Ok(tasks)
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Canonical list of built-in system UDF/UDAF/UDWF names.
///
/// Used by `SHOW FUNCTIONS` to list system functions.
pub const SYSTEM_FUNCTION_NAMES: &[&str] = &[
    "doc_get",
    "doc_exists",
    "doc_array_contains",
    "vector_distance",
    "multi_vector_search",
    "rrf_score",
    "bm25_score",
    "text_match",
    "st_dwithin",
    "st_contains",
    "st_intersects",
    "st_within",
    "st_distance",
    "geo_distance",
    "time_bucket",
    "ts_rate",
    "ts_derivative",
    "ts_moving_avg",
    "ts_ema",
    "ts_delta",
    "ts_interpolate",
    "ts_lag",
    "ts_lead",
    "ts_rank",
    "ts_percentile",
    "ts_stddev",
    "ts_correlate",
    "ts_zscore",
    "ts_bollinger_upper",
    "ts_bollinger_lower",
    "ts_bollinger_mid",
    "ts_bollinger_width",
    "ts_moving_percentile",
    "approx_count_distinct",
    "approx_percentile",
    "approx_topk",
    "approx_count",
    "round",
    "distribute",
    "allocate",
    "convert_currency",
    "nextval",
    "currval",
    "setval",
    "next_preview",
];
