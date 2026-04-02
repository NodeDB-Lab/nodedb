use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

use crate::control::planner::converter::PlanConverter;
use crate::control::security::credential::CredentialStore;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;

use super::catalog::NodeDbSchemaProvider;
use super::udf::InlineUserFunctions;

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
}

/// DataFusion session context for the Control Plane.
///
/// Wraps a DataFusion `SessionContext` configured for NodeDB's hybrid
/// execution model. SQL/DSL queries are parsed and logically planned here,
/// then converted to `PhysicalPlan` variants for dispatch to the Data Plane.
///
/// The planning pipeline is:
/// 1. Parse SQL → analyzed logical plan (DataFusion analyzer, UDFs registered)
/// 2. Extract user UDF references from the plan
/// 3. Check EXECUTE permission for each referenced UDF
/// 4. Inline user UDF calls (replace with body expressions)
/// 5. Optimize the inlined plan (DataFusion optimizer)
/// 6. Convert to NodeDB physical plan(s)
/// 7. Inject RLS predicates
///
/// This type is `Send + Sync` — lives on the Control Plane (Tokio).
pub struct QueryContext {
    session: SessionContext,
    converter: PlanConverter,
    /// User UDF definitions for inlining + permission extraction.
    /// Empty if no user functions exist for the tenant.
    inliner: InlineUserFunctions,
}

impl QueryContext {
    /// Create a new query context without catalog integration.
    ///
    /// Collections won't be resolvable by name. Use `with_catalog()` for
    /// production use where `SELECT * FROM <collection>` should work.
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");

        let session = SessionContext::new_with_config(config);
        register_udfs(&session);

        Self {
            session,
            converter: PlanConverter::new(),
            inliner: InlineUserFunctions::new(),
        }
    }

    /// Create a query context from SharedState.
    ///
    /// Collections and change streams are visible to DataFusion as tables.
    /// This is the standard constructor — all query paths should use this.
    pub fn for_state(state: &crate::control::state::SharedState, tenant_id: u32) -> Self {
        let ctx = Self::with_catalog(
            Arc::clone(&state.credentials),
            tenant_id,
            Arc::clone(&state.stream_registry),
            Arc::clone(&state.cdc_router),
            Arc::clone(&state.mv_registry),
        );
        // Register sequence UDFs (nextval, currval, setval) with this tenant's context.
        register_sequence_udfs(
            &ctx.session,
            Arc::clone(&state.sequence_registry),
            tenant_id,
        );
        ctx
    }

    /// Override the default rounding mode for `ROUND()` from a session parameter.
    ///
    /// Call after `for_state()` when the connection's `rounding_mode` parameter
    /// differs from the default `HALF_EVEN`. Re-registers the ROUND UDF with
    /// the session-specific default.
    pub fn set_rounding_mode(&self, mode: &str) {
        use super::udf::RoundDecimal;
        use datafusion::logical_expr::ScalarUDF;
        self.session
            .register_udf(ScalarUDF::new_from_impl(RoundDecimal::new(mode)));
    }

    /// Create a query context with catalog + stream + MV integration.
    ///
    /// Prefer `for_state()` when SharedState is available.
    pub fn with_catalog(
        credentials: Arc<CredentialStore>,
        tenant_id: u32,
        stream_registry: Arc<crate::event::cdc::StreamRegistry>,
        cdc_router: Arc<crate::event::cdc::CdcRouter>,
        mv_registry: Arc<crate::event::streaming_mv::MvRegistry>,
    ) -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");

        let session = SessionContext::new_with_config(config);
        register_udfs(&session);

        // Load and register user-defined functions for this tenant.
        let inliner = load_user_functions(&session, &credentials, tenant_id);

        // Register our custom schema provider so DataFusion can resolve
        // collection and stream names during SQL planning.
        let schema_provider = Arc::new(NodeDbSchemaProvider::new(
            Arc::clone(&credentials),
            tenant_id,
            stream_registry,
            cdc_router,
            mv_registry,
        ));
        let catalog = MemoryCatalogProvider::new();
        catalog
            .register_schema("public", schema_provider)
            .expect("register schema");
        session.register_catalog("nodedb", Arc::new(catalog));

        Self {
            session,
            converter: PlanConverter::with_credentials(credentials),
            inliner,
        }
    }

    /// Parse a SQL string into a fully optimized DataFusion logical plan.
    ///
    /// Skips EXECUTE permission checking (for internal use, event triggers, etc.).
    pub async fn sql_to_logical(
        &self,
        sql: &str,
    ) -> crate::Result<datafusion::logical_expr::LogicalPlan> {
        let df = self
            .session
            .sql(sql)
            .await
            .map_err(|e| crate::Error::PlanError {
                detail: format!("SQL parse: {e}"),
            })?;
        let (state, plan) = df.into_parts();
        let inlined = self
            .inliner
            .inline(plan)
            .map_err(|e| crate::Error::PlanError {
                detail: format!("UDF inlining: {e}"),
            })?;
        let optimized = state
            .optimize(&inlined)
            .map_err(|e| crate::Error::PlanError {
                detail: format!("optimization: {e}"),
            })?;
        Ok(optimized)
    }

    /// Parse SQL and convert to NodeDB physical plan(s).
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

    /// Parse SQL, check EXECUTE permissions, convert to physical plan, inject RLS.
    ///
    /// This is the primary query entry point. The pipeline:
    /// 1. Parse SQL → analyzed logical plan (UDFs registered as ScalarUDF markers)
    /// 2. Extract user UDF references from the pre-inline plan
    /// 3. Check EXECUTE permission for each referenced UDF
    /// 4. Inline UDFs (replace calls with body expressions — SECURITY INVOKER
    ///    is enforced because the inlined body runs in the caller's plan context)
    /// 5. Optimize the inlined plan
    /// 6. Convert to NodeDB physical plan(s)
    /// 7. Inject RLS predicates (applies to inlined UDF body subqueries too)
    #[allow(clippy::too_many_arguments)]
    pub async fn plan_sql_with_rls(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        sec: &PlanSecurityContext<'_>,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        // Step 1: Parse + analyze (UDFs still as ScalarFunction nodes, not inlined).
        let df = self
            .session
            .sql(sql)
            .await
            .map_err(|e| crate::Error::PlanError {
                detail: format!("SQL parse: {e}"),
            })?;
        let (state, plan) = df.into_parts();

        // Step 2-3: Check EXECUTE permissions on referenced user UDFs.
        let udf_refs = self.inliner.extract_udf_references(&plan);
        for func_name in &udf_refs {
            if !sec
                .permissions
                .check_function(sec.identity, func_name, sec.roles)
            {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: sec.identity.tenant_id,
                    resource: format!("EXECUTE on function '{func_name}'"),
                });
            }
        }

        // Step 4: Inline UDFs (SECURITY INVOKER — body runs in caller's context).
        let inlined = self
            .inliner
            .inline(plan)
            .map_err(|e| crate::Error::PlanError {
                detail: format!("UDF inlining: {e}"),
            })?;

        // Step 5: Optimize.
        let optimized = state
            .optimize(&inlined)
            .map_err(|e| crate::Error::PlanError {
                detail: format!("optimization: {e}"),
            })?;

        // Step 6: Convert to physical plan.
        let mut tasks = self.converter.convert(&optimized, tenant_id)?;

        // Step 7: Inject RLS (applies to inlined body expressions too).
        super::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;
        Ok(tasks)
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

/// Register all system UDFs on an arbitrary `SessionContext`.
///
/// Exposed for use by the DDL body validator (CREATE FUNCTION) which needs
/// a temporary session with all system UDFs registered.
pub fn register_udfs_on(session: &SessionContext) {
    register_udfs(session);
}

/// Load user-defined functions from the catalog, register as ScalarUDFs,
/// and return the inliner (NOT registered as an AnalyzerRule).
///
/// UDFs are registered as ScalarUDFs so DataFusion can parse SQL that
/// references them. The actual inlining happens in the planning pipeline
/// after EXECUTE permission checking.
fn load_user_functions(
    session: &SessionContext,
    credentials: &CredentialStore,
    tenant_id: u32,
) -> InlineUserFunctions {
    use super::udf::UserDefinedFunction;
    use datafusion::logical_expr::ScalarUDF;

    let mut inliner = InlineUserFunctions::new();

    let catalog = match credentials.catalog() {
        Some(c) => c,
        None => return inliner,
    };

    let functions = match catalog.load_functions_for_tenant(tenant_id) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(tenant_id, error = %e, "failed to load user functions");
            return inliner;
        }
    };

    for func in &functions {
        if let Some(udf_impl) = UserDefinedFunction::from_stored(func) {
            let param_names = udf_impl.param_names().to_vec();
            let body_sql = udf_impl.body_sql().to_string();

            session.register_udf(ScalarUDF::new_from_impl(udf_impl));
            inliner.add(func.name.clone(), param_names, body_sql);
        } else {
            tracing::warn!(
                function = %func.name,
                "skipping user function with unmappable types"
            );
        }
    }

    inliner
}

/// Canonical list of built-in system UDF/UDAF/UDWF names.
///
/// Used by `SHOW FUNCTIONS` to list system functions alongside user-defined ones.
/// Must match the registrations in [`register_udfs`] below and
/// [`nodedb_query::ts_udfs::register_timeseries_udfs`].
pub const SYSTEM_FUNCTION_NAMES: &[&str] = &[
    // Scalar UDFs (registered below).
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
    // Timeseries scalar (nodedb-query).
    "time_bucket",
    // Timeseries window functions (nodedb-query).
    "ts_rate",
    "ts_derivative",
    "ts_moving_avg",
    "ts_ema",
    "ts_delta",
    "ts_interpolate",
    "ts_lag",
    "ts_lead",
    "ts_rank",
    // Timeseries aggregate functions (nodedb-query).
    "ts_percentile",
    "ts_stddev",
    "ts_correlate",
    // Approximate aggregates (nodedb-query).
    "approx_count_distinct",
    "approx_percentile",
    "approx_topk",
    "approx_count",
    // Accounting math functions.
    "round",
    "distribute",
    "allocate",
    // Sequence functions (registered per-tenant in for_state).
    "nextval",
    "currval",
    "setval",
    "next_preview",
];

fn register_udfs(session: &SessionContext) {
    use super::udf::spatial::{
        GeoDistance, StContains, StDistance, StDwithin, StIntersects, StWithin,
    };
    use super::udf::{
        Allocate, Bm25Score, Distribute, DocArrayContains, DocExists, DocGet, MultiVectorSearch,
        RoundDecimal, RrfScore, TextMatch, VectorDistance,
    };
    use datafusion::logical_expr::ScalarUDF;
    session.register_udf(ScalarUDF::new_from_impl(DocGet::new()));
    session.register_udf(ScalarUDF::new_from_impl(DocExists::new()));
    session.register_udf(ScalarUDF::new_from_impl(DocArrayContains::new()));
    session.register_udf(ScalarUDF::new_from_impl(VectorDistance::new()));
    session.register_udf(ScalarUDF::new_from_impl(MultiVectorSearch::new()));
    session.register_udf(ScalarUDF::new_from_impl(RrfScore::new()));
    session.register_udf(ScalarUDF::new_from_impl(Bm25Score::new()));
    session.register_udf(ScalarUDF::new_from_impl(TextMatch::new()));
    // Spatial UDFs.
    session.register_udf(ScalarUDF::new_from_impl(StDwithin::new()));
    session.register_udf(ScalarUDF::new_from_impl(StContains::new()));
    session.register_udf(ScalarUDF::new_from_impl(StIntersects::new()));
    session.register_udf(ScalarUDF::new_from_impl(StWithin::new()));
    session.register_udf(ScalarUDF::new_from_impl(StDistance::new()));
    session.register_udf(ScalarUDF::new_from_impl(GeoDistance::new()));
    // Accounting math UDFs.
    session.register_udf(ScalarUDF::new_from_impl(RoundDecimal::new("HALF_EVEN")));
    session.register_udf(ScalarUDF::new_from_impl(Distribute::new()));
    session.register_udf(ScalarUDF::new_from_impl(Allocate::new()));
    // Timeseries UDFs (window + aggregate).
    nodedb_query::ts_udfs::register_timeseries_udfs(session);
}

/// Register sequence UDFs (nextval, currval, setval) bound to a specific
/// tenant and SequenceRegistry. Called from `for_state()` which has access
/// to SharedState.
fn register_sequence_udfs(
    session: &SessionContext,
    registry: Arc<crate::control::sequence::SequenceRegistry>,
    tenant_id: u32,
) {
    use super::udf::sequence::{CurrVal, NextPreview, NextVal, SetVal};
    use datafusion::logical_expr::ScalarUDF;

    session.register_udf(ScalarUDF::new_from_impl(NextVal::new(
        Arc::clone(&registry),
        tenant_id,
    )));
    session.register_udf(ScalarUDF::new_from_impl(CurrVal::new(
        Arc::clone(&registry),
        tenant_id,
    )));
    session.register_udf(ScalarUDF::new_from_impl(NextPreview::new(
        Arc::clone(&registry),
        tenant_id,
    )));
    session.register_udf(ScalarUDF::new_from_impl(SetVal::new(registry, tenant_id)));
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
