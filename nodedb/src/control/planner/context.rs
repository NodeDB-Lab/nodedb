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
///
/// The catalog adapter is **constructed per-plan**, not cached on
/// the context, because the adapter's `recorded_versions` field
/// is per-plan state. Holding a shared adapter across concurrent
/// plans would interleave their recorded descriptor sets and
/// poison the plan-cache keys. The context stores the inputs
/// needed to construct an adapter (credentials, optional
/// `Weak<SharedState>` for lease integration, tenant id,
/// retention registry) and builds a fresh one on every planning
/// call.
pub struct QueryContext {
    catalog_inputs: Option<CatalogInputs>,
    /// Retention policy registry for auto-tier routing.
    retention_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
    /// Array catalog handle — required for `CREATE ARRAY` / `DROP ARRAY` /
    /// `INSERT INTO ARRAY` / `DELETE FROM ARRAY` to resolve and persist
    /// catalog entries. `None` for sub-planners that don't own array DDL.
    array_catalog: Option<crate::control::array_catalog::ArrayCatalogHandle>,
    /// WAL allocator — required by array DML for `wal_lsn` allocation.
    wal: Option<Arc<crate::wal::WalManager>>,
}

/// Inputs needed to construct an `OriginCatalog` per plan call.
///
/// Tenant is intentionally **not** stored here: every plan call passes the
/// effective tenant to `build_adapter`, so a single `QueryContext` shared
/// across a pgwire handler can serve queries from connections belonging to
/// different tenants without cross-tenant catalog resolution.
#[derive(Clone)]
struct CatalogInputs {
    credentials: Arc<CredentialStore>,
    shared: Option<std::sync::Weak<crate::control::state::SharedState>>,
    retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
}

impl CatalogInputs {
    fn build_adapter(&self, tenant_id: u32) -> super::catalog_adapter::OriginCatalog {
        if let Some(weak) = &self.shared
            && let Some(shared) = weak.upgrade()
        {
            super::catalog_adapter::OriginCatalog::new_with_lease(
                &shared,
                tenant_id,
                self.retention_policy_registry.clone(),
            )
        } else {
            super::catalog_adapter::OriginCatalog::new(
                Arc::clone(&self.credentials),
                tenant_id,
                self.retention_policy_registry.clone(),
            )
        }
    }
}

impl QueryContext {
    /// Create a new query context without catalog integration.
    pub fn new() -> Self {
        Self {
            catalog_inputs: None,
            retention_registry: None,
            array_catalog: None,
            wal: None,
        }
    }

    /// Create a query context from `SharedState` without lease
    /// integration. Used by internal sub-planners (check
    /// constraints, type guards, ANALYZE, procedural DML, event
    /// trigger dispatch) that run inside a pgwire handler whose
    /// outer query already acquired leases. Re-acquiring via a
    /// sub-planner would be redundant — the lease store's fast
    /// path would return instantly anyway, but going through the
    /// sub-planner without a direct `Arc<SharedState>` reference
    /// would require threading one through every call site.
    pub fn for_state(state: &crate::control::state::SharedState) -> Self {
        Self::with_catalog(
            Arc::clone(&state.credentials),
            Some(Arc::clone(&state.retention_policy_registry)),
        )
    }

    /// Create a query context with descriptor lease integration.
    /// Used by the top-level pgwire dispatch so every user
    /// query's plan acquires descriptor leases before execution.
    /// Callers must hold an `Arc<SharedState>` — the adapter
    /// downgrades to `Weak` internally.
    pub fn for_state_with_lease(state: &Arc<crate::control::state::SharedState>) -> Self {
        let retention = Some(Arc::clone(&state.retention_policy_registry));
        Self {
            catalog_inputs: Some(CatalogInputs {
                credentials: Arc::clone(&state.credentials),
                shared: Some(Arc::downgrade(state)),
                retention_policy_registry: retention.clone(),
            }),
            retention_registry: retention,
            array_catalog: Some(state.array_catalog.clone()),
            wal: Some(Arc::clone(&state.wal)),
        }
    }

    /// Override the default rounding mode for `ROUND()`.
    ///
    /// No-op: rounding mode is handled at execution time, not planning.
    pub fn set_rounding_mode(&self, _mode: &str) {}

    /// Create a query context with catalog integration but no
    /// lease acquisition. Used by `for_state` and by callers
    /// that construct a context without an `Arc<SharedState>`.
    pub fn with_catalog(
        credentials: Arc<CredentialStore>,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        let catalog_inputs = Some(CatalogInputs {
            credentials,
            shared: None,
            retention_policy_registry: retention_policy_registry.clone(),
        });

        Self {
            catalog_inputs,
            retention_registry: retention_policy_registry,
            array_catalog: None,
            wal: None,
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
        self.plan_with_nodedb_sql(sql, tenant_id).map(|(t, _)| t)
    }

    /// Core planning via nodedb-sql: parse → plan → optimize → convert.
    ///
    /// Returns the compiled physical tasks and the
    /// [`super::descriptor_set::DescriptorVersionSet`] recording
    /// every descriptor the planner touched. The version set is
    /// used as the plan-cache key AND as the input to
    /// `SharedState::acquire_plan_lease_scope` so cache hits
    /// and fresh plans share the same lease-acquisition path.
    fn plan_with_nodedb_sql(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
    ) -> crate::Result<(
        Vec<super::physical::PhysicalTask>,
        super::descriptor_set::DescriptorVersionSet,
    )> {
        let inputs = match &self.catalog_inputs {
            Some(i) => i,
            None => {
                return Err(crate::Error::PlanError {
                    detail: "no catalog available for SQL planning".into(),
                });
            }
        };
        // Fresh adapter per plan call: the adapter's
        // `recorded_versions` field is per-plan state, and
        // two concurrent plans through a shared QueryContext
        // would otherwise interleave their recorded sets.
        let catalog = inputs.build_adapter(tenant_id.as_u32());
        let plans = nodedb_sql::plan_sql(sql, &catalog).map_err(|e| match e {
            nodedb_sql::SqlError::RetryableSchemaChanged { descriptor } => {
                crate::Error::RetryableSchemaChanged { descriptor }
            }
            nodedb_sql::SqlError::CollectionDeactivated {
                name,
                retention_expires_at_ns,
                ..
            } => crate::Error::CollectionDeactivated {
                tenant_id,
                collection: name,
                retention_expires_at_ns,
            },
            other => crate::Error::PlanError {
                detail: format!("{other}"),
            },
        })?;
        let version_set = catalog.take_recorded_versions();
        let ctx = super::sql_plan_convert::ConvertContext {
            retention_registry: self.retention_registry.clone(),
            array_catalog: self.array_catalog.clone(),
            credentials: self
                .catalog_inputs
                .as_ref()
                .map(|i| Arc::clone(&i.credentials)),
            wal: self.wal.clone(),
        };
        let tasks = super::sql_plan_convert::convert(&plans, tenant_id, &ctx)?;
        Ok((tasks, version_set))
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
        returning: bool,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        self.plan_sql_with_rls_and_versions(sql, tenant_id, sec, returning)
            .await
            .map(|(tasks, _)| tasks)
    }

    /// Variant of [`plan_sql_with_rls_returning`] that also
    /// returns the `DescriptorVersionSet` recorded during
    /// planning. The pgwire plan cache uses the set as its
    /// freshness witness, and the handler feeds it into
    /// `SharedState::acquire_plan_lease_scope` to take the
    /// refcounts that must stay non-zero through execute.
    pub async fn plan_sql_with_rls_and_versions(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        sec: &PlanSecurityContext<'_>,
        _returning: bool,
    ) -> crate::Result<(
        Vec<super::physical::PhysicalTask>,
        super::descriptor_set::DescriptorVersionSet,
    )> {
        let (mut tasks, version_set) = self.plan_with_nodedb_sql(sql, tenant_id)?;

        // Inject RLS predicates.
        super::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;

        // Inject permission tree filters (hierarchical ACL).
        if let Some(cache) = sec.permission_cache {
            super::rls_injection::inject_permission_tree(&mut tasks, cache, sec.auth)?;
        }

        Ok((tasks, version_set))
    }

    /// Plan SQL with bound parameters and RLS injection.
    ///
    /// Used by prepared statement execution to bind parameters at the AST level
    /// (not via SQL text substitution), then plan and inject RLS as normal.
    pub async fn plan_sql_with_params_and_rls(
        &self,
        sql: &str,
        params: &[nodedb_sql::ParamValue],
        tenant_id: crate::types::TenantId,
        sec: &PlanSecurityContext<'_>,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        let inputs = match &self.catalog_inputs {
            Some(i) => i,
            None => {
                return Err(crate::Error::PlanError {
                    detail: "no catalog available for SQL planning".into(),
                });
            }
        };
        // Fresh adapter per plan call: same rationale as
        // `plan_with_nodedb_sql`. The params-and-rls path does
        // not currently surface the recorded version set to
        // callers (prepared statements with bound parameters go
        // through a different cache key), but constructing the
        // adapter fresh keeps the adapter's state per-plan and
        // allows future extension.
        let catalog = inputs.build_adapter(tenant_id.as_u32());
        let plans = nodedb_sql::plan_sql_with_params(sql, params, &catalog).map_err(|e| {
            crate::Error::PlanError {
                detail: format!("{e}"),
            }
        })?;
        let ctx = super::sql_plan_convert::ConvertContext {
            retention_registry: self.retention_registry.clone(),
            array_catalog: self.array_catalog.clone(),
            credentials: self
                .catalog_inputs
                .as_ref()
                .map(|i| Arc::clone(&i.credentials)),
            wal: self.wal.clone(),
        };
        let mut tasks = super::sql_plan_convert::convert(&plans, tenant_id, &ctx)?;

        // Inject RLS predicates.
        super::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;

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
