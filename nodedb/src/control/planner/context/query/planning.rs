// SPDX-License-Identifier: BUSL-1.1

//! SQL-planning methods of [`QueryContext`].
//!
//! A child module of `query` (directory module) holding the larger planning
//! methods in a second inherent `impl QueryContext` block to keep each file
//! within the size limit. As a child of `query` it has full access to
//! `QueryContext`'s private fields.

use std::sync::Arc;

use super::QueryContext;
use crate::control::planner::context::security::PlanSecurityContext;
use crate::control::server::response_shape::schema::OutputSchema;

/// Bundled arguments for [`QueryContext::plan_sql_with_rls`].
pub struct PlanSqlWithRlsParams<'a> {
    pub sql: &'a str,
    pub tenant_id: crate::types::TenantId,
    pub database_id: crate::types::DatabaseId,
    pub sec: &'a PlanSecurityContext<'a>,
}

impl QueryContext {
    /// Parse SQL and convert to NodeDB physical plan(s).
    ///
    /// Uses nodedb-sql for parsing and planning, then converts to PhysicalTasks.
    /// `database_id` scopes catalog lookups to a specific database namespace.
    /// Pass `DatabaseId::DEFAULT` when no explicit database scope is needed
    /// (e.g. internal queries from event triggers or scheduled jobs).
    pub async fn plan_sql(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
    )> {
        self.plan_with_nodedb_sql(sql, tenant_id, database_id)
            .map(|(t, schema, _, _)| (t, schema))
    }

    /// Core planning via nodedb-sql: parse → plan → optimize → convert.
    ///
    /// Returns the compiled physical tasks and the
    /// [`crate::control::planner::descriptor_set::DescriptorVersionSet`] recording
    /// every descriptor the planner touched. The version set is
    /// used as the plan-cache key AND as the input to
    /// `SharedState::acquire_plan_lease_scope` so cache hits
    /// and fresh plans share the same lease-acquisition path.
    pub(super) fn plan_with_nodedb_sql(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        nodedb_sql::types::PlanCacheEligibility,
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
        let catalog = inputs.build_adapter(tenant_id.as_u64(), database_id);
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
        // Fold catalog-dependent cast expressions (::regclass, ::regtype) to
        // constant OID literals at plan time, before crossing the bridge.
        // The data-plane evaluator is pure and has no catalog access.
        let plans: Vec<_> = plans
            .into_iter()
            .map(|p| {
                nodedb_sql::planner::catalog_fold::fold_catalog_exprs_in_plan(
                    p,
                    &catalog,
                    database_id,
                    tenant_id.as_u64(),
                )
            })
            .collect();
        let version_set = catalog.take_recorded_versions();
        let ctx = crate::control::planner::sql_plan_convert::ConvertContext {
            retention_registry: self.retention_registry.clone(),
            array_catalog: self.array_catalog.clone(),
            credentials: self
                .catalog_inputs
                .as_ref()
                .map(|i| Arc::clone(&i.credentials)),
            wal: self.wal.clone(),
            surrogate_assigner: self.surrogate_assigner.clone(),
            cluster_enabled: self.cluster_enabled,
            bitemporal_retention_registry: self.bitemporal_retention_registry.clone(),
            max_vector_dim: self
                .max_vector_dim
                .load(std::sync::atomic::Ordering::Relaxed),
            force_shuffle_join: self
                .force_shuffle_join
                .load(std::sync::atomic::Ordering::Relaxed),
            shuffle_num_parts: self
                .shuffle_num_parts
                .load(std::sync::atomic::Ordering::Relaxed) as usize,
            force_shuffle_agg: self
                .force_shuffle_agg
                .load(std::sync::atomic::Ordering::Relaxed),
            shuffle_agg_num_parts: self
                .shuffle_agg_num_parts
                .load(std::sync::atomic::Ordering::Relaxed)
                as usize,
            broadcast_threshold_bytes: self
                .broadcast_threshold_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            shuffle_agg_threshold: self
                .shuffle_agg_threshold
                .load(std::sync::atomic::Ordering::Relaxed),
            database_id,
            tenant_id,
        };
        let output_schema =
            crate::control::planner::sql_plan_convert::output_schema::build_output_schema(
                &plans,
                &catalog,
                database_id,
            );
        let cache_eligibility = if plans
            .iter()
            .all(|plan| plan.cache_eligibility().is_cacheable())
        {
            nodedb_sql::types::PlanCacheEligibility::Cacheable
        } else {
            nodedb_sql::types::PlanCacheEligibility::DataDependent
        };
        let tasks = crate::control::planner::sql_plan_convert::convert(&plans, tenant_id, &ctx)?;
        Ok((tasks, output_schema, version_set, cache_eligibility))
    }

    /// Parse SQL, inject RLS predicates, convert to physical plan.
    ///
    /// This is the primary query entry point.
    pub async fn plan_sql_with_rls(
        &self,
        params: PlanSqlWithRlsParams<'_>,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
    )> {
        let PlanSqlWithRlsParams {
            sql,
            tenant_id,
            database_id,
            sec,
        } = params;
        self.plan_sql_with_rls_returning(sql, tenant_id, database_id, sec, false)
            .await
    }

    /// Plan SQL with RLS injection, optionally propagating a RETURNING flag.
    pub async fn plan_sql_with_rls_returning(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
        returning: bool,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
    )> {
        self.plan_sql_with_rls_and_versions(sql, tenant_id, database_id, sec, returning)
            .await
            .map(|(tasks, schema, _, _)| (tasks, schema))
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
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
        _returning: bool,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        nodedb_sql::types::PlanCacheEligibility,
    )> {
        let (mut tasks, output_schema, version_set, cache_eligibility) =
            self.plan_with_nodedb_sql(sql, tenant_id, database_id)?;

        // Inject RLS predicates.
        crate::control::planner::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;

        // Inject permission tree filters (hierarchical ACL).
        if let Some(cache) = sec.permission_cache {
            crate::control::planner::rls_injection::inject_permission_tree(
                &mut tasks, cache, sec.auth,
            )?;
        }

        Ok((tasks, output_schema, version_set, cache_eligibility))
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
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
    )> {
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
        let catalog = inputs.build_adapter(tenant_id.as_u64(), database_id);
        let raw_plans = nodedb_sql::plan_sql_with_params(sql, params, &catalog).map_err(|e| {
            crate::Error::PlanError {
                detail: format!("{e}"),
            }
        })?;
        let plans: Vec<_> = raw_plans
            .into_iter()
            .map(|p| {
                nodedb_sql::planner::catalog_fold::fold_catalog_exprs_in_plan(
                    p,
                    &catalog,
                    database_id,
                    tenant_id.as_u64(),
                )
            })
            .collect();
        let ctx = crate::control::planner::sql_plan_convert::ConvertContext {
            retention_registry: self.retention_registry.clone(),
            array_catalog: self.array_catalog.clone(),
            credentials: self
                .catalog_inputs
                .as_ref()
                .map(|i| Arc::clone(&i.credentials)),
            wal: self.wal.clone(),
            surrogate_assigner: self.surrogate_assigner.clone(),
            cluster_enabled: self.cluster_enabled,
            bitemporal_retention_registry: self.bitemporal_retention_registry.clone(),
            max_vector_dim: self
                .max_vector_dim
                .load(std::sync::atomic::Ordering::Relaxed),
            force_shuffle_join: self
                .force_shuffle_join
                .load(std::sync::atomic::Ordering::Relaxed),
            shuffle_num_parts: self
                .shuffle_num_parts
                .load(std::sync::atomic::Ordering::Relaxed) as usize,
            force_shuffle_agg: self
                .force_shuffle_agg
                .load(std::sync::atomic::Ordering::Relaxed),
            shuffle_agg_num_parts: self
                .shuffle_agg_num_parts
                .load(std::sync::atomic::Ordering::Relaxed)
                as usize,
            broadcast_threshold_bytes: self
                .broadcast_threshold_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            shuffle_agg_threshold: self
                .shuffle_agg_threshold
                .load(std::sync::atomic::Ordering::Relaxed),
            database_id,
            tenant_id,
        };
        let output_schema =
            crate::control::planner::sql_plan_convert::output_schema::build_output_schema(
                &plans,
                &catalog,
                database_id,
            );
        let mut tasks =
            crate::control::planner::sql_plan_convert::convert(&plans, tenant_id, &ctx)?;

        // Inject RLS predicates.
        crate::control::planner::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;

        if let Some(cache) = sec.permission_cache {
            crate::control::planner::rls_injection::inject_permission_tree(
                &mut tasks, cache, sec.auth,
            )?;
        }

        Ok((tasks, output_schema))
    }
}
