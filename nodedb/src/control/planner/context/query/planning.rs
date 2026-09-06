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
use crate::control::planner::sql_plan_convert::PlanningPurpose;
use crate::control::server::response_shape::schema::OutputSchema;

/// Map a planner error onto its Control-Plane equivalent.
///
/// One mapping for every `plan_sql*` call site. Four copies of this match
/// existed and had already drifted — only one of them mapped
/// `RetryableSchemaChanged`, so the same condition was retryable on one path
/// and a flat plan error on the others. A new variant added to `SqlError`
/// reaches every site through here or none.
fn map_plan_error(error: nodedb_sql::SqlError, tenant_id: crate::types::TenantId) -> crate::Error {
    match error {
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
        nodedb_sql::SqlError::UnknownTable { name } => crate::Error::CollectionNotFound {
            tenant_id,
            collection: name,
        },
        nodedb_sql::SqlError::UndefinedFunction { name } => {
            crate::Error::UndefinedFunction { name }
        }
        nodedb_sql::SqlError::NotNullViolation { table, column } => {
            crate::Error::NotNullViolation { table, column }
        }
        // A constant expression that divides by zero is the same condition the
        // row-scope evaluator raises, so it carries the same code.
        nodedb_sql::SqlError::DivisionByZero => crate::Error::DivisionByZero,
        nodedb_sql::SqlError::InvalidLimitValue { clause, value } => {
            crate::Error::InvalidLimitValue { clause, value }
        }
        other => crate::Error::PlanError {
            detail: other.to_string(),
        },
    }
}

/// Bundled arguments for [`QueryContext::plan_sql_with_rls`].
pub struct PlanSqlWithRlsParams<'a> {
    pub sql: &'a str,
    pub tenant_id: crate::types::TenantId,
    pub database_id: crate::types::DatabaseId,
    pub sec: &'a PlanSecurityContext<'a>,
}

impl QueryContext {
    /// Core planning via nodedb-sql: parse → plan → optimize → convert.
    ///
    /// PRIVATE, and it stays private. Its result is a task set with no policy
    /// resolved against it — no row filters injected, no write image admitted,
    /// no unredactable read refused. Every transport that reached this
    /// directly became a hole through which row-level security did not apply,
    /// so the only caller is
    /// [`Self::plan_sql_with_rls_and_versions_for_purpose`], which runs the
    /// injection pass over what comes back. Work that genuinely has no
    /// requester passes a
    /// [`SystemPlanSecurity`](crate::control::planner::context::SystemPlanSecurity)
    /// context and still goes through that one door.
    ///
    /// Returns the compiled physical tasks and the
    /// [`crate::control::planner::descriptor_set::DescriptorVersionSet`] recording
    /// every descriptor the planner touched. The version set is
    /// used as the plan-cache key AND as the input to
    /// `SharedState::acquire_plan_lease_scope` so cache hits
    /// and fresh plans share the same lease-acquisition path.
    fn plan_with_nodedb_sql_for_purpose(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        purpose: PlanningPurpose,
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
        let catalog = if purpose == PlanningPurpose::Metadata {
            // Metadata requests intentionally do not participate in descriptor
            // lease admission; they only need a stable catalog snapshot for
            // authorization and response shaping.
            crate::control::planner::catalog_adapter::OriginCatalog::new(
                Arc::clone(&inputs.credentials),
                tenant_id.as_u64(),
                database_id,
                inputs.retention_policy_registry.clone(),
            )
        } else {
            inputs.build_adapter(tenant_id.as_u64(), database_id)
        };
        let plans =
            nodedb_sql::plan_sql(sql, &catalog).map_err(|e| map_plan_error(e, tenant_id))?;
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
            .collect::<nodedb_sql::Result<_>>()
            .map_err(|error| map_plan_error(error, tenant_id))?;
        let version_set = catalog.take_recorded_versions();
        let ctx = crate::control::planner::sql_plan_convert::ConvertContext {
            purpose,
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
        returning: bool,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        nodedb_sql::types::PlanCacheEligibility,
    )> {
        self.plan_sql_with_rls_and_versions_for_purpose(
            sql,
            tenant_id,
            database_id,
            sec,
            returning,
            PlanningPurpose::Execute,
        )
        .await
    }

    /// Plan only the metadata required to authorize a Parse/Describe or
    /// EXPLAIN request. Returned tasks are descriptive and must never be
    /// cached, leased, expanded, or dispatched.
    pub async fn plan_sql_with_rls_metadata(
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
        self.plan_sql_with_rls_and_versions_for_purpose(
            sql,
            tenant_id,
            database_id,
            sec,
            false,
            PlanningPurpose::Metadata,
        )
        .await
        .map(|(tasks, schema, _, _)| (tasks, schema))
    }

    async fn plan_sql_with_rls_and_versions_for_purpose(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
        _returning: bool,
        purpose: PlanningPurpose,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        nodedb_sql::types::PlanCacheEligibility,
    )> {
        let (mut tasks, output_schema, mut version_set, cache_eligibility) =
            self.plan_with_nodedb_sql_for_purpose(sql, tenant_id, database_id, purpose)?;

        // Versions read BEFORE injection, never after: injection reads live
        // policy/grant state under its own lock, and a mutation racing in
        // between would otherwise let a post-injection read stamp a version
        // newer than what was actually filtered against, making a plan built
        // from stale state compare as fresh forever. A pre-injection read
        // only ever under-states freshness (an extra harmless replan), never
        // over-states it.
        let permission_tree_version = sec
            .permission_cache
            .map(|c| c.tenant_version(tenant_id.as_u64()))
            .unwrap_or(0);
        let rls_version = sec.rls_store.tenant_version(tenant_id.as_u64());

        // Inject RLS predicates.
        crate::control::planner::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;

        // Refuse what column redaction cannot cover (aggregates over a
        // redacted column, graph traversals), before anything is dispatched.
        crate::control::planner::redaction_refusal::refuse_unredactable_tasks(
            &tasks,
            sec.auth,
            sec.redaction_store,
        )?;

        // Inject permission tree filters (hierarchical ACL).
        if let Some(cache) = sec.permission_cache {
            crate::control::planner::rls_injection::inject_permission_tree(
                &mut tasks, cache, sec.auth,
            )?;
        }

        version_set.set_permission_tree_version(permission_tree_version);
        version_set.set_rls_version(rls_version);

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
        self.plan_sql_with_params_and_rls_and_versions(sql, params, tenant_id, database_id, sec)
            .await
            .map(|(tasks, schema, _)| (tasks, schema))
    }

    /// Parameterized RLS planning plus the descriptor versions observed by its
    /// fresh catalog adapter. Prepared statements require the same fail-closed
    /// descriptor lease admission as fresh and cached simple-query plans.
    pub async fn plan_sql_with_params_and_rls_and_versions(
        &self,
        sql: &str,
        params: &[nodedb_sql::ParamValue],
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
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
        // `plan_with_nodedb_sql_for_purpose`. Its recorded version set is returned to the
        // caller so parameterized plans participate in descriptor admission.
        let catalog = inputs.build_adapter(tenant_id.as_u64(), database_id);
        let raw_plans = nodedb_sql::plan_sql_with_params(sql, params, &catalog)
            .map_err(|error| map_plan_error(error, tenant_id))?;
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
            .collect::<nodedb_sql::Result<_>>()
            .map_err(|error| map_plan_error(error, tenant_id))?;
        let ctx = crate::control::planner::sql_plan_convert::ConvertContext {
            purpose: PlanningPurpose::Execute,
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

        // Versions read BEFORE injection — see the comment on the sibling
        // planning path in this file for why a post-injection read is unsafe.
        let permission_tree_version = sec
            .permission_cache
            .map(|c| c.tenant_version(tenant_id.as_u64()))
            .unwrap_or(0);
        let rls_version = sec.rls_store.tenant_version(tenant_id.as_u64());

        // Inject RLS predicates.
        crate::control::planner::rls_injection::inject_rls(&mut tasks, sec.rls_store, sec.auth)?;

        // Refuse what column redaction cannot cover (aggregates over a
        // redacted column, graph traversals), before anything is dispatched.
        crate::control::planner::redaction_refusal::refuse_unredactable_tasks(
            &tasks,
            sec.auth,
            sec.redaction_store,
        )?;

        if let Some(cache) = sec.permission_cache {
            crate::control::planner::rls_injection::inject_permission_tree(
                &mut tasks, cache, sec.auth,
            )?;
        }

        let mut version_set = catalog.take_recorded_versions();
        version_set.set_permission_tree_version(permission_tree_version);
        version_set.set_rls_version(rls_version);
        Ok((tasks, output_schema, version_set))
    }
}
