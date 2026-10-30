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
use crate::control::planner::plan_error_map::map_plan_error;
use crate::control::planner::sql_plan_convert::PlanningPurpose;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::returning::{
    ReturningClause, attach_returning_spec, resolve_returning_for_plans,
};

/// Resolve a DML `RETURNING` clause against `plans` and build the announced
/// output schema from it in one step.
///
/// Both the fresh-adapter path ([`QueryContext::plan_with_nodedb_sql_for_purpose`])
/// and the parameterized path ([`QueryContext::plan_sql_with_params_and_rls_and_versions`])
/// resolve the clause before conversion and need the same output schema built
/// from it, so the pairing lives here once.
pub(super) fn resolve_returning_and_output_schema<C: nodedb_sql::catalog::SqlCatalog>(
    plans: &[nodedb_sql::types::SqlPlan],
    returning_items: Option<&str>,
    catalog: &C,
    database_id: crate::types::DatabaseId,
    tenant_id: crate::types::TenantId,
) -> crate::Result<(OutputSchema, Option<ReturningClause>)> {
    let returning = resolve_returning_for_plans(plans, returning_items, catalog, tenant_id)?;
    let output_schema =
        crate::control::planner::sql_plan_convert::output_schema::build_output_schema(
            plans,
            catalog,
            database_id,
            returning
                .as_ref()
                .map(|clause| clause.projection.as_slice()),
        )
        .map_err(|error| map_plan_error(error, tenant_id))?;
    Ok((output_schema, returning))
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
    ///
    /// `returning_items` is the raw text after a DML `RETURNING` keyword. It
    /// resolves here against the planned target: the announced output schema
    /// carries the projection, and every task carries the Data-Plane spec.
    async fn plan_with_nodedb_sql_for_purpose(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        purpose: PlanningPurpose,
        returning_items: Option<&str>,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        nodedb_sql::types::PlanCacheEligibility,
    )> {
        let inputs = &self.catalog_inputs;
        // Fresh adapter per plan call: the adapter's
        // `recorded_versions` field is per-plan state, and
        // two concurrent plans through a shared QueryContext
        // will otherwise interleave their recorded sets.
        let catalog = if purpose == PlanningPurpose::Metadata {
            // Metadata requests intentionally do not participate in descriptor
            // lease admission; they only need a stable catalog snapshot for
            // authorization and response shaping. They never execute, so
            // sequence functions resolve to their type and advance nothing.
            let catalog = crate::control::planner::catalog_adapter::OriginCatalog::new(
                Arc::clone(&inputs.credentials),
                inputs.array_catalog.clone(),
                tenant_id.as_u64(),
                database_id,
                inputs.retention_policy_registry.clone(),
            )
            .describing();
            match inputs.sequence_registry() {
                Some(registry) => catalog.with_sequence_registry(registry),
                None => catalog,
            }
        } else {
            inputs.build_adapter(tenant_id.as_u64(), database_id)
        };
        // `nextval` records into the calling session's map and `currval` reads
        // only from it, so the adapter must know which session is planning.
        // `Arc` because the converters evaluate column DEFAULTs that read the
        // catalog — `nextval` and `currval` — after planning returns.
        let catalog = Arc::new(catalog.with_session_sequences(self.session_sequences()));
        let plans = nodedb_sql::plan_sql(sql, catalog.as_ref())
            .map_err(|e| map_plan_error(e, tenant_id))?;
        // Fold catalog-dependent cast expressions (::regclass, ::regtype) to
        // constant OID literals at plan time, before crossing the bridge.
        // The data-plane evaluator is pure and has no catalog access.
        let plans: Vec<_> = plans
            .into_iter()
            .map(|p| {
                nodedb_sql::planner::catalog_fold::fold_catalog_exprs_in_plan(
                    p,
                    catalog.as_ref(),
                    database_id,
                    tenant_id.as_u64(),
                )
            })
            .collect::<nodedb_sql::Result<_>>()
            .map_err(|error| map_plan_error(error, tenant_id))?;
        let version_set = catalog.take_recorded_versions();
        let (output_schema, returning) = resolve_returning_and_output_schema(
            &plans,
            returning_items,
            catalog.as_ref(),
            database_id,
            tenant_id,
        )?;
        let cache_eligibility =
            crate::control::planner::sql_plan_convert::batch_cache_eligibility(&plans);
        let mut tasks = self
            .convert_prefetched(&plans, purpose, database_id, tenant_id)
            .await?;
        // Attached before the tasks leave: RLS injection, caching, expansion,
        // and dispatch all read the plan after this point.
        if let Some(clause) = &returning {
            attach_returning_spec(&mut tasks, &clause.spec)?;
        }
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
        self.plan_sql_with_rls_returning(sql, tenant_id, database_id, sec, None)
            .await
    }

    /// Plan SQL with RLS injection, announcing a DML `RETURNING` clause.
    ///
    /// `returning_items` is the item text `strip_returning` split off the
    /// statement, so the write announces the columns it projects. `None` for
    /// a statement that carries no clause.
    pub async fn plan_sql_with_rls_returning(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
        returning_items: Option<&str>,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
    )> {
        self.plan_sql_with_rls_and_versions(sql, tenant_id, database_id, sec, returning_items)
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
        returning_items: Option<&str>,
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
            returning_items,
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
            None,
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
        returning_items: Option<&str>,
        purpose: PlanningPurpose,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        nodedb_sql::types::PlanCacheEligibility,
    )> {
        let (mut tasks, output_schema, mut version_set, cache_eligibility) = self
            .plan_with_nodedb_sql_for_purpose(sql, tenant_id, database_id, purpose, returning_items)
            .await?;
        self.apply_task_policy(&mut tasks, sec, tenant_id, &mut version_set)
            .await?;
        let cache_eligibility = self
            .pin_statement_cut(&mut tasks, purpose, cache_eligibility)
            .await?;
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
        returning_items: Option<&str>,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
    )> {
        self.plan_sql_with_params_and_rls_and_versions(
            sql,
            params,
            tenant_id,
            database_id,
            sec,
            returning_items,
        )
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
        returning_items: Option<&str>,
    ) -> crate::Result<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        OutputSchema,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
    )> {
        let inputs = &self.catalog_inputs;
        // Fresh adapter per plan call: same rationale as
        // `plan_with_nodedb_sql_for_purpose`. Its recorded version set is returned to the
        // caller so parameterized plans participate in descriptor admission.
        // `nextval` records into the calling session's map and `currval` reads
        // only from it, so the adapter must know which session is planning.
        // `Arc` because the converters evaluate column DEFAULTs that read the
        // catalog after planning returns.
        let catalog = Arc::new(
            inputs
                .build_adapter(tenant_id.as_u64(), database_id)
                .with_session_sequences(self.session_sequences()),
        );
        let raw_plans = nodedb_sql::plan_sql_with_params(sql, params, catalog.as_ref())
            .map_err(|error| map_plan_error(error, tenant_id))?;
        let plans: Vec<_> = raw_plans
            .into_iter()
            .map(|p| {
                nodedb_sql::planner::catalog_fold::fold_catalog_exprs_in_plan(
                    p,
                    catalog.as_ref(),
                    database_id,
                    tenant_id.as_u64(),
                )
            })
            .collect::<nodedb_sql::Result<_>>()
            .map_err(|error| map_plan_error(error, tenant_id))?;
        let (output_schema, returning) = resolve_returning_and_output_schema(
            &plans,
            returning_items,
            catalog.as_ref(),
            database_id,
            tenant_id,
        )?;
        let mut tasks = self
            .convert_prefetched(&plans, PlanningPurpose::Execute, database_id, tenant_id)
            .await?;
        if let Some(clause) = &returning {
            attach_returning_spec(&mut tasks, &clause.spec)?;
        }
        let mut version_set = catalog.take_recorded_versions();
        self.apply_task_policy(&mut tasks, sec, tenant_id, &mut version_set)
            .await?;
        // A parameterized plan never enters a plan cache, so its verdict
        // is not kept.
        self.pin_statement_cut(
            &mut tasks,
            PlanningPurpose::Execute,
            nodedb_sql::types::PlanCacheEligibility::DataDependent,
        )
        .await?;
        Ok((tasks, output_schema, version_set))
    }
}
