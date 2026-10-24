// SPDX-License-Identifier: BUSL-1.1

//! Planning of `SqlPlan`s a caller builds against the catalog, and the policy
//! pass every planned task set takes before it leaves the planner.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_sql::catalog::{ArrayCatalogView, SqlCatalog};
use nodedb_sql::engine_rules::{BulkLoadParams, BulkLoadPlan};
use nodedb_sql::types::SqlPlan;

use super::QueryContext;
use super::planning::resolve_returning_and_output_schema;
use crate::control::planner::context::security::PlanSecurityContext;
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::planner::plan_error_map::map_plan_error;
use crate::control::planner::sql_plan_convert::PlanningPurpose;
use crate::control::server::response_shape::schema::OutputSchema;

/// How a bulk load's target loads rows, answered by its engine rules.
#[derive(Debug)]
pub enum BulkLoadRoute {
    /// Each row loads as one `INSERT` statement.
    RowInserts,
    /// The rows decode against this array schema and load in cell chunks.
    ArrayCells(Box<ArrayCatalogView>),
}

impl QueryContext {
    /// Ask `collection`'s engine rules how a bulk load into it loads rows.
    ///
    /// The error is the planner's own, so a caller can render an engine's
    /// refusal (timeseries, spatial) with its exact text.
    pub fn bulk_load_route(
        &self,
        collection: &str,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
    ) -> nodedb_sql::Result<BulkLoadRoute> {
        let catalog = self
            .catalog_inputs
            .build_adapter(tenant_id.as_u64(), database_id);
        let params = BulkLoadParams {
            collection: collection.to_string(),
            cells: Vec::new(),
        };
        match nodedb_sql::planner::dml::plan_bulk_load(params, &catalog)? {
            BulkLoadPlan::RowInserts => Ok(BulkLoadRoute::RowInserts),
            BulkLoadPlan::CellChunks(_) => catalog
                .lookup_array(collection)
                .map(|view| BulkLoadRoute::ArrayCells(Box::new(view)))
                .ok_or_else(|| nodedb_sql::SqlError::Unsupported {
                    detail: format!("COPY: array '{collection}' was dropped"),
                }),
        }
    }

    /// Plan the `SqlPlan`s that `build` returns, then convert and secure them.
    ///
    /// `build` runs against a fresh catalog adapter, the one SQL planning
    /// uses. A bulk load builds its plans this way: it has rows, not SQL
    /// text. The task set takes the same policy pass as planned SQL: row
    /// filters, redaction refusals and the permission tree. Returns the
    /// tasks, the output schema, and the descriptor versions the adapter
    /// recorded, for lease admission.
    pub async fn plan_built_with_rls_and_versions<B>(
        &self,
        build: B,
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
    ) -> crate::Result<(Vec<PhysicalTask>, OutputSchema, DescriptorVersionSet)>
    where
        B: FnOnce(&dyn SqlCatalog) -> nodedb_sql::Result<Vec<SqlPlan>> + Send,
    {
        let catalog = self
            .catalog_inputs
            .build_adapter(tenant_id.as_u64(), database_id)
            .with_session_sequences(self.session_sequences());
        let plans = build(&catalog).map_err(|error| map_plan_error(error, tenant_id))?;
        let (output_schema, _) =
            resolve_returning_and_output_schema(&plans, None, &catalog, database_id, tenant_id)?;
        let mut tasks = self
            .convert_prefetched(&plans, PlanningPurpose::Execute, database_id, tenant_id)
            .await?;
        let mut version_set = catalog.take_recorded_versions();
        self.apply_task_policy(&mut tasks, sec, tenant_id, &mut version_set)
            .await?;
        // A built plan never enters a plan cache, so its verdict is not kept.
        self.pin_statement_cut(
            &mut tasks,
            PlanningPurpose::Execute,
            nodedb_sql::types::PlanCacheEligibility::DataDependent,
        )
        .await?;
        Ok((tasks, output_schema, version_set))
    }

    /// Inject row filters and permission-tree filters into `tasks`, and
    /// refuse a task that column redaction cannot cover. Records the policy
    /// versions the pass read into `version_set`.
    ///
    /// The versions are read BEFORE injection, never after. Injection reads
    /// live policy and grant state under its own lock. With a mutation racing
    /// in between, a post-injection read stamps a version newer than the one
    /// filtered against, and a plan built from stale state then compares as
    /// fresh forever. A pre-injection read only under-states freshness,
    /// which costs one harmless replan.
    ///
    /// The permission tree is read here, after planning's last await, so no
    /// lock is held across a request to a surrogate's collection home.
    pub(super) async fn apply_task_policy(
        &self,
        tasks: &mut [PhysicalTask],
        sec: &PlanSecurityContext<'_>,
        tenant_id: crate::types::TenantId,
        version_set: &mut DescriptorVersionSet,
    ) -> crate::Result<()> {
        let permission_view = sec.permission_tree.read().await;
        let permission_cache = permission_view.as_deref();
        let permission_tree_version = permission_cache
            .map(|c| c.tenant_version(tenant_id.as_u64()))
            .unwrap_or(0);
        let rls_version = sec.rls_store.tenant_version(tenant_id.as_u64());
        let redaction_version = sec.redaction_store.version();

        // Refuse a read of a redacted column outside the projection before
        // row-level security adds its own predicates: a read policy's
        // predicate is the administrator's, not the requester's.
        crate::control::planner::redaction_refusal::refuse_redacted_expression_reads_in_tasks(
            tasks,
            &crate::control::planner::redaction_refusal::RedactionReadCheck {
                auth: sec.auth,
                store: sec.redaction_store,
                catalog: self.catalog_inputs.credentials.catalog(),
            },
        )?;

        crate::control::planner::rls_injection::inject_rls(
            tasks,
            sec.rls_store,
            self.catalog_inputs.credentials.catalog(),
            sec.auth,
        )?;

        // Refuse what column redaction cannot cover (aggregates over a
        // redacted column, graph traversals), before anything is dispatched.
        crate::control::planner::redaction_refusal::refuse_unredactable_tasks(
            tasks,
            sec.auth,
            sec.redaction_store,
        )?;

        // Inject permission tree filters (hierarchical ACL).
        if let Some(cache) = permission_cache {
            crate::control::planner::rls_injection::inject_permission_tree(tasks, cache, sec.auth)?;
        }

        version_set.set_permission_tree_version(permission_tree_version);
        version_set.set_rls_version(rls_version);
        version_set.set_redaction_version(redaction_version);
        Ok(())
    }
}
