// SPDX-License-Identifier: BUSL-1.1

//! SQL-text gateway execution with descriptor-version-aware plan caching.

use std::sync::Arc;

use tracing::debug;

use crate::Error;
use crate::control::server::shared::clone_write::CloneCheckedTask;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::core::{Gateway, QueryContext, authorized_plan_for_context};
use super::lowered_plan::LoweredPlan;
use super::plan_cache::{PlanCacheKey, SqlKey, hash_placeholder_types, hash_sql};
use super::version_set::{permission_tree_version_key, rls_version_key};

impl Gateway {
    /// Execute SQL through the two-phase, descriptor-version-aware plan cache.
    ///
    /// `plan_fn` runs at most once on a cache miss. `authorize_fn` clone-checks
    /// and authorizes the exact plan — the cached plan or the newly planned
    /// value — the same way every other dispatch entry point does.
    ///
    /// `plan_fn` returns the plan together with its
    /// [`nodedb_sql::types::PlanCacheEligibility`] verdict. A `DataDependent`
    /// plan enters neither the plan cache nor the version-set side cache, so
    /// the next execution re-plans and re-evaluates its volatile calls.
    pub async fn execute_sql<Authorize, AuthorizeFut>(
        &self,
        ctx: &QueryContext,
        sql: &str,
        placeholder_types: &[&str],
        plan_fn: impl FnOnce() -> Result<LoweredPlan, Error>,
        authorize_fn: Authorize,
    ) -> Result<Vec<Vec<u8>>, Error>
    where
        Authorize: Fn(PhysicalPlan) -> AuthorizeFut,
        AuthorizeFut: std::future::Future<Output = Result<CloneCheckedTask, Error>>,
    {
        let sql_hash = hash_sql(sql);
        let ph_hash = hash_placeholder_types(placeholder_types);
        let sql_key = SqlKey {
            sql_text_hash: sql_hash,
            placeholder_types_hash: ph_hash,
        };

        // Recover the prior version set, verify it against current catalog
        // descriptors, then use the complete cache key only while current.
        if let Some(stored_vs) = self.plan_cache.lookup_version_set(&sql_key) {
            let shared = self.shared()?;
            let catalog = shared.credentials.catalog();
            let tenant_id = ctx.tenant_id.as_u64();
            // Read before reverify runs, not derived per-name inside it: both
            // pseudo-entries share one live tenant snapshot the same way the
            // real collection entries share one catalog snapshot.
            let permission_tree_version = shared
                .permission_cache
                .read()
                .await
                .tenant_version(tenant_id);
            let rls_version = shared.rls.tenant_version(tenant_id);
            let ptree_key = permission_tree_version_key(tenant_id);
            let rls_key = rls_version_key(tenant_id);
            let current_vs = stored_vs.reverify(|name| {
                if name == ptree_key.as_str() {
                    return permission_tree_version;
                }
                if name == rls_key.as_str() {
                    return rls_version;
                }
                catalog
                    .get_collection(ctx.database_id, tenant_id, name)
                    .ok()
                    .flatten()
                    .map(|collection| collection.descriptor_version.max(1))
                    .unwrap_or(0)
            });
            if current_vs == stored_vs {
                let full_key = PlanCacheKey {
                    sql_text_hash: sql_hash,
                    placeholder_types_hash: ph_hash,
                    version_set: stored_vs.clone(),
                };
                if let Some(cached_plan) = self.plan_cache.get(&full_key) {
                    debug!(sql = %sql, "gateway: plan cache hit (two-phase)");
                    let checked = authorize_fn(cached_plan.as_ref().clone()).await?;
                    let plan = authorized_plan_for_context(ctx, checked)?;
                    return self
                        .execute_with_version_set(ctx, plan, stored_vs)
                        .await
                        .map(|(payloads, _watermarks, _read_version)| payloads);
                }
            }
        }

        let lowered = plan_fn()?;
        let cacheable = lowered.is_cacheable();
        let plan = lowered.plan;
        let actual_vs = self
            .collect_version_set(&plan, ctx.tenant_id.as_u64(), ctx.database_id)
            .await?;

        // A volatile plan froze its `nextval` / `now()` / UUID values while it
        // was built. Admitting it would replay this execution's values into
        // every later one, so it skips both caches and re-plans next time.
        // The side cache is skipped too: its entry is pruned only alongside the
        // plan entry it maps to, so storing one without a plan leaves an orphan
        // that survives every DDL invalidation and buys no hit.
        if cacheable {
            let actual_key = PlanCacheKey {
                sql_text_hash: sql_hash,
                placeholder_types_hash: ph_hash,
                version_set: actual_vs.clone(),
            };
            self.plan_cache
                .insert_version_set(sql_key, actual_vs.clone());
            self.plan_cache.insert(actual_key, Arc::new(plan.clone()));
        }

        let checked = authorize_fn(plan.clone()).await?;
        let plan = authorized_plan_for_context(ctx, checked)?;
        self.execute_with_version_set(ctx, plan, actual_vs)
            .await
            .map(|(payloads, _watermarks, _read_version)| payloads)
    }
}
