// SPDX-License-Identifier: BUSL-1.1

//! The version set a gateway plan is cached and checked under.

use crate::Error;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::core::Gateway;
use super::version_set::{
    GatewayVersionSet, collection_version, permission_tree_version_key, rls_version_key,
    with_arrays, with_continuous_aggregates,
};

impl Gateway {
    /// Collect the descriptor version set for a plan using the current catalog.
    ///
    /// `tenant_id` must match the authenticated tenant of the query so that
    /// the catalog key lookup (`"{tenant_id}:{collection_name}"`) finds the
    /// correct descriptor version. Using tenant 0 here returns version 0
    /// for every collection stored under any other tenant, causing spurious
    /// `DescriptorMismatch` rejections at the leader.
    ///
    /// `database_id` scopes the catalog lookup to the session's current database
    /// so that a plan from one database cannot be served under another.
    ///
    /// Also folds in the tenant's permission-tree / RLS versions as pseudo
    /// entries (see [`permission_tree_version_key`]).
    pub(super) async fn collect_version_set(
        &self,
        plan: &PhysicalPlan,
        tenant_id: u64,
        database_id: DatabaseId,
    ) -> Result<GatewayVersionSet, Error> {
        let shared = self.shared()?;
        let catalog = shared.credentials.catalog();

        let vs = GatewayVersionSet::from_plan(plan, |name| {
            collection_version(catalog, database_id, tenant_id, name)
        })?;

        let permission_tree_version = shared
            .permission_cache
            .read()
            .await
            .tenant_version(tenant_id);
        let rls_version = shared.rls.tenant_version(tenant_id);

        // A continuous aggregate read as a relation folds in its own catalog
        // version, so a cached plan over it stops matching once it changes.
        let vs = with_continuous_aggregates(vs, plan, catalog, database_id.as_u64(), tenant_id)?;
        // An array read folds in its own catalog version, so a cached plan
        // over it stops matching once the array is altered or dropped.
        let vs = with_arrays(
            vs,
            plan,
            &shared.array_catalog,
            database_id.as_u64(),
            tenant_id,
        );

        Ok(vs
            .with_extra(
                permission_tree_version_key(tenant_id),
                permission_tree_version,
            )
            .with_extra(rls_version_key(tenant_id), rls_version))
    }
}
