// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use crate::control::security::credential::CredentialStore;

/// Inputs needed to construct an `OriginCatalog` per plan call.
///
/// Tenant is intentionally **not** stored here: every plan call passes the
/// effective tenant to `build_adapter`, so a single `QueryContext` shared
/// across a pgwire handler can serve queries from connections belonging to
/// different tenants without cross-tenant catalog resolution.
#[derive(Clone)]
pub(super) struct CatalogInputs {
    pub(super) credentials: Arc<CredentialStore>,
    /// The node's array catalog, which every adapter resolves array names
    /// against.
    pub(super) array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    pub(super) shared: Option<std::sync::Weak<crate::control::state::SharedState>>,
    pub(super) retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
    /// The node-wide sequence counters a column DEFAULT that calls `nextval`
    /// draws from. A context built with a weak `shared` reads them from
    /// there instead.
    pub(super) sequence_registry: Option<Arc<crate::control::sequence::SequenceRegistry>>,
}

impl CatalogInputs {
    /// The node-wide sequence counters these inputs plan against, if any.
    pub(super) fn sequence_registry(
        &self,
    ) -> Option<Arc<crate::control::sequence::SequenceRegistry>> {
        match self.shared.as_ref().and_then(|weak| weak.upgrade()) {
            Some(shared) => Some(Arc::clone(&shared.sequence_registry)),
            None => self.sequence_registry.clone(),
        }
    }

    pub(super) fn build_adapter(
        &self,
        tenant_id: u64,
        database_id: crate::types::DatabaseId,
    ) -> super::super::catalog_adapter::OriginCatalog {
        if let Some(weak) = &self.shared
            && let Some(shared) = weak.upgrade()
        {
            super::super::catalog_adapter::OriginCatalog::new_with_lease(
                &shared,
                tenant_id,
                database_id,
                self.retention_policy_registry.clone(),
            )
        } else {
            let catalog = super::super::catalog_adapter::OriginCatalog::new(
                Arc::clone(&self.credentials),
                self.array_catalog.clone(),
                tenant_id,
                database_id,
                self.retention_policy_registry.clone(),
            );
            match &self.sequence_registry {
                Some(registry) => catalog.with_sequence_registry(Arc::clone(registry)),
                None => catalog,
            }
        }
    }
}
