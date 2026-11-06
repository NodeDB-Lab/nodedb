// SPDX-License-Identifier: BUSL-1.1

//! Boot load of the in-memory registries the metadata applier's post-apply
//! side effects write.
//!
//! The metadata group restarts above its durable applied floor, so no entry
//! at or below the floor applies again. Every registry a post-apply side
//! effect writes is rebuilt here from the catalog rows that entry wrote.
//! Production boot and every constructor that resumes a durable catalog call
//! this one loader, so both rebuild the same state.

use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::blacklist::store::BlacklistStore;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::permission::PermissionStore;
use crate::control::security::redaction::RedactionStore;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::security::role::RoleStore;
use crate::control::state::SharedState;
use crate::control::trigger::TriggerRegistry;

/// The catalog-backed registries boot rebuilds.
pub(super) struct CatalogRegistries<'a> {
    pub(super) api_keys: &'a ApiKeyStore,
    pub(super) roles: &'a RoleStore,
    pub(super) permissions: &'a PermissionStore,
    pub(super) blacklist: &'a BlacklistStore,
    pub(super) trigger_registry: &'a TriggerRegistry,
    pub(super) stream_registry: &'a crate::event::cdc::StreamRegistry,
    pub(super) group_registry: &'a crate::event::cdc::GroupRegistry,
    pub(super) schedule_registry: &'a crate::event::scheduler::ScheduleRegistry,
    pub(super) synonym_registry: &'a crate::control::synonym::SynonymRegistry,
    pub(super) custom_type_registry: &'a crate::control::custom_type::CustomTypeRegistry,
    pub(super) retention_policy_registry:
        &'a crate::engine::timeseries::retention_policy::RetentionPolicyRegistry,
    pub(super) alert_registry: &'a crate::event::alert::AlertRegistry,
    pub(super) ep_topic_registry: &'a crate::event::topic::EpTopicRegistry,
    pub(super) mv_registry: &'a crate::event::streaming_mv::MvRegistry,
    pub(super) sequence_registry: &'a crate::control::sequence::SequenceRegistry,
    pub(super) rls_store: &'a RlsPolicyStore,
    pub(super) redaction_store: &'a RedactionStore,
}

impl CatalogRegistries<'_> {
    /// Load every registry from `catalog`. The security stores and the topic
    /// registry fail boot on a read error. Every other registry logs it and
    /// starts empty.
    pub(super) fn load(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        self.api_keys.load_from(catalog)?;
        self.roles.load_from(catalog)?;
        self.permissions.load_from(catalog)?;
        self.blacklist.load_from(catalog)?;
        self.trigger_registry.load_all(catalog);
        self.stream_registry.load_from_catalog(catalog);
        self.group_registry.load_from_catalog(catalog);
        for stream in self.stream_registry.list_all() {
            crate::event::cdc::sink_owner::register_sink_groups(self.group_registry, &stream);
        }
        self.schedule_registry.load_from_catalog(catalog);
        if let Err(e) = self.synonym_registry.reload_from_catalog(catalog) {
            tracing::warn!(error = %e, "boot: failed to load synonym groups from catalog");
        }
        if let Err(e) = self.custom_type_registry.reload_from_catalog(catalog) {
            tracing::warn!(error = %e, "boot: failed to load custom types from catalog");
        }
        if let Ok(rp_defs) = catalog.load_all_retention_policies() {
            self.retention_policy_registry.load(rp_defs);
        }
        self.alert_registry.load_from_catalog(catalog);
        self.ep_topic_registry.load_from_catalog(catalog)?;
        self.mv_registry.load_from_catalog(catalog);
        self.sequence_registry.load_from_catalog(catalog);
        self.load_rls(catalog);
        self.load_redaction(catalog);
        Ok(())
    }

    /// Install every stored RLS policy. A row that cannot be compiled against
    /// its collection installs as a restrictive deny-all
    /// (`StoredRlsPolicy::rehydrate`) and is never skipped.
    fn load_rls(&self, catalog: &SystemCatalog) {
        match catalog.load_all_rls_policies() {
            Ok(stored) => {
                for s in &stored {
                    self.rls_store
                        .install_replicated_policy(s.rehydrate(catalog));
                }
                if !stored.is_empty() {
                    tracing::info!(
                        rls_policies = stored.len(),
                        "loaded RLS policies from catalog"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to load RLS policies"),
        }
    }

    /// Install every stored redaction policy that converts to a runtime
    /// policy, and log each one that does not.
    fn load_redaction(&self, catalog: &SystemCatalog) {
        match catalog.load_all_redaction_policies() {
            Ok(stored) => {
                let mut loaded = 0usize;
                for s in &stored {
                    match s.to_runtime() {
                        Ok(p) => {
                            self.redaction_store.install_replicated_policy(p);
                            loaded += 1;
                        }
                        Err(e) => {
                            tracing::warn!(
                                name = %s.name,
                                collection = %s.collection,
                                error = %e,
                                "boot replay: skipped invalid redaction policy"
                            );
                        }
                    }
                }
                if loaded > 0 {
                    tracing::info!(
                        redaction_policies = loaded,
                        "loaded redaction policies from catalog"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to load redaction policies"),
        }
    }
}

impl SharedState {
    /// Rebuild the host registries, the caches, the array catalog, and the
    /// permission-tree definitions from this state's catalog, as production
    /// boot does. A constructor that resumes a durable catalog calls it
    /// once, on registries that start empty. The trigger registry appends on
    /// load, so a second call duplicates every trigger.
    ///
    /// Production boot fills the array catalog in
    /// `bootstrap::data_plane::load_array_catalog` and the tree definitions
    /// in `auth_fence::cache_from_catalog`. Both run the same loaders as
    /// this function: `array_catalog::persist::register_loaded` and
    /// `auth_fence::load_tree_defs`.
    pub(crate) fn load_catalog_host_state(&self) -> crate::Result<()> {
        let catalog = self.credentials.catalog();
        CatalogRegistries {
            api_keys: &self.api_keys,
            roles: &self.roles,
            permissions: &self.permissions,
            blacklist: &self.blacklist,
            trigger_registry: &self.trigger_registry,
            stream_registry: &self.stream_registry,
            group_registry: &self.group_registry,
            schedule_registry: &self.schedule_registry,
            synonym_registry: &self.synonym_registry,
            custom_type_registry: &self.custom_type_registry,
            retention_policy_registry: &self.retention_policy_registry,
            alert_registry: &self.alert_registry,
            ep_topic_registry: &self.ep_topic_registry,
            mv_registry: &self.mv_registry,
            sequence_registry: &self.sequence_registry,
            rls_store: &self.rls,
            redaction_store: &self.redaction,
        }
        .load(catalog)?;
        load_catalog_caches(self, catalog);
        self.mirror.load_from_catalog(catalog)?;
        crate::control::array_catalog::persist::register_loaded(
            &mut self
                .array_catalog
                .write()
                .unwrap_or_else(|p| p.into_inner()),
            catalog.load_all_arrays(),
        )?;
        // The authorization fence holds this cache's source index, so the
        // definitions load into the cache in place. Nothing else holds the
        // lock while a constructor runs.
        let mut cache = self
            .permission_cache
            .try_write()
            .map_err(|_| crate::Error::Internal {
                detail: "permission cache is locked while the state is still being built".into(),
            })?;
        crate::control::security::auth_fence::load_tree_defs(&mut cache, catalog)?;
        Ok(())
    }
}

/// Populate the per-database DML audit cache, the collection-to-database
/// map, and the idle-timeout cache from `catalog`. A read error logs and
/// leaves that cache empty.
pub(super) fn load_catalog_caches(state: &SharedState, catalog: &SystemCatalog) {
    if let Err(e) = state.audit_dml_cache.load_from_catalog(catalog) {
        tracing::warn!(error = %e, "boot: failed to populate audit_dml_cache from catalog");
    }
    if let Err(e) = state.collection_to_database.load_from_catalog(catalog) {
        tracing::warn!(
            error = %e,
            "boot: failed to populate collection_to_database cache from catalog"
        );
    }
    if let Err(e) = state.idle_timeout_cache.load_from_catalog(catalog) {
        tracing::warn!(
            error = %e,
            "boot: failed to populate idle_timeout_cache from catalog"
        );
    }
}
