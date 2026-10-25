// SPDX-License-Identifier: BUSL-1.1

//! Rebuild every in-memory registry the replicated tables feed, after a
//! metadata image replaced them.
//!
//! Each registry is cleared and loaded from the catalog through the loader
//! boot uses, so the node holds exactly what a restart on the new catalog
//! loads.

use std::sync::Arc;
use std::time::Duration;

use crate::control::catalog_entry::post_apply::{quota, tenant};
use crate::control::cluster::metadata_applier::{seed_host_tables, seed_metadata_cache};
use crate::control::cluster::recovery_check::registry_verify::{
    alert, api_keys, change_stream, consumer_group, credential, materialized_view, permissions,
    redaction_policy, retention_policy, rls_policy, roles, schedule, triggers,
};
use crate::control::state::SharedState;
use crate::control::surrogate::{SurrogateRegistry, SurrogateRegistryMode};

use super::inventory::{Inventory, now_ms};

/// Registries owned by the Raft wiring rather than by `SharedState`.
pub struct RaftOwnedState {
    /// The join-token mirror the metadata applier and the bootstrap listener
    /// share.
    pub token_state: nodedb_cluster::SharedTokenStateMirror,
    /// The transport whose pre-authorized enrollment identities mirror the
    /// catalog rows.
    pub transport: Option<Arc<nodedb_cluster::NexarTransport>>,
}

/// Rebuild every catalog-derived registry. `before` is the inventory read
/// before the replace: objects it holds and the catalog no longer does are
/// removed from the registries that keep them outside the catalog.
pub(super) async fn reload_registries(
    shared: &Arc<SharedState>,
    raft: &RaftOwnedState,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();

    // Auth and tenancy.
    credential::repair_credentials(&shared.credentials, catalog)?;
    api_keys::repair_api_keys(&shared.api_keys, catalog)?;
    roles::repair_roles(&shared.roles, catalog)?;
    permissions::repair_permissions(&shared.permissions, catalog)?;
    rls_policy::repair_rls_policies(&shared.rls, catalog)?;
    redaction_policy::repair_redaction_policies(&shared.redaction, catalog)?;
    shared.auth_users.reload_from_catalog(catalog)?;
    shared.scope_grants.reload_from_catalog(catalog)?;
    shared.quota_manager.load_from(catalog)?;
    reload_tenants(shared, before, after);

    // DDL objects.
    triggers::repair_triggers(&shared.trigger_registry, catalog)?;
    shared
        .sequence_registry
        .reload_from_catalog(catalog, |database_id, tenant_id, name| {
            let key = (database_id, tenant_id, name.to_string());
            before
                .sequences
                .get(&key)
                .is_some_and(|was| after.sequences.get(&key) == Some(was))
        })?;
    shared.synonym_registry.reload_from_catalog(catalog)?;
    shared.custom_type_registry.reload_from_catalog(catalog)?;
    catalog.reload_custom_crs_set()?;
    shared.block_cache.clear();

    // Event Plane definitions.
    schedule::repair_schedules(&shared.schedule_registry, catalog)?;
    alert::repair_alerts(&shared.alert_registry, catalog)?;
    materialized_view::repair_mvs(&shared.mv_registry, catalog)?;
    change_stream::repair_change_streams(&shared.stream_registry, catalog)?;
    consumer_group::repair_consumer_groups(&shared.group_registry, catalog)?;
    retention_policy::repair_retention_policies(&shared.retention_policy_registry, catalog)?;
    shared.ep_topic_registry.load_from_catalog(catalog)?;

    // Caches keyed by database or collection.
    shared.audit_dml_cache.load_from_catalog(catalog)?;
    shared.collection_to_database.load_from_catalog(catalog)?;
    shared.idle_timeout_cache.load_from_catalog(catalog)?;

    // Replicated watermarks.
    shared.database_registry.restore_persisted(
        catalog.get_database_hwm()?,
        catalog.get_database_reserve_index()?,
    );
    reload_surrogate_registry(shared)?;
    if let Some(registry) = shared.producer_registry.as_deref() {
        registry.reload_from_catalog()?;
    }

    // Quota enforcement.
    for db in before.database_quotas.difference(&after.database_quotas) {
        quota::delete_database(*db, shared);
    }
    for (db, tenant_id) in before.tenant_quotas.difference(&after.tenant_quotas) {
        quota::delete_tenant(*db, *tenant_id, shared);
    }
    crate::bootstrap::quota_replay::replay_quotas(shared);

    // Metadata-group host state.
    seed_host_tables(shared)?;
    seed_metadata_cache(&shared.metadata_cache, catalog)?;

    // Join tokens and enrollment identities.
    let tokens = catalog
        .list_join_token_states()?
        .into_iter()
        .map(|state| (state.token_hash, state))
        .collect();
    *raft.token_state.lock().unwrap_or_else(|p| p.into_inner()) = tokens;
    if let Some(transport) = raft.transport.as_ref() {
        reload_preauthorizations(transport, before, after);
    }

    // Authorization state last: it reads everything above.
    crate::control::security::permission_tree::reload::reload_all(shared, None).await?;
    shared.authorization_fence.note_snapshot_installed();
    Ok(())
}

/// Seed the default quota for every tenant in the catalog and drop the
/// quota of every tenant it no longer holds.
fn reload_tenants(shared: &Arc<SharedState>, before: &Inventory, after: &Inventory) {
    for tenant_id in before.tenants.difference(&after.tenants) {
        tenant::delete(*tenant_id, Arc::clone(shared));
    }
    let mut tenants = shared.tenants.lock().unwrap_or_else(|p| p.into_inner());
    for tenant_id in &after.tenants {
        let tid = crate::types::TenantId::new(*tenant_id);
        if !tenants.has_quota(tid) {
            tenants.set_quota(
                tid,
                crate::control::security::tenant::TenantQuota::default(),
            );
        }
    }
}

/// Reset the surrogate registry to the catalog's watermark and reserve
/// cursor, never below what this node already issued.
fn reload_surrogate_registry(shared: &SharedState) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    let persisted = catalog
        .get_surrogate_hwm()?
        .max(catalog.max_bound_surrogate()?.as_u32());
    let reserve_index = catalog.get_surrogate_reserve_index()?;
    let mut registry = shared
        .surrogate_registry
        .write()
        .unwrap_or_else(|p| p.into_inner());
    let floor = persisted.max(registry.current_hwm());
    let cluster = matches!(registry.mode(), SurrogateRegistryMode::Cluster(_));
    *registry = if cluster {
        SurrogateRegistry::from_persisted_cluster(floor, reserve_index)
    } else {
        SurrogateRegistry::from_persisted_hwm(floor)
    };
    Ok(())
}

/// Revoke the pre-authorizations the catalog dropped and install the ones
/// it holds.
fn reload_preauthorizations(
    transport: &nodedb_cluster::NexarTransport,
    before: &Inventory,
    after: &Inventory,
) {
    let now = now_ms();
    for (spki, expires_at_ms) in &before.preauthorizations {
        if !after.preauthorizations.contains_key(spki) {
            transport.revoke_peer_preauthorization(
                spki,
                Duration::from_millis(expires_at_ms.saturating_sub(now)),
            );
        }
    }
    for (spki, expires_at_ms) in &after.preauthorizations {
        let ttl = Duration::from_millis(expires_at_ms.saturating_sub(now));
        if !transport.preauthorize_peer_identity(*spki, ttl) {
            tracing::error!(
                ?spki,
                "enrollment preauthorization capacity exhausted during snapshot install; \
                 identity not admitted"
            );
        }
    }
}
