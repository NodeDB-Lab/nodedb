// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for tenant `CatalogEntry` variants.
//!
//! `PutTenant` seeds the in-memory `TenantStore` with default quota
//! so reads work immediately on every node. The quota record itself is not
//! part of `StoredTenant`; it replicates as `PutTenantQuota`.
//!
//! `DeleteTenant` removes the in-memory quota entry and releases the
//! tenant's caps in every database. Tenant data is left in place —
//! operators must call `PURGE TENANT <id> CONFIRM` separately if they
//! want it gone.

use std::sync::Arc;

use crate::control::security::catalog::{StoredCollection, StoredTenant, StoredUser};
use crate::control::security::sessions::KillReason;
use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;
use crate::types::TenantId;

pub fn put(stored: StoredTenant, shared: Arc<SharedState>) {
    let tid = TenantId::new(stored.tenant_id);
    let mut tenants = match shared.tenants.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    // Only seed default quota if the tenant has no quota yet — never
    // clobber an `ALTER TENANT SET QUOTA` value the operator already set.
    if !tenants.has_quota(tid) {
        tenants.set_quota(tid, TenantQuota::default());
    }
    tracing::debug!(
        tenant = stored.tenant_id,
        name = %stored.name,
        "post_apply: tenant identity replicated"
    );
}

pub fn put_with_admin(tenant: StoredTenant, admin: StoredUser, shared: Arc<SharedState>) {
    shared.credentials.install_replicated_user(&admin, None);
    put(tenant, shared);
}

pub fn delete(tenant_id: u64, shared: Arc<SharedState>) {
    let tid = TenantId::new(tenant_id);
    {
        let mut tenants = match shared.tenants.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        tenants.remove_quota(tid);
    }
    super::quota::release_tenant_scope(tid, &shared);
    tracing::debug!(tenant = tenant_id, "post_apply: tenant identity removed");
}

/// Synchronous in-memory side effects for `MoveTenantCutover`.
///
/// Ends the tenant's sessions on this node, so every node's sessions of the
/// moved tenant stop running against the source database. A session stops at its
/// next boundary. The session that issued the move, if the tenant holds it,
/// receives the result of its statement first.
///
/// The gateway plan cache is invalidated unconditionally by
/// `invalidate_gateway_cache_for_entry`.
pub fn move_cutover_sync(
    tenant_id: u64,
    source_db_id: u64,
    target_db_id: u64,
    collections: &[StoredCollection],
    shared: Arc<SharedState>,
) {
    // The kill is a watch send. The audit row of the move stays on the issuing
    // node, so this path does no audit sync on the applier.
    let sessions_killed = shared
        .session_registry
        .kill_sessions_for_tenant(tenant_id, KillReason::AdminKill);
    tracing::info!(
        tenant = tenant_id,
        source_db = source_db_id,
        target_db = target_db_id,
        collection_count = collections.len(),
        sessions_killed,
        "post_apply: move_tenant_cutover applied"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::sessions::SessionParams;
    use crate::wal::WalManager;

    fn session_in_tenant(user_id: u64, tenant_id: u64) -> SessionParams {
        SessionParams {
            user_id,
            username: format!("user{user_id}"),
            db_user: format!("user{user_id}"),
            peer_addr: "127.0.0.1:5555".to_string(),
            protocol: "native".to_string(),
            auth_method: "password".to_string(),
            tenant_id,
            credential_version: 0,
            current_database: None,
            token_expiry_ms: None,
            api_key_id: None,
        }
    }

    /// This post-apply runs on every node, so the kill reaches every node's
    /// sessions. A user whose id equals the tenant's number keeps its session.
    #[test]
    fn the_cutover_ends_the_moved_tenants_sessions_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("t.wal")).expect("wal"));
        let (dispatcher, _) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        let moved = state
            .session_registry
            .register("moved", &session_in_tenant(5, 7))
            .expect("register");
        let other = state
            .session_registry
            .register("other", &session_in_tenant(7, 8))
            .expect("register");

        move_cutover_sync(7, 70, 71, &[], Arc::clone(&state));

        assert_eq!(*moved.borrow(), KillReason::AdminKill);
        assert_eq!(*other.borrow(), KillReason::Alive);
    }
}
