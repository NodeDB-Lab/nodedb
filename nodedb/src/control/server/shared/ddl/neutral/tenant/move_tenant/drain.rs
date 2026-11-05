// SPDX-License-Identifier: BUSL-1.1

//! Drain phase for `MOVE TENANT`.
//!
//! Every live collection and every array of the source database goes through
//! the replicated descriptor-lease drain. While it is active, every node's planner refuses a
//! new plan on a drained collection as a retryable schema change. The phase
//! returns once no node holds a lease on any of them, so no write to a moving
//! collection is in flight when the capture runs, and none starts after.
//!
//! The drain is owned by this move ([`move_tenant_drain_owner`]) and covers
//! every descriptor version, so a DDL on a moving collection neither ends it
//! nor escapes it by bumping the version. It stays active until the cutover's
//! catalog entry applies, which ends it on every node, or until [`release`]
//! ends it.

use std::time::{Duration, Instant};

use crate::control::lease::{
    drain_for_owner_async, end_drain_async, move_source_descriptor, move_tenant_drain_owner,
};
use crate::control::security::audit::AuditEvent;
use crate::control::security::sessions::KillReason;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};
use nodedb_cluster::DescriptorId;
use nodedb_types::NodeDbError;

/// Run the drain phase.
///
/// A drain that does not finish within `timeout`, or a catalog read or
/// propose error, ends every drain this phase started and fails with the
/// retryable drain-timeout error. The source is left untouched.
pub async fn run(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    timeout: Duration,
) -> Result<(), NodeDbError> {
    let timed_out = || {
        NodeDbError::move_tenant_drain_timeout(
            tenant_id.as_u64().to_string(),
            source_db_id.as_u64().to_string(),
        )
    };

    let killed = state
        .session_registry
        .kill_sessions_for_tenant(tenant_id.as_u64(), KillReason::AdminKill);
    state.audit_record(
        AuditEvent::SessionRevoked,
        Some(tenant_id),
        "move_tenant",
        &format!(
            "tenant_id={} source_db={} sessions_killed={killed}",
            tenant_id.as_u64(),
            source_db_id.as_u64()
        ),
    );

    let descriptors = source_descriptors(state, source_db_id)
        .map_err(|e| timed_out().with_cause(crate::error_classify::classify(&e)))?;
    let owner = move_tenant_drain_owner(tenant_id.as_u64(), source_db_id.as_u64());
    let deadline = Instant::now() + timeout;
    for id in descriptors {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Err(e) = drain_for_owner_async(state, id, owner.clone(), remaining).await {
            release(state, tenant_id, source_db_id).await;
            return Err(timed_out().with_cause(crate::error_classify::classify(&e)));
        }
    }
    Ok(())
}

/// End the drain on every live collection of the source database.
///
/// Called on every path that fails before the cutover's catalog entry
/// applies, so the source collections are still in the catalog. An error
/// leaves that drain active, and plans on its collection keep failing as
/// retryable until a later `release` or a restart.
pub async fn release(state: &SharedState, tenant_id: TenantId, source_db_id: DatabaseId) {
    let descriptors = match source_descriptors(state, source_db_id) {
        Ok(descriptors) => descriptors,
        Err(error) => {
            tracing::warn!(
                tenant = tenant_id.as_u64(),
                source_db = source_db_id.as_u64(),
                %error,
                "MOVE TENANT drain release: cannot read the source collections"
            );
            return;
        }
    };
    let owner = move_tenant_drain_owner(tenant_id.as_u64(), source_db_id.as_u64());
    for id in descriptors {
        if let Err(error) = end_drain_async(state, id.clone(), owner.clone()).await {
            tracing::warn!(
                tenant = tenant_id.as_u64(),
                descriptor = ?id,
                %error,
                "MOVE TENANT drain release: the drain end did not apply"
            );
        }
    }
}

/// Each live source collection's descriptor, then each source array's.
fn source_descriptors(
    state: &SharedState,
    source_db_id: DatabaseId,
) -> crate::Result<Vec<DescriptorId>> {
    let catalog = state.credentials.catalog();
    let mut descriptors: Vec<DescriptorId> = catalog
        .load_all_collections(source_db_id)?
        .into_iter()
        .filter(|c| c.is_active)
        .map(|c| move_source_descriptor(source_db_id.as_u64(), c.tenant_id, &c.name))
        .collect();
    descriptors.extend(super::arrays::source_descriptors(catalog, source_db_id)?);
    Ok(descriptors)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
        }
    }

    /// The tenant's sessions end. A user whose id equals the tenant's number keeps
    /// its session, and the user-keyed bus carries no event for that number.
    #[tokio::test]
    async fn the_drain_phase_ends_the_moved_tenants_sessions_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"));
        let (dispatcher, _) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        let mut si_rx = state.credentials.subscribe_session_invalidation();
        let moved = state
            .session_registry
            .register("moved", &session_in_tenant(5, 7))
            .expect("register");
        let other = state
            .session_registry
            .register("other", &session_in_tenant(7, 8))
            .expect("register");

        run(
            &state,
            TenantId::new(7),
            DatabaseId::new(70),
            Duration::from_secs(1),
        )
        .await
        .expect("a source database with no collections drains at once");

        assert_eq!(*moved.borrow(), KillReason::AdminKill);
        assert_eq!(*other.borrow(), KillReason::Alive);
        assert!(si_rx.try_recv().is_err());
    }
}
