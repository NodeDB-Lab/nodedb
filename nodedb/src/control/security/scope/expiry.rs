//! Periodic scope grant expiry processor.
//!
//! Spawns a Tokio task that periodically checks for expired scope grants
//! and executes `ON EXPIRE` actions (automatic downgrade or hard revoke).
//! Runs on the Control Plane.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::grant::{ScopeGrantStore, ScopeStatus};

/// Spawn the periodic scope expiry check task.
///
/// Checks all grants every `interval_secs` (default: 60s) for:
/// 1. Grants that have entered grace period → log warning.
/// 2. Grants that are fully expired → execute `on_expire_action`.
pub fn spawn_expiry_task(
    grant_store: Arc<ScopeGrantStore>,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    let interval = Duration::from_secs(interval_secs.max(10)); // Minimum 10s.

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Skip first tick (fires immediately).
        ticker.tick().await;

        loop {
            ticker.tick().await;
            process_expired_grants(&grant_store);
        }
    })
}

/// A scope lifecycle event for CDC/webhook/audit.
#[derive(Debug, Clone)]
pub struct ScopeEvent {
    pub event_type: &'static str,
    pub scope_name: String,
    pub grantee_type: String,
    pub grantee_id: String,
    pub detail: String,
}

/// Process all expired and grace-period grants. Returns emitted events.
pub fn process_expired_grants_with_events(store: &ScopeGrantStore) -> Vec<ScopeEvent> {
    let mut events = Vec::new();
    process_expired_grants_inner(store, &mut events);
    events
}

/// Process all expired and grace-period grants.
fn process_expired_grants(store: &ScopeGrantStore) {
    let _ = process_expired_grants_with_events(store);
}

fn process_expired_grants_inner(store: &ScopeGrantStore, events: &mut Vec<ScopeEvent>) {
    let all_grants = store.list(None);
    let mut expired_count = 0u32;
    let mut grace_count = 0u32;

    for grant in &all_grants {
        if grant.expires_at == 0 {
            continue; // Permanent — skip.
        }

        match grant.status() {
            ScopeStatus::Grace => {
                grace_count += 1;
                info!(
                    scope = %grant.scope_name,
                    grantee = %grant.grantee_id,
                    grantee_type = %grant.grantee_type,
                    "scope grant in grace period"
                );
                events.push(ScopeEvent {
                    event_type: "scope.grace_entered",
                    scope_name: grant.scope_name.clone(),
                    grantee_type: grant.grantee_type.clone(),
                    grantee_id: grant.grantee_id.clone(),
                    detail: format!("expires_at={}", grant.expires_at),
                });
            }
            ScopeStatus::Expired => {
                expired_count += 1;
                events.push(ScopeEvent {
                    event_type: "scope.expired",
                    scope_name: grant.scope_name.clone(),
                    grantee_type: grant.grantee_type.clone(),
                    grantee_id: grant.grantee_id.clone(),
                    detail: grant.on_expire_action.clone(),
                });
                execute_on_expire(store, grant);
            }
            ScopeStatus::Active | ScopeStatus::None => {}
        }
    }

    if expired_count > 0 || grace_count > 0 {
        info!(
            expired = expired_count,
            grace = grace_count,
            "scope expiry check completed"
        );
    }
}

/// Execute the `on_expire_action` for a fully expired grant.
fn execute_on_expire(store: &ScopeGrantStore, grant: &super::grant::ScopeGrant) {
    let action = &grant.on_expire_action;

    if action.is_empty() {
        // No action configured — just let it stay expired.
        // The grant is already filtered out of effective_scopes().
        return;
    }

    if action == "revoke_all" {
        // Hard cutoff: remove the grant entirely.
        match store.revoke(&grant.scope_name, &grant.grantee_type, &grant.grantee_id) {
            Ok(_) => {
                info!(
                    scope = %grant.scope_name,
                    grantee = %grant.grantee_id,
                    "expired scope grant revoked (ON EXPIRE REVOKE ALL)"
                );
            }
            Err(e) => {
                warn!(
                    scope = %grant.scope_name,
                    error = %e,
                    "failed to revoke expired scope grant"
                );
            }
        }
        return;
    }

    if let Some(downgrade_scope) = action.strip_prefix("grant:") {
        // Automatic downgrade: grant a replacement scope.
        match store.grant(
            downgrade_scope,
            &grant.grantee_type,
            &grant.grantee_id,
            "system:expiry",
            0, // Permanent (no expiry on the downgrade).
            0,
            "",
        ) {
            Ok(_) => {
                info!(
                    old_scope = %grant.scope_name,
                    new_scope = %downgrade_scope,
                    grantee = %grant.grantee_id,
                    "expired scope downgraded (ON EXPIRE GRANT)"
                );
            }
            Err(e) => {
                warn!(
                    scope = %grant.scope_name,
                    downgrade = %downgrade_scope,
                    error = %e,
                    "failed to grant downgrade scope on expiry"
                );
            }
        }

        // Remove the expired original grant.
        let _ = store.revoke(&grant.scope_name, &grant.grantee_type, &grant.grantee_id);
    }
}

#[cfg(test)]
mod tests {
    use super::super::grant::ScopeGrantStore;
    use super::*;

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    #[test]
    fn expired_grant_with_revoke_all() {
        let store = ScopeGrantStore::new();
        let past = now_secs() - 100;
        store
            .grant("pro:all", "org", "acme", "admin", past, 0, "revoke_all")
            .unwrap();

        // Grant exists but is expired.
        assert!(!store.has_scope("u1", &["acme".into()], "pro:all"));

        // Process expiry — should revoke.
        process_expired_grants(&store);

        // Grant should be gone.
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn expired_grant_with_downgrade() {
        let store = ScopeGrantStore::new();
        let past = now_secs() - 100;
        store
            .grant(
                "pro:all",
                "org",
                "acme",
                "admin",
                past,
                0,
                "grant:free:basic",
            )
            .unwrap();

        process_expired_grants(&store);

        // pro:all should be gone, free:basic should exist.
        assert!(!store.has_scope("u1", &["acme".into()], "pro:all"));
        assert!(store.has_scope("u1", &["acme".into()], "free:basic"));
    }

    #[test]
    fn grace_period_still_effective() {
        let store = ScopeGrantStore::new();
        // Expired 10s ago but grace is 60s.
        let past = now_secs() - 10;
        store
            .grant("pro:all", "org", "acme", "admin", past, 60, "revoke_all")
            .unwrap();

        // In grace period — still effective.
        assert!(store.has_scope("u1", &["acme".into()], "pro:all"));

        // Process expiry — should NOT revoke (still in grace).
        process_expired_grants(&store);
        assert_eq!(store.count(), 1); // Still there.
    }
}
