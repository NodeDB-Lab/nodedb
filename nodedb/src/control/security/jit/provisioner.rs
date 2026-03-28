//! JIT (Just-In-Time) user provisioning from JWT claims.
//!
//! When `jit_provisioning = true`, users are automatically created in
//! `_system.auth_users` on their first JWT authentication. Subsequent
//! requests update `last_seen` and optionally sync changed claims.
//!
//! This bridges Mode 1 (JWT-only, no backend) to Mode 2 (JWT + DB state)
//! by creating server-side records for externally-authenticated users.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{debug, info};

use super::auth_user::{AuthUserRecord, AuthUserStore};
use crate::control::security::auth_context::AuthStatus;
use crate::control::security::jwt::JwtClaims;

/// JIT provisioning configuration.
#[derive(Debug, Clone)]
pub struct JitConfig {
    /// Enable automatic user creation from JWT claims.
    pub enabled: bool,
    /// Sync claims on each request (update email, roles, etc.).
    pub sync_claims: bool,
}

impl Default for JitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sync_claims: true,
        }
    }
}

/// Provision or update a user from JWT claims.
///
/// Called after successful JWT validation. Creates the user if they don't
/// exist (when JIT is enabled), or updates `last_seen` and syncs claims.
/// Also populates `_system.org_members` from `$auth.org_ids` if present.
///
/// Returns the user's `AuthStatus` for the auth flow to check.
pub fn provision_from_jwt(
    store: &AuthUserStore,
    claims: &JwtClaims,
    provider_name: &str,
    config: &JitConfig,
    org_store: Option<&crate::control::security::org::store::OrgStore>,
) -> crate::Result<AuthStatus> {
    let user_id = if claims.user_id != 0 {
        claims.user_id.to_string()
    } else {
        claims.sub.clone()
    };

    if user_id.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "JWT has no user identifier (sub or user_id claim)".into(),
        });
    }

    // Check if user already exists.
    if let Some(existing) = store.get(&user_id) {
        // User exists — check if active.
        if !existing.is_active {
            return Ok(existing.status); // Deactivated → caller denies.
        }

        // Update last_seen.
        store.touch(&user_id)?;

        // Sync claims if enabled.
        if config.sync_claims {
            sync_claims(store, &user_id, claims)?;
        }

        return Ok(existing.status);
    }

    // User doesn't exist — create if JIT provisioning is enabled.
    if !config.enabled {
        // No JIT → user must be pre-provisioned. Return Active and let
        // the regular auth flow handle it (they'll authenticate via
        // CredentialStore or get rejected).
        return Ok(AuthStatus::Active);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let username = if claims.sub.is_empty() {
        format!("jwt_user_{}", claims.user_id)
    } else {
        claims.sub.clone()
    };

    let email = claims
        .extra
        .get("email")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let record = AuthUserRecord {
        id: user_id.clone(),
        username,
        email,
        tenant_id: claims.tenant_id,
        provider: provider_name.into(),
        first_seen: now,
        last_seen: now,
        is_active: true,
        status: AuthStatus::Active,
        is_external: true,
        synced_claims: extract_sync_claims(claims),
    };

    store.upsert(record)?;
    info!(
        user_id = %user_id,
        provider = %provider_name,
        tenant_id = claims.tenant_id,
        "JIT user provisioned"
    );

    // Populate org memberships from $auth.org_ids claim.
    if let Some(org_store) = org_store {
        let org_ids: Vec<String> = claims
            .extra
            .get("org_ids")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        for org_id in &org_ids {
            // JIT create org if needed.
            org_store.ensure_org(org_id, claims.tenant_id)?;
            // Add membership (idempotent — upsert pattern).
            let _ = org_store.add_member(org_id, &user_id, "member");
        }

        if !org_ids.is_empty() {
            debug!(user_id = %user_id, orgs = ?org_ids, "JIT org memberships populated");
        }
    }

    Ok(AuthStatus::Active)
}

/// Sync changed claims from a JWT to an existing auth user record.
fn sync_claims(store: &AuthUserStore, user_id: &str, claims: &JwtClaims) -> crate::Result<()> {
    let Some(mut user) = store.get(user_id) else {
        return Ok(());
    };

    let new_claims = extract_sync_claims(claims);
    if user.synced_claims == new_claims {
        return Ok(()); // No changes.
    }

    debug!(user_id = %user_id, "syncing JWT claims");

    // Update email if changed.
    if let Some(email) = claims.extra.get("email").and_then(|v| v.as_str()) {
        user.email = email.to_string();
    }

    // Update status from claim if present.
    if let Some(status) = claims.extra.get("status").and_then(|v| v.as_str())
        && let Ok(s) = status.parse::<AuthStatus>()
    {
        user.status = s;
        user.is_active = matches!(
            s,
            AuthStatus::Active | AuthStatus::Restricted | AuthStatus::ReadOnly
        );
    }

    user.synced_claims = new_claims;
    store.upsert(user)?;
    Ok(())
}

/// Extract claims worth syncing from a JWT.
fn extract_sync_claims(claims: &JwtClaims) -> HashMap<String, String> {
    let mut map = HashMap::new();

    if !claims.sub.is_empty() {
        map.insert("sub".into(), claims.sub.clone());
    }
    for key in ["email", "org_id", "status", "name"] {
        if let Some(val) = claims.extra.get(key).and_then(|v| v.as_str()) {
            map.insert(key.into(), val.to_string());
        }
    }
    // Serialize array claims as comma-separated.
    for key in ["roles", "groups", "permissions", "org_ids"] {
        if let Some(arr) = claims.extra.get(key).and_then(|v| v.as_array()) {
            let strs: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            if !strs.is_empty() {
                map.insert(key.into(), strs.join(","));
            }
        }
    }
    // Also include top-level roles.
    if !claims.roles.is_empty() && !map.contains_key("roles") {
        map.insert("roles".into(), claims.roles.join(","));
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_claims(sub: &str, user_id: u64) -> JwtClaims {
        let mut extra = HashMap::new();
        extra.insert("email".into(), serde_json::json!("test@example.com"));
        JwtClaims {
            sub: sub.into(),
            tenant_id: 1,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1_700_000_000,
            iss: "test-provider".into(),
            aud: "nodedb".into(),
            user_id,
            is_superuser: false,
            extra,
        }
    }

    #[test]
    fn jit_creates_user_on_first_auth() {
        let store = AuthUserStore::new();
        let claims = test_claims("alice", 42);
        let config = JitConfig {
            enabled: true,
            sync_claims: true,
        };

        let status = provision_from_jwt(&store, &claims, "test", &config, None).unwrap();
        assert_eq!(status, AuthStatus::Active);
        assert!(store.is_active("42"));

        let user = store.get("42").unwrap();
        assert_eq!(user.username, "alice");
        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.provider, "test");
    }

    #[test]
    fn jit_disabled_doesnt_create() {
        let store = AuthUserStore::new();
        let claims = test_claims("bob", 99);
        let config = JitConfig {
            enabled: false,
            sync_claims: true,
        };

        let status = provision_from_jwt(&store, &claims, "test", &config, None).unwrap();
        assert_eq!(status, AuthStatus::Active);
        assert!(store.get("99").is_none()); // Not created.
    }

    #[test]
    fn deactivated_user_returns_suspended() {
        let store = AuthUserStore::new();
        let claims = test_claims("alice", 42);
        let config = JitConfig {
            enabled: true,
            sync_claims: true,
        };

        // First auth → create.
        provision_from_jwt(&store, &claims, "test", &config, None).unwrap();

        // Deactivate.
        store.deactivate("42").unwrap();

        // Second auth → returns Suspended status.
        let status = provision_from_jwt(&store, &claims, "test", &config, None).unwrap();
        assert_eq!(status, AuthStatus::Suspended);
    }

    #[test]
    fn claim_sync_updates_email() {
        let store = AuthUserStore::new();
        let config = JitConfig {
            enabled: true,
            sync_claims: true,
        };

        let claims1 = test_claims("alice", 42);
        provision_from_jwt(&store, &claims1, "test", &config, None).unwrap();
        assert_eq!(store.get("42").unwrap().email, "test@example.com");

        // Second auth with changed email.
        let mut claims2 = test_claims("alice", 42);
        claims2
            .extra
            .insert("email".into(), serde_json::json!("alice@new.com"));
        provision_from_jwt(&store, &claims2, "test", &config, None).unwrap();
        assert_eq!(store.get("42").unwrap().email, "alice@new.com");
    }
}
