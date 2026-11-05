// SPDX-License-Identifier: BUSL-1.1

//! Identity resolution: TLS client certificate, API key, and trust mode.

use nodedb_types::id::DatabaseId;
use smallvec::SmallVec;

use crate::control::security::apikey::ApiKeyRecord;
use crate::control::security::audit::AuditEvent;
use crate::control::security::credential::record::UserRecord;
use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet};
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Resolve an identity from a TLS client certificate CN.
///
/// Maps the certificate Common Name to a username in the credential store.
/// Used when `auth.mode = "certificate"` and client presents a TLS cert.
pub fn resolve_certificate_identity(
    state: &SharedState,
    cn: &str,
    peer_addr: &str,
) -> crate::Result<AuthenticatedIdentity> {
    // Map cert CN to username (direct mapping: CN = username).
    let identity = stored_user_identity(state, cn, AuthMethod::Certificate).ok_or_else(|| {
        record_auth_violation(
            state,
            AuthViolation {
                subject: ViolationSubject::Username(cn),
                tenant_id: None,
                source: peer_addr,
                detail: &format!("mTLS auth failed: no user for cert CN '{cn}'"),
            },
        );
        state.auth_metrics.record_auth_failure("certificate");
        crate::Error::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: format!("no user mapped to certificate CN '{cn}'"),
        }
    })?;

    state.audit_record(
        AuditEvent::AuthSuccess,
        Some(identity.tenant_id),
        peer_addr,
        &format!("mTLS cert auth: {cn}"),
    );
    state.auth_metrics.record_auth_success("certificate");

    Ok(identity)
}

/// Build the owner's `DatabaseSet` from a `UserRecord`.
///
/// - Superuser → `DatabaseSet::All`.
/// - Service account with non-empty `accessible_databases` → `DatabaseSet::Some(...)`.
/// - Regular user → databases from `_system.database_grants`, always including `DEFAULT`.
fn build_owner_database_set(state: &SharedState, user: &UserRecord) -> DatabaseSet {
    if user.is_superuser {
        return DatabaseSet::All;
    }
    if user.is_service_account && !user.accessible_databases.is_empty() {
        return DatabaseSet::Some(SmallVec::from_iter(
            user.accessible_databases.iter().copied(),
        ));
    }
    // Regular user or legacy service account: read from database_grants.
    let db_ids = state
        .credentials
        .catalog()
        .list_user_grant_databases(user.user_id)
        .ok()
        .unwrap_or_else(|| vec![DatabaseId::DEFAULT]);
    DatabaseSet::Some(SmallVec::from_iter(db_ids))
}

/// Build a session identity from a persisted user, including live database grants.
///
/// [`CredentialStore::to_identity`](crate::control::security::credential::CredentialStore::to_identity)
/// only materializes credential fields. Session bind must additionally resolve the
/// user's default database and the current `_system.database_grants` set.
pub fn stored_user_identity(
    state: &SharedState,
    username: &str,
    method: AuthMethod,
) -> Option<AuthenticatedIdentity> {
    let user = state.credentials.get_user(username)?;
    let mut identity = state.credentials.to_identity(username, method)?;
    identity.default_database =
        (user.default_database_id != 0).then(|| DatabaseId::new(user.default_database_id));
    identity.accessible_databases = build_owner_database_set(state, &user);
    Some(identity)
}

/// Build the identity of a verified API key from its owner's current record.
fn api_key_record_identity(
    state: &SharedState,
    key_record: &ApiKeyRecord,
) -> Option<AuthenticatedIdentity> {
    let user = state.credentials.get_user(&key_record.username)?;

    let owner_set = build_owner_database_set(state, &user);

    // Compute effective database set: owner_set ∩ key_set.
    // Empty key.accessible_databases means "inherit from owner at this bind" — live,
    // not a snapshot, so subsequent owner narrowing is automatically honored.
    let key_set = if key_record.accessible_databases.is_empty() {
        owner_set.clone()
    } else {
        DatabaseSet::Some(SmallVec::from_iter(
            key_record.accessible_databases.iter().copied(),
        ))
    };
    let effective = owner_set.intersect(&key_set);

    let mut identity =
        state
            .api_keys
            .to_identity(key_record, user.roles, user.is_superuser, effective);
    identity.default_database =
        (user.default_database_id != 0).then(|| DatabaseId::new(user.default_database_id));
    Some(identity)
}

/// The current form of a session's identity, or `None` when it no longer stands.
///
/// `None` covers a dropped or inactive user, a name that returned under a new
/// id, and a revoked or expired API key. Not for an OIDC identity, which has no
/// credential-store record.
pub fn rebuild_session_identity(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Option<AuthenticatedIdentity> {
    let current = match &identity.api_key_id {
        Some(key_id) => {
            let key_record = state
                .api_keys
                .get_key(key_id)
                .filter(ApiKeyRecord::is_valid)?;
            api_key_record_identity(state, &key_record)?
        }
        None => stored_user_identity(state, &identity.username, identity.auth_method.clone())?,
    };
    (current.user_id == identity.user_id).then_some(current)
}

/// Verify an API key token and build an authenticated identity.
///
/// Shared by native protocol and HTTP API authentication paths.
/// Returns `None` if the token is invalid or the owner user is not found.
pub fn verify_api_key_identity(
    state: &SharedState,
    token: &str,
    peer_addr: &str,
    protocol: &str,
) -> Option<AuthenticatedIdentity> {
    let key_record = state.api_keys.verify_key(token)?;
    let identity = api_key_record_identity(state, &key_record)?;

    state.audit_record(
        AuditEvent::AuthSuccess,
        Some(identity.tenant_id),
        peer_addr,
        &format!(
            "{protocol} api_key auth: {} (key {})",
            identity.username, key_record.key_id
        ),
    );
    state.auth_metrics.record_auth_success("api_key");

    Some(identity)
}

/// Resolve an explicit trust-mode username to a durable stored identity.
pub fn trust_identity(state: &SharedState, username: &str) -> Option<AuthenticatedIdentity> {
    stored_user_identity(state, username, AuthMethod::Trust)
}

/// Resolve the configured durable principal for protocols that auto-authenticate
/// in trust mode without a client-supplied username.
pub fn configured_trust_identity(state: &SharedState) -> Option<AuthenticatedIdentity> {
    let username = state.credentials.configured_trust_superuser().ok()??;
    stored_user_identity(state, &username, AuthMethod::Trust)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::apikey::CreateKeyParams;
    use crate::control::security::identity::Role;
    use crate::wal::WalManager;

    use super::*;

    #[tokio::test]
    async fn api_key_identity_uses_owner_default_and_key_database_scope() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        let owner_default = DatabaseId::new(71);
        let key_database = DatabaseId::new(72);
        let user_id = state
            .credentials
            .create_service_account(
                "api-owner",
                TenantId::new(1),
                vec![Role::ReadWrite],
                vec![owner_default, key_database],
            )
            .expect("create service account");
        let stored = state
            .credentials
            .prepare_set_default_database("api-owner", owner_default.as_u64())
            .expect("set owner default database");
        state.credentials.install_replicated_user(&stored, None);
        let token = state
            .api_keys
            .create_key(
                CreateKeyParams {
                    username: "api-owner",
                    user_id,
                    tenant_id: TenantId::new(1),
                    expires_secs: 0,
                    scope: vec![],
                    accessible_databases: vec![key_database],
                },
                None,
            )
            .expect("create API key");

        let identity =
            verify_api_key_identity(&state, &token, "127.0.0.1", "native").expect("verify API key");

        assert_eq!(identity.default_database, Some(owner_default));
        assert!(identity.accessible_databases.contains(key_database));
        assert!(!identity.accessible_databases.contains(owner_default));
    }
}
