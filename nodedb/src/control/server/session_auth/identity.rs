// SPDX-License-Identifier: BUSL-1.1

//! Identity resolution: TLS client certificate, API key, and trust mode.

use crate::control::security::audit::AuditEvent;
use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::security::session_identity::{api_key_record_identity, stored_user_identity};
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

    use nodedb_types::id::DatabaseId;

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
