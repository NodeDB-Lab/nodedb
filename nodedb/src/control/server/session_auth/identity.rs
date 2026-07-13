// SPDX-License-Identifier: BUSL-1.1

//! Identity resolution: TLS client certificate, API key, and trust mode.

use nodedb_types::id::DatabaseId;
use smallvec::SmallVec;

use crate::control::security::audit::AuditEvent;
use crate::control::security::credential::record::UserRecord;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};
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
        state.audit_record(
            AuditEvent::AuthFailure,
            None,
            peer_addr,
            &format!("mTLS auth failed: no user for cert CN '{cn}'"),
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

    let identity =
        state
            .api_keys
            .to_identity(&key_record, user.roles, user.is_superuser, effective);

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

/// Build a default trust-mode identity for a given username.
///
/// Used by both explicit auth requests and auto-auth on first frame.
pub fn trust_identity(state: &SharedState, username: &str) -> AuthenticatedIdentity {
    if let Some(id) = stored_user_identity(state, username, AuthMethod::Trust) {
        id
    } else {
        AuthenticatedIdentity {
            user_id: 0,
            username: username.to_string(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::Trust,
            roles: vec![Role::Superuser],
            is_superuser: true,
            default_database: None,
            accessible_databases: DatabaseSet::All,
        }
    }
}
