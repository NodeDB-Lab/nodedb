// SPDX-License-Identifier: BUSL-1.1

//! Session identity construction from stored users and API keys.

use nodedb_types::id::DatabaseId;
use smallvec::SmallVec;

use crate::control::security::apikey::ApiKeyRecord;
use crate::control::security::credential::record::UserRecord;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet};
use crate::control::state::SharedState;

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
pub(crate) fn api_key_record_identity(
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
