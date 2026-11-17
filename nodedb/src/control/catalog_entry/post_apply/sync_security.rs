// SPDX-License-Identifier: BUSL-1.1

//! Synchronous in-memory cache updates for identity, access, and ownership entries.

use std::sync::Arc;

use super::{
    api_key, auth_user, oidc_provider, owner, permission, redaction, rls, role, scope_grant, user,
};
use crate::control::security::catalog::{
    StoredApiKey, StoredAuthUser, StoredOidcProvider, StoredOwner, StoredPermission,
    StoredRedactionPolicy, StoredRlsPolicy, StoredRole, StoredScopeGrant, StoredUser,
};
use crate::control::state::SharedState;

/// An identity, access, or ownership catalog entry, borrowed from its
/// `CatalogEntry`.
///
/// `apply_post_apply_side_effects_sync` builds one for each security variant
/// it routes here. [`apply_security_sync`] matches it exhaustively, so a
/// variant added here without a side effect fails to compile.
pub(super) enum SecurityEntry<'a> {
    PutUser(&'a StoredUser),
    DropUser {
        username: &'a str,
    },
    PutRole(&'a StoredRole),
    DeleteRole {
        name: &'a str,
    },
    PutApiKey(&'a StoredApiKey),
    RevokeApiKey {
        key_id: &'a str,
    },
    PutAuthUser(&'a StoredAuthUser),
    PutRlsPolicy(&'a StoredRlsPolicy),
    DeleteRlsPolicy {
        tenant_id: u64,
        collection: &'a str,
        name: &'a str,
    },
    PutRedactionPolicy(&'a StoredRedactionPolicy),
    DeleteRedactionPolicy {
        tenant_id: u64,
        collection: &'a str,
        for_role: &'a str,
    },
    PutPermission(&'a StoredPermission),
    DeletePermission {
        target: &'a str,
        grantee: &'a str,
        permission: &'a str,
    },
    PutScopeGrant(&'a StoredScopeGrant),
    DeleteScopeGrant {
        scope_name: &'a str,
        grantee_type: &'a str,
        grantee_id: &'a str,
    },
    PutOwner(&'a StoredOwner),
    DeleteOwner {
        object_type: &'a str,
        database_id: u64,
        tenant_id: u64,
        object_name: &'a str,
    },
    PutOidcProvider(&'a StoredOidcProvider),
    DeleteOidcProvider {
        name: &'a str,
    },
    /// `PutCredential` or `DropCredential`. A credential is read from the
    /// catalog row on each use, so no cache holds it.
    Credential,
}

/// Run the sync side effect of an identity, access, or ownership entry.
pub(super) fn apply_security_sync(entry: SecurityEntry<'_>, shared: &Arc<SharedState>) {
    match entry {
        SecurityEntry::PutUser(stored) => {
            user::put(
                stored.clone(),
                Arc::clone(shared),
                Some(crate::control::security::buses::SessionInvalidationReason::RoleAltered),
            );
        }
        SecurityEntry::DropUser { username } => {
            user::drop_user(username.to_owned(), Arc::clone(shared));
        }
        SecurityEntry::PutRole(stored) => role::put(stored.clone(), Arc::clone(shared)),
        SecurityEntry::DeleteRole { name } => role::delete(name.to_owned(), Arc::clone(shared)),
        SecurityEntry::PutApiKey(stored) => api_key::put(stored.clone(), Arc::clone(shared)),
        SecurityEntry::RevokeApiKey { key_id } => {
            api_key::revoke(key_id.to_owned(), Arc::clone(shared));
        }
        SecurityEntry::PutAuthUser(stored) => auth_user::put(stored.clone(), Arc::clone(shared)),
        SecurityEntry::PutRlsPolicy(stored) => rls::put(stored.clone(), Arc::clone(shared)),
        SecurityEntry::DeleteRlsPolicy {
            tenant_id,
            collection,
            name,
        } => {
            rls::delete(
                tenant_id,
                collection.to_owned(),
                name.to_owned(),
                Arc::clone(shared),
            );
        }
        SecurityEntry::PutRedactionPolicy(stored) => {
            redaction::put(stored.clone(), Arc::clone(shared));
        }
        SecurityEntry::DeleteRedactionPolicy {
            tenant_id,
            collection,
            for_role,
        } => {
            redaction::delete(
                tenant_id,
                collection.to_owned(),
                for_role.to_owned(),
                Arc::clone(shared),
            );
        }
        SecurityEntry::PutPermission(stored) => {
            permission::put(stored.clone(), Arc::clone(shared));
        }
        SecurityEntry::DeletePermission {
            target,
            grantee,
            permission: perm,
        } => {
            permission::delete(
                target.to_owned(),
                grantee.to_owned(),
                perm.to_owned(),
                Arc::clone(shared),
            );
        }
        SecurityEntry::PutScopeGrant(stored) => {
            scope_grant::put(stored.clone(), Arc::clone(shared));
        }
        SecurityEntry::DeleteScopeGrant {
            scope_name,
            grantee_type,
            grantee_id,
        } => {
            scope_grant::delete(
                scope_name.to_owned(),
                grantee_type.to_owned(),
                grantee_id.to_owned(),
                Arc::clone(shared),
            );
        }
        SecurityEntry::PutOwner(stored) => owner::put(stored.clone(), Arc::clone(shared)),
        SecurityEntry::DeleteOwner {
            object_type,
            database_id,
            tenant_id,
            object_name,
        } => {
            owner::delete(
                object_type.to_owned(),
                database_id,
                tenant_id,
                object_name.to_owned(),
                Arc::clone(shared),
            );
        }
        SecurityEntry::PutOidcProvider(provider) => oidc_provider::put(provider, shared),
        SecurityEntry::DeleteOidcProvider { name } => oidc_provider::delete(name, shared),
        SecurityEntry::Credential => {}
    }
}
