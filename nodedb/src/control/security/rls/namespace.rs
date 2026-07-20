// SPDX-License-Identifier: BUSL-1.1

//! Namespace-scoped authorization helper.
//!
//! Extends collection-level grants with namespace scoping:
//! `GRANT READ ON namespace.collection TO role`.
//! Namespaces are dot-separated prefixes.

use crate::control::security::audit::NoopAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::types::DatabaseId;

/// Check tenant + namespace authorization for a collection.
/// Order: direct collection grant → namespace prefix grants → wildcard.
pub fn check_namespace_authz(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    required_permission: Permission,
    permission_store: &PermissionStore,
    role_store: &RoleStore,
) -> bool {
    if identity.is_superuser {
        return true;
    }

    if permission_store.check(
        identity,
        required_permission,
        database_id,
        collection,
        role_store,
        &NoopAuditEmitter,
    ) {
        return true;
    }

    let parts: Vec<&str> = collection.split('.').collect();
    for i in (0..parts.len()).rev() {
        let namespace = parts[..i].join(".");
        if !namespace.is_empty()
            && permission_store.check(
                identity,
                required_permission,
                database_id,
                &namespace,
                role_store,
                &NoopAuditEmitter,
            )
        {
            return true;
        }
    }

    permission_store.check(
        identity,
        required_permission,
        database_id,
        "*",
        role_store,
        &NoopAuditEmitter,
    )
}
