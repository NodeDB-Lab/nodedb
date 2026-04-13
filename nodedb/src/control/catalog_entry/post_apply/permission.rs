//! Post-apply side effects for permission grant `CatalogEntry`
//! variants. After the synchronous applier has written the redb
//! row, install the in-memory grant on every node so the
//! `PermissionStore::check` evaluator sees the new state.

use std::sync::Arc;

use crate::control::security::catalog::StoredPermission;
use crate::control::state::SharedState;

pub fn put(stored: StoredPermission, shared: Arc<SharedState>) {
    shared.permissions.install_replicated_permission(&stored);
    tracing::debug!(
        target = %stored.target,
        grantee = %stored.grantee,
        permission = %stored.permission,
        "post_apply: permission grant replicated"
    );
}

pub fn delete(target: String, grantee: String, permission: String, shared: Arc<SharedState>) {
    let removed = shared
        .permissions
        .install_replicated_revoke(&target, &grantee, &permission);
    tracing::debug!(
        %target, %grantee, %permission, removed,
        "post_apply: permission revoke replicated"
    );
}
