//! Role post-apply side effects — upsert / remove the in-memory
//! `roles` cache.

use std::sync::Arc;

use crate::control::security::catalog::StoredRole;
use crate::control::state::SharedState;

pub fn put(stored: StoredRole, shared: Arc<SharedState>) {
    shared.roles.install_replicated_role(&stored);
}

pub fn delete(name: String, shared: Arc<SharedState>) {
    shared.roles.install_replicated_drop_role(&name);
}
