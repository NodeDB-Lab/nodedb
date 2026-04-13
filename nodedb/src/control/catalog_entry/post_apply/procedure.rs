//! Procedure post-apply side effects — same block-cache
//! invalidation pattern as Function.

use std::sync::Arc;

use crate::control::security::catalog::procedure_types::StoredProcedure;
use crate::control::state::SharedState;

pub fn put(proc: StoredProcedure, shared: Arc<SharedState>) {
    shared.block_cache.clear();
    super::owner::install_from_parent(
        "procedure",
        proc.tenant_id,
        &proc.name,
        &proc.owner,
        &shared,
    );
}

pub fn delete(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    shared.block_cache.clear();
    shared
        .permissions
        .install_replicated_remove_owner("procedure", tenant_id, &name);
}
