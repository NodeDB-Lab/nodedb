//! Procedure post-apply side effects — same block-cache
//! invalidation pattern as Function.

use std::sync::Arc;

use crate::control::security::catalog::procedure_types::StoredProcedure;
use crate::control::state::SharedState;

pub fn put(_proc: StoredProcedure, shared: Arc<SharedState>) {
    shared.block_cache.clear();
}

pub fn delete(_tenant_id: u32, _name: String, shared: Arc<SharedState>) {
    shared.block_cache.clear();
}
