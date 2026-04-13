//! Function post-apply side effects — clear the parsed block
//! cache so the next call re-parses the new body.

use std::sync::Arc;

use crate::control::security::catalog::function_types::StoredFunction;
use crate::control::state::SharedState;

pub fn put(_func: StoredFunction, shared: Arc<SharedState>) {
    // The block cache is keyed by body-SQL hash, not (tenant,
    // name), so point invalidation isn't possible. Clearing the
    // whole cache mirrors PostgreSQL's "any DDL invalidates
    // prepared plans" behavior — cache is small, reparse is cheap.
    shared.block_cache.clear();
}

pub fn delete(_tenant_id: u32, _name: String, shared: Arc<SharedState>) {
    shared.block_cache.clear();
}
