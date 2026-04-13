//! Trigger post-apply side effects — sync the in-memory
//! `trigger_registry`.

use std::sync::Arc;

use crate::control::security::catalog::trigger_types::StoredTrigger;
use crate::control::state::SharedState;

pub fn put(stored: StoredTrigger, shared: Arc<SharedState>) {
    // `register` is an upsert: inserts new triggers and replaces
    // on OR REPLACE / ALTER ENABLE/DISABLE.
    shared.trigger_registry.register(stored);
}

pub fn delete(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    shared.trigger_registry.unregister(tenant_id, &name);
}
