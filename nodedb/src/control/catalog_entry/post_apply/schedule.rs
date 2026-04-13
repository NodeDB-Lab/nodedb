//! Schedule post-apply side effects — sync the in-memory cron
//! registry so the scheduler executor picks up the change on its
//! next tick.

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::scheduler::types::ScheduleDef;

pub fn put(stored: ScheduleDef, shared: Arc<SharedState>) {
    super::owner::install_from_parent(
        "schedule",
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    shared.schedule_registry.register(stored);
}

pub fn delete(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    shared.schedule_registry.unregister(tenant_id, &name);
    shared
        .permissions
        .install_replicated_remove_owner("schedule", tenant_id, &name);
}
