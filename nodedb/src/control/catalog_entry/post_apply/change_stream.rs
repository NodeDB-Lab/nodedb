//! ChangeStream post-apply side effects — sync the in-memory
//! `stream_registry` and tear down the CDC router buffer on drop.

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::cdc::stream_def::ChangeStreamDef;

pub fn put(stored: ChangeStreamDef, shared: Arc<SharedState>) {
    super::owner::install_from_parent(
        "change_stream",
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    shared.stream_registry.register(stored);
}

pub fn delete(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    shared.stream_registry.unregister(tenant_id, &name);
    shared.cdc_router.remove_buffer(tenant_id, &name);
    shared
        .permissions
        .install_replicated_remove_owner("change_stream", tenant_id, &name);
}
