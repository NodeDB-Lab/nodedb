//! Sequence post-apply side effects — sync the in-memory
//! `sequence_registry` so `NEXTVAL` / `CURRVAL` on followers sees
//! the replicated definition immediately.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};
use crate::control::state::SharedState;

pub fn put(stored: StoredSequence, shared: Arc<SharedState>) {
    super::owner::install_from_parent(
        "sequence",
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    if let Err(e) = shared.sequence_registry.create(stored.clone()) {
        debug!(
            sequence = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: sequence_registry create (ignored — already exists?)"
        );
    }
}

pub fn delete(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    if let Err(e) = shared.sequence_registry.remove(tenant_id, &name) {
        debug!(
            sequence = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: sequence_registry remove (ignored)"
        );
    }
    shared
        .permissions
        .install_replicated_remove_owner("sequence", tenant_id, &name);
}

pub fn put_state(state: SequenceState, shared: Arc<SharedState>) {
    // ALTER SEQUENCE RESTART ships a fresh `SequenceState`;
    // replicate the counter into the in-memory registry handle so
    // `NEXTVAL` on every node returns from the new value.
    if let Err(e) =
        shared
            .sequence_registry
            .restart(state.tenant_id, &state.name, state.current_value)
    {
        debug!(
            sequence = %state.name,
            tenant = state.tenant_id,
            error = %e,
            "catalog_entry: sequence_registry restart (ignored — sequence may be missing on fresh follower)"
        );
    }
}
