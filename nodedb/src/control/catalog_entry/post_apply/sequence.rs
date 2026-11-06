// SPDX-License-Identifier: BUSL-1.1

//! Sequence post-apply side effects — sync the in-memory
//! `sequence_registry` so `NEXTVAL` / `CURRVAL` on followers sees
//! the replicated definition immediately.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};
use crate::control::state::SharedState;

pub fn put(stored: StoredSequence, shared: Arc<SharedState>) {
    super::owner::install_from_parent(
        "sequence",
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    // A capture's image installs the counter row ahead of its definition.
    let held = shared
        .credentials
        .catalog()
        .get_sequence_state(stored.database_id, stored.tenant_id, &stored.name)
        .unwrap_or_else(|e| {
            warn!(sequence = %stored.name, error = %e, "catalog_entry: sequence row did not read");
            None
        });
    if let Err(e) = shared.sequence_registry.create_at(stored.clone(), held) {
        debug!(
            sequence = %stored.name,
            database = stored.database_id,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: sequence_registry create (ignored — already exists?)"
        );
    }
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    if let Err(e) = shared
        .sequence_registry
        .remove(database_id, tenant_id, &name)
    {
        debug!(
            sequence = %name,
            database = database_id,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: sequence_registry remove (ignored)"
        );
    }
    shared
        .permissions
        .install_replicated_remove_owner("sequence", database_id, tenant_id, &name);
}

pub fn put_state(state: SequenceState, shared: Arc<SharedState>) {
    // ALTER SEQUENCE RESTART and setval ship a fresh `SequenceState`. Every node
    // takes the counter and ends its ranges, so `nextval` continues from it. A
    // definition that arrives later reads the row.
    shared.sequence_registry.install_state(&state);
}
