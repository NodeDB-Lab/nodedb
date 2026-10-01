// SPDX-License-Identifier: BUSL-1.1

//! Post-construction wiring for [`super::SharedState::open`].
//!
//! Runs after the `Arc<SharedState>` exists: populates catalog-backed
//! caches that need a `state` handle to hydrate, and spawns the array GC
//! background task. Pure extraction of the tail of `open()` — no behavior
//! change.

use std::sync::Arc;

use super::super::SharedState;

/// Populate per-database DML audit cache and collection→database reverse
/// map from the catalog so the Event Plane consumer has accurate data from
/// the first write after startup, then wire the session-handle audit sink.
pub(super) fn hydrate_caches(state: &Arc<SharedState>) {
    SharedState::wire_session_handle_audit(state);

    let catalog = state.credentials.catalog();
    if let Err(e) = state.audit_dml_cache.load_from_catalog(catalog) {
        tracing::warn!(error = %e, "boot: failed to populate audit_dml_cache from catalog");
    }
    if let Err(e) = state.collection_to_database.load_from_catalog(catalog) {
        tracing::warn!(
            error = %e,
            "boot: failed to populate collection_to_database cache from catalog"
        );
    }
    if let Err(e) = state.idle_timeout_cache.load_from_catalog(catalog) {
        tracing::warn!(
            error = %e,
            "boot: failed to populate idle_timeout_cache from catalog"
        );
    }
    if let Some(metrics) = &state.system_metrics {
        state.wal.set_metrics(Arc::clone(metrics));
    }
    state
        .maintenance_budget
        .set_metrics(Arc::clone(&state.database_metrics));
    state
        .maintenance_budget
        .set_database_name(nodedb_types::DatabaseId::DEFAULT, "default");
    if let Ok(databases) = catalog.list_databases() {
        for db in databases {
            state.maintenance_budget.set_database_name(db.id, &db.name);
        }
    }
}

/// Spawn the array GC background task. The handle is stored by the caller
/// (main.rs) which has mutable access at that point via `Arc::get_mut`.
/// The task shuts itself down via `ShutdownWatch`, so dropping the handle
/// here is safe — the task keeps running until shutdown is signalled.
pub(super) fn spawn_array_gc(state: &Arc<SharedState>) {
    let _gc_handle = crate::control::array_sync::spawn_gc_task(
        Arc::clone(&state.array_sync_op_log),
        Arc::clone(&state.array_snapshot_store),
        Arc::clone(&state.array_ack_registry),
        Arc::clone(&state.array_snapshot_hlcs),
        Arc::clone(&state.shutdown),
        crate::control::array_sync::gc_task::DEFAULT_GC_INTERVAL,
    );
    // `array_gc_handle` in SharedState stays None; main.rs may install the
    // handle via Arc::get_mut after open() returns (before cloning the Arc).
}
