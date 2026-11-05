// SPDX-License-Identifier: BUSL-1.1

//! The production background loops a harness node starts.
//!
//! The harness does not call `spawn_background_loops`. It starts the loops a
//! cluster test depends on here, each with the production spawn function.
//! Each loop registers on `shared.loop_registry`, so cluster shutdown stops it.

use std::sync::Arc;

use nodedb::bootstrap::background_loops::storage_bytes::{
    spawn_storage_sampler, storage_sample_period,
};
use nodedb::control::state::SharedState;

/// Start the constraint reconcile loop, the KV TTL reaper and the
/// stored-bytes loop.
///
/// Every interval comes from the environment overrides the server reads. A
/// test that sets `NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS` or
/// `NODEDB_STORAGE_SAMPLE_INTERVAL_MS` is honoured as in production. Parsing
/// it a second way here would let the two drift apart.
pub(super) fn spawn_production_loops(
    shared: &Arc<SharedState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut loop_config = nodedb::ServerConfig::default();
    nodedb::config::server::apply_env_overrides(&mut loop_config)
        .map_err(|e| format!("environment override rejected: {e}"))?;

    // CRDT constraint reconcile (leader-gated): constraint delivery to every
    // replica's validator.
    nodedb::bootstrap::constraint_reconcile::spawn_constraint_reconcile(
        Arc::clone(shared),
        loop_config
            .tuning
            .maintenance
            .constraint_reconcile_interval_ms,
    );

    // A key's TTL ends only through the reap its group's leader proposes, so
    // a cluster without the reaper never expires a key.
    nodedb::bootstrap::background_loops::kv_expiry::spawn_kv_expiry_reaper(shared, &loop_config);

    // Each node samples its share and aggregates the cluster storage total.
    spawn_storage_sampler(
        shared,
        storage_sample_period(loop_config.tuning.maintenance.storage_sample_interval_ms),
    );
    Ok(())
}
