// SPDX-License-Identifier: BUSL-1.1

//! Post-`SharedState::open` catalog steps: fire the catalog-open gate,
//! replay the surrogate WAL, rebuild the open redo streams, read back the cut
//! barriers, and bootstrap the superuser credential.

use std::sync::Arc;

use nodedb::ServerConfig;
use nodedb::bootstrap;
use nodedb::control::startup::ReadyGate;
use nodedb::control::state::SharedState;

/// Runs the post-open catalog steps that belong right after
/// `SharedState::open` + wiring return, kept out of `main()` for readability.
pub(crate) async fn run(
    shared: &Arc<SharedState>,
    wal_records: &Arc<[nodedb_wal::WalRecord]>,
    replay_tombstones: &nodedb_wal::TombstoneSet,
    config: &ServerConfig,
    catalog_gate: &ReadyGate,
) -> anyhow::Result<()> {
    // System catalog (redb) is open — fire the ClusterCatalogOpen gate.
    catalog_gate.fire();

    // Replay surrogate WAL records into the in-memory registry.
    bootstrap::credentials::replay_surrogate_wal(shared, wal_records, replay_tombstones);

    // Open chunked redo streams, before any data group applies an entry.
    let streams = shared.redo_chunks.rebuild(wal_records)?;
    if streams.open > 0 {
        tracing::info!(
            open = streams.open,
            closed = streams.closed,
            "chunked redo streams rebuilt from the WAL"
        );
    }

    // Cut barriers, for the apply loop's commit HLC floors.
    shared
        .pitr
        .install_recorded_cuts(nodedb::control::pitr::restore_point::load_recorded_cuts(
            shared.credentials.catalog(),
        )?);

    // Bootstrap superuser credential (or warn about trust mode).
    bootstrap::credentials::bootstrap_superuser(shared, config)?;

    Ok(())
}
