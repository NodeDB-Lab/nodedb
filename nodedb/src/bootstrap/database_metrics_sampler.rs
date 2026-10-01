// SPDX-License-Identifier: BUSL-1.1

//! Background sampler for per-database Prometheus metrics.
//!
//! Samples live gauges every 10 seconds across memory, admission, storage,
//! virtual queue depths, and rolling-window WAL fsync latency.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::control::metrics::histogram::HistogramSnapshot;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

/// Spawn the 10-second periodic database metrics sampler.
pub fn spawn_database_metrics_sampler(shared: &Arc<SharedState>) {
    let shared_sampler = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "database_metrics_sampler",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            let mut last_fsync_snapshot: HistogramSnapshot = shared_sampler
                .system_metrics
                .as_ref()
                .map(|s| s.wal_fsync_seconds.snapshot_counts())
                .unwrap_or_default();

            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                let catalog = shared_sampler.credentials.catalog();
                let databases = match catalog.list_databases() {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!(error = %e, "database_metrics_sampler: catalog list error");
                        continue;
                    }
                };

                // Compute rolling P99 fsync latency across the 10s tick window.
                let curr_fsync_snapshot = shared_sampler
                    .system_metrics
                    .as_ref()
                    .map(|s| s.wal_fsync_seconds.snapshot_counts())
                    .unwrap_or_default();
                let wal_latency_p99 =
                    last_fsync_snapshot.delta_percentile(&curr_fsync_snapshot, 0.99);
                last_fsync_snapshot = curr_fsync_snapshot;

                // Lock dispatcher ONCE per sampler tick to read virtual-queue depths for all databases.
                let queue_depths: HashMap<u64, u64> = {
                    let d = shared_sampler
                        .dispatcher
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    databases
                        .iter()
                        .map(|db| (db.id.as_u64(), d.virtual_queue_depth(db.id.as_u64())))
                        .collect()
                };

                for db in databases {
                    let depth = queue_depths.get(&db.id.as_u64()).copied().unwrap_or(0);
                    sample_database(
                        &shared_sampler,
                        catalog,
                        db.id,
                        &db.name,
                        depth,
                        wal_latency_p99,
                    );
                }
            }
        },
    );
    info!("database metrics sampler running");
}

/// Sample and record gauges for a single database (internal helper, not pub).
fn sample_database(
    shared: &SharedState,
    catalog: &SystemCatalog,
    db_id: nodedb_types::DatabaseId,
    db_name: &str,
    bridge_queue_depth: u64,
    wal_latency_p99: u64,
) {
    // 1. connections: active connections holding an admission permit for this database
    let connections = shared
        .admission_registry
        .database_live_connections(db_id)
        .map(u64::from)
        .unwrap_or(0);
    shared
        .database_metrics
        .set_connections(db_name, connections);

    // 2. memory: resident memory in bytes allocated by this database in governor
    let memory_bytes = shared.governor.database_usage_bytes(db_id) as u64;
    shared
        .database_metrics
        .set_memory_bytes(db_name, memory_bytes);

    // 3. storage: real on-disk storage usage in bytes from system catalog tables
    let storage_bytes = catalog.database_storage_bytes(db_id).unwrap_or(0);
    shared
        .database_metrics
        .set_storage_bytes(db_name, storage_bytes);

    // 4. bridge_queue_depth: sum of SPSC bridge virtual-queue depths across cores
    shared
        .database_metrics
        .set_bridge_queue_depth(db_name, bridge_queue_depth);

    // 5. wal_latency_p99: node-wide rolling 10s P99 group-commit fsync latency
    shared
        .database_metrics
        .set_wal_latency_p99(db_name, wal_latency_p99);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::metrics::histogram::HistogramSnapshot;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;
    use nodedb_mem::engine::EngineId;
    use nodedb_types::{DatabaseId, TenantId};

    #[tokio::test]
    async fn sampler_records_memory_connections_and_wal_fsync() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("sampler-test.wal")).expect("open wal"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("construct state");

        if let Some(metrics) = shared.system_metrics.as_ref() {
            for _ in 0..90 {
                metrics.record_wal_fsync(100);
            }
            for _ in 0..10 {
                metrics.record_wal_fsync(500);
            }
        }

        let db_id = DatabaseId::new(42);
        let db_name = "test_analytics";

        // Reserve memory in governor for db_id.
        let _token = shared
            .governor
            .try_reserve(db_id, TenantId::new(1), EngineId::Vector, 8192)
            .expect("reserve memory");

        // Acquire connection permit for db_id.
        let _permit = shared
            .admission_registry
            .try_acquire_database(db_id)
            .expect("acquire permit");

        let catalog = shared.credentials.catalog();
        let curr_snapshot = shared
            .system_metrics
            .as_ref()
            .map(|s| s.wal_fsync_seconds.snapshot_counts())
            .unwrap_or_default();
        let delta_p99 = HistogramSnapshot::default().delta_percentile(&curr_snapshot, 0.99);

        // Run sample_database.
        sample_database(&shared, catalog, db_id, db_name, 5, delta_p99);

        // Verify gauge outputs in DatabaseMetricsRegistry.
        let mut text = String::new();
        shared.database_metrics.render_prometheus(&mut text);
        assert!(text.contains(&format!(
            "nodedb_database_connections{{database=\"{db_name}\"}} 1"
        )));
        assert!(text.contains(&format!(
            "nodedb_database_memory_used_bytes{{database=\"{db_name}\"}} 8192"
        )));
        assert!(text.contains(&format!(
            "nodedb_database_bridge_queue_depth{{database=\"{db_name}\"}} 5"
        )));
        assert!(text.contains(&format!(
            "nodedb_database_wal_commit_latency_p99_us{{database=\"{db_name}\"}}"
        )));
    }
}
