// SPDX-License-Identifier: BUSL-1.1

//! Per-database metrics sampler: publishes gauges and counter deltas that
//! no hot path writes.
//!
//! It samples every `SAMPLE_PERIOD`. Stored bytes sample on their own loop,
//! so a slow core never delays these gauges.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::DatabaseId;

use crate::control::metrics::connections::publish_connections;
use crate::control::metrics::histogram::HistogramSnapshot;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// Period between samples.
const SAMPLE_PERIOD: Duration = Duration::from_secs(5);

/// State carried between samples.
#[derive(Debug, Default)]
pub(crate) struct SamplerState {
    /// Cumulative maintenance lease seconds seen at the previous sample.
    lease_secs: HashMap<DatabaseId, f64>,
    /// WAL commit histogram counts at the previous sample.
    wal_commit: HashMap<DatabaseId, HistogramSnapshot>,
}

/// Publish one sample of every per-database gauge and counter delta.
///
/// Catalog errors skip that database, the rest still publish, and the first
/// error returns. The registry never loses an entry here.
pub(crate) fn sample_once(shared: &SharedState, prev: &mut SamplerState) -> crate::Result<()> {
    let registry = &shared.database_metrics;
    let ids = registry.database_ids();

    let memory: HashMap<DatabaseId, u64> = shared
        .governor
        .database_usage_snapshot()
        .into_iter()
        .map(|u| (u.database_id, u.allocated_bytes as u64))
        .collect();
    let depths = {
        let dispatcher = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        dispatcher.database_queue_depths()
    };
    for &id in &ids {
        registry.set_memory_bytes(id, memory.get(&id).copied().unwrap_or(0));
        registry.set_bridge_queue_depth(id, depths.get(&id).copied().unwrap_or(0));
    }

    publish_connections(registry, shared);
    publish_wal_commit_p99(shared, &ids, prev);
    publish_lease_deltas(shared, prev);

    let catalog = shared.credentials.catalog();
    let mut array_tenants: HashMap<DatabaseId, HashSet<u64>> = HashMap::new();
    for a in catalog.load_all_arrays()? {
        array_tenants
            .entry(a.array_id.database_id)
            .or_default()
            .insert(a.array_id.tenant_id.as_u64());
    }
    let mut first_error = None;
    for &id in &ids {
        match catalog.load_all_collections(id) {
            Ok(collections) => {
                let active = collections.iter().filter(|c| c.is_active).count();
                let mut tenants: HashSet<u64> = collections.iter().map(|c| c.tenant_id).collect();
                if let Some(extra) = array_tenants.get(&id) {
                    tenants.extend(extra);
                }
                registry.set_collections(id, active as u64);
                registry.set_tenants(id, tenants.len() as u64);
            }
            Err(e) => {
                first_error.get_or_insert(e);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Publish the WAL commit P99 of the window since the previous sample.
///
/// Stored snapshots of databases no longer in the registry are dropped.
fn publish_wal_commit_p99(shared: &SharedState, ids: &[DatabaseId], prev: &mut SamplerState) {
    let mut current = HashMap::with_capacity(ids.len());
    for &id in ids {
        let Some(counters) = shared.database_metrics.get(id) else {
            continue;
        };
        let now = counters.wal_commit_latency.counts_snapshot();
        let p99 = match prev.wal_commit.get(&id) {
            Some(before) => now.since(before).percentile(0.99),
            None => now.percentile(0.99),
        };
        shared.database_metrics.set_wal_commit_p99_us(id, p99);
        current.insert(id, now);
    }
    prev.wal_commit = current;
}

/// Add the growth of each database's cumulative lease seconds to the registry.
///
/// A counter below its previous value was cleared, so it restarts from its
/// current value.
fn publish_lease_deltas(shared: &SharedState, prev: &mut SamplerState) {
    let snapshot = shared.maintenance_budget.cumulative_lease_secs_snapshot();
    let mut current = HashMap::with_capacity(snapshot.len());
    for (db, total) in snapshot {
        let before = prev.lease_secs.get(&db).copied().unwrap_or(0.0);
        let delta = if total >= before {
            total - before
        } else {
            total
        };
        if delta > 0.0 {
            shared
                .database_metrics
                .add_maintenance_lease_secs(db, delta);
        }
        current.insert(db, total);
    }
    prev.lease_secs = current;
}

/// Spawn the loop that samples per-database metrics every `SAMPLE_PERIOD`.
///
/// A sampling error logs a warning and the next tick retries.
pub(crate) fn spawn_database_metrics_sampler(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "database_metrics_sampler",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut prev = SamplerState::default();
            let mut tick = tokio::time::interval(SAMPLE_PERIOD);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(error) = sample_once(&state, &mut prev) {
                            tracing::warn!(error = %error, "database metrics sample failed");
                        }
                    }
                }
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;
    use nodedb_mem::EngineId;
    use nodedb_types::TenantId;

    fn make_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("shared state");
        (shared, dir)
    }

    fn hold_lease(shared: &SharedState, db: DatabaseId) {
        let lease = shared
            .maintenance_budget
            .try_acquire(db, 0.0)
            .expect("lease");
        std::thread::sleep(Duration::from_millis(5));
        drop(lease);
    }

    #[test]
    fn sample_once_publishes_gauges_and_lease_deltas() {
        let (shared, _dir) = make_state();
        let (a, b) = (DatabaseId::new(21), DatabaseId::new(22));
        shared.database_metrics.ensure_database(a);
        shared.database_metrics.ensure_database(b);
        let _ma = shared
            .governor
            .try_reserve(a, TenantId::new(1), EngineId::Vector, 4096)
            .expect("reserve a");
        let _mb = shared
            .governor
            .try_reserve(b, TenantId::new(1), EngineId::Vector, 1024)
            .expect("reserve b");
        let _conn = shared.connection_tracker.track_native(a);
        shared.database_metrics.set_bridge_queue_depth(b, 9);
        hold_lease(&shared, a);

        let mut prev = SamplerState::default();
        sample_once(&shared, &mut prev).expect("first sample");
        let reg = &shared.database_metrics;
        assert_eq!(reg.memory_bytes(a), 4096);
        assert_eq!(reg.memory_bytes(b), 1024);
        let counters_a = reg.get(a).expect("entry a");
        let counters_b = reg.get(b).expect("entry b");
        let load = |v: &std::sync::atomic::AtomicU64| v.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(load(&counters_a.connections), 1);
        assert_eq!(load(&counters_b.connections), 0);
        assert_eq!(load(&counters_b.bridge_queue_depth), 0);
        let first = load(&counters_a.maintenance_lease_micros_total);
        assert!(first > 0);
        assert_eq!(load(&counters_b.maintenance_lease_micros_total), 0);

        // The WAL p99 covers only the window since the previous sample.
        for _ in 0..100 {
            reg.observe_wal_commit(a, 800_000);
        }
        sample_once(&shared, &mut prev).expect("slow window");
        assert!(reg.wal_commit_latency_p99_us(a) > 500_000);
        for _ in 0..100 {
            reg.observe_wal_commit(a, 40);
        }
        sample_once(&shared, &mut prev).expect("fast window");
        assert!(reg.wal_commit_latency_p99_us(a) <= 50);
        sample_once(&shared, &mut prev).expect("empty window");
        assert_eq!(reg.wal_commit_latency_p99_us(a), 0);
        let after_wal = load(&counters_a.maintenance_lease_micros_total);
        assert_eq!(after_wal, first);

        // An unchanged counter adds nothing. A new lease adds only its delta.
        sample_once(&shared, &mut prev).expect("second sample");
        assert_eq!(load(&counters_a.maintenance_lease_micros_total), first);
        hold_lease(&shared, a);
        sample_once(&shared, &mut prev).expect("third sample");
        let third = load(&counters_a.maintenance_lease_micros_total);
        assert!(third > first);

        // A cleared counter restarts from its new value.
        shared.maintenance_budget.clear_database(a);
        hold_lease(&shared, a);
        sample_once(&shared, &mut prev).expect("fourth sample");
        assert!(load(&counters_a.maintenance_lease_micros_total) > third);
    }

    #[test]
    fn sample_once_zeroes_gauges_of_databases_without_usage() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(31);
        shared.database_metrics.ensure_database(db);
        shared.database_metrics.set_memory_bytes(db, 77);
        let mut prev = SamplerState::default();
        sample_once(&shared, &mut prev).expect("sample");
        assert_eq!(shared.database_metrics.memory_bytes(db), 0);
        assert!(shared.database_metrics.get(db).is_some());
    }
}
