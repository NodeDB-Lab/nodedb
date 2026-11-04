// SPDX-License-Identifier: BUSL-1.1

//! Per-database metric counters, keyed by `DatabaseId`.
//!
//! The registry is the single home of every per-database series. Names
//! resolve at render time from a caller-supplied id-to-name map, so a
//! rename or drop never leaves a stale series. Hot-path writers update an
//! existing entry only. `ensure_database` creates entries, so an in-flight
//! request after DROP cannot resurrect a series.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use nodedb_types::DatabaseId;

use super::histogram::AtomicHistogram;

/// Live atomic counters for one database.
///
/// Counters are cumulative since process start. Gauges hold the last sample.
#[derive(Debug, Default)]
pub struct DatabaseCounters {
    /// Cumulative requests admitted.
    pub qps_total: AtomicU64,
    /// Cumulative query errors.
    pub errors_total: AtomicU64,
    /// Current collection count.
    pub collections: AtomicU64,
    /// Current tenant count.
    pub tenants: AtomicU64,
    /// Current memory usage in bytes.
    pub memory_bytes: AtomicU64,
    /// Current storage usage in bytes.
    pub storage_bytes: AtomicU64,
    /// Storage limit in bytes. Zero means unlimited or unknown.
    pub storage_limit_bytes: AtomicU64,
    /// Current active connection count.
    pub connections: AtomicU64,
    /// SPSC bridge virtual-queue depth snapshot.
    pub bridge_queue_depth: AtomicU64,
    /// WAL group-commit latency observations in microseconds.
    pub wal_commit_latency: AtomicHistogram,
    /// Cumulative wall microseconds spent holding a maintenance lease.
    pub maintenance_lease_micros_total: AtomicU64,
    /// Mirror replication lag in milliseconds (`now_ms - last_apply_ms`).
    /// Zero for non-mirror databases or a promoted mirror.
    pub mirror_lag_ms: AtomicU64,
}

/// Registry of per-database counter handles.
#[derive(Debug, Default)]
pub struct DatabaseMetricsRegistry {
    pub(super) counters: RwLock<HashMap<DatabaseId, Arc<DatabaseCounters>>>,
}

macro_rules! store_setter {
    ($(#[$doc:meta])* $fn:ident, $field:ident) => {
        $(#[$doc])*
        pub fn $fn(&self, db: DatabaseId, value: u64) {
            if let Some(c) = self.get(db) {
                c.$field.store(value, Ordering::Relaxed);
            }
        }
    };
}

macro_rules! load_reader {
    ($(#[$doc:meta])* $fn:ident, $field:ident) => {
        $(#[$doc])*
        pub fn $fn(&self, db: DatabaseId) -> u64 {
            self.get(db)
                .map_or(0, |c| c.$field.load(Ordering::Relaxed))
        }
    };
}

impl DatabaseMetricsRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create the entry for `db` if absent and return its handle.
    pub fn ensure_database(&self, db: DatabaseId) -> Arc<DatabaseCounters> {
        if let Some(c) = self.get(db) {
            return c;
        }
        let mut w = self.counters.write().unwrap_or_else(|p| p.into_inner());
        Arc::clone(w.entry(db).or_default())
    }

    /// Return the handle for `db`, or `None` when no entry exists.
    pub fn get(&self, db: DatabaseId) -> Option<Arc<DatabaseCounters>> {
        let r = self.counters.read().unwrap_or_else(|p| p.into_inner());
        r.get(&db).map(Arc::clone)
    }

    /// Remove every series of `db`.
    pub fn remove_database(&self, db: DatabaseId) {
        let mut w = self.counters.write().unwrap_or_else(|p| p.into_inner());
        w.remove(&db);
    }

    /// Count one admitted request. No-op when `db` has no entry.
    pub fn record_qps(&self, db: DatabaseId) {
        if let Some(c) = self.get(db) {
            c.qps_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Count one query error. No-op when `db` has no entry.
    pub fn record_error(&self, db: DatabaseId) {
        if let Some(c) = self.get(db) {
            c.errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record one WAL group-commit latency in microseconds.
    /// No-op when `db` has no entry.
    pub fn observe_wal_commit(&self, db: DatabaseId, micros: u64) {
        if let Some(c) = self.get(db) {
            c.wal_commit_latency.observe(micros);
        }
    }

    /// WAL commit latency P99 in microseconds. Zero without observations.
    pub fn wal_commit_latency_p99_us(&self, db: DatabaseId) -> u64 {
        self.get(db)
            .map_or(0, |c| c.wal_commit_latency.percentile(0.99))
    }

    /// Add wall seconds spent holding a maintenance lease.
    ///
    /// Stored as integer microseconds. Negative or non-finite input adds zero.
    pub fn add_maintenance_lease_secs(&self, db: DatabaseId, secs: f64) {
        let us = if secs.is_finite() && secs > 0.0 {
            (secs * 1_000_000.0).round() as u64
        } else {
            0
        };
        if us == 0 {
            return;
        }
        if let Some(c) = self.get(db) {
            c.maintenance_lease_micros_total
                .fetch_add(us, Ordering::Relaxed);
        }
    }

    store_setter!(
        /// Set the collection-count gauge.
        set_collections, collections
    );
    store_setter!(
        /// Set the tenant-count gauge.
        set_tenants, tenants
    );
    store_setter!(
        /// Set the memory-bytes gauge.
        set_memory_bytes, memory_bytes
    );
    store_setter!(
        /// Set the storage-bytes gauge.
        set_storage_bytes, storage_bytes
    );
    store_setter!(
        /// Set the storage limit in bytes. Zero means unlimited.
        set_storage_limit, storage_limit_bytes
    );
    store_setter!(
        /// Set the active-connections gauge.
        set_connections, connections
    );
    store_setter!(
        /// Set the bridge queue-depth snapshot.
        set_bridge_queue_depth, bridge_queue_depth
    );
    store_setter!(
        /// Set the mirror replication lag in milliseconds.
        ///
        /// Computed as `now_ms - last_apply_ms` on the wall clock, the same
        /// basis as the bounded-staleness read rejection.
        set_mirror_lag_ms, mirror_lag_ms
    );

    load_reader!(
        /// Storage limit in bytes. Zero means unlimited or unknown.
        storage_limit, storage_limit_bytes
    );
    load_reader!(
        /// Storage usage in bytes. Zero means unknown.
        storage_bytes, storage_bytes
    );
    load_reader!(
        /// Memory usage in bytes. Zero means unknown.
        memory_bytes, memory_bytes
    );
    load_reader!(
        /// Cumulative requests admitted. Zero means unknown.
        qps_total, qps_total
    );
}

/// Snapshot struct for a single database's quota metrics.
///
/// Produced from live counters and the catalog quota record on each SHOW
/// DATABASE USAGE query. Not used in the hot path — allocation is fine.
#[derive(Debug, Clone)]
pub struct DatabaseQuotaMetrics {
    pub database_id: DatabaseId,
    pub database_name: String,
    pub qps_total: u64,
    pub memory_bytes_used: u64,
    pub memory_bytes_limit: u64,
    pub storage_bytes_used: u64,
    pub storage_bytes_limit: u64,
    pub connections_active: u64,
    pub connections_limit: u64,
    pub bridge_queue_depth: u64,
    pub wal_commit_latency_p99_us: u64,
    pub maintenance_lease_seconds_used: f64,
    pub maintenance_lease_pct_limit: u8,
}

impl DatabaseQuotaMetrics {
    /// Whether any quota is exceeded.
    pub fn is_over_quota(&self) -> bool {
        (self.memory_bytes_limit > 0 && self.memory_bytes_used > self.memory_bytes_limit)
            || (self.storage_bytes_limit > 0 && self.storage_bytes_used > self.storage_bytes_limit)
            || (self.connections_limit > 0 && self.connections_active > self.connections_limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(n: u64) -> DatabaseId {
        DatabaseId::new(n)
    }

    #[test]
    fn ensure_database_is_idempotent() {
        let reg = DatabaseMetricsRegistry::new();
        let a = reg.ensure_database(db(1));
        let b = reg.ensure_database(db(1));
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn record_qps_increments_existing_entry() {
        let reg = DatabaseMetricsRegistry::new();
        reg.ensure_database(db(1));
        reg.record_qps(db(1));
        reg.record_qps(db(1));
        assert_eq!(reg.qps_total(db(1)), 2);
    }

    #[test]
    fn record_qps_on_unknown_id_creates_nothing() {
        let reg = DatabaseMetricsRegistry::new();
        reg.record_qps(db(9));
        reg.record_error(db(9));
        reg.set_memory_bytes(db(9), 5);
        reg.observe_wal_commit(db(9), 5);
        reg.add_maintenance_lease_secs(db(9), 1.0);
        assert!(reg.get(db(9)).is_none());
        assert!(reg.counters.read().unwrap().is_empty());
    }

    #[test]
    fn remove_database_drops_every_series() {
        let reg = DatabaseMetricsRegistry::new();
        reg.ensure_database(db(1));
        reg.record_qps(db(1));
        reg.set_storage_bytes(db(1), 10);
        reg.remove_database(db(1));
        let names = HashMap::from([(db(1), "a".to_string())]);
        let mut out = String::new();
        reg.render_prometheus(&names, &mut out);
        assert!(!out.contains(r#"database="a""#));
        assert_eq!(reg.storage_bytes(db(1)), 0);
        reg.record_qps(db(1));
        assert!(reg.get(db(1)).is_none());
    }

    #[test]
    fn wal_commit_p99_reads_back() {
        let reg = DatabaseMetricsRegistry::new();
        reg.ensure_database(db(1));
        assert_eq!(reg.wal_commit_latency_p99_us(db(1)), 0);
        for _ in 0..100 {
            reg.observe_wal_commit(db(1), 400);
        }
        let p99 = reg.wal_commit_latency_p99_us(db(1));
        assert!(p99 > 100 && p99 <= 500, "p99 was {p99}");
    }

    #[test]
    fn storage_limit_round_trips_and_defaults_to_zero() {
        let reg = DatabaseMetricsRegistry::new();
        assert_eq!(reg.storage_limit(db(1)), 0);
        reg.ensure_database(db(1));
        reg.set_storage_limit(db(1), 4096);
        assert_eq!(reg.storage_limit(db(1)), 4096);
    }

    #[test]
    fn maintenance_lease_ignores_non_positive_input() {
        let reg = DatabaseMetricsRegistry::new();
        reg.ensure_database(db(1));
        reg.add_maintenance_lease_secs(db(1), -1.0);
        reg.add_maintenance_lease_secs(db(1), f64::NAN);
        reg.add_maintenance_lease_secs(db(1), 1.5);
        let c = reg.ensure_database(db(1));
        assert_eq!(
            c.maintenance_lease_micros_total.load(Ordering::Relaxed),
            1_500_000
        );
    }

    #[test]
    fn over_quota_detection() {
        let m = DatabaseQuotaMetrics {
            database_id: DatabaseId::DEFAULT,
            database_name: "test".into(),
            qps_total: 0,
            memory_bytes_used: 200,
            memory_bytes_limit: 100,
            storage_bytes_used: 0,
            storage_bytes_limit: 0,
            connections_active: 0,
            connections_limit: 0,
            bridge_queue_depth: 0,
            wal_commit_latency_p99_us: 0,
            maintenance_lease_seconds_used: 0.0,
            maintenance_lease_pct_limit: 25,
        };
        assert!(m.is_over_quota());
    }
}
