// SPDX-License-Identifier: BUSL-1.1

//! Per-database quota usage tracking and Prometheus rendering.
//!
//! `DatabaseQuotaMetrics` is a snapshot struct produced on each scrape from
//! the live atomic counters embedded in `SystemMetrics`. It carries the same
//! resource dimensions as `TenantQuotaMetrics` plus infrastructure metrics
//! specific to the database layer (bridge queue depth, WAL commit latency,
//! and maintenance CPU seconds).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use nodedb_types::DatabaseId;

/// Live atomic counters for one database.
///
/// Stored inside `DatabaseMetricsRegistry` and updated from the hot path.
/// All fields are `AtomicU64`; counters are cumulative since process start.
#[derive(Debug, Default)]
pub struct DatabaseCounters {
    /// Cumulative requests admitted for this database.
    pub qps_total: AtomicU64,
    /// Current memory usage in bytes (sampled from governor).
    pub memory_bytes: AtomicU64,
    /// Current storage usage in bytes.
    pub storage_bytes: AtomicU64,
    /// Current active connection count.
    pub connections: AtomicU64,
    /// SPSC bridge virtual-queue depth snapshot.
    pub bridge_queue_depth: AtomicU64,
    /// WAL commit latency P99 in microseconds. Node-wide metric sampled from the node's
    /// single WalManager; every database reports the same node-wide value.
    pub wal_commit_latency_p99_us: AtomicU64,
    /// Cumulative maintenance CPU-seconds consumed by this database (in microseconds).
    pub maintenance_cpu_seconds_total: AtomicU64,
    /// Mirror replication lag in milliseconds (`now_ms - last_apply_ms`).
    /// Zero for non-mirror databases or a promoted mirror.
    pub mirror_lag_ms: AtomicU64,
}

/// Registry of per-database atomic counter handles.
///
/// `Arc<DatabaseCounters>` handles are cloned and handed to subsystems
/// (WFQ, WAL, governor) for lock-free updates. The registry is the
/// control-plane-side collection point for Prometheus scrapes.
#[derive(Debug, Default)]
pub struct DatabaseMetricsRegistry {
    /// Map from `DatabaseId` to its display name and live counter handle.
    ///
    /// The registry is write-locked only on first registration or removal of a database.
    counters: RwLock<HashMap<DatabaseId, (String, Arc<DatabaseCounters>)>>,
}

impl DatabaseMetricsRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create the counter handle for `(db_id, db_name)`.
    ///
    /// The returned `Arc<DatabaseCounters>` is stable for the process lifetime.
    pub fn get_or_create(&self, db_id: DatabaseId, db_name: &str) -> Arc<DatabaseCounters> {
        // Fast path: read lock.
        {
            let r = self.counters.read().unwrap_or_else(|p| p.into_inner());
            if let Some((_, c)) = r.get(&db_id) {
                return Arc::clone(c);
            }
        }
        // Slow path: write lock.
        let mut w = self.counters.write().unwrap_or_else(|p| p.into_inner());
        w.entry(db_id)
            .or_insert_with(|| (db_name.to_string(), Arc::new(DatabaseCounters::default())))
            .1
            .clone()
    }

    /// Remove all metrics for `db_id` from the registry (e.g. on DROP DATABASE).
    pub fn remove(&self, db_id: DatabaseId) {
        let mut w = self.counters.write().unwrap_or_else(|p| p.into_inner());
        w.remove(&db_id);
    }

    /// Increment the QPS counter for `db_id` by 1.
    pub fn record_qps(&self, db_id: DatabaseId, db_name: &str) {
        self.get_or_create(db_id, db_name)
            .qps_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Set the memory-bytes gauge for `db_id`.
    pub fn set_memory_bytes(&self, db_id: DatabaseId, db_name: &str, bytes: u64) {
        self.get_or_create(db_id, db_name)
            .memory_bytes
            .store(bytes, Ordering::Relaxed);
    }

    /// Set the storage-bytes gauge for `db_id`.
    pub fn set_storage_bytes(&self, db_id: DatabaseId, db_name: &str, bytes: u64) {
        self.get_or_create(db_id, db_name)
            .storage_bytes
            .store(bytes, Ordering::Relaxed);
    }

    /// Set the active-connections gauge for `db_id`.
    pub fn set_connections(&self, db_id: DatabaseId, db_name: &str, count: u64) {
        self.get_or_create(db_id, db_name)
            .connections
            .store(count, Ordering::Relaxed);
    }

    /// Set the bridge queue-depth snapshot for `db_id`.
    pub fn set_bridge_queue_depth(&self, db_id: DatabaseId, db_name: &str, depth: u64) {
        self.get_or_create(db_id, db_name)
            .bridge_queue_depth
            .store(depth, Ordering::Relaxed);
    }

    /// Set the WAL commit latency P99 (microseconds) for `db_id`.
    ///
    /// Note: WAL latency is a node-wide metric (one WalManager per node); all databases
    /// report the same node-wide latency.
    pub fn set_wal_latency_p99(&self, db_id: DatabaseId, db_name: &str, us: u64) {
        self.get_or_create(db_id, db_name)
            .wal_commit_latency_p99_us
            .store(us, Ordering::Relaxed);
    }

    /// Set the mirror replication lag in milliseconds for `db_id`.
    ///
    /// Computed as `now_ms - last_apply_ms` using wall-clock time. The
    /// bounded-staleness read rejection path uses the same wall-clock basis,
    /// so the metric and the rejection gate are always consistent.
    pub fn set_mirror_lag_ms(&self, db_id: DatabaseId, db_name: &str, lag_ms: u64) {
        self.get_or_create(db_id, db_name)
            .mirror_lag_ms
            .store(lag_ms, Ordering::Relaxed);
    }

    /// Set cumulative maintenance CPU-seconds for `db_id`.
    ///
    /// Converts to integer microseconds (rounded) to avoid `AtomicF64` complexity.
    /// Negative or non-finite inputs are clamped to zero so accidental underflow
    /// in upstream timing arithmetic cannot subtract from the cumulative counter.
    pub fn set_maintenance_cpu_secs(&self, db_id: DatabaseId, db_name: &str, secs: f64) {
        let us = if secs.is_finite() && secs > 0.0 {
            (secs * 1_000_000.0).round() as u64
        } else {
            0
        };
        self.get_or_create(db_id, db_name)
            .maintenance_cpu_seconds_total
            .store(us, Ordering::Relaxed);
    }

    /// Render all registered databases as Prometheus text format 0.0.4.
    ///
    /// Labels: `database="<name>"`.
    pub fn render_prometheus(&self, out: &mut String) {
        use std::fmt::Write as _;

        let r = self.counters.read().unwrap_or_else(|p| p.into_inner());
        if r.is_empty() {
            return;
        }

        let mut pairs: Vec<_> = r.values().map(|(name, c)| (name.as_str(), c)).collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));

        macro_rules! emit_counter {
            ($name:literal, $help:literal, $field:ident) => {
                let _ = write!(
                    out,
                    "# HELP {} {}\n# TYPE {} counter\n",
                    $name, $help, $name
                );
                for (db, c) in &pairs {
                    let _ = writeln!(
                        out,
                        r#"{}{{database="{}"}} {}"#,
                        $name,
                        db,
                        c.$field.load(Ordering::Relaxed)
                    );
                }
                out.push('\n');
            };
        }

        macro_rules! emit_gauge {
            ($name:literal, $help:literal, $field:ident) => {
                let _ = write!(out, "# HELP {} {}\n# TYPE {} gauge\n", $name, $help, $name);
                for (db, c) in &pairs {
                    let _ = writeln!(
                        out,
                        r#"{}{{database="{}"}} {}"#,
                        $name,
                        db,
                        c.$field.load(Ordering::Relaxed)
                    );
                }
                out.push('\n');
            };
        }

        emit_counter!(
            "nodedb_database_qps_total",
            "Cumulative requests admitted per database",
            qps_total
        );
        emit_gauge!(
            "nodedb_database_memory_used_bytes",
            "Current memory usage per database in bytes",
            memory_bytes
        );
        emit_gauge!(
            "nodedb_database_storage_used_bytes",
            "Current storage usage per database in bytes",
            storage_bytes
        );
        emit_gauge!(
            "nodedb_database_connections",
            "Active connections per database",
            connections
        );
        emit_gauge!(
            "nodedb_database_bridge_queue_depth",
            "SPSC bridge virtual-queue depth per database",
            bridge_queue_depth
        );
        emit_gauge!(
            "nodedb_database_wal_commit_latency_p99_us",
            "WAL commit latency P99 in microseconds (node-wide; shared across all databases)",
            wal_commit_latency_p99_us
        );
        emit_counter!(
            "nodedb_database_maintenance_cpu_us_total",
            "Cumulative maintenance CPU microseconds consumed per database",
            maintenance_cpu_seconds_total
        );
        emit_gauge!(
            "nodedb_database_mirror_lag_ms",
            "Mirror replication lag in milliseconds (wall-clock now minus last apply timestamp); \
             zero for non-mirror or promoted databases",
            mirror_lag_ms
        );
    }
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
    pub maintenance_cpu_seconds_used: f64,
    pub maintenance_cpu_pct_limit: u8,
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

    #[test]
    fn registry_get_or_create_idempotent() {
        let reg = DatabaseMetricsRegistry::new();
        let db = DatabaseId::new(1);
        let a = reg.get_or_create(db, "mydb");
        let b = reg.get_or_create(db, "mydb");
        // Both arcs point to the same allocation.
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn record_qps_increments() {
        let reg = DatabaseMetricsRegistry::new();
        let db = DatabaseId::new(1);
        reg.record_qps(db, "db1");
        reg.record_qps(db, "db1");
        let c = reg.get_or_create(db, "db1");
        assert_eq!(c.qps_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn render_prometheus_contains_labels() {
        let reg = DatabaseMetricsRegistry::new();
        reg.record_qps(DatabaseId::new(1), "alpha");
        reg.record_qps(DatabaseId::new(2), "beta");
        let mut out = String::new();
        reg.render_prometheus(&mut out);
        assert!(out.contains(r#"database="alpha""#));
        assert!(out.contains(r#"database="beta""#));
        assert!(out.contains("nodedb_database_qps_total"));
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
            maintenance_cpu_seconds_used: 0.0,
            maintenance_cpu_pct_limit: 25,
        };
        assert!(m.is_over_quota());
    }

    #[test]
    fn gauge_setters_update_counters() {
        let reg = DatabaseMetricsRegistry::new();
        let db = DatabaseId::new(1);
        reg.set_connections(db, "db1", 42);
        reg.set_memory_bytes(db, "db1", 1024 * 1024);
        reg.set_storage_bytes(db, "db1", 10 * 1024 * 1024);
        reg.set_bridge_queue_depth(db, "db1", 7);
        reg.set_wal_latency_p99(db, "db1", 1500);

        let c = reg.get_or_create(db, "db1");
        assert_eq!(c.connections.load(Ordering::Relaxed), 42);
        assert_eq!(c.memory_bytes.load(Ordering::Relaxed), 1024 * 1024);
        assert_eq!(c.storage_bytes.load(Ordering::Relaxed), 10 * 1024 * 1024);
        assert_eq!(c.bridge_queue_depth.load(Ordering::Relaxed), 7);
        assert_eq!(c.wal_commit_latency_p99_us.load(Ordering::Relaxed), 1500);
    }

    #[test]
    fn remove_clears_database_metrics() {
        let reg = DatabaseMetricsRegistry::new();
        let db = DatabaseId::new(99);
        reg.set_connections(db, "db_to_drop", 10);
        assert_eq!(
            reg.get_or_create(db, "db_to_drop")
                .connections
                .load(Ordering::Relaxed),
            10
        );
        reg.remove(db);
        let mut out = String::new();
        reg.render_prometheus(&mut out);
        assert!(!out.contains(r#"database="db_to_drop""#));
    }
}
