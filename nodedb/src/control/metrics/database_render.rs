// SPDX-License-Identifier: BUSL-1.1

//! Prometheus text rendering for `DatabaseMetricsRegistry`.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use nodedb_types::DatabaseId;

use super::database::{DatabaseCounters, DatabaseMetricsRegistry};

/// Escape a label value per the Prometheus text format.
fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn emit(
    out: &mut String,
    name: &str,
    help: &str,
    kind: &str,
    rows: &[(String, Arc<DatabaseCounters>)],
    value: impl Fn(&mut String, &DatabaseCounters),
) {
    let _ = write!(out, "# HELP {name} {help}\n# TYPE {name} {kind}\n");
    for (label, c) in rows {
        let _ = write!(out, r#"{name}{{database="{label}"}} "#);
        value(out, c);
        out.push('\n');
    }
    out.push('\n');
}

fn load(
    f: impl Fn(&DatabaseCounters) -> &std::sync::atomic::AtomicU64,
) -> impl Fn(&mut String, &DatabaseCounters) {
    move |out, c| {
        let _ = write!(out, "{}", f(c).load(Ordering::Relaxed));
    }
}

impl DatabaseMetricsRegistry {
    /// Render all series as Prometheus text format 0.0.4.
    ///
    /// `names` maps each id to its current database name. An entry whose id
    /// has no name is not rendered. Labels: `database="<name>"`.
    pub fn render_prometheus(&self, names: &HashMap<DatabaseId, String>, out: &mut String) {
        let mut rows: Vec<(String, Arc<DatabaseCounters>)> = {
            let r = self.counters.read().unwrap_or_else(|p| p.into_inner());
            r.iter()
                .filter_map(|(id, c)| names.get(id).map(|n| (escape_label(n), Arc::clone(c))))
                .collect()
        };
        if rows.is_empty() {
            return;
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        emit(
            out,
            "nodedb_database_qps_total",
            "Cumulative requests admitted per database",
            "counter",
            &rows,
            load(|c| &c.qps_total),
        );
        emit(
            out,
            "nodedb_database_errors_total",
            "Cumulative query errors per database",
            "counter",
            &rows,
            load(|c| &c.errors_total),
        );
        emit(
            out,
            "nodedb_database_collections_total",
            "Collections registered per database",
            "gauge",
            &rows,
            load(|c| &c.collections),
        );
        emit(
            out,
            "nodedb_database_tenants_total",
            "Tenants registered per database",
            "gauge",
            &rows,
            load(|c| &c.tenants),
        );
        emit(
            out,
            "nodedb_database_memory_bytes",
            "Memory used per database in bytes",
            "gauge",
            &rows,
            load(|c| &c.memory_bytes),
        );
        emit(
            out,
            "nodedb_database_storage_bytes",
            "Storage used per database in bytes",
            "gauge",
            &rows,
            load(|c| &c.storage_bytes),
        );
        emit(
            out,
            "nodedb_database_connections",
            "Active connections per database",
            "gauge",
            &rows,
            load(|c| &c.connections),
        );
        emit(
            out,
            "nodedb_database_bridge_queue_depth",
            "SPSC bridge virtual-queue depth per database",
            "gauge",
            &rows,
            load(|c| &c.bridge_queue_depth),
        );
        emit(
            out,
            "nodedb_database_wal_commit_latency_p99_us",
            "WAL commit latency P99 in microseconds per database over the last sampling window",
            "gauge",
            &rows,
            load(|c| &c.wal_commit_latency_p99_us),
        );
        emit(
            out,
            "nodedb_database_maintenance_lease_seconds_total",
            "Cumulative wall seconds each database held a maintenance lease",
            "counter",
            &rows,
            |out, c| {
                let us = c.maintenance_lease_micros_total.load(Ordering::Relaxed);
                let _ = write!(out, "{:.6}", us as f64 / 1_000_000.0);
            },
        );
        emit(
            out,
            "nodedb_database_mirror_lag_ms",
            "Mirror replication lag in milliseconds (wall-clock now minus last apply); zero for non-mirror or promoted databases",
            "gauge",
            &rows,
            load(|c| &c.mirror_lag_ms),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(n: u64) -> DatabaseId {
        DatabaseId::new(n)
    }

    #[test]
    fn render_resolves_names_and_skips_unknown_ids() {
        let reg = DatabaseMetricsRegistry::new();
        reg.ensure_database(db(1));
        reg.ensure_database(db(2));
        reg.record_qps(db(1));
        reg.record_qps(db(1));
        reg.record_qps(db(2));
        let names = HashMap::from([(db(1), "alpha".to_string())]);
        let mut out = String::new();
        reg.render_prometheus(&names, &mut out);
        assert!(out.contains(r#"nodedb_database_qps_total{database="alpha"} 2"#));
        assert_eq!(out.matches("nodedb_database_qps_total{").count(), 1);
    }

    #[test]
    fn render_follows_rename() {
        let reg = DatabaseMetricsRegistry::new();
        reg.ensure_database(db(1));
        let mut out = String::new();
        reg.render_prometheus(&HashMap::from([(db(1), "new".to_string())]), &mut out);
        assert!(out.contains(r#"database="new""#));
    }

    #[test]
    fn render_emits_lease_seconds_and_wal_p99() {
        let reg = DatabaseMetricsRegistry::new();
        reg.ensure_database(db(1));
        reg.add_maintenance_lease_secs(db(1), 2.5);
        reg.observe_wal_commit(db(1), 90);
        let mut out = String::new();
        reg.render_prometheus(&HashMap::from([(db(1), "a".to_string())]), &mut out);
        assert!(
            out.contains(
                r#"nodedb_database_maintenance_lease_seconds_total{database="a"} 2.500000"#
            )
        );
        assert!(out.contains("wall seconds each database held a maintenance lease"));
        assert!(out.contains("nodedb_database_wal_commit_latency_p99_us{database=\"a\"}"));
    }

    #[test]
    fn render_empty_registry_writes_nothing() {
        let reg = DatabaseMetricsRegistry::new();
        let mut out = String::new();
        reg.render_prometheus(&HashMap::new(), &mut out);
        assert!(out.is_empty());
    }
}
