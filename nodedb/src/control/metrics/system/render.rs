// SPDX-License-Identifier: BUSL-1.1

//! Prometheus text-format rendering for `SystemMetrics`.

use super::fields::SystemMetrics;

impl SystemMetrics {
    /// Serialize all metrics as Prometheus text format 0.0.4.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(8192);
        self.prometheus_core(&mut out);
        self.prometheus_engines(&mut out);
        self.prometheus_catalog_sanity(&mut out);
        self.prometheus_shutdown_phases(&mut out);
        self.prometheus_cdc_stream_drops(&mut out);
        self.prometheus_backpressure(&mut out);
        self.prometheus_database_metrics(&mut out);
        self.purge.write_prometheus(&mut out);
        self.io_metrics.write_prometheus(&mut out);
        self.spsc_throttle.write_prometheus(&mut out);
        self.core_fail_stops.write_prometheus(&mut out);
        out
    }

    /// Emit per-database labeled counters and gauges.
    ///
    /// Six series:
    /// - `nodedb_database_queries_total{database="..."}` — cumulative queries
    /// - `nodedb_database_errors_total{database="..."}` — cumulative errors
    /// - `nodedb_database_collections_total{database="..."}` — current collection count
    /// - `nodedb_database_tenants_total{database="..."}` — tenant count (stub-zero)
    /// - `nodedb_database_memory_bytes{database="..."}` — memory usage (stub-zero)
    /// - `nodedb_database_storage_bytes{database="..."}` — storage usage (stub-zero)
    pub(super) fn prometheus_database_metrics(&self, out: &mut String) {
        use std::fmt::Write as _;

        let queries = self
            .database_queries_by_name
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !queries.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_database_queries_total Queries executed against each database\n\
                 # TYPE nodedb_database_queries_total counter\n",
            );
            let mut pairs: Vec<_> = queries.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (db, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_database_queries_total{{database="{db}"}} {count}"#
                );
            }
        }
        drop(queries);

        let errors = self
            .database_errors_by_name
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !errors.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_database_errors_total Query errors against each database\n\
                 # TYPE nodedb_database_errors_total counter\n",
            );
            let mut pairs: Vec<_> = errors.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (db, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_database_errors_total{{database="{db}"}} {count}"#
                );
            }
        }
        drop(errors);

        let colls = self
            .database_collections_by_name
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !colls.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_database_collections_total Collections registered in each database\n\
                 # TYPE nodedb_database_collections_total gauge\n",
            );
            let mut pairs: Vec<_> = colls.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (db, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_database_collections_total{{database="{db}"}} {count}"#
                );
            }
        }
        drop(colls);

        let tenants = self
            .database_tenants_by_name
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !tenants.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_database_tenants_total Tenants registered in each database\n\
                 # TYPE nodedb_database_tenants_total gauge\n",
            );
            let mut pairs: Vec<_> = tenants.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (db, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_database_tenants_total{{database="{db}"}} {count}"#
                );
            }
        }
        drop(tenants);

        let mem = self
            .database_memory_bytes_by_name
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !mem.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_database_memory_bytes Memory used by each database (bytes)\n\
                 # TYPE nodedb_database_memory_bytes gauge\n",
            );
            let mut pairs: Vec<_> = mem.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (db, bytes) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_database_memory_bytes{{database="{db}"}} {bytes}"#
                );
            }
        }
        drop(mem);

        let storage = self
            .database_storage_bytes_by_name
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !storage.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_database_storage_bytes Storage used by each database (bytes)\n\
                 # TYPE nodedb_database_storage_bytes gauge\n",
            );
            let mut pairs: Vec<_> = storage.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (db, bytes) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_database_storage_bytes{{database="{db}"}} {bytes}"#
                );
            }
        }
    }

    /// Increment the per-database query counter.
    pub fn record_database_query(&self, db_name: &str) {
        if let Ok(mut m) = self.database_queries_by_name.write() {
            *m.entry(db_name.to_string()).or_insert(0) += 1;
        }
    }

    /// Increment the per-database error counter.
    pub fn record_database_error(&self, db_name: &str) {
        if let Ok(mut m) = self.database_errors_by_name.write() {
            *m.entry(db_name.to_string()).or_insert(0) += 1;
        }
    }

    /// Set (or update) the per-database collection count gauge.
    pub fn set_database_collections(&self, db_name: &str, count: u64) {
        if let Ok(mut m) = self.database_collections_by_name.write() {
            m.insert(db_name.to_string(), count);
        }
    }

    /// Set (or update) the per-database tenant count gauge.
    ///
    /// Stub-zero at registration; per-database tenant tracking is wired
    /// alongside the per-database quota work.
    pub fn set_database_tenants(&self, db_name: &str, count: u64) {
        if let Ok(mut m) = self.database_tenants_by_name.write() {
            m.insert(db_name.to_string(), count);
        }
    }

    /// Set (or update) the per-database memory usage gauge (bytes).
    ///
    /// Stub-zero at database creation; per-database memory accounting is wired
    /// alongside the memory governor per-database budget work.
    pub fn set_database_memory_bytes(&self, db_name: &str, bytes: u64) {
        if let Ok(mut m) = self.database_memory_bytes_by_name.write() {
            m.insert(db_name.to_string(), bytes);
        }
    }

    /// Read the current per-database memory gauge (bytes). Returns 0 if no
    /// sample has been recorded yet — `0` is a valid gauge state, not a
    /// "missing" sentinel. Used by `SHOW DATABASE USAGE` to render live usage.
    pub fn database_memory_bytes(&self, db_name: &str) -> u64 {
        self.database_memory_bytes_by_name
            .read()
            .ok()
            .and_then(|m| m.get(db_name).copied())
            .unwrap_or(0)
    }

    /// Read the current per-database storage gauge (bytes). See
    /// [`Self::database_memory_bytes`] for "no sample" semantics.
    pub fn database_storage_bytes(&self, db_name: &str) -> u64 {
        self.database_storage_bytes_by_name
            .read()
            .ok()
            .and_then(|m| m.get(db_name).copied())
            .unwrap_or(0)
    }

    /// Read the cumulative per-database query counter. Cumulative since
    /// process start — not a rate. `SHOW DATABASE USAGE` exposes this for
    /// `max_qps`-budgeted workloads as the raw counter; rate computation is
    /// the caller's responsibility (delta over a sampling window).
    pub fn database_queries_total(&self, db_name: &str) -> u64 {
        self.database_queries_by_name
            .read()
            .ok()
            .and_then(|m| m.get(db_name).copied())
            .unwrap_or(0)
    }

    /// Set (or update) the per-database storage usage gauge (bytes).
    ///
    /// Stub-zero at database creation; per-database storage accounting is wired
    /// alongside the compaction per-database tracking work.
    pub fn set_database_storage_bytes(&self, db_name: &str, bytes: u64) {
        if let Ok(mut m) = self.database_storage_bytes_by_name.write() {
            m.insert(db_name.to_string(), bytes);
        }
    }

    pub(super) fn prometheus_backpressure(&self, out: &mut String) {
        use std::fmt::Write as _;
        let critical = self
            .backpressure_critical_by_engine
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let emergency = self
            .backpressure_emergency_by_engine
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !critical.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_backpressure_critical_total Write handlers that entered Critical-pressure flush path\n\
                 # TYPE nodedb_backpressure_critical_total counter\n",
            );
            let mut pairs: Vec<_> = critical.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (engine, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_backpressure_critical_total{{engine="{engine}"}} {count}"#
                );
            }
        }
        if !emergency.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_backpressure_emergency_total Write handlers rejected by Emergency-pressure\n\
                 # TYPE nodedb_backpressure_emergency_total counter\n",
            );
            let mut pairs: Vec<_> = emergency.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (engine, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_backpressure_emergency_total{{engine="{engine}"}} {count}"#
                );
            }
        }
    }

    /// Emit `nodedb_cdc_events_dropped_total{tenant,stream}` labelled counters.
    pub(super) fn prometheus_cdc_stream_drops(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .cdc_events_dropped_by_stream
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_cdc_events_dropped_total CDC events dropped from stream buffers due to overflow\n\
             # TYPE nodedb_cdc_events_dropped_total counter\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for ((tenant_id, stream_name), count) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_cdc_events_dropped_total{{tenant="{tenant_id}",stream="{stream_name}"}} {count}"#
            );
        }
    }

    /// Emit `nodedb_shutdown_phase_duration_seconds{phase}` gauges.
    pub(super) fn prometheus_shutdown_phases(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .shutdown_phase_durations_ms
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_shutdown_phase_duration_seconds Duration of each shutdown phase in the last graceful shutdown\n\
             # TYPE nodedb_shutdown_phase_duration_seconds gauge\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (phase, ms) in pairs {
            let secs = *ms as f64 / 1_000.0;
            let _ = writeln!(
                out,
                r#"nodedb_shutdown_phase_duration_seconds{{phase="{phase}"}} {secs}"#
            );
        }
    }

    /// Emit `nodedb_catalog_sanity_check_total{registry,outcome}` labeled counters.
    pub(super) fn prometheus_catalog_sanity(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .catalog_sanity_check_totals
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_catalog_sanity_check_total Catalog sanity check outcomes per registry\n\
             # TYPE nodedb_catalog_sanity_check_total counter\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for ((registry, outcome), count) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_catalog_sanity_check_total{{registry="{registry}",outcome="{outcome}"}} {count}"#
            );
        }
    }

    /// Emit `nodedb_segments_quarantined_total{engine,collection}` counters and
    /// `nodedb_segments_quarantined_active{engine,collection}` gauges from a
    /// live registry snapshot.
    ///
    /// Called from the `/metrics` HTTP handler which has direct access to
    /// `SharedState::quarantine_registry`. The `SystemMetrics` struct does not
    /// hold a quarantine counter to avoid requiring a notification path between
    /// the registry and the metrics store — the registry is the source of truth.
    pub fn prometheus_segment_quarantine_active(
        out: &mut String,
        active_counts: &std::collections::HashMap<(String, String), u64>,
    ) {
        use std::fmt::Write as _;
        if active_counts.is_empty() {
            return;
        }
        let mut pairs: Vec<_> = active_counts.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let _ = out.write_str(
            "# HELP nodedb_segments_quarantined_active Currently-quarantined segment count per engine and collection\n\
             # TYPE nodedb_segments_quarantined_active gauge\n",
        );
        for ((engine, collection), count) in &pairs {
            let _ = writeln!(
                out,
                r#"nodedb_segments_quarantined_active{{engine="{engine}",collection="{collection}"}} {count}"#
            );
        }
        // Emit total (same value per process run — quarantines are permanent within a run).
        let _ = out.write_str(
            "# HELP nodedb_segments_quarantined_total Cumulative segments quarantined due to repeated CRC failures\n\
             # TYPE nodedb_segments_quarantined_total counter\n",
        );
        for ((engine, collection), count) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_segments_quarantined_total{{engine="{engine}",collection="{collection}"}} {count}"#
            );
        }
    }
}
