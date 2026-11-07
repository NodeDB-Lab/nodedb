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
        self.purge.write_prometheus(&mut out);
        self.io_metrics.write_prometheus(&mut out);
        self.spsc_throttle.write_prometheus(&mut out);
        self.dispatch_capacity.write_prometheus(&mut out);
        super::super::overload_responses::overload_responses().write_prometheus(&mut out);
        self.core_fail_stops.write_prometheus(&mut out);
        out
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

    /// Emit `nodedb_segments_quarantined_total` counters and
    /// `nodedb_segments_quarantined_active` gauges from a live registry
    /// snapshot.
    ///
    /// Labels are `engine`, `database_id`, `tenant_id`, and `collection`. A
    /// Raft snapshot chunk has no collection, so its scope labels are empty.
    /// The registry is the source of truth, so `SystemMetrics` holds no
    /// quarantine counter of its own.
    pub fn prometheus_segment_quarantine_active(
        out: &mut String,
        active_counts: &std::collections::HashMap<
            crate::storage::quarantine::QuarantineCountKey,
            u64,
        >,
    ) {
        use std::fmt::Write as _;
        if active_counts.is_empty() {
            return;
        }
        let mut pairs: Vec<_> = active_counts.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let series: Vec<(String, u64)> = pairs
            .into_iter()
            .map(|(key, count)| {
                let (database_id, tenant_id, collection) = match &key.scope {
                    Some(scope) => (
                        scope.database_id.to_string(),
                        scope.tenant_id.to_string(),
                        scope.collection.as_str(),
                    ),
                    None => (String::new(), String::new(), ""),
                };
                // A collection name is client text, so it can hold a quote or a newline.
                let collection = crate::control::metrics::database_render::escape_label(collection);
                let labels = format!(
                    r#"engine="{}",database_id="{database_id}",tenant_id="{tenant_id}",collection="{collection}""#,
                    key.engine
                );
                (labels, *count)
            })
            .collect();
        let _ = out.write_str(
            "# HELP nodedb_segments_quarantined_active Currently-quarantined segment count per engine and collection\n\
             # TYPE nodedb_segments_quarantined_active gauge\n",
        );
        for (labels, count) in &series {
            let _ = writeln!(
                out,
                "nodedb_segments_quarantined_active{{{labels}}} {count}"
            );
        }
        // Quarantine is permanent within a run, so the total equals the active count.
        let _ = out.write_str(
            "# HELP nodedb_segments_quarantined_total Cumulative segments quarantined due to repeated CRC failures\n\
             # TYPE nodedb_segments_quarantined_total counter\n",
        );
        for (labels, count) in &series {
            let _ = writeln!(out, "nodedb_segments_quarantined_total{{{labels}}} {count}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::storage::quarantine::{CollectionScope, QuarantineCountKey, QuarantineEngine};

    #[test]
    fn a_collection_name_cannot_break_out_of_its_label() {
        let mut counts = HashMap::new();
        counts.insert(
            QuarantineCountKey {
                engine: QuarantineEngine::Columnar,
                scope: Some(CollectionScope::new(1, 2, "a\"b\\c\nd")),
            },
            1u64,
        );
        let mut out = String::new();
        SystemMetrics::prometheus_segment_quarantine_active(&mut out, &counts);
        assert!(
            out.contains(r#"collection="a\"b\\c\nd""#),
            "the label must be escaped: {out}"
        );
    }
}
