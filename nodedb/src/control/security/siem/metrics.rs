// SPDX-License-Identifier: BUSL-1.1

//! Prometheus rendering for SIEM export buffering and delivery counters.
//!
//! An audit event that never reaches the SIEM is a compliance gap, and a
//! `tracing::warn!` about it is gone at the next restart. These counters make
//! both failure modes — buffer overflow and webhook delivery failure —
//! visible on the `/metrics` scrape, next to the metering drop counters that
//! already use this pattern.

use std::fmt::Write as _;

use super::exporter::SiemExporter;

/// Append SIEM export metrics to a Prometheus text-exposition buffer.
pub fn render_prometheus(siem: &SiemExporter, out: &mut String) {
    if !siem.is_configured() {
        return;
    }

    out.push_str(
        "# HELP nodedb_siem_dropped_events_total Audit events evicted from a SIEM export buffer without being delivered because the buffer hit its configured ceiling.\n",
    );
    out.push_str("# TYPE nodedb_siem_dropped_events_total counter\n");
    let _ = writeln!(
        out,
        "nodedb_siem_dropped_events_total{{buffer=\"audit\"}} {}",
        siem.dropped_audit_events()
    );
    let _ = writeln!(
        out,
        "nodedb_siem_dropped_events_total{{buffer=\"auth\"}} {}",
        siem.dropped_auth_events()
    );

    out.push_str(
        "# HELP nodedb_siem_delivered_events_total Events accepted by the SIEM webhook.\n",
    );
    out.push_str("# TYPE nodedb_siem_delivered_events_total counter\n");
    let _ = writeln!(
        out,
        "nodedb_siem_delivered_events_total {}",
        siem.delivered_events()
    );

    out.push_str(
        "# HELP nodedb_siem_delivery_failures_total SIEM webhook delivery attempts that failed; the batch stays buffered for retry.\n",
    );
    out.push_str("# TYPE nodedb_siem_delivery_failures_total counter\n");
    let _ = writeln!(
        out,
        "nodedb_siem_delivery_failures_total {}",
        siem.delivery_failures()
    );

    out.push_str("# HELP nodedb_siem_buffered_events Events currently awaiting SIEM delivery.\n");
    out.push_str("# TYPE nodedb_siem_buffered_events gauge\n");
    let _ = writeln!(out, "nodedb_siem_buffered_events {}", siem.buffered_count());

    out.push_str(
        "# HELP nodedb_siem_buffer_capacity Configured per-buffer ceiling for SIEM export.\n",
    );
    out.push_str("# TYPE nodedb_siem_buffer_capacity gauge\n");
    let _ = writeln!(
        out,
        "nodedb_siem_buffer_capacity {}",
        siem.buffer_capacity()
    );
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::{AuditEntry, AuditEvent};
    use crate::control::security::siem::config::SiemConfig;

    fn entry() -> AuditEntry {
        AuditEntry {
            seq: 1,
            timestamp_us: 0,
            event: AuditEvent::AdminAction,
            tenant_id: None,
            database_id: None,
            auth_user_id: String::new(),
            auth_user_name: String::new(),
            session_id: String::new(),
            source: "test".into(),
            detail: "d".into(),
            prev_hash: String::new(),
        }
    }

    #[test]
    fn unconfigured_exporter_renders_nothing() {
        let mut out = String::new();
        render_prometheus(
            &SiemExporter::strict_for_test(SiemConfig::default()),
            &mut out,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn surfaces_buffer_overflow_drops() {
        let exporter = SiemExporter::strict_for_test(SiemConfig {
            destinations: vec!["webhook".into()],
            buffer_size: 1,
            ..Default::default()
        });
        exporter.push_audit(entry());
        exporter.push_audit(entry()); // Evicts the first.

        let mut out = String::new();
        render_prometheus(&exporter, &mut out);
        assert!(out.contains("nodedb_siem_dropped_events_total{buffer=\"audit\"} 1"));
        assert!(out.contains("nodedb_siem_dropped_events_total{buffer=\"auth\"} 0"));
        assert!(out.contains("nodedb_siem_buffered_events 1"));
        assert!(out.contains("nodedb_siem_buffer_capacity 1"));
        assert!(out.contains("nodedb_siem_delivery_failures_total 0"));
    }
}
