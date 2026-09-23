// SPDX-License-Identifier: BUSL-1.1

//! Node-wide record of Data Plane cores that fail-stopped.
//!
//! A core fail-stops when its state is unknown: a rollback failed part way,
//! or the work owed after a committed record's install failed. Such a core
//! refuses every request until restart. Its latch lives on the core. This
//! record is the node-wide view `/healthz`, the native `STATUS` and the
//! `nodedb_data_plane_core_fail_stopped` gauge read.
//!
//! The first report wins, like the Calvin halt and metadata-apply wedge
//! markers: a fail-stopped core stays stopped until restart, so a later
//! report cannot replace the cause an operator must act on. The gauge counts
//! every stopped core.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Why the first Data Plane core fail-stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreFailStopReport {
    pub core_id: usize,
    /// The cause label, as the core's ERROR line names it.
    pub cause: &'static str,
    pub detail: String,
}

/// First-report-wins record of fail-stopped Data Plane cores. Never clears.
#[derive(Debug, Default)]
pub struct CoreFailStops {
    first: OnceLock<CoreFailStopReport>,
    stopped: AtomicU64,
}

impl CoreFailStops {
    /// Record one core that fail-stopped. A core reports once.
    pub fn record(&self, report: CoreFailStopReport) {
        self.stopped.fetch_add(1, Ordering::Relaxed);
        let _ = self.first.set(report);
    }

    /// The first core that fail-stopped, if one did.
    pub fn report(&self) -> Option<&CoreFailStopReport> {
        self.first.get()
    }

    pub fn is_stopped(&self) -> bool {
        self.first.get().is_some()
    }

    /// Number of cores that fail-stopped.
    pub fn stopped_cores(&self) -> u64 {
        self.stopped.load(Ordering::Relaxed)
    }

    /// Append the `nodedb_data_plane_core_fail_stopped` gauge.
    pub fn write_prometheus(&self, out: &mut String) {
        use std::fmt::Write as _;
        let _ = writeln!(
            out,
            "# HELP nodedb_data_plane_core_fail_stopped Data Plane cores that stopped \
             serving because their state is unknown\n\
             # TYPE nodedb_data_plane_core_fail_stopped gauge\n\
             nodedb_data_plane_core_fail_stopped {}",
            self.stopped_cores()
        );
    }
}

/// Readiness-probe rendering for a node with a fail-stopped core: `503`,
/// degraded. The other cores keep serving.
pub fn to_http_response(
    report: &CoreFailStopReport,
    stopped_cores: u64,
) -> (axum::http::StatusCode, serde_json::Value) {
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        serde_json::json!({
            "status": "degraded",
            "reason": "data_plane_core_fail_stopped",
            "core_id": report.core_id,
            "cause": report.cause,
            "error": report.detail,
            "stopped_cores": stopped_cores,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(core_id: usize) -> CoreFailStopReport {
        CoreFailStopReport {
            core_id,
            cause: "rollback_failed",
            detail: "undo entry 3 failed".into(),
        }
    }

    #[test]
    fn a_fresh_record_reports_no_stopped_core() {
        let stops = CoreFailStops::default();
        assert!(!stops.is_stopped());
        assert_eq!(stops.stopped_cores(), 0);
    }

    #[test]
    fn the_first_report_wins_and_every_report_is_counted() {
        let stops = CoreFailStops::default();
        stops.record(report(2));
        stops.record(report(5));
        assert_eq!(stops.report().map(|r| r.core_id), Some(2));
        assert_eq!(stops.stopped_cores(), 2);
    }

    #[test]
    fn the_gauge_counts_stopped_cores() {
        let stops = CoreFailStops::default();
        stops.record(report(1));
        let mut out = String::new();
        stops.write_prometheus(&mut out);
        assert!(
            out.contains("nodedb_data_plane_core_fail_stopped 1"),
            "{out}"
        );
    }

    #[test]
    fn the_readiness_body_names_the_core_and_cause() {
        let (status, body) = to_http_response(&report(4), 1);
        assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["core_id"], serde_json::json!(4));
        assert_eq!(body["cause"], serde_json::json!("rollback_failed"));
    }
}
