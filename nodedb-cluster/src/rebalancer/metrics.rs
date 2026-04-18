//! Per-node load metrics and scoring.
//!
//! `LoadMetrics` is the raw per-node observation the rebalancer loop
//! consumes. `normalized_score` folds a `LoadMetrics` plus a set of
//! `LoadWeights` into a single `f64` so different nodes can be
//! compared on one axis — the hotter the score, the more work the
//! node is doing relative to the cluster.
//!
//! Weights are configurable because different workloads care about
//! different dimensions: a write-heavy OLTP cluster wants high
//! `writes` weight, an analytical cluster wants high `bytes`
//! weight, and a very uniform vshard layout wants high `vshards`
//! weight. The defaults (1.0 each) are a balanced starting point.

use async_trait::async_trait;

use crate::error::Result;

/// Raw load observation for a single node.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoadMetrics {
    pub node_id: u64,
    /// Count of vshards this node is currently leading.
    pub vshards_led: u32,
    /// Total bytes stored across all vshards on this node.
    pub bytes_stored: u64,
    /// Writes per second (rolling average, caller-defined window).
    pub writes_per_sec: f64,
    /// Reads per second (rolling average, caller-defined window).
    pub reads_per_sec: f64,
    /// Aggregate QPS across every vshard this node currently leads.
    /// Sourced from the per-vshard histograms that `SHOW RANGES`
    /// exposes — hot vshards pull their owning node's score up.
    pub qps_recent: f64,
    /// Hottest per-vshard p95 latency observed on this node, in
    /// microseconds. Used to bias the rebalancer away from nodes
    /// whose tail latency is degrading even when raw QPS looks fine.
    pub p95_latency_us: u64,
    /// Per-core CPU utilization (0.0–1.0). Used by the
    /// back-pressure gate to pause the rebalancer when the cluster
    /// is already stressed.
    pub cpu_utilization: f64,
}

/// Relative weights for the load dimensions. Scaled linearly; the
/// absolute values don't matter, only their ratios.
#[derive(Debug, Clone, Copy)]
pub struct LoadWeights {
    pub vshards: f64,
    pub bytes: f64,
    pub writes: f64,
    pub reads: f64,
    /// Weight on aggregate per-vshard QPS. Defaults to the same as
    /// `reads` so a cluster with only point-reads doesn't suddenly
    /// double-count them; operators dial this up when they see
    /// hotspotting that plain `reads_per_sec` failed to catch.
    pub qps: f64,
    /// Weight on p95 latency, expressed per millisecond. The
    /// default (`0.001`) keeps latency an order of magnitude below
    /// throughput signals so ordinary traffic doesn't trip moves.
    pub latency_per_ms: f64,
}

impl Default for LoadWeights {
    fn default() -> Self {
        Self {
            vshards: 1.0,
            bytes: 1.0,
            writes: 1.0,
            reads: 1.0,
            qps: 1.0,
            latency_per_ms: 0.001,
        }
    }
}

/// Collapse a `LoadMetrics` observation into a single scalar score
/// using `weights`. Higher = hotter.
///
/// The implementation is a straightforward weighted sum — each field
/// is scaled by its weight and added. Bytes are divided by a
/// reasonable unit (1 MiB) so the float stays in a comparable range
/// to the per-second rates; otherwise a moderately-sized dataset
/// would swamp the qps signal entirely.
pub fn normalized_score(m: &LoadMetrics, weights: &LoadWeights) -> f64 {
    const BYTES_UNIT: f64 = 1_048_576.0; // 1 MiB
    let p95_ms = m.p95_latency_us as f64 / 1_000.0;
    weights.vshards * m.vshards_led as f64
        + weights.bytes * (m.bytes_stored as f64 / BYTES_UNIT)
        + weights.writes * m.writes_per_sec
        + weights.reads * m.reads_per_sec
        + weights.qps * m.qps_recent
        + weights.latency_per_ms * p95_ms
}

/// Injection seam for collecting load metrics from every node in the
/// cluster. Production impls talk to the metrics endpoint via the
/// transport; tests inject synthetic values.
#[async_trait]
pub trait LoadMetricsProvider: Send + Sync {
    /// Return a snapshot of every known node's current load metrics.
    /// The returned slice may be in any order — the rebalancer plan
    /// sorts internally for determinism.
    async fn snapshot(&self) -> Result<Vec<LoadMetrics>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(id: u64, v: u32, bytes_mib: u64, w: f64, r: f64) -> LoadMetrics {
        LoadMetrics {
            node_id: id,
            vshards_led: v,
            bytes_stored: bytes_mib * 1_048_576,
            writes_per_sec: w,
            reads_per_sec: r,
            qps_recent: 0.0,
            p95_latency_us: 0,
            cpu_utilization: 0.0,
        }
    }

    #[test]
    fn default_weights_are_uniform() {
        let w = LoadWeights::default();
        assert_eq!(w.vshards, 1.0);
        assert_eq!(w.bytes, 1.0);
        assert_eq!(w.writes, 1.0);
        assert_eq!(w.reads, 1.0);
    }

    #[test]
    fn zero_metrics_score_zero() {
        let metrics = m(1, 0, 0, 0.0, 0.0);
        assert_eq!(normalized_score(&metrics, &LoadWeights::default()), 0.0);
    }

    #[test]
    fn score_sums_all_dimensions_with_default_weights() {
        // 4 vshards + 8 MiB + 2 wps + 3 rps = 17.0
        let metrics = m(1, 4, 8, 2.0, 3.0);
        let score = normalized_score(&metrics, &LoadWeights::default());
        assert!((score - 17.0).abs() < 1e-9);
    }

    #[test]
    fn weights_scale_dimensions_independently() {
        let metrics = m(1, 10, 0, 0.0, 0.0);
        let w = LoadWeights {
            vshards: 5.0,
            ..Default::default()
        };
        assert!((normalized_score(&metrics, &w) - 50.0).abs() < 1e-9);
    }

    #[test]
    fn hotter_node_has_higher_score() {
        let cold = m(1, 1, 1, 1.0, 1.0);
        let hot = m(2, 10, 100, 100.0, 100.0);
        let w = LoadWeights::default();
        assert!(normalized_score(&hot, &w) > normalized_score(&cold, &w));
    }

    #[test]
    fn bytes_scale_via_mib_unit() {
        // 1 MiB with bytes weight = 1.0 contributes 1.0, not 1_048_576.
        let metrics = m(1, 0, 1, 0.0, 0.0);
        assert!((normalized_score(&metrics, &LoadWeights::default()) - 1.0).abs() < 1e-9);
    }
}
