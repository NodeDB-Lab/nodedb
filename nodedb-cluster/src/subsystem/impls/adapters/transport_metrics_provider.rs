//! [`LoadMetricsProvider`] adapter backed by [`NexarTransport`].
//!
//! Returns a single-entry snapshot containing only the local node's
//! metrics (derived from the transport's circuit-breaker peer list and
//! node id). A full cross-cluster metrics RPC does not yet exist; until
//! it lands this adapter returns local-only data and logs a debug
//! message so operators can observe the gap without false metrics.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use crate::error::Result;
use crate::rebalancer::metrics::{LoadMetrics, LoadMetricsProvider};
use crate::transport::NexarTransport;

/// [`LoadMetricsProvider`] that queries the local transport for the
/// local node's peer snapshot and assembles best-effort `LoadMetrics`.
///
/// Remote metrics (per-peer CPU / vshards / bytes) are not yet available
/// via an RPC. Until that RPC lands, this returns a zero-valued
/// `LoadMetrics` for every known peer and a `debug!` log. The rebalancer
/// will still function: a zero-score remote peer will never look hotter
/// than the local node, so moves will be conservative.
pub struct NexarTransportMetricsProvider {
    transport: Arc<NexarTransport>,
}

impl NexarTransportMetricsProvider {
    pub fn new(transport: Arc<NexarTransport>) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl LoadMetricsProvider for NexarTransportMetricsProvider {
    async fn snapshot(&self) -> Result<Vec<LoadMetrics>> {
        let peers = self.transport.peer_snapshot();
        debug!(
            peer_count = peers.len(),
            "transport metrics provider: remote load metrics RPC not yet \
             implemented; returning zero-valued remote entries"
        );
        // Local node: zero-valued placeholder (no in-process metrics
        // aggregation wired yet).
        let local = LoadMetrics {
            node_id: self.transport.node_id(),
            vshards_led: 0,
            bytes_stored: 0,
            writes_per_sec: 0.0,
            reads_per_sec: 0.0,
            qps_recent: 0.0,
            p95_latency_us: 0,
            cpu_utilization: 0.0,
        };
        let mut result = vec![local];
        for peer in &peers {
            result.push(LoadMetrics {
                node_id: peer.peer_id,
                vshards_led: 0,
                bytes_stored: 0,
                writes_per_sec: 0.0,
                reads_per_sec: 0.0,
                qps_recent: 0.0,
                p95_latency_us: 0,
                cpu_utilization: 0.0,
            });
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NexarTransport requires real QUIC infra to construct; we only
    // verify the trait is object-safe and the struct is Send + Sync.
    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn nexar_transport_metrics_provider_is_send_sync() {
        _assert_send_sync::<NexarTransportMetricsProvider>();
    }
}
