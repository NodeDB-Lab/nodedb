// SPDX-License-Identifier: BUSL-1.1

//! [`ReachabilityProber`] — the injection seam for reachability probes.
//!
//! Implementations:
//!
//! - [`TransportProber`] wraps an `Arc<NexarTransport>` and sends a
//!   `RaftRpc::Ping` to the peer as a recovery probe.
//!   `send_probe_rpc` goes past the open circuit, dials, and records the
//!   outcome on the breaker. The prober is a one-line adapter.
//! - [`NoopProber`] always succeeds. Useful for tests that only want
//!   to verify the loop's tick cadence and shutdown.
//!
//! Tests that want deterministic open→closed transitions construct
//! their own trait impls; see `tests/reachability_loop.rs`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::rpc_codec::{PingRequest, RaftRpc};
use crate::transport::NexarTransport;

/// Abstract probe operation over a single peer.
#[async_trait]
pub trait ReachabilityProber: Send + Sync {
    /// Send one probe to `peer`. Returns `Ok(())` iff the probe
    /// completed successfully (implying the peer is reachable).
    async fn probe(&self, peer: u64) -> Result<()>;
}

/// Production prober: sends a `Ping` via the live transport as a recovery
/// probe. An open circuit lets it through, and its outcome closes or
/// reopens the circuit. The driver does not need to bookkeep anything.
pub struct TransportProber {
    transport: Arc<NexarTransport>,
    self_node_id: u64,
}

impl TransportProber {
    pub fn new(transport: Arc<NexarTransport>, self_node_id: u64) -> Self {
        Self {
            transport,
            self_node_id,
        }
    }
}

#[async_trait]
impl ReachabilityProber for TransportProber {
    async fn probe(&self, peer: u64) -> Result<()> {
        let rpc = RaftRpc::Ping(PingRequest {
            sender_id: self.self_node_id,
            topology_version: 0,
            oldest_snapshot: crate::snapshot_board::NO_SNAPSHOT_REPORT,
        });
        self.transport.send_probe_rpc(peer, rpc).await.map(|_| ())
    }
}

/// Always-succeeds prober for cadence/shutdown tests.
pub struct NoopProber;

#[async_trait]
impl ReachabilityProber for NoopProber {
    async fn probe(&self, _peer: u64) -> Result<()> {
        Ok(())
    }
}
