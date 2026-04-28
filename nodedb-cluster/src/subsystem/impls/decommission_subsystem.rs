//! [`DecommissionSubsystem`] — wraps the [`DecommissionObserver`] lifecycle.
//!
//! Depends on `swim` because the observer polls topology state that is
//! populated by the metadata Raft group after SWIM has established cluster
//! membership.
//!
//! # Shutdown integration
//!
//! The observer emits its own `watch<bool>` when the local node reaches
//! `Decommissioned` state. That signal is the cooperative shutdown trigger
//! for all background tasks (SWIM detector, Raft loops, transport accept
//! loops). The wiring point for propagating this signal to the wider
//! cluster shutdown (`ShutdownWatch`) is declared here with a
//! `tracing::warn!` placeholder until the cluster-level `ShutdownWatch`
//! integration lands as a dedicated initiative.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::watch;
use tracing::warn;

use crate::decommission::observer::DecommissionObserver;

use super::super::context::BootstrapCtx;
use super::super::errors::{BootstrapError, ShutdownError};
use super::super::health::SubsystemHealth;
use super::super::r#trait::{ClusterSubsystem, SubsystemHandle};

/// Owns the decommission observer lifecycle.
pub struct DecommissionSubsystem {
    /// The numeric local node id.
    local_node_id: u64,
    /// How often the observer polls the topology for its own state.
    poll_interval: Duration,
}

impl DecommissionSubsystem {
    pub fn new(local_node_id: u64, poll_interval: Duration) -> Self {
        Self {
            local_node_id,
            poll_interval,
        }
    }
}

#[async_trait]
impl ClusterSubsystem for DecommissionSubsystem {
    fn name(&self) -> &'static str {
        "decommission"
    }

    fn dependencies(&self) -> &'static [&'static str] {
        &["swim"]
    }

    async fn start(&self, ctx: &BootstrapCtx) -> Result<SubsystemHandle, BootstrapError> {
        let (observer, decommission_rx) = DecommissionObserver::new(
            Arc::clone(&ctx.topology),
            self.local_node_id,
            self.poll_interval,
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // When the observer fires (local node decommissioned), it flips its
        // own watch channel. We bridge that signal to the subsystem's
        // shutdown watch so the registry sees a cooperative exit.
        //
        // TODO: also trigger cluster-wide ShutdownWatch here once that
        // integration initiative lands.
        let mut decommission_rx_clone = decommission_rx.clone();
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            while decommission_rx_clone.changed().await.is_ok() {
                if *decommission_rx_clone.borrow() {
                    warn!("decommission observer fired: local node is leaving cluster");
                    let _ = shutdown_tx_clone.send(true);
                    // Wiring point for cluster-wide ShutdownWatch:
                    //   cluster_shutdown_tx.send(true);
                    // This is left as a `warn!` until the ShutdownWatch
                    // integration initiative is scoped and landed.
                    return;
                }
            }
        });

        let task = tokio::spawn(async move { observer.run(shutdown_rx).await });

        Ok(SubsystemHandle::new("decommission", task, shutdown_tx))
    }

    async fn shutdown(&self, _deadline: Instant) -> Result<(), ShutdownError> {
        // Driven by SubsystemHandle::shutdown_tx — no extra state needed.
        Ok(())
    }

    fn health(&self) -> SubsystemHealth {
        SubsystemHealth::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decommission_name_and_deps() {
        let s = DecommissionSubsystem::new(1, Duration::from_secs(5));
        assert_eq!(s.name(), "decommission");
        assert_eq!(s.dependencies(), &["swim"]);
    }
}
