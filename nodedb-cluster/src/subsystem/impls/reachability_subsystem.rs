//! [`ReachabilitySubsystem`] — wraps the [`ReachabilityDriver`] lifecycle.
//!
//! Depends on `swim` because the driver probes peers that SWIM knows
//! about via the shared [`CircuitBreaker`]. The driver only fires if
//! there are open-circuit peers; SWIM must be up to start accumulating
//! failures that open breakers.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::reachability::driver::{ReachabilityDriver, ReachabilityDriverConfig};
use crate::reachability::prober::TransportProber;

use super::super::context::BootstrapCtx;
use super::super::errors::{BootstrapError, ShutdownError};
use super::super::health::SubsystemHealth;
use super::super::r#trait::{ClusterSubsystem, SubsystemHandle};

/// Owns the reachability driver lifecycle.
pub struct ReachabilitySubsystem {
    cfg: ReachabilityDriverConfig,
}

impl ReachabilitySubsystem {
    pub fn new(cfg: ReachabilityDriverConfig) -> Self {
        Self { cfg }
    }
}

#[async_trait]
impl ClusterSubsystem for ReachabilitySubsystem {
    fn name(&self) -> &'static str {
        "reachability"
    }

    fn dependencies(&self) -> &'static [&'static str] {
        &["swim"]
    }

    async fn start(&self, ctx: &BootstrapCtx) -> Result<SubsystemHandle, BootstrapError> {
        let prober = Arc::new(TransportProber::new(
            Arc::clone(&ctx.transport),
            ctx.transport.node_id(),
        ));
        let breaker = Arc::clone(ctx.transport.circuit_breaker());
        let driver = Arc::new(ReachabilityDriver::new(breaker, prober, self.cfg.clone()));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move { driver.run(shutdown_rx).await });

        Ok(SubsystemHandle::new("reachability", task, shutdown_tx))
    }

    async fn shutdown(&self, _deadline: Instant) -> Result<(), ShutdownError> {
        // Shutdown is driven entirely by the `SubsystemHandle::shutdown_tx`
        // watch that the registry holds. The driver listens on that watch
        // and exits cleanly. Nothing extra needed here.
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
    fn reachability_name_and_deps() {
        let s = ReachabilitySubsystem::new(ReachabilityDriverConfig::default());
        assert_eq!(s.name(), "reachability");
        assert_eq!(s.dependencies(), &["swim"]);
    }
}
