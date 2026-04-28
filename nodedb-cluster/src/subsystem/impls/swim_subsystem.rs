//! [`SwimSubsystem`] — wraps the SWIM failure detector lifecycle.
//!
//! This is the root subsystem (no dependencies) that spawns the SWIM
//! run loop with all membership subscribers attached **before** the
//! UDP socket starts exchanging probes. This eliminates the first-rumour
//! race: `spawn_with_subscribers` adds subscribers to the
//! `FailureDetector` before `detector.run()` is called, so the very
//! first `on_state_change` callback fires on the first probe round.
//!
//! Subscribers attached here:
//! - [`RoutingLivenessHook`] — invalidates routing leader hints when
//!   SWIM marks a peer Suspect / Dead / Left.
//!
//! The rebalancer kick hook is wired separately by
//! [`RebalancerSubsystem`] via the kick `Arc<Notify>` returned during
//! construction.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use nodedb_types::NodeId;
use tokio::sync::watch;

use crate::routing::RoutingTable;
use crate::routing_liveness::{NodeIdResolver, RoutingLivenessHook};
use crate::swim::bootstrap::{SwimHandle, spawn_with_subscribers};
use crate::swim::config::SwimConfig;
use crate::swim::detector::UdpTransport;
use crate::swim::subscriber::MembershipSubscriber;
use crate::topology::ClusterTopology;

use super::super::context::BootstrapCtx;
use super::super::errors::{BootstrapError, ShutdownError};
use super::super::health::SubsystemHealth;
use super::super::r#trait::{ClusterSubsystem, SubsystemHandle};

/// Configuration required to start the SWIM failure detector.
#[derive(Clone)]
pub struct SwimSubsystemConfig {
    /// SWIM protocol tuning.
    pub swim: SwimConfig,
    /// This node's stable identity (used in member records).
    pub local_id: NodeId,
    /// The address the SWIM UDP socket will bind to.
    pub swim_addr: SocketAddr,
    /// Initial seed peers for the membership list.
    pub seeds: Vec<SocketAddr>,
}

/// Owns the SWIM failure detector lifetime.
///
/// `RoutingLivenessHook` is attached as a subscriber before the run
/// loop starts; other subsystems (Rebalancer) attach additional
/// subscribers via `extra_subscribers` at construction time.
pub struct SwimSubsystem {
    cfg: SwimSubsystemConfig,
    routing: Arc<RwLock<RoutingTable>>,
    topology: Arc<RwLock<ClusterTopology>>,
    extra_subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    /// Stashed handle after `start()` so `shutdown()` can stop it.
    handle: Mutex<Option<SwimHandle>>,
}

impl SwimSubsystem {
    pub fn new(
        cfg: SwimSubsystemConfig,
        routing: Arc<RwLock<RoutingTable>>,
        topology: Arc<RwLock<ClusterTopology>>,
        extra_subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    ) -> Self {
        Self {
            cfg,
            routing,
            topology,
            extra_subscribers,
            handle: Mutex::new(None),
        }
    }
}

#[async_trait]
impl ClusterSubsystem for SwimSubsystem {
    fn name(&self) -> &'static str {
        "swim"
    }

    fn dependencies(&self) -> &'static [&'static str] {
        &[]
    }

    async fn start(&self, _ctx: &BootstrapCtx) -> Result<SubsystemHandle, BootstrapError> {
        // Build a resolver that maps SWIM NodeId strings to routing-table
        // numeric ids by scanning the live topology.
        let topology = Arc::clone(&self.topology);
        let resolver: NodeIdResolver = Arc::new(move |node_id| {
            let topo = topology.read().unwrap_or_else(|p| p.into_inner());
            // Topology nodes are keyed by numeric u64; the SWIM NodeId
            // carries the same numeric id encoded as a decimal string in
            // production (placeholder "seed:…" entries are handled by the
            // `None` return below which is silently ignored by the hook).
            node_id
                .as_str()
                .parse::<u64>()
                .ok()
                .filter(|&id| topo.get_node(id).is_some())
        });

        let routing_hook = Arc::new(RoutingLivenessHook::new(
            Arc::clone(&self.routing),
            resolver,
        ));

        let mut subscribers: Vec<Arc<dyn MembershipSubscriber>> = vec![routing_hook];
        subscribers.extend(self.extra_subscribers.iter().cloned());

        let mac_key = _ctx.transport.mac_key();
        let transport = UdpTransport::bind(self.cfg.swim_addr, mac_key)
            .await
            .map_err(|e| BootstrapError::SubsystemStart {
                name: "swim",
                cause: Box::new(e),
            })?;

        let swim_handle = spawn_with_subscribers(
            self.cfg.swim.clone(),
            self.cfg.local_id.clone(),
            self.cfg.swim_addr,
            self.cfg.seeds.clone(),
            Arc::new(transport),
            subscribers,
        )
        .await
        .map_err(|e| BootstrapError::SubsystemStart {
            name: "swim",
            cause: Box::new(e),
        })?;

        // Extract the shutdown channel from the SWIM handle to build a
        // `SubsystemHandle`. We store the `SwimHandle` so `shutdown()`
        // can call `swim_handle.shutdown()` for graceful drain.
        //
        // The `SubsystemHandle` owns a dummy task that finishes immediately
        // — the real work is in the SWIM detector task that `SwimHandle`
        // holds. The shutdown watch is shared: when the registry sends
        // `true` on `shutdown_tx`, the SWIM detector's recv loop sees it
        // via the same receiver that `SwimHandle` already subscribes to.
        let (dummy_tx, _dummy_rx) = watch::channel(false);
        // We share the _real_ shutdown sender via the stored handle.
        // The subsystem handle's watch is used only for signalling; the
        // actual wait is done in `shutdown()` which calls `SwimHandle::shutdown`.
        let dummy_join = tokio::spawn(async {});
        let subsystem_handle = SubsystemHandle::new("swim", dummy_join, dummy_tx);

        {
            let mut guard = self.handle.lock().unwrap_or_else(|p| p.into_inner());
            *guard = Some(swim_handle);
        }

        Ok(subsystem_handle)
    }

    async fn shutdown(&self, deadline: Instant) -> Result<(), ShutdownError> {
        let maybe_handle = {
            let mut guard = self.handle.lock().unwrap_or_else(|p| p.into_inner());
            guard.take()
        };
        let Some(swim_handle) = maybe_handle else {
            return Ok(());
        };

        let timeout = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(timeout, swim_handle.shutdown()).await {
            Ok(()) => Ok(()),
            Err(_elapsed) => Err(ShutdownError::DeadlineExceeded { name: "swim" }),
        }
    }

    fn health(&self) -> SubsystemHealth {
        let guard = self.handle.lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_some() {
            SubsystemHealth::Running
        } else {
            SubsystemHealth::Stopped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swim_subsystem_name_and_deps() {
        let dummy_cfg = SwimSubsystemConfig {
            swim: crate::swim::config::SwimConfig {
                probe_interval: std::time::Duration::from_millis(100),
                probe_timeout: std::time::Duration::from_millis(40),
                indirect_probes: 2,
                suspicion_mult: 4,
                min_suspicion: std::time::Duration::from_millis(500),
                initial_incarnation: crate::swim::incarnation::Incarnation::ZERO,
                max_piggyback: 6,
                fanout_lambda: 3,
            },
            local_id: NodeId::new("1"),
            swim_addr: "127.0.0.1:0".parse().unwrap(),
            seeds: vec![],
        };
        let routing = Arc::new(RwLock::new(RoutingTable::uniform(1, &[1], 1)));
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        let s = SwimSubsystem::new(dummy_cfg, routing, topology, vec![]);
        assert_eq!(s.name(), "swim");
        assert!(s.dependencies().is_empty());
    }

    #[test]
    fn health_is_stopped_before_start() {
        let dummy_cfg = SwimSubsystemConfig {
            swim: crate::swim::config::SwimConfig {
                probe_interval: std::time::Duration::from_millis(100),
                probe_timeout: std::time::Duration::from_millis(40),
                indirect_probes: 2,
                suspicion_mult: 4,
                min_suspicion: std::time::Duration::from_millis(500),
                initial_incarnation: crate::swim::incarnation::Incarnation::ZERO,
                max_piggyback: 6,
                fanout_lambda: 3,
            },
            local_id: NodeId::new("1"),
            swim_addr: "127.0.0.1:0".parse().unwrap(),
            seeds: vec![],
        };
        let routing = Arc::new(RwLock::new(RoutingTable::uniform(1, &[1], 1)));
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        let s = SwimSubsystem::new(dummy_cfg, routing, topology, vec![]);
        assert_eq!(s.health(), SubsystemHealth::Stopped);
    }
}
