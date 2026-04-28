//! `BootstrapCtx` — shared context passed to every subsystem at start time.
//!
//! This struct is a plain value-bag of `Arc`-wrapped cluster components.
//! Subsystems read from it during `start()` to obtain the handles they need.
//! Nothing is spawned from here.

use std::sync::{Arc, Mutex, RwLock};

use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;
use crate::transport::NexarTransport;

use super::health::ClusterHealth;

/// Context available to every subsystem at startup.
///
/// All fields are `Arc`-wrapped so they can be cheaply cloned into
/// long-lived subsystem tasks without requiring `BootstrapCtx` itself
/// to be `'static`.
///
/// `topology` and `routing` are behind `RwLock` so subsystems that
/// receive live state updates (e.g. `DecommissionObserver`,
/// `RoutingLivenessHook`) can observe mutations without copying the
/// full snapshot on every tick.
///
/// `multi_raft` is behind a `Mutex` because `MultiRaft::tick` takes
/// `&mut self` — the same type used by `MigrationExecutor`.
pub struct BootstrapCtx {
    /// Cluster topology — current node/group membership view.
    pub topology: Arc<RwLock<ClusterTopology>>,

    /// Routing table — maps vShards to Raft groups and peer addresses.
    pub routing: Arc<RwLock<RoutingTable>>,

    /// QUIC transport layer — used by subsystems that need to send RPCs
    /// to peers (e.g. SWIM, decommission coordinator).
    pub transport: Arc<NexarTransport>,

    /// Multi-Raft handle — subsystems that need to propose or read log
    /// entries obtain a reference here.
    pub multi_raft: Arc<Mutex<MultiRaft>>,

    /// Shared health aggregator — subsystems write their own health state
    /// here so the registry and monitoring layer can observe it.
    pub health: ClusterHealth,
}

impl BootstrapCtx {
    /// Construct a new context from its component parts.
    pub fn new(
        topology: Arc<RwLock<ClusterTopology>>,
        routing: Arc<RwLock<RoutingTable>>,
        transport: Arc<NexarTransport>,
        multi_raft: Arc<Mutex<MultiRaft>>,
        health: ClusterHealth,
    ) -> Self {
        Self {
            topology,
            routing,
            transport,
            multi_raft,
            health,
        }
    }
}
