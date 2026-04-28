//! Cluster startup entry point: dispatches to bootstrap, join, or restart.
//!
//! The decision tree is deliberately small and delegates every
//! non-trivial choice to a dedicated module:
//!
//! - **restart** (`super::restart`) — if the catalog already reports
//!   this node as bootstrapped, we always take the restart path,
//!   regardless of `seed_nodes` or `force_bootstrap`. The catalog is
//!   the authoritative source of truth once it exists.
//! - **bootstrap** (`super::bootstrap_fn`) — taken when this node is
//!   the elected bootstrapper (lowest-addr seed), or when the operator
//!   forced it via `ClusterConfig::force_bootstrap`, or when no other
//!   seed is running. See [`super::probe::should_bootstrap`].
//! - **join** (`super::join`) — everything else. The join path owns
//!   its own retry-with-backoff loop and leader-redirect handling, so
//!   this dispatcher does not need to retry at this layer.

use std::sync::Arc;
use std::time::Duration;

use nodedb_types::NodeId;

use crate::catalog::ClusterCatalog;
use crate::error::Result;
use crate::lifecycle_state::ClusterLifecycleTracker;
use crate::migration_executor::MigrationExecutor;
use crate::reachability::driver::ReachabilityDriverConfig;
use crate::rebalancer::driver::RebalancerLoopConfig;
use crate::subsystem::context::BootstrapCtx;
use crate::subsystem::health::ClusterHealth;
use crate::subsystem::{
    DecommissionSubsystem, ReachabilitySubsystem, RebalancerSubsystem, RunningCluster,
    SubsystemRegistry, SwimSubsystem, SwimSubsystemConfig,
};
use crate::transport::NexarTransport;

use super::bootstrap_fn::bootstrap;
use super::config::{ClusterConfig, ClusterState};
use super::join::join;
use super::probe::should_bootstrap;
use super::restart::restart;

/// Register the default set of cluster subsystems into `registry`.
///
/// Called by [`start_cluster`] after the initial cluster state is resolved
/// and the `BootstrapCtx` is assembled. The four subsystems are:
///
/// 1. `SwimSubsystem` (root, `deps = []`) — failure detector with
///    `RoutingLivenessHook` attached before the UDP socket opens.
///    `RoutingLivenessHook` is NOT its own subsystem; it is wired
///    inside `SwimSubsystem::start()` as a SWIM subscriber.
///
/// 2. `ReachabilitySubsystem` (deps: swim) — periodic probe loop that
///    drives the shared circuit breaker's Open → Half-Open transitions.
///
/// 3. `DecommissionSubsystem` (deps: swim) — polls local node topology
///    state and fires a shutdown signal when the node is decommissioned.
///
/// 4. `RebalancerSubsystem` (deps: swim + reachability) — load-based
///    vShard mover backed by `MigrationExecutor`.
///
/// `RoutingLivenessHook` is wired inside `SwimSubsystem` — it is not
/// registered as a top-level subsystem because it is sync/cheap and runs
/// directly on the SWIM detector event loop, not as a separate task.
pub fn register_default_subsystems(
    registry: &mut SubsystemRegistry,
    config: &ClusterConfig,
    ctx: &BootstrapCtx,
    executor: Arc<MigrationExecutor>,
) {
    let swim_cfg = SwimSubsystemConfig {
        swim: crate::swim::config::SwimConfig::default(),
        local_id: NodeId::new(config.node_id.to_string()),
        // Use the explicit SWIM UDP addr if set; otherwise let the OS
        // pick an ephemeral port by binding to port 0 on the listen addr.
        swim_addr: config.swim_udp_addr.unwrap_or_else(|| {
            let mut a = config.listen_addr;
            a.set_port(0);
            a
        }),
        seeds: config.seed_nodes.clone(),
    };

    registry.register(Arc::new(SwimSubsystem::new(
        swim_cfg,
        Arc::clone(&ctx.routing),
        Arc::clone(&ctx.topology),
        vec![],
    )));

    registry.register(Arc::new(ReachabilitySubsystem::new(
        ReachabilityDriverConfig::default(),
    )));

    registry.register(Arc::new(DecommissionSubsystem::new(
        ctx.transport.node_id(),
        Duration::from_secs(5),
    )));

    registry.register(Arc::new(RebalancerSubsystem::new(
        RebalancerLoopConfig::default(),
        executor,
    )));
}

/// Start the cluster state machine — bootstrap, join, or restart.
///
/// Returns the initialized [`ClusterState`] only. Subsystems are NOT
/// spawned here: they share an `Arc<Mutex<MultiRaft>>` with the
/// `RaftLoop`, which only exists after the host calls
/// [`crate::raft_loop::RaftLoop::new`]. The host therefore drives a
/// two-step startup:
///
/// ```text
/// 1. start_cluster(...)              -> ClusterState
/// 2. start_raft(...)                 -> RaftLoop owns multi_raft
/// 3. start_cluster_subsystems(...)   -> spawn subsystems sharing the
///                                       loop's multi_raft Arc
/// ```
///
/// This split exists because [`crate::raft_loop::RaftLoop::new`] takes
/// `MultiRaft` *by value* and re-wraps it in its own
/// `Arc<Mutex<MultiRaft>>`. If we registered subsystems before the
/// hand-off, every subsystem would hold a strong clone of the
/// pre-hand-off Arc, blocking `Arc::try_unwrap` in the host's
/// `init.rs`. The host runs `start_raft` between steps 1 and 3, then
/// passes the loop's shared multi_raft handle to step 3.
///
/// `lifecycle` is the caller-owned phase tracker. This function
/// transitions it to `Restarting` / `Bootstrapping` / `Joining` as
/// the dispatcher picks a branch, and to `Failed` on terminal error.
pub async fn start_cluster(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: Arc<NexarTransport>,
    lifecycle: &ClusterLifecycleTracker,
) -> Result<ClusterState> {
    // Authoritative catalog state wins — a previously bootstrapped
    // node always takes the restart path on boot.
    let cluster_state = if catalog.is_bootstrapped()? {
        lifecycle.to_restarting();
        restart(config, catalog, &transport).inspect_err(|e| {
            lifecycle.to_failed(format!("restart failed: {e}"));
        })?
    } else {
        // No existing state — decide bootstrap vs join.
        let is_seed = config.seed_nodes.contains(&config.listen_addr);
        if is_seed && should_bootstrap(config, &transport).await {
            lifecycle.to_bootstrapping();
            bootstrap(config, catalog).inspect_err(|e| {
                lifecycle.to_failed(format!("bootstrap failed: {e}"));
            })?
        } else {
            join(config, catalog, &transport, lifecycle).await?
        }
    };

    Ok(cluster_state)
}

/// Spawn the default cluster subsystems sharing `raft_multi_raft` with
/// the running [`crate::raft_loop::RaftLoop`].
///
/// Called after [`start_cluster`] has produced [`ClusterState`] and
/// the host has handed `MultiRaft` over to the `RaftLoop`. The host
/// passes `raft_multi_raft = raft_loop.multi_raft_handle()` here so
/// subsystems use the same `Arc<Mutex<MultiRaft>>` the loop owns —
/// no double-ownership, no orphan Arcs blocking shutdown.
///
/// The returned [`RunningCluster`] keeps subsystem background tasks
/// alive; dropping it signals all of them to shut down. The host
/// **must** call [`RunningCluster::shutdown_all`] explicitly during
/// orderly shutdown so subsystems release their `MultiRaft` Arc
/// before the loop exits.
pub async fn start_cluster_subsystems(
    config: &ClusterConfig,
    topology: Arc<std::sync::RwLock<crate::topology::ClusterTopology>>,
    routing: Arc<std::sync::RwLock<crate::routing::RoutingTable>>,
    transport: Arc<NexarTransport>,
    raft_multi_raft: Arc<std::sync::Mutex<crate::multi_raft::MultiRaft>>,
) -> Result<RunningCluster> {
    let health = ClusterHealth::new();
    let ctx = BootstrapCtx::new(
        Arc::clone(&topology),
        Arc::clone(&routing),
        Arc::clone(&transport),
        Arc::clone(&raft_multi_raft),
        health,
    );

    let executor = Arc::new(MigrationExecutor::new(
        Arc::clone(&raft_multi_raft),
        Arc::clone(&routing),
        Arc::clone(&topology),
        Arc::clone(&transport),
    ));

    let mut registry = SubsystemRegistry::new();
    register_default_subsystems(&mut registry, config, &ctx, executor);

    registry
        .start_all(&ctx)
        .await
        .map_err(|e| crate::error::ClusterError::Storage {
            detail: format!("subsystem start failed: {e}"),
        })
}
