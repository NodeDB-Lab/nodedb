// SPDX-License-Identifier: BUSL-1.1

//! `SharedState::open` + subsystem/cluster wiring + quota ceiling + login
//! rate-limit capacities — the boot phase that produces the final,
//! fully-wired `Arc<SharedState>`.

use std::sync::Arc;

use nodedb::ServerConfig;
use nodedb::bootstrap;
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::cluster::ClusterHandle;
use nodedb::control::startup::StartupGate;
use nodedb::control::state::SharedState;

/// Resources this phase borrows from Data Plane bootstrap to build
/// `SharedState`, bundled so the call site doesn't juggle six separate
/// `Arc::clone` arguments.
pub(crate) struct SharedStateInputs<'a> {
    pub(crate) dispatcher: Dispatcher,
    pub(crate) wal: Arc<nodedb::wal::WalManager>,
    pub(crate) quiesce: Arc<nodedb::bridge::quiesce::CollectionQuiesce>,
    pub(crate) array_catalog: nodedb::control::array_catalog::ArrayCatalogHandle,
    pub(crate) quarantine_registry: Arc<nodedb::storage::quarantine::QuarantineRegistry>,
    pub(crate) governor: Arc<nodedb_mem::governor::MemoryGovernor>,
    pub(crate) system_metrics: Arc<nodedb::control::metrics::SystemMetrics>,
    pub(crate) maintenance_budget: Arc<nodedb::control::maintenance::MaintenanceBudgetTracker>,
    pub(crate) cluster_handle: &'a ClusterHandle,
    pub(crate) startup_gate: &'a Arc<StartupGate>,
    pub(crate) root_span: &'a tracing::Span,
}

/// Open `SharedState`, wire subsystems/cluster handles into it, then apply
/// the global quota ceiling and login rate-limit capacities. Runs between
/// Data Plane bootstrap and the post-open catalog steps, kept out of
/// `main()` for readability.
pub(crate) async fn open_and_wire_state(
    config: &ServerConfig,
    inputs: SharedStateInputs<'_>,
) -> anyhow::Result<Arc<SharedState>> {
    let SharedStateInputs {
        dispatcher,
        wal,
        quiesce,
        array_catalog,
        quarantine_registry,
        governor,
        system_metrics,
        maintenance_budget,
        cluster_handle,
        startup_gate,
        root_span,
    } = inputs;

    // Create shared state with persistent system catalog. `system_metrics` is
    // the registry the Data-Plane cores already hold, so `/metrics` reads it.
    //
    // `is_cluster` is the static, deployment-time signal for surrogate
    // registry mode: whether this process was configured with a `[cluster]`
    // section at all — never a property of the seed list's length, and
    // never inferred from live topology gossip observed after `start_raft`
    // runs. A single-voter cluster (one seed, `[cluster]` present) is still
    // `Cluster` mode: it commits through Raft like any other group size.
    // No `[cluster]` section means no cluster transport and no way to ever
    // join a group, so it stays `Local` for the process lifetime.
    let is_cluster = config.cluster.is_some();
    let mut shared = SharedState::open(
        nodedb::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: Arc::clone(&quiesce),
            array_catalog: Arc::clone(&array_catalog),
            system_metrics: Arc::clone(&system_metrics),
        },
        Arc::clone(&wal),
        &config.catalog_path(),
        nodedb::control::state::OpenConfig {
            auth: &config.auth,
            egress: Arc::new(config.egress.checked_policy()?),
        },
        config.tuning.clone(),
        is_cluster,
        Arc::clone(&governor),
    )?;

    // Install startup gate, wire subsystems and cluster handles into SharedState.
    bootstrap::state_wiring::wire_state(
        &mut shared,
        config,
        startup_gate,
        cluster_handle,
        bootstrap::state_wiring::SharedStateComponents {
            quarantine_registry: Arc::clone(&quarantine_registry),
            array_catalog: Arc::clone(&array_catalog),
            maintenance_budget: Arc::clone(&maintenance_budget),
        },
        root_span,
    )
    .await?;

    // Wire global quota ceiling from server config so `ALTER DATABASE SET QUOTA`
    // can validate the sum-of-database-quotas against the cluster's physical
    // resources. `memory_limit` and `max_connections` are the only dimensions
    // the server config currently constrains; storage and QPS pass through as
    // zero (= no ceiling) until [server.storage_limit] / [server.qps_limit]
    // land. The ALTER handler treats zero on any dimension as "skip that check".
    {
        use nodedb::control::security::catalog::GlobalQuotaCeiling;
        let mem_u64 = u64::try_from(config.server.memory_limit).unwrap_or(u64::MAX);
        let conn_u64 = u64::try_from(config.server.max_connections).unwrap_or(u64::MAX);
        shared.set_quota_ceiling(GlobalQuotaCeiling {
            max_memory_bytes: mem_u64,
            max_storage_bytes: 0,
            max_qps: 0,
            max_connections: conn_u64,
        });
    }

    // Apply login rate-limit capacities from cluster config (or defaults).
    {
        let (ip_cap, user_cap) = config
            .cluster
            .as_ref()
            .map(|c| {
                (
                    c.login_attempts_per_ip_per_min,
                    c.login_attempts_per_user_per_min,
                )
            })
            .unwrap_or((30, 10));
        shared.rate_limiter.set_login_capacities(ip_cap, user_cap);
    }

    Ok(shared)
}
