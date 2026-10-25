// SPDX-License-Identifier: BUSL-1.1

//! Wiring of optional subsystems and cluster handles into SharedState.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::control::array_catalog::ArrayCatalogHandle;
use crate::control::cluster::ClusterHandle;
use crate::control::startup::StartupGate;
use crate::control::state::SharedState;
use crate::storage::quarantine::QuarantineRegistry;

/// Optional subsystem components wired into [`SharedState`] by [`wire_state`].
pub struct SharedStateComponents {
    pub quarantine_registry: Arc<QuarantineRegistry>,
    pub array_catalog: ArrayCatalogHandle,
    pub maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
}

/// Wire all optional subsystems into SharedState after `SharedState::open`.
///
/// This includes: startup gate, cluster handles, JWKS, cold storage, snapshot
/// storage, quarantine storage, backup KEK, OTLP exporter, gateway, and
/// bitemporal retention registry.
///
/// Async because JWKS provider discovery fetches each provider's key set over
/// the network. Bootstrap already runs inside the server's Tokio runtime, so
/// that fetch must be awaited — driving it with a nested `block_on` aborts
/// startup outright.
pub async fn wire_state(
    shared: &mut Arc<SharedState>,
    config: &ServerConfig,
    startup_gate: &Arc<StartupGate>,
    cluster_handle: &ClusterHandle,
    components: SharedStateComponents,
    root_span: &tracing::Span,
) -> anyhow::Result<()> {
    let SharedStateComponents {
        quarantine_registry,
        array_catalog,
        maintenance_budget,
    } = components;
    // Install startup gate. `/healthz` and the HTTP startup gate read it, so
    // a state left on the test helpers' pre-fired gate would report ready
    // and open every route during boot.
    //
    // The data directory is installed with it, before any step below reads
    // `state.data_dir`: the restore-generation seal and the PITR node life
    // read their files from it.
    let state = Arc::get_mut(shared).ok_or_else(|| {
        anyhow::anyhow!("startup gate: SharedState is already shared before the gate was installed")
    })?;
    state.startup = Arc::clone(startup_gate);
    state.data_dir = config.server.data_dir.clone();

    // Replay surrogate WAL records.
    // Note: wal_records are not passed here — caller must handle surrogate replay
    // before calling this function (it needs the catalog opened by SharedState::open).

    // Install quarantine registry.
    if let Some(state) = Arc::get_mut(shared) {
        state.quarantine_registry = Arc::clone(&quarantine_registry);
    }

    // Wire cluster handles. Every server runs one: a real cluster, or the
    // synthesized one-node cluster when `[cluster]` is absent.
    {
        let state = Arc::get_mut(shared).ok_or_else(|| {
            anyhow::anyhow!(
                "cluster wiring: SharedState is already shared before the cluster handle was installed"
            )
        })?;
        wire_cluster_handle(state, cluster_handle, &config.server.data_dir)?;
        root_span.record("node_id", cluster_handle.node_id);
    }

    // The JWKS registry verifies static and catalog OIDC providers alike.
    super::jwks_registry::wire_jwks_registry(shared, config).await?;

    // Initialise cold storage (L2 tiering and the WAL archive).
    if let Some(ref cold_settings) = config.cold_storage {
        let cold_config = cold_settings.to_cold_storage_config();
        match crate::storage::cold::ColdStorage::new(cold_config) {
            Ok(cold) => {
                if let Some(state) = Arc::get_mut(shared) {
                    state.cold_storage = Some(Arc::new(cold));
                    info!("cold storage (L2 tiering) initialised");
                } else {
                    tracing::warn!(
                        "cold storage: Arc::get_mut failed (unexpected clone), skipping"
                    );
                }
            }
            // With PITR on, truncation without an archive deletes segments
            // recovery needs, so a cold store that cannot open stops the boot.
            Err(e) if config.pitr.enabled => {
                return Err(crate::Error::Config {
                    detail: format!("pitr.enabled = true but [cold_storage] failed to open: {e}"),
                }
                .into());
            }
            Err(e) => {
                tracing::warn!(error = %e, "cold storage init failed, tiering disabled");
            }
        }
    }
    if config.pitr.enabled && shared.cold_storage.is_none() {
        return Err(crate::config::server::missing_cold_storage().into());
    }

    // Initialise snapshot storage.
    {
        let snap_cfg = config
            .snapshot_storage
            .as_ref()
            .map(|s| s.to_snapshot_storage_config())
            .unwrap_or_else(crate::config::server::SnapshotStorageSettings::default_storage_config);
        match crate::storage::snapshot_writer::build_snapshot_store(
            &snap_cfg,
            &config.server.data_dir,
        ) {
            Ok(store) => {
                if let Some(state) = Arc::get_mut(shared) {
                    state.snapshot_storage = store;
                    info!("snapshot storage initialised");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "snapshot storage init failed — aborting startup");
                std::process::exit(1);
            }
        }
    }

    // A cluster restore's generation, sealed before any Raft group starts.
    crate::control::pitr::seal_restored_generation(shared).await?;

    // PITR: this node life, and the base catalog rebuilt from snapshot storage.
    if config.pitr.enabled {
        crate::control::pitr::wire_pitr(shared, Arc::clone(&cluster_handle.catalog)).await?;
    }

    // Initialise quarantine storage.
    {
        let q_cfg = config
            .quarantine_storage
            .as_ref()
            .map(|s| s.to_quarantine_storage_config())
            .unwrap_or_else(
                crate::config::server::QuarantineStorageSettings::default_storage_config,
            );
        match crate::storage::quarantine::registry::build_quarantine_store(
            &q_cfg,
            &config.server.data_dir,
        ) {
            Ok(store) => {
                if let Some(state) = Arc::get_mut(shared) {
                    state.quarantine_storage = store;
                    info!("quarantine storage initialised");
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "quarantine storage init failed — aborting startup"
                );
                std::process::exit(1);
            }
        }
    }

    // Wire maintenance budget tracker (shared with Data Plane cores so
    // ALTER DATABASE SET QUOTA updates live caps immediately).
    if let Some(state) = Arc::get_mut(shared) {
        state.maintenance_budget = Arc::clone(&maintenance_budget);
    }

    // Object-store access for BACKUP / RESTORE DATABASE.
    if let Some(settings) = &config.backup_storage
        && let Some(state) = Arc::get_mut(shared)
    {
        state.backup_storage = Some(Arc::new(settings.clone()));
    }
    if !config.backup.schedule.is_empty()
        && let Some(state) = Arc::get_mut(shared)
    {
        state.backup_schedules = config.backup.schedule.clone();
    }

    // Load and wire backup KEK.
    if let Some(ref benc) = config.backup_encryption {
        match std::fs::read(&benc.key_path) {
            Ok(raw) if raw.len() == 32 => {
                let mut key_bytes = [0u8; 32];
                key_bytes.copy_from_slice(&raw);
                if let Some(state) = Arc::get_mut(shared) {
                    state.backup_kek = Some(Arc::new(key_bytes));
                }
                if let Some(ref enc) = config.encryption
                    && enc.key_file() == Some(benc.key_path.as_path())
                {
                    tracing::warn!(
                        path = %benc.key_path.display(),
                        "backup_encryption.key_path matches encryption.key_path — \
                         backup KEK and WAL KEK should be distinct for security isolation"
                    );
                }
                info!(key_path = %benc.key_path.display(), "backup encryption enabled");
            }
            Ok(raw) => {
                tracing::error!(
                    path = %benc.key_path.display(),
                    len = raw.len(),
                    "backup encryption key must be exactly 32 bytes — aborting startup"
                );
                std::process::exit(1);
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    path = %benc.key_path.display(),
                    "failed to load backup encryption key — aborting startup"
                );
                std::process::exit(1);
            }
        }
    }

    super::remote_sources::wire_remote_sources(shared, config)?;

    // Wire OTLP trace exporter and misc config fields.
    if let Some(state) = Arc::get_mut(shared) {
        let otlp = &config.observability.otlp.export;
        state.trace_exporter = if otlp.enabled && !otlp.endpoint.is_empty() {
            crate::control::trace_export::TraceExporter::new(
                otlp.endpoint.clone(),
                std::time::Duration::from_secs(5),
                Arc::clone(state.remote_sources.egress()),
            )
            .map_err(|e| crate::Error::Config {
                detail: format!("OTLP trace exporter: {e}"),
            })?
        } else {
            crate::control::trace_export::TraceExporter::disabled()
        };
        state.debug_endpoints_enabled = config.observability.debug_endpoints_enabled;
        state.scheduler_config = config.scheduler.clone();
    }

    // OTLP metrics push. It holds no clone of the shared state, so the
    // `Arc::get_mut` installs below still succeed.
    let otlp = &config.observability.otlp.export;
    let metrics_exporter = crate::control::otel::exporter::ExporterConfig {
        endpoint: if otlp.enabled {
            otlp.endpoint.clone()
        } else {
            String::new()
        },
        metrics_interval: std::time::Duration::from_secs(otlp.metrics_interval_secs),
        export_traces: otlp.enabled,
        export_metrics: otlp.enabled,
    };
    if crate::control::otel::exporter::spawn_metrics_exporter(shared, metrics_exporter).map_err(
        |e| crate::Error::Config {
            detail: format!("OTLP metrics exporter: the endpoint fails the [egress] policy: {e}"),
        },
    )? {
        info!(endpoint = %otlp.endpoint, "OTLP metrics export running");
    }

    // The gateway's weak back-reference outlives this call, so every
    // `Arc::get_mut` install above must run BEFORE it: one placed after
    // it no-ops.
    install_gateway(shared)?;

    // Hydrate bitemporal retention registry from array catalog.
    {
        // The Data Plane cores already share this lock. A core that panicked
        // while holding it leaves it poisoned.
        let guard = array_catalog.read().map_err(|_| crate::Error::Internal {
            detail: "array catalog lock is poisoned; array bitemporal retention \
                     cannot be seeded at startup"
                .into(),
        })?;
        for entry in guard.all_entries() {
            if let Some(audit_ms) = entry.audit_retain_ms {
                if audit_ms < 0 {
                    continue;
                }
                let retention = nodedb_types::config::BitemporalRetention {
                    data_retain_ms: 0,
                    audit_retain_ms: audit_ms as u64,
                    minimum_audit_retain_ms: entry.minimum_audit_retain_ms.unwrap_or(0),
                };
                // Keyed by the array's own identity, as the `PutArray`
                // post-apply registers it.
                if let Err(e) = shared.bitemporal_retention_registry.register(
                    entry.array_id.database_id,
                    entry.array_id.tenant_id,
                    entry.name.clone(),
                    crate::engine::bitemporal::BitemporalEngineKind::Array,
                    retention,
                ) {
                    tracing::warn!(
                        array = %entry.name,
                        error = %e,
                        "failed to register array bitemporal retention at startup"
                    );
                }
            }
        }
    }

    Ok(())
}

/// Wire `handle`, the node's cluster handle, into `state` before the state
/// is shared: node id, topology, routing, transport, the metadata cache, the
/// group apply watchers, and the cross-shard event sender.
///
/// Every host runs this before `start_raft`: boot for a real cluster and for
/// the synthesized one-node cluster, and every in-process test host.
///
/// The cross-shard event SENDER lets a trigger body writing to a
/// remote-homed collection reach the owning node instead of being
/// mis-written to the local core. The Event Plane spawns the dispatcher's
/// drain task (`spawn_dispatcher_task`), which injects the transport from
/// `cluster_transport`. Its spawn gate requires the dispatcher, metrics, and
/// DLQ this sets. Raft group setup opens the dedup store under `data_dir`.
pub fn wire_cluster_handle(
    state: &mut SharedState,
    handle: &ClusterHandle,
    data_dir: &std::path::Path,
) -> crate::Result<()> {
    state.node_id = handle.node_id;
    state.cluster_topology = Some(Arc::clone(&handle.topology));
    state.cluster_routing = Some(Arc::clone(&handle.routing));
    state.cluster_transport = Some(Arc::clone(&handle.transport));
    state.metadata_cache = Arc::clone(&handle.metadata_cache);
    state.group_watchers = Arc::clone(&handle.group_watchers);
    state.migration_tracker = Some(Arc::clone(&handle.migration_tracker));

    let cross_shard_metrics = Arc::new(crate::event::cross_shard::CrossShardMetrics::new());
    state.cross_shard_dispatcher = Some(Arc::new(
        crate::event::cross_shard::CrossShardDispatcher::new(
            handle.node_id,
            Arc::clone(&cross_shard_metrics),
        ),
    ));
    state.cross_shard_dlq = Some(Arc::new(std::sync::Mutex::new(
        crate::event::cross_shard::CrossShardDlq::open(data_dir)?,
    )));
    state.cross_shard_metrics = Some(cross_shard_metrics);
    Ok(())
}

/// Construct and install the gateway and the DDL plan-cache invalidator.
///
/// `Gateway` holds a `Weak<SharedState>` back-reference to its own
/// `SharedState`. `Arc::get_mut` requires strong count 1 and weak count 0,
/// so it cannot install the gateway. `gateway`/`gateway_invalidator` are
/// `OnceLock`s instead, set through `&self` exactly once.
///
/// Every `Arc::get_mut` install on `shared` must run before this call.
/// A later `get_mut` sees the weak reference and no-ops.
///
/// A second call is a wiring bug and returns `Error::Internal`.
pub fn install_gateway(shared: &Arc<SharedState>) -> crate::Result<()> {
    let gateway = Arc::new(crate::control::gateway::Gateway::new(Arc::clone(shared)));
    let invalidator = Arc::new(crate::control::gateway::PlanCacheInvalidator::new(
        &gateway.plan_cache,
    ));
    let already = |what: &str| crate::Error::Internal {
        detail: format!("{what} is already installed; install_gateway ran twice"),
    };
    shared
        .gateway
        .set(gateway)
        .map_err(|_| already("gateway"))?;
    shared
        .gateway_invalidator
        .set(invalidator)
        .map_err(|_| already("gateway plan-cache invalidator"))?;
    Ok(())
}
