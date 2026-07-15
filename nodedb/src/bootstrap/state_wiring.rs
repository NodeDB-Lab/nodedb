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
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    pub system_metrics: Arc<crate::control::metrics::SystemMetrics>,
    pub array_catalog: ArrayCatalogHandle,
    pub maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
}

/// Wire all optional subsystems into SharedState after `SharedState::open`.
///
/// This includes: startup gate, cluster handles, JWKS, cold storage, snapshot
/// storage, quarantine storage, memory governor, backup KEK, OTLP exporter,
/// gateway, and bitemporal retention registry.
pub fn wire_state(
    shared: &mut Arc<SharedState>,
    config: &ServerConfig,
    startup_gate: &Arc<StartupGate>,
    cluster_handle: Option<&ClusterHandle>,
    components: SharedStateComponents,
    root_span: &tracing::Span,
) -> anyhow::Result<()> {
    let SharedStateComponents {
        quarantine_registry,
        governor,
        system_metrics,
        array_catalog,
        maintenance_budget,
    } = components;
    // Install startup gate.
    if let Some(state) = Arc::get_mut(shared) {
        state.startup = Arc::clone(startup_gate);
    }

    // Replay surrogate WAL records.
    // Note: wal_records are not passed here — caller must handle surrogate replay
    // before calling this function (it needs the catalog opened by SharedState::open).

    // Install quarantine registry.
    if let Some(state) = Arc::get_mut(shared) {
        state.quarantine_registry = Arc::clone(&quarantine_registry);
    }

    // Wire cluster handles.
    if let Some(handle) = cluster_handle
        && let Some(state) = Arc::get_mut(shared)
    {
        state.node_id = handle.node_id;
        state.cluster_topology = Some(Arc::clone(&handle.topology));
        state.cluster_routing = Some(Arc::clone(&handle.routing));
        state.cluster_transport = Some(Arc::clone(&handle.transport));
        state.metadata_cache = Arc::clone(&handle.metadata_cache);
        state.group_watchers = Arc::clone(&handle.group_watchers);
        root_span.record("node_id", handle.node_id);
    }

    // Initialise JWKS registry.
    if let Some(ref jwt_config) = config.auth.jwt
        && !jwt_config.providers.is_empty()
        && let Some(state) = Arc::get_mut(shared)
    {
        let registry = tokio::runtime::Handle::current().block_on(
            crate::control::security::jwks::registry::JwksRegistry::init(jwt_config.clone()),
        )?;
        state.jwks_registry = Some(Arc::new(registry));
        info!(
            "JWKS registry initialised with {} providers",
            jwt_config.providers.len()
        );
    }

    // Initialise cold storage (L2 tiering).
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
            Err(e) => {
                tracing::warn!(error = %e, "cold storage init failed, tiering disabled");
            }
        }
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

    // Wire memory governor.
    if let Some(state) = Arc::get_mut(shared) {
        state.governor = Some(Arc::clone(&governor));
    }

    // Wire maintenance budget tracker (shared with Data Plane cores so
    // ALTER DATABASE SET QUOTA updates live caps immediately).
    if let Some(state) = Arc::get_mut(shared) {
        state.maintenance_budget = Arc::clone(&maintenance_budget);
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
                    && enc.key_path == benc.key_path
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

    // Wire OTLP trace exporter and misc config fields.
    if let Some(state) = Arc::get_mut(shared) {
        let otlp = &config.observability.otlp.export;
        state.trace_exporter = if otlp.enabled && !otlp.endpoint.is_empty() {
            crate::control::trace_export::TraceExporter::new(
                otlp.endpoint.clone(),
                std::time::Duration::from_secs(5),
            )
            .map_err(|e| crate::Error::Config {
                detail: format!("OTLP trace exporter: {e}"),
            })?
        } else {
            crate::control::trace_export::TraceExporter::disabled()
        };
        state.debug_endpoints_enabled = config.observability.debug_endpoints_enabled;
        state.data_dir = config.server.data_dir.clone();
        state.scheduler_config = config.scheduler.clone();
    }

    // Construct and install the gateway + DDL plan-cache invalidator.
    {
        let shared_for_gateway = Arc::clone(shared);
        if let Some(state) = Arc::get_mut(shared) {
            let gateway = Arc::new(crate::control::gateway::Gateway::new(shared_for_gateway));
            let invalidator = Arc::new(crate::control::gateway::PlanCacheInvalidator::new(
                &gateway.plan_cache,
            ));
            state.gateway = Some(Arc::clone(&gateway));
            state.gateway_invalidator = Some(invalidator);
        }
    }

    // Hydrate bitemporal retention registry from array catalog.
    {
        let guard = array_catalog
            .read()
            .expect("array catalog lock poisoned at startup");
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
                if let Err(e) = shared.bitemporal_retention_registry.register(
                    // The array catalog is global (not database-scoped), so its
                    // bitemporal retention is registered under the default
                    // database. Per-database array isolation is a separate
                    // initiative.
                    crate::types::DatabaseId::DEFAULT,
                    crate::types::TenantId::new(0),
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

    // Wire system metrics into shared state.
    if let Some(state) = Arc::get_mut(shared) {
        state.system_metrics = Some(Arc::clone(&system_metrics));
    }

    Ok(())
}
