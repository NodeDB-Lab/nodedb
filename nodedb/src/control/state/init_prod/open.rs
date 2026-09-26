// SPDX-License-Identifier: BUSL-1.1

//! `SharedState::open` — production constructor loading from disk.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use nodedb_types::config::TuningConfig;

use crate::control::request_tracker::RequestTracker;
use crate::control::security::metering::store::UsageStore;
use crate::control::security::ratelimit::limiter::RateLimiter;
use crate::control::security::tenant::{TenantIsolation, TenantQuota};
use crate::control::server::sync::dlq::{DlqConfig, SyncDlq};
use crate::wal::WalManager;

use super::handles::DataPlaneHandles;
use crate::control::state::SharedState;

impl SharedState {
    /// Create shared state with persistent credential store (for production).
    ///
    /// `is_cluster` is the static, deployment-time choice (from
    /// `config.cluster.is_some()` — presence of a `[cluster]` section,
    /// not seed-list length) between `Local` and `Cluster`
    /// surrogate-registry mode — see `SurrogateRegistryMode`.
    pub fn open(
        handles: DataPlaneHandles,
        wal: Arc<WalManager>,
        catalog_path: &std::path::Path,
        auth_config: &crate::config::auth::AuthConfig,
        tuning: TuningConfig,
        is_cluster: bool,
        governor: Arc<nodedb_mem::MemoryGovernor>,
    ) -> crate::Result<Arc<Self>> {
        let DataPlaneHandles {
            dispatcher,
            quiesce,
            array_catalog,
            system_metrics,
        } = handles;
        let super::bootstrap::ProdBootstrap {
            credentials,
            producer_registry,
            api_keys,
            roles,
            permissions,
            blacklist,
            trigger_registry,
            stream_registry,
            group_registry,
            schedule_registry,
            synonym_registry,
            custom_type_registry,
            retention_policy_registry,
            alert_registry,
            alert_hysteresis,
            ep_topic_registry,
            mv_registry,
            sequence_registry,
            rls_store,
            redaction_store,
            shared_audit,
            database_registry,
            surrogate_registry_handle,
            surrogate_assigner,
            permission_cache,
            shutdown,
            loop_registry,
            startup_gate,
            prod_session_registry,
            si_bus,
            uc_bus,
            bus_consumer_handle,
        } = super::bootstrap::run(&wal, catalog_path, auth_config, is_cluster)?;

        let super::auth_parts::AuthParts {
            metering_config,
            rate_limit_config,
            http_client,
            siem,
            risk_scorer,
            escalation,
            tls_policy,
            auth_users,
            scope_grants,
            quota_manager,
        } = super::auth_parts::build(auth_config, &credentials)?;

        let state = Arc::new(Self {
            outcome_floor: dispatcher.outcome_floor(),
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            quiesce,
            http_client,
            credentials: Arc::clone(&credentials),
            audit: shared_audit,
            api_keys,
            roles,
            permissions,
            trigger_registry,
            array_catalog,
            array_sync_op_log: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                std::sync::Arc::new(crate::control::array_sync::OriginOpLog::open(data_dir)?)
            },
            array_ack_registry: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                crate::control::array_sync::ArrayAckRegistry::open(data_dir)?
            },
            array_snapshot_store: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                crate::control::array_sync::OriginSnapshotStore::open(data_dir)?
            },
            array_snapshot_hlcs: std::sync::Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::<
                    (nodedb_types::DatabaseId, u64, String),
                    nodedb_array::sync::hlc::Hlc,
                >::new(),
            )),
            array_gc_handle: None,
            session_invalidation_bus: si_bus,
            user_change_bus: uc_bus,
            bus_consumer_handle,
            array_sync_schemas: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                let schema_db = {
                    let dir = data_dir.join("array_sync");
                    std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
                        engine: "array_sync".into(),
                        detail: format!("create array_sync dir: {e}"),
                    })?;
                    let path = dir.join("schema_docs.redb");
                    std::sync::Arc::new(redb::Database::create(&path).map_err(|e| {
                        crate::Error::Storage {
                            engine: "array_sync".into(),
                            detail: format!("schema_registry db open: {e}"),
                        }
                    })?)
                };
                let replica_id = nodedb_array::sync::ReplicaId::new(0);
                let hlc_gen =
                    std::sync::Arc::new(nodedb_array::sync::HlcGenerator::new(replica_id));
                std::sync::Arc::new(crate::control::array_sync::OriginSchemaRegistry::open(
                    schema_db, replica_id, hlc_gen,
                )?)
            },
            array_delivery: std::sync::Arc::new(
                crate::control::array_sync::ArrayDeliveryRegistry::new(),
            ),
            array_subscriber_cursors: {
                let data_dir = catalog_path.parent().unwrap_or(std::path::Path::new("."));
                let cursor_db = {
                    let dir = data_dir.join("array_sync");
                    std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
                        engine: "array_sync".into(),
                        detail: format!("create array_sync dir for cursors: {e}"),
                    })?;
                    let path = dir.join("subscriber_cursors.redb");
                    std::sync::Arc::new(redb::Database::create(&path).map_err(|e| {
                        crate::Error::Storage {
                            engine: "array_sync".into(),
                            detail: format!("subscriber_cursor db open: {e}"),
                        }
                    })?)
                };
                let store = crate::control::array_sync::SubscriberStore::open(cursor_db)?;
                std::sync::Arc::new(crate::control::array_sync::SubscriberMap::new(store))
            },
            array_merger_registry: std::sync::Arc::new(
                crate::control::array_sync::MergerRegistry::new(),
            ),
            mirror_link_registry: Arc::new(crate::control::mirror::MirrorLinkRegistry::new()),
            database_registry,
            surrogate_registry: surrogate_registry_handle,
            surrogate_assigner,
            block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache::new(
                4096,
            ),
            stream_registry: Arc::clone(&stream_registry),
            cdc_router: Arc::new(
                crate::event::cdc::CdcRouter::new(stream_registry)
                    .with_metrics(Arc::clone(&system_metrics)),
            ),
            group_registry,
            offset_store: Arc::new(crate::event::cdc::OffsetStore::open(
                catalog_path.parent().unwrap_or(std::path::Path::new(".")),
            )?),
            retention_policy_registry,
            bitemporal_retention_registry: Arc::new(
                crate::engine::bitemporal::BitemporalRetentionRegistry::new(),
            ),
            alert_registry,
            alert_hysteresis,
            schedule_registry,
            synonym_registry,
            custom_type_registry,
            job_history: Arc::new(crate::event::scheduler::JobHistoryStore::open(
                catalog_path.parent().unwrap_or(std::path::Path::new(".")),
            )?),
            ep_topic_registry,
            webhook_manager: crate::event::webhook::WebhookManager::new(shutdown.raw_receiver()),
            mv_registry,
            consumer_assignments: crate::event::cdc::consumer_group::ConsumerAssignments::new(),
            watermark_tracker: Arc::new(crate::event::watermark_tracker::WatermarkTracker::new()),
            event_plane_budget: Arc::new(crate::event::budget::EventPlaneBudget::new()),
            cross_shard_dispatcher: None,
            cross_shard_dlq: None,
            cross_shard_metrics: None,
            hwm_store: None,
            kafka_manager: crate::event::kafka::KafkaManager::new(shutdown.raw_receiver()),
            definition_sync_fanout: std::sync::Arc::new(
                crate::control::server::sync::definition_fanout::DefinitionSyncFanout::new(),
            ),
            crdt_sync_delivery: Arc::new(crate::event::crdt_sync::CrdtSyncDelivery::new()),
            delta_packager: Arc::new(crate::event::crdt_sync::DeltaPackager::new()),
            mv_persistence: Arc::new(crate::event::streaming_mv::MvPersistence::open(
                catalog_path.parent().unwrap_or(std::path::Path::new(".")),
            )?),
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
            cluster_topology: None,
            cluster_routing: None,
            cluster_transport: None,
            node_id: 0,
            metadata_cache: Arc::new(std::sync::RwLock::new(nodedb_cluster::MetadataCache::new())),
            catalog_change_tx: tokio::sync::broadcast::channel(
                crate::control::cluster::metadata_applier::CATALOG_CHANNEL_CAPACITY,
            )
            .0,
            group_watchers: Arc::new(nodedb_cluster::GroupAppliedWatchers::new()),
            metadata_ddl_lock: std::sync::Mutex::new(()),
            metadata_ddl_owner: std::sync::Mutex::new(None),
            metadata_ddl_applied_token: std::sync::atomic::AtomicU64::new(0),
            metadata_ddl_token_seq: std::sync::atomic::AtomicU64::new(1),
            pending_ddl: crate::control::pending_ddl::PendingDdlTable::new(),
            metadata_apply_wedge: std::sync::Arc::default(),
            sequencer_halt: std::sync::Arc::default(),
            core_stall: std::sync::Arc::default(),
            metadata_raft: std::sync::OnceLock::new(),
            propose_tracker: std::sync::OnceLock::new(),
            raft_proposer: std::sync::OnceLock::new(),
            async_raft_proposer_pair: std::sync::OnceLock::new(),
            vshard_admission_sequencer: Arc::new(
                crate::control::vshard_admission::VShardAdmissionSequencer::new(),
            ),
            raft_compactor: std::sync::OnceLock::new(),
            raft_applied_index_sink: std::sync::OnceLock::new(),
            raft_read_gate: std::sync::OnceLock::new(),
            cluster_epoch: std::sync::OnceLock::new(),
            raft_status_fn: std::sync::OnceLock::new(),
            cluster_observer: std::sync::OnceLock::new(),
            loop_metrics_registry: nodedb_cluster::LoopMetricsRegistry::new(),
            per_vshard_metrics: crate::control::metrics::PerVShardMetricsRegistry::new(),
            health_monitor: std::sync::OnceLock::new(),
            trace_exporter: crate::control::trace_export::TraceExporter::disabled(),
            debug_endpoints_enabled: false,
            migration_tracker: None,
            rls: rls_store,
            blacklist,
            auth_users,
            orgs: crate::control::security::org::store::OrgStore::new(),
            scope_defs: crate::control::security::scope::store::ScopeStore::new(),
            scope_grants,
            rate_limiter: RateLimiter::new(rate_limit_config.clone()),
            session_handles:
                crate::control::security::session_handle::SessionHandleStore::from_config(
                    &auth_config.session,
                ),
            session_registry: prod_session_registry,
            escalation,
            usage_counter: Arc::new(
                crate::control::security::metering::counter::UsageCounter::new(),
            ),
            usage_store: Arc::new(UsageStore::with_bounds(
                metering_config.max_usage_events,
                metering_config.max_tracked_scopes,
            )),
            quota_manager,
            metering_config: metering_config.clone(),
            auth_api_keys: crate::control::security::auth_apikey::AuthApiKeyStore::new(),
            impersonation: crate::control::security::impersonation::ImpersonationStore::default(),
            emergency: crate::control::security::emergency::EmergencyState::default(),
            auth_metrics: crate::control::security::observability::AuthMetrics::new(),
            ceilings: crate::control::security::ceiling::CeilingStore::new(),
            redaction: redaction_store,
            risk_scorer,
            tls_policy,
            siem,
            jwks_registry: None,
            sync_dlq: Mutex::new(SyncDlq::new(DlqConfig::default())),
            audit_retention_days: auth_config.audit_retention_days,
            audit_max_entries: auth_config.audit_max_entries,
            idle_timeout_secs: auth_config.idle_timeout_secs,
            session_absolute_timeout_secs: auth_config.session_absolute_timeout_secs,
            shape_registry: Arc::new(crate::control::server::sync::shape::ShapeRegistry::new()),
            change_stream: crate::control::change_stream::ChangeStream::new(4096),
            notify_bus: crate::control::notify_bus::NotifyBus::default(),
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            raft_propose_leader_change_retries: AtomicU64::new(0),
            request_id_counter: AtomicU64::new(1),
            shuffle_id_counter: AtomicU64::new(1),
            // Use the pre-created Arc so the CdcRouter (above) and this
            // metrics endpoint share the same SystemMetrics registry.
            system_metrics: Some(Arc::clone(&system_metrics)),
            database_metrics: Arc::new(crate::control::metrics::DatabaseMetricsRegistry::new()),
            quota_ceiling: Arc::new(std::sync::RwLock::new(
                crate::control::security::catalog::GlobalQuotaCeiling::default(),
            )),
            retention_settings: Arc::new(std::sync::RwLock::new(
                crate::config::server::RetentionSettings::default(),
            )),
            governor,
            maintenance_budget: Arc::new(
                crate::control::maintenance::MaintenanceBudgetTracker::new(),
            ),
            producer_registry,
            ts_partition_registries: Some(Mutex::new(std::collections::HashMap::new())),
            cold_storage: None,
            snapshot_storage: Arc::new(object_store::memory::InMemory::new()),
            quarantine_storage: Arc::new(object_store::memory::InMemory::new()),
            hlc_clock: Arc::new(nodedb_types::HlcClock::new()),
            tenant_write_hlc: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            tenant_marks: crate::control::state::tenant_marks::TenantMarks::load(
                credentials.catalog(),
            )?,
            lease_admission_gate: Mutex::new(()),
            lease_grant_gate: Arc::new(Mutex::new(())),
            lease_drain: Arc::new(crate::control::lease::DescriptorDrainTracker::new()),
            lease_refcount: Arc::new(crate::control::lease::LeaseRefCount::new()),
            sequencer_inbox: std::sync::OnceLock::new(),
            reservation_inbox: std::sync::OnceLock::new(),
            sequencer_metrics: std::sync::OnceLock::new(),
            calvin_completion_registry: std::sync::OnceLock::new(),
            ollp_orchestrator: std::sync::OnceLock::new(),
            limits: nodedb_types::protocol::Limits::default(),
            tuning,
            scheduler_config: crate::config::server::SchedulerConfig::default(),
            data_dir: std::path::PathBuf::new(),
            trigger_dlq: std::sync::OnceLock::new(),
            action_requeue: std::sync::OnceLock::new(),
            sink_ledgers: std::sync::OnceLock::new(),
            // Production stores live under real on-disk paths, not a temp dir.
            _test_state_dir: None,
            schema_version: crate::control::server::shared::session::plan_cache::SchemaVersion::new(
            ),
            materialized_sum_index:
                crate::control::planner::materialized_sum::MaterializedSumIndex::default(),
            sequence_registry,
            dml_counter:
                crate::control::server::shared::ddl::neutral::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            calvin: crate::control::state::calvin_local::CalvinLocalState::new(),
            write_order_locks: Arc::new(
                crate::control::server::shared::write_admission::KeyedWriteOrderLock::new(),
            ),
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            authorization_fence: Arc::new(
                crate::control::security::auth_fence::AuthorizationFence::new(
                    permission_cache.sources(),
                ),
            ),
            permission_cache: Arc::new(tokio::sync::RwLock::new(permission_cache)),
            gateway_invalidator: std::sync::OnceLock::new(),
            gateway: std::sync::OnceLock::new(),
            backup_kek: None,
            quarantine_registry: Arc::new(crate::storage::quarantine::QuarantineRegistry::new()),
            admission_registry: Arc::new(
                crate::control::server::admission::AdmissionRegistry::new(),
            ),
            audit_dml_cache: Arc::new(crate::control::state::audit_dml_cache::AuditDmlCache::new()),
            idle_timeout_cache: Arc::new(
                crate::control::state::idle_timeout_cache::IdleTimeoutCache::new(),
            ),
            collection_to_database: Arc::new(
                crate::control::state::collection_to_database::CollectionToDatabase::new(),
            ),
            lsn_ms_map: Arc::new(Mutex::new(nodedb_types::temporal::LsnMsMap::new())),
            materialize_freeze: crate::control::clone::MaterializeFreezeRegistry::new(),
            shuffle_registry: Arc::new(
                crate::control::server::shuffle::ShuffleReceiverRegistry::new(
                    catalog_path
                        .parent()
                        .unwrap_or(std::path::Path::new("."))
                        .to_path_buf(),
                ),
            ),
            shutdown: Arc::clone(&shutdown),
            loop_registry: Arc::clone(&loop_registry),
            data_plane_drain: crate::control::shutdown::DataPlaneDrain::new(),
            startup: Arc::clone(&startup_gate),
        });

        crate::event::topic::hydrate_topic_buffers(&state)?;
        super::post_init::hydrate_caches(&state);
        super::post_init::spawn_array_gc(&state);

        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::ratelimit::config::RateLimitConfig;
    use crate::control::security::ratelimit::limiter::LoginRateLimitOutcome;

    /// Build a `SharedState` via the production `open()` path with a
    /// caller-supplied `AuthConfig`, so tests can observe how operator
    /// config actually threads through construction.
    fn open_with_auth_config(
        dir: &std::path::Path,
        auth_config: &crate::config::auth::AuthConfig,
    ) -> Arc<SharedState> {
        let wal_dir = dir.join("wal");
        std::fs::create_dir_all(&wal_dir).expect("create wal dir");
        let wal = Arc::new(WalManager::open_for_testing(&wal_dir).expect("open wal"));
        let (dispatcher, _) = crate::bridge::dispatch::Dispatcher::new(1, 16);
        let catalog_path = dir.join("catalog.redb");
        SharedState::open(
            DataPlaneHandles {
                dispatcher,
                quiesce: crate::bridge::quiesce::CollectionQuiesce::new(),
                array_catalog: crate::control::array_catalog::ArrayCatalog::handle(),
                system_metrics: Arc::new(crate::control::metrics::SystemMetrics::new()),
            },
            wal,
            &catalog_path,
            auth_config,
            TuningConfig::default(),
            false,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open shared state")
    }

    #[test]
    fn configured_rate_limit_is_applied_not_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        // `RateLimitConfig::default()` is `enabled: false` with `default_burst:
        // 200` — a distinctive small, enabled burst here can only show up if
        // this exact config was threaded into the constructed `RateLimiter`.
        let auth_config = crate::config::auth::AuthConfig {
            rate_limit: Some(RateLimitConfig {
                enabled: true,
                default_qps: 3,
                default_burst: 3,
                ..Default::default()
            }),
            ..Default::default()
        };

        let state = open_with_auth_config(dir.path(), &auth_config);

        for i in 0..3 {
            let r = state.rate_limiter.check("u1", &[], None, "point_get", None);
            assert!(
                r.allowed,
                "request {i} should be allowed under configured burst=3"
            );
        }
        let r = state.rate_limiter.check("u1", &[], None, "point_get", None);
        assert!(
            !r.allowed,
            "4th request must be denied by the operator-configured burst=3, \
             not the hardcoded RateLimitConfig::default() burst=200"
        );
    }

    #[test]
    fn unconfigured_rate_limit_falls_back_to_disabled_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let auth_config = crate::config::auth::AuthConfig::default();
        assert!(auth_config.rate_limit.is_none());

        let state = open_with_auth_config(dir.path(), &auth_config);

        // `RateLimitConfig::default()` has `enabled: false`, so every
        // request is admitted regardless of volume.
        for _ in 0..500 {
            let r = state.rate_limiter.check("u1", &[], None, "point_get", None);
            assert!(r.allowed);
        }
    }

    #[test]
    fn login_capacities_still_apply_after_configured_rate_limit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let auth_config = crate::config::auth::AuthConfig {
            rate_limit: Some(RateLimitConfig {
                enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let state = open_with_auth_config(dir.path(), &auth_config);

        // Mirrors `main_boot::shared_state`'s post-construction call.
        state.rate_limiter.set_login_capacities(5, 100);

        for _ in 0..5 {
            assert!(matches!(
                state.rate_limiter.check_login("10.0.0.9", "victim"),
                LoginRateLimitOutcome::Allowed
            ));
            state
                .rate_limiter
                .record_login_failure("10.0.0.9", "victim");
        }
        assert!(
            matches!(
                state.rate_limiter.check_login("10.0.0.9", "victim"),
                LoginRateLimitOutcome::IpExceeded { .. }
            ),
            "login brute-force capacities set via set_login_capacities must \
             still apply after threading the configured RateLimitConfig"
        );
    }
}
