// SPDX-License-Identifier: BUSL-1.1

//! SharedState constructors: new (test) and new_with_credentials (test+catalog).

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::metering::config::MeteringConfig;
use crate::control::security::metering::quota::QuotaManager;
use crate::control::security::metering::store::UsageStore;
use crate::control::security::permission::PermissionStore;
use crate::control::security::ratelimit::config::RateLimitConfig;
use crate::control::security::ratelimit::limiter::RateLimiter;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::security::role::RoleStore;
use crate::control::security::tenant::{TenantIsolation, TenantQuota};
use crate::control::server::sync::dlq::{DlqConfig, SyncDlq};
use crate::wal::WalManager;

use super::SharedState;

impl SharedState {
    /// Monotonic counter for unique test temp dirs (prevents redb lock collisions).
    fn unique_test_id() -> u64 {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Create shared state with a pre-built credential store (for tests that need catalog).
    ///
    /// `is_cluster` is the static, deployment-time surrogate-registry mode choice —
    /// same predicate as `SharedState::open`'s `is_cluster` (whether this node's
    /// caller is about to wire it into a real Raft cluster), not a property of the
    /// credential store. Almost every caller is a single-process fixture and passes
    /// `false`; the cluster test harness passes `true`.
    pub fn new_with_credentials(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        credentials: Arc<CredentialStore>,
        is_cluster: bool,
    ) -> crate::Result<Arc<Self>> {
        let wal_for_assigner = Arc::clone(&wal);
        let mut state = Self::new_inner(dispatcher, wal)?;
        if let Some(s) = Arc::get_mut(&mut state) {
            // Rebuild the surrogate assigner against the supplied
            // credential store. `new_inner` constructs the assigner
            // from a fresh in-memory `CredentialStore` with its own
            // in-memory catalog; the supplied store carries the durable
            // catalog whose surrogate watermark this fixture must resume.
            let registry = Arc::clone(&s.surrogate_registry);
            // Seed the registry's high-watermark AND applied-reserve cursor
            // from the catalog so restarts in a re-opened test fixture pick up
            // where the previous session left off — and so cluster-mode
            // metadata-log replay skips already-applied reservations rather
            // than double-counting `G`.
            let catalog = credentials.catalog();
            // The catalog-derived floor mirrors the production bootstrap: the
            // singleton is flushed lazily, so the highest surrogate any live
            // binding refers to is the value the allocator can never start
            // below. Mode selection mirrors `init_prod/bootstrap.rs::run`:
            // `is_cluster` (never seed-list length) picks `Cluster` vs
            // `Local`, seeding the applied-reserve cursor only in the
            // former.
            if let Ok(hwm) = catalog.get_surrogate_hwm()
                && let Ok(bound_floor) = catalog.max_bound_surrogate()
                && let Ok(mut reg) = registry.write()
            {
                let floor = hwm.max(bound_floor.as_u32());
                *reg = if is_cluster {
                    let reserve_index = catalog.get_surrogate_reserve_index().unwrap_or(0);
                    crate::control::surrogate::SurrogateRegistry::from_persisted_cluster(
                        floor,
                        reserve_index,
                    )
                } else {
                    crate::control::surrogate::SurrogateRegistry::from_persisted_hwm(floor)
                };
            }
            let wal_appender: Arc<dyn crate::control::surrogate::SurrogateWalAppender> = Arc::new(
                crate::control::surrogate::WalSurrogateAppender::new(wal_for_assigner),
            );
            s.surrogate_assigner = Arc::new(crate::control::surrogate::SurrogateAssigner::new(
                Arc::clone(&registry),
                Arc::clone(&credentials),
                wal_appender,
            ));
            // Catalog-backed security stores, rebuilt for the same reason the
            // surrogate watermark above is: this constructor's whole purpose
            // is to resume a durable catalog, and a memory-only store here
            // silently drops every auth-user status and scope grant the
            // previous session persisted — so a restart fixture would report
            // a clean slate rather than what was actually saved.
            s.auth_users =
                crate::control::security::jit::auth_user::AuthUserStore::open(catalog.clone())?;
            s.scope_grants =
                crate::control::security::scope::grant::ScopeGrantStore::open(catalog)?;
            // Same reasoning as the grants above: a quota definition is a
            // durable catalog object, and a memory-only manager here would
            // report every cap as absent after a restart.
            s.quota_manager = QuotaManager::open(
                s.metering_config.max_tracked_quota_grantees,
                credentials.catalog(),
            )?;
            s.credentials = credentials;
            s.ep_topic_registry
                .load_from_catalog(s.credentials.catalog())?;
            crate::event::topic::hydrate_topic_buffers(s)?;
        }
        Ok(state)
    }

    /// Create shared state with in-memory credential store (for tests).
    pub fn new(dispatcher: Dispatcher, wal: Arc<WalManager>) -> crate::Result<Arc<Self>> {
        Self::new_inner(dispatcher, wal)
    }

    /// Create shared state whose risk scorer is built from `risk_config` instead
    /// of the disabled default (tests exercising the risk gate; production wires
    /// the same configuration from `[auth.risk]`).
    pub fn new_with_risk_config(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        risk_config: crate::control::security::risk::RiskConfig,
    ) -> crate::Result<Arc<Self>> {
        let mut state = Self::new_inner(dispatcher, wal)?;
        let s = Arc::get_mut(&mut state).ok_or_else(|| crate::Error::Internal {
            detail: "shared state was already shared before the risk scorer could be installed"
                .into(),
        })?;
        s.risk_scorer = crate::control::security::risk::RiskScorer::new(risk_config);
        Ok(state)
    }

    /// Create shared state whose TLS policy is built from `tls_policy_config`
    /// instead of the disabled default (for tests that exercise transport
    /// enforcement). Production wires the same configuration from
    /// `[auth.tls_policy]`, through the same fallible parse: an unparseable
    /// `min_tls_version` is an error here exactly as it is at startup.
    pub fn new_with_tls_policy_config(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        tls_policy_config: crate::control::security::tls_policy::TlsPolicyConfig,
    ) -> crate::Result<Arc<Self>> {
        let policy =
            crate::control::security::tls_policy::TlsPolicy::from_config(&tls_policy_config)?;
        let mut state = Self::new_inner(dispatcher, wal)?;
        let s = Arc::get_mut(&mut state).ok_or_else(|| crate::Error::Internal {
            detail: "shared state was already shared before the TLS policy could be installed"
                .into(),
        })?;
        s.tls_policy = policy;
        Ok(state)
    }

    fn new_inner(dispatcher: Dispatcher, wal: Arc<WalManager>) -> crate::Result<Arc<Self>> {
        let shutdown = Arc::new(crate::control::shutdown::ShutdownWatch::new());
        let loop_registry = Arc::new(crate::control::shutdown::LoopRegistry::new());
        // Test helpers get a pre-fired gate so listeners start accepting
        // immediately. Production code (main.rs) replaces this with a real
        // StartupSequencer after calling `SharedState::open`.
        let startup_gate = crate::control::startup::StartupGate::pre_fired();
        let test_id = Self::unique_test_id();
        // One auto-cleaning temp directory that roots the test constructor's
        // on-disk stores (CDC offsets, job history, MV persistence). Held in the
        // `_test_state_dir` field below so it — and every store under it — is
        // removed when the state drops, instead of leaking `/tmp/nodedb-test-*`.
        let test_state_dir =
            tempfile::tempdir().expect("failed to create test state temp directory");
        let test_credentials = Arc::new(CredentialStore::new()?);
        let test_surrogate_registry: crate::control::surrogate::SurrogateRegistryHandle = Arc::new(
            std::sync::RwLock::new(crate::control::surrogate::SurrogateRegistry::new()),
        );
        let test_surrogate_assigner = Arc::new(crate::control::surrogate::SurrogateAssigner::new(
            Arc::clone(&test_surrogate_registry),
            Arc::clone(&test_credentials),
            Arc::new(crate::control::surrogate::NoopWalAppender),
        ));
        let shared_audit = Arc::new(Mutex::new(AuditLog::new(10_000)));
        let test_session_registry =
            Arc::new(crate::control::security::sessions::SessionRegistry::new());
        let (si_bus, uc_bus, bus_consumer_task) = super::buses_init::init_security_buses(
            Arc::clone(&shared_audit),
            Arc::clone(&test_session_registry),
        );
        let bus_consumer_handle = bus_consumer_task;
        // Wire buses into the credential store so test mutations publish events.
        test_credentials.set_buses(
            Arc::new(
                crate::control::security::buses::SessionInvalidationBus::from_existing(
                    si_bus.sender(),
                ),
            ),
            Arc::new(
                crate::control::security::buses::UserChangeBus::from_existing(uc_bus.sender()),
            ),
        );

        // No `AuthConfig` is available to this test/in-memory constructor —
        // use `MeteringConfig::default()` explicitly (rather than the
        // `UsageStore`/`QuotaManager` `Default` impls) so the effective
        // bounds are pinned to the same config type the production
        // constructor reads, and can't silently diverge from it.
        let metering_config = MeteringConfig::default();

        // No `AuthConfig` is available to this test/in-memory constructor —
        // use `RateLimitConfig::default()` explicitly (rather than the
        // `RateLimiter::default()` impl) so the limiter's config is pinned
        // to the same config type the production constructor reads, and
        // can't silently diverge from it.
        let rate_limit_config = RateLimitConfig::default();

        let state = Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            quiesce: crate::bridge::quiesce::CollectionQuiesce::new(),
            http_client: Arc::new(reqwest::Client::new()),
            credentials: Arc::clone(&test_credentials),
            audit: shared_audit,
            api_keys: ApiKeyStore::new(),
            roles: RoleStore::new(),
            permissions: PermissionStore::new(),
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
            cluster_topology: None,
            cluster_routing: None,
            cluster_transport: None,
            node_id: 0,
            node_incarnation: 0,
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
            rls: RlsPolicyStore::new(),
            blacklist: crate::control::security::blacklist::store::BlacklistStore::new(),
            auth_users: crate::control::security::jit::auth_user::AuthUserStore::new(),
            orgs: crate::control::security::org::store::OrgStore::new(),
            scope_defs: crate::control::security::scope::store::ScopeStore::new(),
            scope_grants: crate::control::security::scope::grant::ScopeGrantStore::new(),
            rate_limiter: RateLimiter::new(rate_limit_config),
            session_handles: crate::control::security::session_handle::SessionHandleStore::default(
            ),
            session_registry: test_session_registry,
            escalation: crate::control::security::escalation::EscalationEngine::default(),
            usage_counter: Arc::new(
                crate::control::security::metering::counter::UsageCounter::new(),
            ),
            usage_store: Arc::new(UsageStore::with_bounds(
                metering_config.max_usage_events,
                metering_config.max_tracked_scopes,
            )),
            quota_manager: QuotaManager::with_bounds(metering_config.max_tracked_quota_grantees),
            metering_config,
            auth_api_keys: crate::control::security::auth_apikey::AuthApiKeyStore::new(),
            impersonation: crate::control::security::impersonation::ImpersonationStore::default(),
            emergency: crate::control::security::emergency::EmergencyState::default(),
            auth_metrics: crate::control::security::observability::AuthMetrics::new(),
            ceilings: crate::control::security::ceiling::CeilingStore::new(),
            redaction: crate::control::security::redaction::RedactionStore::new(),
            risk_scorer: crate::control::security::risk::RiskScorer::default(),
            tls_policy: crate::control::security::tls_policy::TlsPolicy::default(),
            siem: crate::control::security::siem::SiemExporter::default(),
            jwks_registry: None,
            sync_dlq: Mutex::new(SyncDlq::new(DlqConfig::default())),
            audit_retention_days: 0,
            audit_max_entries: 0,
            idle_timeout_secs: 0,
            session_absolute_timeout_secs: 0,
            shape_registry: Arc::new(crate::control::server::sync::shape::ShapeRegistry::new()),
            change_stream: crate::control::change_stream::ChangeStream::new(4096),
            notify_bus: crate::control::notify_bus::NotifyBus::default(),
            trigger_registry: crate::control::trigger::TriggerRegistry::new(),
            array_catalog: crate::control::array_catalog::ArrayCatalog::handle(),
            array_sync_op_log: {
                std::sync::Arc::new(
                    crate::control::array_sync::OriginOpLog::open_in_memory()
                        .expect("failed to open test array op-log"),
                )
            },
            array_ack_registry: {
                crate::control::array_sync::ArrayAckRegistry::open_in_memory()
                    .expect("failed to open test ack registry")
            },
            array_snapshot_store: {
                crate::control::array_sync::OriginSnapshotStore::open_in_memory()
                    .expect("failed to open test snapshot store")
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
                let db = std::sync::Arc::new(
                    redb::Database::builder()
                        .create_with_backend(redb::backends::InMemoryBackend::new())
                        .expect("failed to create test schema_registry db"),
                );
                let replica_id = nodedb_array::sync::ReplicaId::new(0);
                let hlc_gen =
                    std::sync::Arc::new(nodedb_array::sync::HlcGenerator::new(replica_id));
                std::sync::Arc::new(
                    crate::control::array_sync::OriginSchemaRegistry::open(db, replica_id, hlc_gen)
                        .expect("failed to open test array schema registry"),
                )
            },
            array_delivery: std::sync::Arc::new(
                crate::control::array_sync::ArrayDeliveryRegistry::new(),
            ),
            array_subscriber_cursors: {
                let store = crate::control::array_sync::SubscriberStore::in_memory()
                    .expect("failed to open test subscriber store");
                std::sync::Arc::new(crate::control::array_sync::SubscriberMap::new(store))
            },
            array_merger_registry: std::sync::Arc::new(
                crate::control::array_sync::MergerRegistry::new(),
            ),
            mirror_link_registry: Arc::new(crate::control::mirror::MirrorLinkRegistry::new()),
            database_registry: crate::control::database::DatabaseRegistry::new(),
            surrogate_registry: Arc::clone(&test_surrogate_registry),
            surrogate_assigner: Arc::clone(&test_surrogate_assigner),
            block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache::new(
                4096,
            ),
            stream_registry: Arc::new(crate::event::cdc::StreamRegistry::new()),
            cdc_router: Arc::new(crate::event::cdc::CdcRouter::new(Arc::new(
                crate::event::cdc::StreamRegistry::new(),
            ))),
            group_registry: crate::event::cdc::GroupRegistry::new(),
            offset_store: {
                let dir = test_state_dir.path().join("offsets");
                Arc::new(
                    crate::event::cdc::OffsetStore::open(&dir)
                        .expect("failed to open test offset store"),
                )
            },
            retention_policy_registry: Arc::new(
                crate::engine::timeseries::retention_policy::RetentionPolicyRegistry::new(),
            ),
            bitemporal_retention_registry: Arc::new(
                crate::engine::bitemporal::BitemporalRetentionRegistry::new(),
            ),
            alert_registry: Arc::new(crate::event::alert::AlertRegistry::new()),
            alert_hysteresis: Arc::new(crate::event::alert::hysteresis::HysteresisManager::new()),
            schedule_registry: Arc::new(crate::event::scheduler::ScheduleRegistry::new()),
            synonym_registry: Arc::new(crate::control::synonym::SynonymRegistry::new()),
            custom_type_registry: Arc::new(crate::control::custom_type::CustomTypeRegistry::new()),
            job_history: {
                let dir = test_state_dir.path().join("history");
                Arc::new(
                    crate::event::scheduler::JobHistoryStore::open(&dir)
                        .expect("failed to open test job history"),
                )
            },
            ep_topic_registry: crate::event::topic::EpTopicRegistry::new(),
            webhook_manager: crate::event::webhook::WebhookManager::new(shutdown.raw_receiver()),
            mv_registry: Arc::new(crate::event::streaming_mv::MvRegistry::new()),
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
            mv_persistence: {
                let dir = test_state_dir.path().join("mvstate");
                Arc::new(
                    crate::event::streaming_mv::MvPersistence::open(&dir)
                        .expect("failed to open test MV persistence"),
                )
            },
            // Owns the temp dir the three stores above live under; dropping the
            // state removes it (and them) instead of leaking under `/tmp`.
            _test_state_dir: Some(test_state_dir),
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            raft_propose_leader_change_retries: AtomicU64::new(0),
            request_id_counter: AtomicU64::new(1),
            shuffle_id_counter: AtomicU64::new(1),
            system_metrics: Some(Arc::new(crate::control::metrics::SystemMetrics::new())),
            database_metrics: Arc::new(crate::control::metrics::DatabaseMetricsRegistry::new()),
            quota_ceiling: Arc::new(std::sync::RwLock::new(
                crate::control::security::catalog::GlobalQuotaCeiling::default(),
            )),
            retention_settings: Arc::new(std::sync::RwLock::new(
                crate::config::server::RetentionSettings::default(),
            )),
            governor: crate::data::executor::core_loop::test_governor(),
            maintenance_budget: Arc::new(
                crate::control::maintenance::MaintenanceBudgetTracker::new(),
            ),
            producer_registry: None,
            ts_partition_registries: Some(Mutex::new(std::collections::HashMap::new())),
            cold_storage: None,
            snapshot_storage: Arc::new(object_store::memory::InMemory::new()),
            quarantine_storage: Arc::new(object_store::memory::InMemory::new()),
            hlc_clock: Arc::new(nodedb_types::HlcClock::new()),
            tenant_write_hlc: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
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
            tuning: TuningConfig::default(),
            scheduler_config: crate::config::server::SchedulerConfig::default(),
            data_dir: std::path::PathBuf::new(),
            trigger_dlq: std::sync::OnceLock::new(),
            action_requeue: std::sync::OnceLock::new(),
            schema_version: crate::control::server::shared::session::plan_cache::SchemaVersion::new(
            ),
            materialized_sum_index:
                crate::control::planner::materialized_sum::MaterializedSumIndex::default(),
            sequence_registry: Arc::new(crate::control::sequence::SequenceRegistry::new()),
            dml_counter:
                crate::control::server::shared::ddl::neutral::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            last_applied_calvin_epoch: Arc::new(AtomicU64::new(0)),
            calvin_counters: crate::control::state::CalvinCounters {
                write_versions_recorded: Arc::new(AtomicU64::new(0)),
                read_set_validation_failures: Arc::new(AtomicU64::new(0)),
                commits_flushed: Arc::new(AtomicU64::new(0)),
                commits_dropped: Arc::new(AtomicU64::new(0)),
            },
            calvin_apply_results: Arc::new(Mutex::new(std::collections::HashMap::new())),
            calvin_lock_managers: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            hot_key_table: Arc::new(Mutex::new(
                crate::control::cluster::calvin::scheduler::lock::HotKeyTable::new(),
            )),
            calvin_promotion_senders: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            write_order_locks: Arc::new(
                crate::control::server::shared::write_admission::KeyedWriteOrderLock::new(),
            ),
            autocommit_lock_seq: std::sync::atomic::AtomicU32::new(0),
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            permission_cache: Arc::new(tokio::sync::RwLock::new(
                crate::control::security::permission_tree::PermissionCache::new(),
            )),
            gateway_invalidator: std::sync::OnceLock::new(),
            gateway: std::sync::OnceLock::new(),
            backup_kek: None,
            quarantine_registry: Arc::new(crate::storage::quarantine::QuarantineRegistry::new()),
            admission_registry: Arc::new(
                crate::control::server::admission::AdmissionRegistry::new(),
            ),
            lsn_ms_map: Arc::new(Mutex::new(nodedb_types::temporal::LsnMsMap::new())),
            audit_dml_cache: Arc::new(crate::control::state::audit_dml_cache::AuditDmlCache::new()),
            idle_timeout_cache: Arc::new(
                crate::control::state::idle_timeout_cache::IdleTimeoutCache::new(),
            ),
            collection_to_database: Arc::new(
                crate::control::state::collection_to_database::CollectionToDatabase::new(),
            ),
            materialize_freeze: crate::control::clone::MaterializeFreezeRegistry::new(),
            shuffle_registry: Arc::new(
                // Test path: no catalog data dir, so stage under a process- and
                // test-unique temp subdir to keep concurrent test inboxes
                // isolated.
                crate::control::server::shuffle::ShuffleReceiverRegistry::new(
                    std::env::temp_dir()
                        .join(format!("nodedb-shuffle-{}-{test_id}", std::process::id(),)),
                ),
            ),
            shutdown: Arc::clone(&shutdown),
            loop_registry: Arc::clone(&loop_registry),
            data_plane_drain: crate::control::shutdown::DataPlaneDrain::new(),
            startup: Arc::clone(&startup_gate),
        });
        Self::wire_session_handle_audit(&state);
        Ok(state)
    }

    /// Point the session-handle store's audit hook at this state's
    /// `AuditLog`, so `SessionHandleFingerprintMismatch` and
    /// `SessionHandleResolveMissSpike` are hash-chained with
    /// the rest of the auth-plane event stream. Captures the audit Arc
    /// directly — a `Weak<Self>` would block the cluster wire-up phase's
    /// `Arc::get_mut` on `SharedState`.
    pub(super) fn wire_session_handle_audit(state: &Arc<Self>) {
        let audit = Arc::clone(&state.audit);
        state.session_handles.set_audit_hook(move |event| {
            if let Ok(mut log) = audit.lock() {
                let _ = log.record(event, None, "session_handle", "");
            }
        });
    }
}
