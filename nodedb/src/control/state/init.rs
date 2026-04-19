//! SharedState constructors: new (test), new_with_credentials (test+catalog), open (production).

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::permission::PermissionStore;
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
    pub fn new_with_credentials(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        credentials: Arc<CredentialStore>,
    ) -> Arc<Self> {
        let mut state = Self::new_inner(dispatcher, wal);
        if let Some(s) = Arc::get_mut(&mut state) {
            s.credentials = credentials;
        }
        state
    }

    /// Create shared state with in-memory credential store (for tests).
    pub fn new(dispatcher: Dispatcher, wal: Arc<WalManager>) -> Arc<Self> {
        Self::new_inner(dispatcher, wal)
    }

    fn new_inner(dispatcher: Dispatcher, wal: Arc<WalManager>) -> Arc<Self> {
        let shutdown = Arc::new(crate::control::shutdown::ShutdownWatch::new());
        let loop_registry = Arc::new(crate::control::shutdown::LoopRegistry::new());
        // Test helpers get a pre-fired gate so listeners start accepting
        // immediately. Production code (main.rs) replaces this with a real
        // StartupSequencer after calling `SharedState::open`.
        let startup_gate = crate::control::startup::StartupGate::pre_fired();
        let test_id = Self::unique_test_id();
        let state = Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            http_client: Arc::new(reqwest::Client::new()),
            credentials: Arc::new(CredentialStore::new()),
            audit: Arc::new(Mutex::new(AuditLog::new(10_000))),
            api_keys: ApiKeyStore::new(),
            roles: RoleStore::new(),
            permissions: PermissionStore::new(),
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
            metadata_applied_index_watcher: Arc::new(
                crate::control::cluster::applied_index_watcher::AppliedIndexWatcher::new(),
            ),
            metadata_raft: std::sync::OnceLock::new(),
            propose_tracker: None,
            raft_proposer: None,
            raft_status_fn: None,
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
            rate_limiter: crate::control::security::ratelimit::limiter::RateLimiter::default(),
            session_handles: crate::control::security::session_handle::SessionHandleStore::default(
            ),
            session_registry: crate::control::security::session_registry::SessionRegistry::new(),
            escalation: crate::control::security::escalation::EscalationEngine::default(),
            usage_counter: Arc::new(
                crate::control::security::metering::counter::UsageCounter::new(),
            ),
            usage_store: Arc::new(crate::control::security::metering::store::UsageStore::default()),
            quota_manager: crate::control::security::metering::quota::QuotaManager::new(),
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
            idle_timeout_secs: 0,
            ws_sessions: std::sync::RwLock::new(std::collections::HashMap::new()),
            topic_registry: crate::control::pubsub::TopicRegistry::new(10_000),
            shape_registry: crate::control::server::sync::shape::ShapeRegistry::new(),
            change_stream: crate::control::change_stream::ChangeStream::new(4096),
            trigger_registry: crate::control::trigger::TriggerRegistry::new(),
            block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache::new(
                4096,
            ),
            stream_registry: Arc::new(crate::event::cdc::StreamRegistry::new()),
            cdc_router: Arc::new(crate::event::cdc::CdcRouter::new(Arc::new(
                crate::event::cdc::StreamRegistry::new(),
            ))),
            group_registry: crate::event::cdc::GroupRegistry::new(),
            offset_store: {
                let dir = std::env::temp_dir().join(format!(
                    "nodedb-test-offsets-{}-{test_id}",
                    std::process::id(),
                ));
                Arc::new(
                    crate::event::cdc::OffsetStore::open(&dir)
                        .expect("failed to open test offset store"),
                )
            },
            retention_policy_registry: Arc::new(
                crate::engine::timeseries::retention_policy::RetentionPolicyRegistry::new(),
            ),
            alert_registry: Arc::new(crate::event::alert::AlertRegistry::new()),
            alert_hysteresis: Arc::new(crate::event::alert::hysteresis::HysteresisManager::new()),
            schedule_registry: Arc::new(crate::event::scheduler::ScheduleRegistry::new()),
            job_history: {
                let dir = std::env::temp_dir().join(format!(
                    "nodedb-test-history-{}-{test_id}",
                    std::process::id(),
                ));
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
            crdt_sync_delivery: Arc::new(crate::event::crdt_sync::CrdtSyncDelivery::new()),
            delta_packager: Arc::new(crate::event::crdt_sync::DeltaPackager::new()),
            mv_persistence: {
                let dir = std::env::temp_dir().join(format!(
                    "nodedb-test-mvstate-{}-{test_id}",
                    std::process::id(),
                ));
                Arc::new(
                    crate::event::streaming_mv::MvPersistence::open(&dir)
                        .expect("failed to open test MV persistence"),
                )
            },
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            system_metrics: Some(Arc::new(crate::control::metrics::SystemMetrics::new())),
            governor: None,
            epoch_tracker: Mutex::new(std::collections::HashMap::new()),
            ts_partition_registries: Some(Mutex::new(std::collections::HashMap::new())),
            cold_storage: None,
            hlc_clock: Arc::new(nodedb_types::HlcClock::new()),
            tenant_write_hlc: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            lease_drain: Arc::new(crate::control::lease::DescriptorDrainTracker::new()),
            lease_refcount: Arc::new(crate::control::lease::LeaseRefCount::new()),
            tuning: TuningConfig::default(),
            data_dir: std::path::PathBuf::new(),
            schema_version: crate::control::server::pgwire::handler::prepared::SchemaVersion::new(),
            sequence_registry: Arc::new(crate::control::sequence::SequenceRegistry::new()),
            dml_counter:
                crate::control::server::pgwire::ddl::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            permission_cache: Arc::new(tokio::sync::RwLock::new(
                crate::control::security::permission_tree::PermissionCache::new(),
            )),
            gateway_invalidator: None,
            gateway: None,
            shutdown: Arc::clone(&shutdown),
            loop_registry: Arc::clone(&loop_registry),
            startup: Arc::clone(&startup_gate),
        });
        Self::wire_session_handle_audit(&state);
        state
    }

    /// Point the session-handle store's audit hook at this state's
    /// `AuditLog`, so `SessionHandleFingerprintMismatch` and
    /// `SessionHandleResolveMissSpike` are hash-chained with
    /// the rest of the auth-plane event stream. Captures the audit Arc
    /// directly — a `Weak<Self>` would block the cluster wire-up phase's
    /// `Arc::get_mut` on `SharedState`.
    fn wire_session_handle_audit(state: &Arc<Self>) {
        let audit = Arc::clone(&state.audit);
        state.session_handles.set_audit_hook(move |event| {
            if let Ok(mut log) = audit.lock() {
                let _ = log.record(event, None, "session_handle", "");
            }
        });
    }

    /// Create shared state with persistent credential store (for production).
    pub fn open(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        catalog_path: &std::path::Path,
        auth_config: &crate::config::auth::AuthConfig,
        tuning: TuningConfig,
    ) -> crate::Result<Arc<Self>> {
        let mut credentials = CredentialStore::open(catalog_path)?;
        credentials.set_lockout_policy(
            auth_config.max_failed_logins,
            auth_config.lockout_duration_secs,
            auth_config.password_expiry_days,
        );

        let api_keys = ApiKeyStore::new();
        let roles = RoleStore::new();
        let permissions = PermissionStore::new();
        let blacklist = crate::control::security::blacklist::store::BlacklistStore::new();
        let trigger_registry = crate::control::trigger::TriggerRegistry::new();
        let stream_registry = Arc::new(crate::event::cdc::StreamRegistry::new());
        let group_registry = crate::event::cdc::GroupRegistry::new();
        let schedule_registry = Arc::new(crate::event::scheduler::ScheduleRegistry::new());
        let retention_policy_registry =
            Arc::new(crate::engine::timeseries::retention_policy::RetentionPolicyRegistry::new());
        let alert_registry = Arc::new(crate::event::alert::AlertRegistry::new());
        let alert_hysteresis = Arc::new(crate::event::alert::hysteresis::HysteresisManager::new());
        let ep_topic_registry = crate::event::topic::EpTopicRegistry::new();
        let mv_registry = Arc::new(crate::event::streaming_mv::MvRegistry::new());
        let sequence_registry = Arc::new(crate::control::sequence::SequenceRegistry::new());
        let rls_store = RlsPolicyStore::new();
        let mut audit_start_seq = 1u64;
        if let Some(catalog) = credentials.catalog() {
            api_keys.load_from(catalog)?;
            roles.load_from(catalog)?;
            permissions.load_from(catalog)?;
            blacklist.load_from(catalog)?;
            trigger_registry.load_all(catalog);
            stream_registry.load_from_catalog(catalog);
            group_registry.load_from_catalog(catalog);
            schedule_registry.load_from_catalog(catalog);
            if let Ok(rp_defs) = catalog.load_all_retention_policies() {
                retention_policy_registry.load(rp_defs);
            }
            alert_registry.load_from_catalog(catalog);
            ep_topic_registry.load_from_catalog(catalog);
            mv_registry.load_from_catalog(catalog);
            sequence_registry.load_from_catalog(catalog);
            match catalog.load_all_rls_policies() {
                Ok(stored) => {
                    let mut loaded = 0usize;
                    for s in &stored {
                        match s.to_runtime() {
                            Ok(p) => {
                                rls_store.install_replicated_policy(p);
                                loaded += 1;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    name = %s.name,
                                    collection = %s.collection,
                                    error = %e,
                                    "boot replay: skipped invalid RLS policy"
                                );
                            }
                        }
                    }
                    if loaded > 0 {
                        tracing::info!(rls_policies = loaded, "loaded RLS policies from catalog");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "failed to load RLS policies"),
            }
            let max_seq = catalog.load_audit_max_seq()?;
            if max_seq > 0 {
                audit_start_seq = max_seq + 1;
            }
        }

        let mut audit_log = AuditLog::new(10_000);
        audit_log.set_next_seq(audit_start_seq);

        // Pre-load permission tree definitions before wrapping in RwLock
        // (avoids blocking_write() which panics inside async runtimes).
        let mut permission_cache =
            crate::control::security::permission_tree::PermissionCache::new();
        if let Some(catalog) = credentials.catalog()
            && let Ok(collections) = catalog.load_all_collections()
        {
            for coll in &collections {
                if let Some(ref def_json) = coll.permission_tree_def
                    && let Ok(def) = sonic_rs::from_str::<
                        crate::control::security::permission_tree::PermissionTreeDef,
                    >(def_json)
                {
                    permission_cache.register_tree_def(coll.tenant_id, &coll.name, def);
                }
            }
        }

        let shutdown = Arc::new(crate::control::shutdown::ShutdownWatch::new());
        let loop_registry = Arc::new(crate::control::shutdown::LoopRegistry::new());
        // A pre-fired placeholder gate is installed here. `main.rs` replaces
        // it after `open()` returns by swapping via `Arc::get_mut`, installing
        // the real gate from the `StartupSequencer` it constructs.
        let startup_gate = crate::control::startup::StartupGate::pre_fired();
        let state = Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            http_client: Arc::new(reqwest::Client::new()),
            credentials: Arc::new(credentials),
            audit: Arc::new(Mutex::new(audit_log)),
            api_keys,
            roles,
            permissions,
            trigger_registry,
            block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache::new(
                4096,
            ),
            stream_registry: Arc::clone(&stream_registry),
            cdc_router: Arc::new(crate::event::cdc::CdcRouter::new(stream_registry)),
            group_registry,
            offset_store: Arc::new(crate::event::cdc::OffsetStore::open(
                catalog_path.parent().unwrap_or(std::path::Path::new(".")),
            )?),
            retention_policy_registry,
            alert_registry,
            alert_hysteresis,
            schedule_registry,
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
            metadata_applied_index_watcher: Arc::new(
                crate::control::cluster::applied_index_watcher::AppliedIndexWatcher::new(),
            ),
            metadata_raft: std::sync::OnceLock::new(),
            propose_tracker: None,
            raft_proposer: None,
            raft_status_fn: None,
            cluster_observer: std::sync::OnceLock::new(),
            loop_metrics_registry: nodedb_cluster::LoopMetricsRegistry::new(),
            per_vshard_metrics: crate::control::metrics::PerVShardMetricsRegistry::new(),
            health_monitor: std::sync::OnceLock::new(),
            trace_exporter: crate::control::trace_export::TraceExporter::disabled(),
            debug_endpoints_enabled: false,
            migration_tracker: None,
            rls: rls_store,
            blacklist,
            auth_users: crate::control::security::jit::auth_user::AuthUserStore::new(),
            orgs: crate::control::security::org::store::OrgStore::new(),
            scope_defs: crate::control::security::scope::store::ScopeStore::new(),
            scope_grants: crate::control::security::scope::grant::ScopeGrantStore::new(),
            rate_limiter: crate::control::security::ratelimit::limiter::RateLimiter::default(),
            session_handles:
                crate::control::security::session_handle::SessionHandleStore::from_config(
                    &auth_config.session,
                ),
            session_registry: crate::control::security::session_registry::SessionRegistry::new(),
            escalation: crate::control::security::escalation::EscalationEngine::default(),
            usage_counter: Arc::new(
                crate::control::security::metering::counter::UsageCounter::new(),
            ),
            usage_store: Arc::new(crate::control::security::metering::store::UsageStore::default()),
            quota_manager: crate::control::security::metering::quota::QuotaManager::new(),
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
            audit_retention_days: auth_config.audit_retention_days,
            idle_timeout_secs: auth_config.idle_timeout_secs,
            ws_sessions: std::sync::RwLock::new(std::collections::HashMap::new()),
            topic_registry: crate::control::pubsub::TopicRegistry::new(10_000),
            shape_registry: crate::control::server::sync::shape::ShapeRegistry::new(),
            change_stream: crate::control::change_stream::ChangeStream::new(4096),
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            system_metrics: Some(Arc::new(crate::control::metrics::SystemMetrics::new())),
            governor: None,
            epoch_tracker: Mutex::new(std::collections::HashMap::new()),
            ts_partition_registries: Some(Mutex::new(std::collections::HashMap::new())),
            cold_storage: None,
            hlc_clock: Arc::new(nodedb_types::HlcClock::new()),
            tenant_write_hlc: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            lease_drain: Arc::new(crate::control::lease::DescriptorDrainTracker::new()),
            lease_refcount: Arc::new(crate::control::lease::LeaseRefCount::new()),
            tuning,
            data_dir: std::path::PathBuf::new(),
            schema_version: crate::control::server::pgwire::handler::prepared::SchemaVersion::new(),
            sequence_registry,
            dml_counter:
                crate::control::server::pgwire::ddl::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            permission_cache: Arc::new(tokio::sync::RwLock::new(permission_cache)),
            gateway_invalidator: None,
            gateway: None,
            shutdown: Arc::clone(&shutdown),
            loop_registry: Arc::clone(&loop_registry),
            startup: Arc::clone(&startup_gate),
        });

        Self::wire_session_handle_audit(&state);

        Ok(state)
    }
}
