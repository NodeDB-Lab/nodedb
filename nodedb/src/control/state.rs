use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

use tracing::warn;

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::permission::PermissionStore;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::security::role::RoleStore;
use crate::control::security::tenant::{QuotaCheck, TenantIsolation, TenantQuota};
use crate::control::server::sync::dlq::{DlqConfig, SyncDlq};
use crate::types::TenantId;
use crate::wal::WalManager;

/// Shared state accessible by all Control Plane sessions.
///
/// This is the glue that connects TCP sessions to the Data Plane via the
/// Dispatcher/SPSC bridge, and to the WAL for the write path.
///
/// All fields are `Send + Sync` — safe for sharing across Tokio tasks.
pub struct SharedState {
    /// Routes requests to Data Plane cores via SPSC.
    pub dispatcher: Mutex<Dispatcher>,

    /// Tracks in-flight requests and routes responses back to sessions.
    pub tracker: RequestTracker,

    /// Write-ahead log for durability.
    pub wal: Arc<WalManager>,

    /// Credential store for user authentication.
    pub credentials: Arc<CredentialStore>,

    /// Audit log for security-relevant events.
    pub audit: Mutex<AuditLog>,

    /// API key store.
    pub api_keys: ApiKeyStore,

    /// Custom role store (inheritance, CRUD).
    pub roles: RoleStore,

    /// Collection-level permission grants + ownership.
    pub permissions: PermissionStore,

    /// Per-tenant quota enforcement.
    pub tenants: Mutex<TenantIsolation>,

    /// Row-Level Security policy store for sync delta enforcement.
    pub rls: RlsPolicyStore,

    /// User + IP blacklist store (O(1) lookup, TTL for temp bans).
    pub blacklist: crate::control::security::blacklist::store::BlacklistStore,

    /// JIT-provisioned auth user store (from JWT claims).
    pub auth_users: crate::control::security::jit::auth_user::AuthUserStore,

    /// Organization store.
    pub orgs: crate::control::security::org::store::OrgStore,

    /// Scope definition store.
    pub scope_defs: crate::control::security::scope::store::ScopeStore,

    /// Scope grant store (who has what scope).
    pub scope_grants: crate::control::security::scope::grant::ScopeGrantStore,

    /// Rate limiter (token bucket, per-user/org hierarchy).
    pub rate_limiter: crate::control::security::ratelimit::limiter::RateLimiter,

    /// Opaque session handle store (POST /api/auth/session → UUID).
    pub session_handles: crate::control::security::session_handle::SessionHandleStore,
    /// Active session registry for KILL SESSIONS.
    pub session_registry: crate::control::security::session_registry::SessionRegistry,
    /// Auto-escalation engine (violations → suspend → ban).
    pub escalation: crate::control::security::escalation::EscalationEngine,
    /// Usage metering counter (per-core atomic, periodic flush).
    pub usage_counter: std::sync::Arc<crate::control::security::metering::counter::UsageCounter>,
    /// Usage metering store (aggregated events).
    pub usage_store: std::sync::Arc<crate::control::security::metering::store::UsageStore>,
    /// Quota manager (enforcement against scope quotas).
    pub quota_manager: crate::control::security::metering::quota::QuotaManager,
    /// Auth-scoped API keys (nda_ format, bound to auth_users).
    pub auth_api_keys: crate::control::security::auth_apikey::AuthApiKeyStore,
    /// Impersonation & delegation store.
    pub impersonation: crate::control::security::impersonation::ImpersonationStore,
    /// Emergency lockdown state + break-glass + two-party auth.
    pub emergency: crate::control::security::emergency::EmergencyState,
    /// Auth observability metrics (Prometheus-compatible).
    pub auth_metrics: crate::control::security::observability::AuthMetrics,
    /// Tenant ceilings (hard limits even superusers respect).
    pub ceilings: crate::control::security::ceiling::CeilingStore,
    /// Column-level redaction policies.
    pub redaction: crate::control::security::redaction::RedactionStore,
    /// Risk scorer for adaptive auth decisions.
    pub risk_scorer: crate::control::security::risk::RiskScorer,
    /// TLS enforcement policy.
    pub tls_policy: crate::control::security::tls_policy::TlsPolicy,
    /// SIEM export adapter.
    pub siem: crate::control::security::siem::SiemExporter,

    /// JWKS registry for multi-provider JWT validation (None = JWT disabled).
    pub jwks_registry:
        Option<std::sync::Arc<crate::control::security::jwks::registry::JwksRegistry>>,

    /// Dead-Letter Queue for sync-rejected deltas.
    pub sync_dlq: Mutex<SyncDlq>,

    /// Audit retention in days (0 = keep forever).
    audit_retention_days: u32,

    /// Idle session timeout in seconds (0 = no timeout).
    idle_timeout_secs: u64,

    /// Cluster topology (None in single-node mode).
    pub cluster_topology: Option<Arc<RwLock<nodedb_cluster::ClusterTopology>>>,

    /// Cluster routing table (None in single-node mode).
    pub cluster_routing: Option<Arc<RwLock<nodedb_cluster::RoutingTable>>>,

    /// Cluster transport for forwarding requests (None in single-node mode).
    pub cluster_transport: Option<Arc<nodedb_cluster::NexarTransport>>,

    /// This node's ID (0 in single-node mode).
    pub node_id: u64,

    /// Propose tracker for distributed writes (None in single-node mode).
    pub propose_tracker: Option<Arc<crate::control::wal_replication::ProposeTracker>>,

    /// Raft propose function — wraps RaftLoop::propose (None in single-node mode).
    /// Signature: (vshard_id, data) → Result<(group_id, log_index)>
    pub raft_proposer: Option<Arc<crate::control::wal_replication::RaftProposer>>,

    /// Query Raft group statuses for observability (None in single-node mode).
    pub raft_status_fn: Option<Arc<dyn Fn() -> Vec<nodedb_cluster::GroupStatus> + Send + Sync>>,

    /// Migration tracker for observability (None in single-node mode).
    pub migration_tracker: Option<Arc<nodedb_cluster::MigrationTracker>>,

    /// WebSocket session registry: tracks last-seen LSN per client session
    /// for reconnection replay. Bounded to 10,000 entries with LRU eviction
    /// (eviction enforced by `save_ws_session()` in ws_rpc.rs).
    pub ws_sessions: std::sync::RwLock<std::collections::HashMap<String, u64>>,

    /// Pub/Sub topic registry with persistent message storage.
    pub topic_registry: crate::control::pubsub::TopicRegistry,

    /// Shape subscription registry for Lite client sync.
    /// Persists across handler invocations; export/import for disk persistence.
    pub shape_registry: crate::control::server::sync::shape::ShapeRegistry,

    /// Change stream bus: broadcasts committed mutations to subscribers.
    /// Used by LISTEN/NOTIFY, live queries, event triggers, and CDC.
    pub change_stream: crate::control::change_stream::ChangeStream,

    /// In-memory trigger registry for fast lookup during DML.
    /// Loaded from catalog on startup, updated by CREATE/DROP TRIGGER DDL.
    pub trigger_registry: crate::control::trigger::TriggerRegistry,

    /// In-memory change stream registry for CDC event routing.
    /// Loaded from catalog on startup, updated by CREATE/DROP CHANGE STREAM DDL.
    /// Arc-wrapped for sharing with the Event Plane's CdcRouter.
    pub stream_registry: Arc<crate::event::cdc::StreamRegistry>,

    /// CDC event router: routes WriteEvents to matching stream buffers.
    /// Shared between Event Plane consumers (write) and DDL handlers (drop cleanup).
    /// Initialized after SharedState construction (set by EventPlane::spawn).
    pub cdc_router: Arc<crate::event::cdc::CdcRouter>,

    /// Total connections rejected due to max_connections limit (monotonic counter).
    pub connections_rejected: AtomicU64,

    /// Total connections accepted since startup (monotonic counter).
    pub connections_accepted: AtomicU64,

    /// System-wide metrics (contention, subscriptions, WAL fsync, etc.).
    /// Served via the HTTP metrics endpoint in Prometheus format.
    pub system_metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,

    /// Fork detection: tracks `lite_id → last_seen_epoch` for sync handshake.
    /// Prevents cloned devices from silently merging data.
    pub epoch_tracker: Mutex<std::collections::HashMap<String, u64>>,

    /// Timeseries partition registries: keyed by "{tenant_id}:{collection_name}".
    /// Stores partition metadata for all timeseries collections.
    pub ts_partition_registries: Option<
        Mutex<
            std::collections::HashMap<
                String,
                crate::engine::timeseries::partition_registry::PartitionRegistry,
            >,
        >,
    >,

    /// L2 cold storage client (None when cold tiering is not configured).
    pub cold_storage: Option<Arc<crate::storage::cold::ColdStorage>>,

    /// Rolling upgrade version tracking (cluster mode only).
    /// Tracks each node's wire format version for N-1 compatibility checks.
    pub cluster_version_state: Mutex<crate::control::rolling_upgrade::ClusterVersionState>,

    /// Performance tuning configuration (deadlines, query limits, engine
    /// knobs, etc.). Immutable after startup — set once from `ServerConfig`.
    pub tuning: TuningConfig,
}

impl SharedState {
    /// Create shared state with in-memory credential store (for tests).
    pub fn new(dispatcher: Dispatcher, wal: Arc<WalManager>) -> Arc<Self> {
        Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            credentials: Arc::new(CredentialStore::new()),
            audit: Mutex::new(AuditLog::new(10_000)),
            api_keys: ApiKeyStore::new(),
            roles: RoleStore::new(),
            permissions: PermissionStore::new(),
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
            cluster_topology: None,
            cluster_routing: None,
            cluster_transport: None,
            node_id: 0,
            propose_tracker: None,
            raft_proposer: None,
            raft_status_fn: None,
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
            usage_counter: std::sync::Arc::new(
                crate::control::security::metering::counter::UsageCounter::new(),
            ),
            usage_store: std::sync::Arc::new(
                crate::control::security::metering::store::UsageStore::default(),
            ),
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
            stream_registry: Arc::new(crate::event::cdc::StreamRegistry::new()),
            cdc_router: Arc::new(crate::event::cdc::CdcRouter::new(Arc::new(
                crate::event::cdc::StreamRegistry::new(),
            ))),
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            system_metrics: Some(Arc::new(crate::control::metrics::SystemMetrics::new())),
            epoch_tracker: Mutex::new(std::collections::HashMap::new()),
            ts_partition_registries: Some(Mutex::new(std::collections::HashMap::new())),
            cold_storage: None,
            cluster_version_state: Mutex::new(
                crate::control::rolling_upgrade::ClusterVersionState::new(),
            ),
            tuning: TuningConfig::default(),
        })
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
        let mut audit_start_seq = 1u64;
        if let Some(catalog) = credentials.catalog() {
            api_keys.load_from(catalog)?;
            roles.load_from(catalog)?;
            permissions.load_from(catalog)?;
            blacklist.load_from(catalog)?;
            trigger_registry.load_all(catalog);
            stream_registry.load_from_catalog(catalog);
            let max_seq = catalog.load_audit_max_seq()?;
            if max_seq > 0 {
                audit_start_seq = max_seq + 1;
            }
        }

        let mut audit_log = AuditLog::new(10_000);
        audit_log.set_next_seq(audit_start_seq);

        Ok(Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            credentials: Arc::new(credentials),
            audit: Mutex::new(audit_log),
            api_keys,
            roles,
            permissions,
            trigger_registry,
            stream_registry: Arc::clone(&stream_registry),
            cdc_router: Arc::new(crate::event::cdc::CdcRouter::new(stream_registry)),
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
            cluster_topology: None,
            cluster_routing: None,
            cluster_transport: None,
            node_id: 0,
            propose_tracker: None,
            raft_proposer: None,
            raft_status_fn: None,
            migration_tracker: None,
            rls: RlsPolicyStore::new(),
            blacklist,
            auth_users: crate::control::security::jit::auth_user::AuthUserStore::new(),
            orgs: crate::control::security::org::store::OrgStore::new(),
            scope_defs: crate::control::security::scope::store::ScopeStore::new(),
            scope_grants: crate::control::security::scope::grant::ScopeGrantStore::new(),
            rate_limiter: crate::control::security::ratelimit::limiter::RateLimiter::default(),
            session_handles: crate::control::security::session_handle::SessionHandleStore::default(
            ),
            session_registry: crate::control::security::session_registry::SessionRegistry::new(),
            escalation: crate::control::security::escalation::EscalationEngine::default(),
            usage_counter: std::sync::Arc::new(
                crate::control::security::metering::counter::UsageCounter::new(),
            ),
            usage_store: std::sync::Arc::new(
                crate::control::security::metering::store::UsageStore::default(),
            ),
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
            epoch_tracker: Mutex::new(std::collections::HashMap::new()),
            ts_partition_registries: Some(Mutex::new(std::collections::HashMap::new())),
            cold_storage: None,
            cluster_version_state: Mutex::new(
                crate::control::rolling_upgrade::ClusterVersionState::new(),
            ),
            tuning,
        }))
    }

    /// Get the idle session timeout in seconds (0 = no timeout).
    pub fn idle_timeout_secs(&self) -> u64 {
        self.idle_timeout_secs
    }

    /// Access to timeseries partition registries.
    pub fn timeseries_registries(
        &self,
    ) -> Option<
        &Mutex<
            std::collections::HashMap<
                String,
                crate::engine::timeseries::partition_registry::PartitionRegistry,
            >,
        >,
    > {
        self.ts_partition_registries.as_ref()
    }

    /// Check tenant quota before dispatching a request. Returns Ok if allowed.
    pub fn check_tenant_quota(&self, tenant_id: TenantId) -> crate::Result<()> {
        let tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(poisoned) => {
                warn!("tenant isolation mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };
        match tenants.check(tenant_id) {
            QuotaCheck::Allowed => Ok(()),
            QuotaCheck::MemoryExceeded { used, limit } => Err(crate::Error::MemoryExhausted {
                engine: format!("tenant {tenant_id}: {used}/{limit} bytes"),
            }),
            QuotaCheck::ConcurrencyExceeded { active, limit } => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: {active}/{limit} concurrent requests"),
            }),
            QuotaCheck::RateLimited { qps, limit } => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: rate limited ({qps}/{limit} qps)"),
            }),
            QuotaCheck::StorageExceeded { used, limit } => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: storage quota ({used}/{limit} bytes)"),
            }),
        }
    }

    /// Record request start for tenant quota tracking.
    pub fn tenant_request_start(&self, tenant_id: TenantId) {
        match self.tenants.lock() {
            Ok(mut t) => t.request_start(tenant_id),
            Err(poisoned) => poisoned.into_inner().request_start(tenant_id),
        }
    }

    /// Record request end for tenant quota tracking.
    pub fn tenant_request_end(&self, tenant_id: TenantId) {
        match self.tenants.lock() {
            Ok(mut t) => t.request_end(tenant_id),
            Err(poisoned) => poisoned.into_inner().request_end(tenant_id),
        }
    }

    /// Reset per-second rate counters. Called by a 1-second timer.
    pub fn reset_tenant_rate_counters(&self) {
        match self.tenants.lock() {
            Ok(mut t) => t.reset_rate_counters(),
            Err(poisoned) => poisoned.into_inner().reset_rate_counters(),
        }
    }

    /// Record an audit event.
    pub fn audit_record(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) {
        match self.audit.lock() {
            Ok(mut log) => {
                log.record(event, tenant_id, source, detail);
            }
            Err(poisoned) => {
                warn!("audit log mutex poisoned, recovering");
                poisoned
                    .into_inner()
                    .record(event, tenant_id, source, detail);
            }
        }
    }

    /// Update per-tenant memory estimates.
    ///
    /// Called periodically (e.g. every 30 seconds). Uses jemalloc stats
    /// to estimate global memory, then distributes proportionally across
    /// tenants based on their request activity.
    pub fn update_tenant_memory_estimates(&self) {
        // Get total allocated from jemalloc.
        let total_allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) as u64;

        let mut tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };

        // Distribute proportionally by total_requests per tenant.
        let tenant_requests: Vec<(crate::types::TenantId, u64)> = {
            let users = self.credentials.list_user_details();
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();
            for user in &users {
                if seen.insert(user.tenant_id) {
                    let total = tenants
                        .usage(user.tenant_id)
                        .map_or(0, |u| u.total_requests);
                    result.push((user.tenant_id, total));
                }
            }
            result
        };

        let total_reqs: u64 = tenant_requests.iter().map(|(_, r)| *r).sum();
        if total_reqs == 0 {
            return;
        }

        for (tid, reqs) in &tenant_requests {
            let proportion = *reqs as f64 / total_reqs as f64;
            let estimated_bytes = (total_allocated as f64 * proportion) as u64;
            tenants.update_memory(*tid, estimated_bytes);
        }
    }

    /// Flush in-memory audit entries to the persistent catalog.
    /// Called periodically (e.g. every 10 seconds) by a background task.
    pub fn flush_audit_log(&self) {
        let entries = match self.audit.lock() {
            Ok(mut log) => log.drain_for_persistence(),
            Err(poisoned) => {
                warn!("audit log mutex poisoned during flush, recovering");
                poisoned.into_inner().drain_for_persistence()
            }
        };

        if entries.is_empty() {
            return;
        }

        if let Some(catalog) = self.credentials.catalog() {
            let stored: Vec<crate::control::security::catalog::StoredAuditEntry> = entries
                .iter()
                .map(|e| crate::control::security::catalog::StoredAuditEntry {
                    seq: e.seq,
                    timestamp_us: e.timestamp_us,
                    event: format!("{:?}", e.event),
                    tenant_id: e.tenant_id.map(|t| t.as_u32()),
                    source: e.source.clone(),
                    detail: e.detail.clone(),
                    prev_hash: e.prev_hash.clone(),
                })
                .collect();

            if let Err(e) = catalog.append_audit_entries(&stored) {
                warn!(error = %e, count = stored.len(), "failed to persist audit entries");
                // Re-insert entries so they're not lost.
                if let Ok(mut log) = self.audit.lock() {
                    for entry in entries {
                        log.record(entry.event, entry.tenant_id, &entry.source, &entry.detail);
                    }
                }
            } else {
                tracing::debug!(count = stored.len(), "flushed audit entries to catalog");

                // Prune old entries based on retention policy.
                if self.audit_retention_days > 0 {
                    let retention_us = self.audit_retention_days as u64 * 86400 * 1_000_000; // days → microseconds
                    let now_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as u64;
                    let cutoff = now_us.saturating_sub(retention_us);
                    match catalog.prune_audit_before(cutoff) {
                        Ok(0) => {}
                        Ok(n) => tracing::info!(
                            pruned = n,
                            days = self.audit_retention_days,
                            "pruned old audit entries"
                        ),
                        Err(e) => warn!(error = %e, "failed to prune old audit entries"),
                    }
                }
            }
        }
    }

    /// Poll responses from all Data Plane cores and route them to waiting sessions.
    ///
    /// This should be called periodically from a background Tokio task.
    pub fn poll_and_route_responses(&self) {
        let responses = match self.dispatcher.lock() {
            Ok(mut d) => d.poll_responses(),
            Err(poisoned) => {
                warn!("dispatcher mutex poisoned, recovering");
                poisoned.into_inner().poll_responses()
            }
        };
        for resp in responses {
            if !self.tracker.complete(resp) {
                warn!("response for unknown or cancelled request");
            }
        }
    }
}
