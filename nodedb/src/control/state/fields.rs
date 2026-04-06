//! SharedState struct definition — all fields for the Control Plane.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::permission::PermissionStore;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::security::role::RoleStore;
use crate::control::security::tenant::TenantIsolation;
use crate::control::server::sync::dlq::SyncDlq;
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
    pub usage_counter: Arc<crate::control::security::metering::counter::UsageCounter>,
    /// Usage metering store (aggregated events).
    pub usage_store: Arc<crate::control::security::metering::store::UsageStore>,
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
    pub jwks_registry: Option<Arc<crate::control::security::jwks::registry::JwksRegistry>>,

    /// Dead-Letter Queue for sync-rejected deltas.
    pub sync_dlq: Mutex<SyncDlq>,

    /// Audit retention in days (0 = keep forever).
    pub(super) audit_retention_days: u32,

    /// Idle session timeout in seconds (0 = no timeout).
    pub(super) idle_timeout_secs: u64,

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
    pub raft_proposer: Option<Arc<crate::control::wal_replication::RaftProposer>>,

    /// Query Raft group statuses for observability (None in single-node mode).
    pub raft_status_fn: Option<Arc<dyn Fn() -> Vec<nodedb_cluster::GroupStatus> + Send + Sync>>,

    /// Migration tracker for observability (None in single-node mode).
    pub migration_tracker: Option<Arc<nodedb_cluster::MigrationTracker>>,

    /// WebSocket session registry: tracks last-seen LSN per client session.
    pub ws_sessions: RwLock<std::collections::HashMap<String, u64>>,

    /// Pub/Sub topic registry with persistent message storage.
    pub topic_registry: crate::control::pubsub::TopicRegistry,

    /// Shape subscription registry for Lite client sync.
    pub shape_registry: crate::control::server::sync::shape::ShapeRegistry,

    /// Change stream bus: broadcasts committed mutations to subscribers.
    pub change_stream: crate::control::change_stream::ChangeStream,

    /// In-memory trigger registry for fast lookup during DML.
    pub trigger_registry: crate::control::trigger::TriggerRegistry,

    /// Cached parsed procedural blocks for triggers and procedures.
    pub block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache,

    /// In-memory change stream registry for CDC event routing.
    pub stream_registry: Arc<crate::event::cdc::StreamRegistry>,

    /// CDC event router: routes WriteEvents to matching stream buffers.
    pub cdc_router: Arc<crate::event::cdc::CdcRouter>,

    /// In-memory consumer group registry.
    pub group_registry: crate::event::cdc::GroupRegistry,

    /// Per-group, per-partition offset tracking (redb-persisted).
    pub offset_store: Arc<crate::event::cdc::OffsetStore>,

    /// In-memory retention policy registry for tiered data lifecycle.
    pub retention_policy_registry:
        Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,

    /// In-memory alert rule registry for threshold alerting.
    pub alert_registry: Arc<crate::event::alert::AlertRegistry>,

    /// Per-group hysteresis state for alert rules.
    pub alert_hysteresis: Arc<crate::event::alert::hysteresis::HysteresisManager>,

    /// In-memory schedule registry for cron scheduler.
    pub schedule_registry: Arc<crate::event::scheduler::ScheduleRegistry>,

    /// Job execution history (redb-persisted).
    pub job_history: Arc<crate::event::scheduler::JobHistoryStore>,

    /// Event Plane durable topic registry.
    pub ep_topic_registry: crate::event::topic::EpTopicRegistry,

    /// Webhook delivery manager for CDC change streams.
    pub webhook_manager: crate::event::webhook::WebhookManager,

    /// Streaming materialized view registry.
    pub mv_registry: Arc<crate::event::streaming_mv::MvRegistry>,

    /// Consumer partition assignment tracker for rebalancing.
    pub consumer_assignments: crate::event::cdc::consumer_group::ConsumerAssignments,

    /// Per-partition watermark tracker for streaming MVs.
    pub watermark_tracker: Arc<crate::event::watermark_tracker::WatermarkTracker>,

    /// Event Plane memory budget (512 MB cap).
    pub event_plane_budget: Arc<crate::event::budget::EventPlaneBudget>,

    /// Cross-shard event dispatcher (None in single-node mode).
    pub cross_shard_dispatcher: Option<Arc<crate::event::cross_shard::CrossShardDispatcher>>,

    /// Cross-shard dead letter queue (None in single-node mode).
    pub cross_shard_dlq: Option<Arc<Mutex<crate::event::cross_shard::CrossShardDlq>>>,

    /// Cross-shard delivery metrics (None in single-node mode).
    pub cross_shard_metrics: Option<Arc<crate::event::cross_shard::CrossShardMetrics>>,

    /// Cross-shard high-water-mark dedup store (None in single-node mode).
    pub hwm_store: Option<Arc<crate::event::cross_shard::HwmStore>>,

    /// Kafka bridge producer manager.
    pub kafka_manager: crate::event::kafka::KafkaManager,

    /// CRDT sync delivery: pushes outbound deltas to connected Lite sessions.
    pub crdt_sync_delivery: Arc<crate::event::crdt_sync::CrdtSyncDelivery>,

    /// CRDT delta packager: converts WriteEvents to outbound deltas.
    pub delta_packager: Arc<crate::event::crdt_sync::DeltaPackager>,

    /// Streaming MV state persistence (redb).
    pub mv_persistence: Arc<crate::event::streaming_mv::MvPersistence>,

    /// Total connections rejected due to max_connections limit.
    pub connections_rejected: AtomicU64,

    /// Total connections accepted since startup.
    pub connections_accepted: AtomicU64,

    /// System-wide metrics (Prometheus format).
    pub system_metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,

    /// Memory governor for per-engine budget enforcement.
    pub governor: Option<Arc<nodedb_mem::MemoryGovernor>>,

    /// Fork detection: tracks `lite_id → last_seen_epoch`.
    pub epoch_tracker: Mutex<std::collections::HashMap<String, u64>>,

    /// Timeseries partition registries.
    pub ts_partition_registries: Option<
        Mutex<
            std::collections::HashMap<
                String,
                crate::engine::timeseries::partition_registry::PartitionRegistry,
            >,
        >,
    >,

    /// L2 cold storage client (None when not configured).
    pub cold_storage: Option<Arc<crate::storage::cold::ColdStorage>>,

    /// Rolling upgrade version tracking (cluster mode only).
    pub cluster_version_state: Mutex<crate::control::rolling_upgrade::ClusterVersionState>,

    /// Keep-alive senders for shutdown watch channels.
    pub(super) _shutdown_senders: Vec<tokio::sync::watch::Sender<bool>>,

    /// Performance tuning configuration.
    pub tuning: TuningConfig,

    /// Schema version counter — bumped on CREATE/DROP/ALTER DDL.
    pub schema_version: crate::control::server::pgwire::handler::prepared::SchemaVersion,

    /// In-memory sequence registry (nextval/currval/setval).
    /// Arc-wrapped for sharing with DataFusion UDFs (nextval, currval, setval).
    pub sequence_registry: Arc<crate::control::sequence::SequenceRegistry>,

    /// Per-collection DML counter for auto-ANALYZE triggering.
    /// Incremented after each successful write dispatch.
    pub dml_counter: crate::control::server::pgwire::ddl::maintenance::auto_analyze::DmlCounter,

    /// Highest WAL LSN confirmed delivered to Data Plane for timeseries catch-up.
    pub wal_catchup_lsn: AtomicU64,

    /// Presence/Awareness manager: ephemeral user state broadcast channels.
    /// Wrapped in `tokio::sync::RwLock` for concurrent reads (broadcast fan-out)
    /// with exclusive writes (upsert/remove/sweep).
    pub presence: Arc<tokio::sync::RwLock<crate::control::server::sync::presence::PresenceManager>>,

    /// Permission tree cache: in-memory resource hierarchy + permission grants.
    /// Used by RLS injection to resolve hierarchical permissions without
    /// crossing to the Data Plane.
    pub permission_cache:
        Arc<tokio::sync::RwLock<crate::control::security::permission_tree::PermissionCache>>,
}
