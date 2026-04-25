//! SharedState struct definition — all fields for the Control Plane.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

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

    /// Collection-scoped scan quiesce registry. Before unlinking on-disk
    /// files during `PurgeCollection`, `purge_async` calls
    /// `begin_drain` + `wait_until_drained` against this registry so
    /// every in-flight scan finishes before the reclaim handler runs.
    /// Scan handlers bump the refcount via `try_start_scan` — new scans
    /// against a draining collection are refused with
    /// `NodeDbError::collection_draining`.
    pub quiesce: Arc<crate::bridge::quiesce::CollectionQuiesce>,

    /// Credential store for user authentication.
    pub credentials: Arc<CredentialStore>,

    /// Audit log for security-relevant events.
    ///
    /// Behind an `Arc` so sub-systems (e.g. `SessionHandleStore`) can hold
    /// a clone of the handle for their own emission path without needing
    /// to call back into `SharedState` — a weak reference would block
    /// `Arc::get_mut` during the cluster wire-up phase.
    ///
    /// **Plane affinity:** Control Plane only. Emitters must be
    /// `Send + Sync` and must never run inside a Data Plane TPC loop —
    /// a contended `Mutex::lock()` would stall the io_uring reactor.
    /// Current emitters (pgwire handlers, HTTP handlers,
    /// `SessionHandleStore` hooks, DDL applier) all live on Tokio.
    pub audit: Arc<Mutex<AuditLog>>,

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

    /// Live view of the replicated metadata catalog. Written by the
    /// `MetadataCommitApplier` as metadata-group entries commit, read
    /// by `OriginCatalog`, pgwire DDL handlers, and HTTP catalog
    /// endpoints. Always present — in single-node / no-cluster mode
    /// the cache stays empty and reads fall through to the legacy
    /// `SystemCatalog` redb path.
    pub metadata_cache: Arc<RwLock<nodedb_cluster::MetadataCache>>,

    /// Broadcasts one event per committed metadata entry. Consumers:
    /// pgwire prepared-statement cache invalidation, HTTP catalog
    /// cache rebuild, CDC schema-change stream. A lagging subscriber
    /// is fine — the applier does not block on delivery.
    pub catalog_change_tx: tokio::sync::broadcast::Sender<
        crate::control::cluster::metadata_applier::CatalogChangeEvent,
    >,

    /// Watcher advanced by the `MetadataCommitApplier` after each
    /// apply batch. Used by `metadata_proposer::propose_catalog_entry`
    /// to synchronously block until a freshly-proposed entry is
    /// visible on this node.
    pub metadata_applied_index_watcher:
        Arc<crate::control::cluster::applied_index_watcher::AppliedIndexWatcher>,

    /// Type-erased handle for proposing to the metadata raft group.
    /// Installed by `cluster::start_raft` after `SharedState::open`
    /// has returned. `None` in single-node / no-cluster mode.
    pub metadata_raft: OnceLock<Arc<dyn crate::control::metadata_proposer::MetadataRaftHandle>>,

    /// Propose tracker for distributed writes (None in single-node mode).
    pub propose_tracker: Option<Arc<crate::control::wal_replication::ProposeTracker>>,

    /// Raft propose function — wraps RaftLoop::propose (None in single-node mode).
    pub raft_proposer: Option<Arc<crate::control::wal_replication::RaftProposer>>,

    /// Query Raft group statuses for observability (None in single-node mode).
    pub raft_status_fn: Option<Arc<dyn Fn() -> Vec<nodedb_cluster::GroupStatus> + Send + Sync>>,

    /// Cluster observability handle — lifecycle tracker + topology +
    /// routing + per-group status provider, bundled.
    ///
    /// Stored as a `OnceLock` so the main binary can populate it
    /// **after** `SharedState::open` has returned an `Arc<SharedState>`
    /// (at which point `Arc::get_mut` would fail because `start_raft`
    /// has already cloned the Arc into background tasks). Set exactly
    /// once in `control::cluster::start_raft`.
    pub cluster_observer: OnceLock<Arc<nodedb_cluster::ClusterObserver>>,

    /// Registry of standardized per-loop metrics (iterations, last
    /// duration, errors by kind, up flag). Populated at startup by
    /// `control::cluster::start_raft` (for the raft, health,
    /// reachability, and rebalancer loops) and by the lease-renewal
    /// spawn site (for `descriptor_lease_loop`). The `/metrics` route
    /// iterates the registry and emits every loop's Prometheus
    /// exposition.
    pub loop_metrics_registry: Arc<nodedb_cluster::LoopMetricsRegistry>,

    /// Per-vShard QPS + latency histograms. Populated by the Control
    /// Plane dispatch site on every request; consumed by `SHOW RANGES`,
    /// the Prometheus route, and the rebalancer's load-metrics
    /// provider (which aggregates the per-vshard rates into per-node
    /// `LoadMetrics` for scoring).
    pub per_vshard_metrics: Arc<crate::control::metrics::PerVShardMetricsRegistry>,

    /// Handle to the cluster health monitor. Set exactly once in
    /// `control::cluster::start_raft`. Used by the Prometheus route
    /// to render the `health_loop_suspect_peers{peer_id}` gauge.
    pub health_monitor: OnceLock<Arc<nodedb_cluster::HealthMonitor>>,

    /// Dispatcher for OTLP trace-span emission. Always set — disabled
    /// instances are no-ops so gateway and executor call sites don't
    /// need conditionals. Configured at startup from
    /// `observability.otlp.export`.
    pub trace_exporter: Arc<crate::control::trace_export::TraceExporter>,

    /// Master kill-switch for the `/cluster/debug/*` HTTP endpoints
    /// (J.5). Populated at startup from
    /// `observability.debug_endpoints_enabled`; defaults to `false`
    /// so a production deployment that forgets to set the flag never
    /// accidentally exposes raft / transport / catalog internals.
    /// The handlers still require a superuser identity — this gate
    /// is the second line of defence.
    pub debug_endpoints_enabled: bool,

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

    /// Shared HTTP client for all outbound emitters (alert webhooks, SIEM
    /// webhooks, OTEL exporter). Constructing `reqwest::Client` allocates
    /// a connection pool, DNS resolver, TLS config, and rustls session
    /// cache — every emitter clones this Arc rather than rebuilding.
    pub http_client: Arc<reqwest::Client>,

    /// In-memory trigger registry for fast lookup during DML.
    pub trigger_registry: crate::control::trigger::TriggerRegistry,

    /// Shared ND-array catalog handle. Mirrors the persisted
    /// `_system.arrays` rows and is shared (by `Arc` clone) with every
    /// Data-Plane `CoreLoop` so dispatch handlers can resolve array
    /// names and schema digests without crossing planes.
    pub array_catalog: crate::control::array_catalog::ArrayCatalogHandle,

    /// Global surrogate registry — the source of monotonic
    /// `Surrogate(u32)` allocation that backs the cross-engine PK ↔
    /// Surrogate map (`_system.surrogate_pk{,_rev}`). Bootstrapped
    /// from the persisted hwm at `SharedState::open` and cloned into
    /// every CP path that calls `assign_surrogate`.
    pub surrogate_registry: crate::control::surrogate::SurrogateRegistryHandle,

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

    /// Per-collection bitemporal audit-retention policy registry.
    /// Populated by DDL (`CREATE COLLECTION ... WITH BITEMPORAL RETENTION`),
    /// consumed by the background enforcement loop that dispatches
    /// `MetaOp::TemporalPurge*`.
    pub bitemporal_retention_registry: Arc<crate::engine::bitemporal::BitemporalRetentionRegistry>,

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

    /// Live retention settings. Wrapped in a `RwLock` so the
    /// `ALTER SYSTEM SET deactivated_collection_retention_days` handler
    /// can mutate it at runtime and the collection-GC sweeper picks up
    /// the new window on its next tick without a restart.
    pub retention_settings: Arc<std::sync::RwLock<crate::config::server::RetentionSettings>>,

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

    /// Hybrid Logical Clock used by the metadata applier to stamp
    /// `modification_hlc` on every persisted `Stored*` descriptor.
    /// Single instance per node; the applier calls `update(remote)`
    /// before every stamp so cross-node causality is preserved.
    pub hlc_clock: Arc<nodedb_types::HlcClock>,

    /// Per-tenant monotonic high-water HLC wall-ns observed for any
    /// dispatched plan. Used by RESTORE to reject envelopes whose
    /// captured watermark is older than the destination cluster's
    /// most recent observed dispatch for the same tenant —
    /// silently overwriting newer committed writes would otherwise
    /// be a correctness bug.
    pub tenant_write_hlc: Arc<std::sync::Mutex<std::collections::HashMap<u32, u64>>>,

    /// Replicated descriptor lease drain state.
    /// Written by the metadata applier on `DescriptorDrainStart`
    /// / `DescriptorDrainEnd` raft entries (and implicitly
    /// cleared on successful `Put*` apply). Read by the lease
    /// acquire path (`force_refresh_lease`) to reject new
    /// acquires at versions being drained, and by the drain
    /// proposer's wait loop to check for completion.
    pub lease_drain: Arc<crate::control::lease::DescriptorDrainTracker>,

    /// Host-side reference count for descriptor leases.
    /// Multiple concurrent queries holding the same descriptor
    /// lease increment this counter on plan and decrement on
    /// query completion; only the first holder performs the
    /// raft acquire and only the last holder performs the
    /// raft release. Enables drain to make progress when all
    /// in-flight queries on a descriptor finish.
    pub lease_refcount: Arc<crate::control::lease::LeaseRefCount>,

    /// Canonical shutdown watch. Every background loop in the
    /// Control Plane and Event Plane subscribes to this and
    /// exits on signal. The `main.rs` ctrl-c handler calls
    /// `signal()` before invoking `loop_registry.shutdown_all`.
    pub shutdown: Arc<crate::control::shutdown::ShutdownWatch>,

    /// Registry of every background loop's join handle. Used
    /// by `main.rs` to await all loops with a shared deadline
    /// on shutdown and report laggards.
    pub loop_registry: Arc<crate::control::shutdown::LoopRegistry>,

    /// Startup phase observer handle. Listeners call
    /// `startup.await_phase(GatewayEnable)` to block until the node
    /// is ready to accept client traffic. `main.rs` drives phase
    /// transitions via a `StartupSequencer` it constructs before
    /// calling `SharedState::open`, then swaps this field via
    /// `Arc::get_mut`. See `control::startup` for the contract.
    pub startup: Arc<crate::control::startup::StartupGate>,

    /// Performance tuning configuration.
    pub tuning: TuningConfig,

    /// The node's on-disk data directory (from `config.data_dir`).
    /// Needed by host-side appliers that write to filesystem — the
    /// L.4 CA-trust applier writes `tls/ca.d/<fp>.crt`, the audit
    /// segment opens under `audit/`, etc. Populated by `main.rs`
    /// before raft starts.
    pub data_dir: std::path::PathBuf,

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

    /// Gateway plan-cache invalidator.
    ///
    /// Called from `catalog_entry::post_apply` after every DDL commit that
    /// mutates a descriptor. Evicts stale gateway plan-cache entries for the
    /// changed collection so subsequent queries re-plan against the new schema.
    ///
    /// `None` until `Gateway::new` runs (after cluster topology is ready).
    pub gateway_invalidator: Option<Arc<crate::control::gateway::PlanCacheInvalidator>>,

    /// The gateway: single entry point for routing physical plans to the
    /// correct cluster node. Constructed after cluster topology is ready
    /// (after `Arc::get_mut` is possible on `SharedState`) and before
    /// listeners bind.
    ///
    /// `None` in the brief window between `SharedState::open` and gateway
    /// construction; listeners should gate on `startup.await_ready()` before
    /// calling `gateway`.
    pub gateway: Option<Arc<crate::control::gateway::Gateway>>,
}
