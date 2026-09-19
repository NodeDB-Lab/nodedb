// SPDX-License-Identifier: BUSL-1.1

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use nodedb_types::config::TuningConfig;
use nodedb_types::protocol::Limits;

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
use crate::control::state::calvin_counters::CalvinCounters;
use crate::wal::WalManager;

/// Atomically installed Raft proposal handles.
pub(super) struct AsyncRaftProposerPair {
    pub(super) sequenced: Arc<crate::control::wal_replication::AsyncRaftProposer>,
    pub(super) raw: Arc<crate::control::wal_replication::AsyncRaftProposer>,
}

pub struct SharedState {
    pub dispatcher: Mutex<Dispatcher>,
    pub tracker: RequestTracker,
    pub wal: Arc<WalManager>,
    /// Collection-scoped scan quiesce registry for safe `PurgeCollection` reclaim.
    pub quiesce: Arc<crate::bridge::quiesce::CollectionQuiesce>,
    pub credentials: Arc<CredentialStore>,
    /// Audit log — Control Plane only; emitters must be `Send + Sync`.
    pub audit: Arc<Mutex<AuditLog>>,
    pub api_keys: ApiKeyStore,
    pub roles: RoleStore,
    pub permissions: PermissionStore,
    /// Per-tenant quota enforcement.
    pub tenants: Mutex<TenantIsolation>,
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
    /// Active session registry for KILL SESSIONS and bus-consumer hard-revoke.
    pub session_registry: Arc<crate::control::security::sessions::SessionRegistry>,
    /// Auto-escalation engine (violations → suspend → ban).
    pub escalation: crate::control::security::escalation::EscalationEngine,
    /// Usage metering counter (per-core atomic, periodic flush).
    pub usage_counter: Arc<crate::control::security::metering::counter::UsageCounter>,
    /// Usage metering store (aggregated events).
    pub usage_store: Arc<crate::control::security::metering::store::UsageStore>,
    /// Quota manager (enforcement against scope quotas).
    pub quota_manager: crate::control::security::metering::quota::QuotaManager,
    /// Usage metering configuration (enabled flag, per-operation costs); no live
    /// mutation, so the bounds are baked into `usage_store`/`quota_manager`.
    pub metering_config: crate::control::security::metering::config::MeteringConfig,
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
    /// Maximum total audit entries in the catalog (0 = unlimited).
    pub(super) audit_max_entries: u64,
    /// Idle session timeout in seconds (0 = no timeout).
    pub(super) idle_timeout_secs: u64,
    /// Absolute session lifetime in seconds (0 = disabled).
    pub(super) session_absolute_timeout_secs: u64,
    /// Cluster topology (None in single-node mode).
    pub cluster_topology: Option<Arc<RwLock<nodedb_cluster::ClusterTopology>>>,
    /// Cluster routing table (None in single-node mode).
    pub cluster_routing: Option<Arc<RwLock<nodedb_cluster::RoutingTable>>>,
    /// Cluster transport for forwarding requests (None in single-node mode).
    pub cluster_transport: Option<Arc<nodedb_cluster::NexarTransport>>,
    /// This node's ID (0 in single-node mode).
    pub node_id: u64,

    /// SWIM incarnation resolved at cluster init; stamped on fenced lease grants (0 standalone).
    pub node_incarnation: u64,
    /// Live view of the replicated metadata catalog. Falls through to legacy redb in single-node mode.
    pub metadata_cache: Arc<RwLock<nodedb_cluster::MetadataCache>>,
    /// Broadcasts one event per committed metadata entry to subscribers (pgwire cache, CDC, etc.).
    pub catalog_change_tx: tokio::sync::broadcast::Sender<
        crate::control::cluster::metadata_applier::CatalogChangeEvent,
    >,
    /// Per-Raft-group apply watermark registry for commit-wait and drain paths.
    pub group_watchers: Arc<nodedb_cluster::GroupAppliedWatchers>,
    /// Serializes this node's descriptor preparation lease (replicated).
    pub metadata_ddl_lock: Mutex<()>,
    /// Replicated preparation owner plus local monotonic apply time.
    pub metadata_ddl_owner: Mutex<Option<(u64, std::time::Instant)>>,
    /// Most recent fenced DDL token applied while its owner remained current.
    pub metadata_ddl_applied_token: AtomicU64,
    /// Per-node uniqueness component for descriptor-preparation lease tokens.
    pub metadata_ddl_token_seq: AtomicU64,
    /// Node-local table of in-flight `DdlPendingPropose` records, rebuilt by
    /// metadata Raft log replay like `metadata_ddl_owner` above.
    pub pending_ddl: crate::control::pending_ddl::PendingDdlTable,
    /// Set once when the metadata applier halts on a failure that re-delivery
    /// cannot clear. The readiness probe reads it so a wedged node stops
    /// reporting itself healthy while every query dies on a lease timeout.
    pub metadata_apply_wedge: Arc<crate::control::cluster::metadata_applier::MetadataApplyWedge>,
    /// Set once when the Calvin sequencer state machine halts on an epoch
    /// regression. Every non-Calvin path keeps serving, so the node stays up —
    /// the health surfaces read this to make the lost capability visible rather
    /// than letting it look like an ordinary node.
    pub sequencer_halt: Arc<crate::control::cluster::SequencerHaltMarker>,
    /// Which Data Plane cores have stopped completing event-loop iterations,
    /// as of the last sampling window. Replaced every window rather than
    /// latched, because a stall can recover. The health surfaces read it so a
    /// core that wedges without panicking stops being invisible.
    pub core_stall: Arc<crate::control::cluster::CoreStallMarker>,
    /// Handle for proposing to the metadata raft group. Set by `start_raft`; None in single-node mode.
    pub metadata_raft: OnceLock<Arc<dyn crate::control::metadata_proposer::MetadataRaftHandle>>,
    /// Propose tracker for distributed writes; absent in single-node mode.
    pub propose_tracker: OnceLock<Arc<crate::control::wal_replication::ProposeTracker>>,
    /// Raft proposer; absent in single-node mode.
    pub raft_proposer: OnceLock<Arc<crate::control::wal_replication::RaftProposer>>,
    /// Atomically installed sequenced and narrowly scoped raw proposal handles.
    pub(super) async_raft_proposer_pair: OnceLock<AsyncRaftProposerPair>,
    pub vshard_admission_sequencer: Arc<crate::control::vshard_admission::VShardAdmissionSequencer>,
    /// Raft log compaction after durable Data-Plane apply; absent in single-node mode.
    pub raft_compactor: OnceLock<Arc<crate::control::wal_replication::RaftCompactor>>,
    /// Durable post-WAL apply index; never Raft's in-memory enqueue watermark.
    pub raft_applied_index_sink:
        OnceLock<Arc<crate::control::wal_replication::RaftAppliedIndexSink>>,
    /// Answers whether a read can be served here: leadership confirmed for a
    /// linearizable read, replica freshness for a bounded-staleness one.
    /// Unset in single-node mode, where neither question arises.
    pub raft_read_gate: OnceLock<Arc<dyn crate::control::cluster::read_index::RaftReadGate>>,
    /// This node's cluster-epoch state. Set once by `start_raft`; unset in
    /// single-node mode, where there is no topology generation to miss.
    pub cluster_epoch: OnceLock<Arc<nodedb_cluster::ClusterEpochState>>,
    /// Query Raft group statuses for observability (unset in single-node mode).
    pub raft_status_fn:
        std::sync::OnceLock<Arc<dyn Fn() -> Vec<nodedb_cluster::GroupStatus> + Send + Sync>>,
    /// Cluster observability handle. Set once by `start_raft`.
    pub cluster_observer: OnceLock<Arc<nodedb_cluster::ClusterObserver>>,
    /// Registry of standardized per-loop metrics. Populated by `start_raft`.
    pub loop_metrics_registry: Arc<nodedb_cluster::LoopMetricsRegistry>,
    /// Per-vShard QPS + latency histograms for `SHOW RANGES`, Prometheus, and rebalancer.
    pub per_vshard_metrics: Arc<crate::control::metrics::PerVShardMetricsRegistry>,
    /// Cluster health monitor handle. Set by `start_raft`.
    pub health_monitor: OnceLock<Arc<nodedb_cluster::HealthMonitor>>,
    /// OTLP trace-span dispatcher (no-op when not configured).
    pub trace_exporter: Arc<crate::control::trace_export::TraceExporter>,
    /// Kill-switch for `/cluster/debug/*` HTTP endpoints (defaults false).
    pub debug_endpoints_enabled: bool,
    /// Migration tracker for observability (None in single-node mode).
    pub migration_tracker: Option<Arc<nodedb_cluster::MigrationTracker>>,
    /// Shape subscription registry for Lite client sync.
    pub shape_registry: Arc<crate::control::server::sync::shape::ShapeRegistry>,
    /// Change stream bus: broadcasts committed mutations to subscribers.
    pub change_stream: crate::control::change_stream::ChangeStream,
    /// PostgreSQL LISTEN/NOTIFY bus: per-tenant channel delivery.
    pub notify_bus: crate::control::notify_bus::NotifyBus,
    /// Shared HTTP client for outbound emitters (alert webhooks, SIEM, OTEL).
    pub http_client: Arc<reqwest::Client>,
    /// In-memory trigger registry for fast lookup during DML.
    pub trigger_registry: crate::control::trigger::TriggerRegistry,
    /// Shared ND-array catalog handle. Arc-cloned into every Data-Plane CoreLoop.
    pub array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    /// Durable op-log for array CRDT sync.
    pub array_sync_op_log: std::sync::Arc<crate::control::array_sync::OriginOpLog>,
    /// Per-replica acknowledged HLC per array for GC and catch-up serving.
    pub array_ack_registry: std::sync::Arc<crate::control::array_sync::ArrayAckRegistry>,
    /// Tile snapshot store for array CRDT sync.
    pub array_snapshot_store: std::sync::Arc<crate::control::array_sync::OriginSnapshotStore>,
    /// Per-database/tenant/array GC boundary HLC. `Hlc::ZERO` means no GC has occurred.
    pub array_snapshot_hlcs: crate::control::array_sync::ArraySnapshotHlcs,
    /// GC background task handle for shutdown await.
    pub array_gc_handle: Option<tokio::task::JoinHandle<()>>,
    /// Session-invalidation broadcast bus.  Dropping the sender shuts down the consumer.
    pub session_invalidation_bus: crate::control::security::buses::SessionInvalidationBus,
    /// User-change broadcast bus.  Dropping the sender shuts down the consumer.
    pub user_change_bus: crate::control::security::buses::UserChangeBus,
    /// Audit-row consumer task for the security buses.  Awaited on graceful shutdown.
    pub bus_consumer_handle: Option<tokio::task::JoinHandle<()>>,
    /// Per-array schema CRDT registry for array sync (survives restarts).
    pub array_sync_schemas: std::sync::Arc<crate::control::array_sync::OriginSchemaRegistry>,
    /// Per-session outbound array CRDT frame channels for the WebSocket send loop.
    pub array_delivery: std::sync::Arc<crate::control::array_sync::ArrayDeliveryRegistry>,
    /// Per-subscriber HLC cursor map for array outbound sync.
    pub array_subscriber_cursors: std::sync::Arc<crate::control::array_sync::SubscriberMap>,
    /// Cross-shard merger registry for HLC-ordered multi-shard delivery.
    pub array_merger_registry: std::sync::Arc<crate::control::array_sync::MergerRegistry>,
    /// Registry of active cross-cluster observer links for mirror databases.
    /// One entry per mirror actively following a source cluster; consulted by
    /// `ALTER DATABASE PROMOTE` to tear down the source link before the catalog
    /// mutation lands. Added on mirror creation/restart-resume, removed on
    /// promotion or `DROP DATABASE`.
    pub mirror_link_registry: Arc<crate::control::mirror::MirrorLinkRegistry>,
    /// Database-id allocator. Threadsafe via internal atomics.
    /// Authoritative allocation is proposed through Raft metadata group 0;
    /// this counter is the local cache (mirrors `SurrogateAssigner` semantics).
    pub database_registry: crate::control::database::DatabaseRegistry,
    /// Global surrogate registry for stable cross-engine PK ↔ Surrogate allocation.
    pub surrogate_registry: crate::control::surrogate::SurrogateRegistryHandle,
    /// CP-side surrogate assigner for INSERT/UPSERT paths.
    pub surrogate_assigner: Arc<crate::control::surrogate::SurrogateAssigner>,
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
    pub bitemporal_retention_registry: Arc<crate::engine::bitemporal::BitemporalRetentionRegistry>,
    /// In-memory alert rule registry for threshold alerting.
    pub alert_registry: Arc<crate::event::alert::AlertRegistry>,
    /// Per-group hysteresis state for alert rules.
    pub alert_hysteresis: Arc<crate::event::alert::hysteresis::HysteresisManager>,
    /// In-memory schedule registry for cron scheduler.
    pub schedule_registry: Arc<crate::event::scheduler::ScheduleRegistry>,
    /// In-memory synonym group registry for FTS query expansion.
    pub synonym_registry: Arc<crate::control::synonym::SynonymRegistry>,
    /// In-memory custom type registry (enum + composite types).
    pub custom_type_registry: Arc<crate::control::custom_type::CustomTypeRegistry>,
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
    /// Definition sync fanout: broadcasts `DefinitionSync` (0x70) frames to
    /// all connected Lite sessions after a successful DDL commit
    /// (CREATE/DROP FUNCTION, TRIGGER, PROCEDURE).
    pub definition_sync_fanout:
        std::sync::Arc<crate::control::server::sync::definition_fanout::DefinitionSyncFanout>,
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
    /// Total Raft propose retries triggered by `RetryableLeaderChange`.
    pub raft_propose_leader_change_retries: AtomicU64,
    /// Per-node monotonic request ID allocator. Starts at 1 (0 is sentinel).
    pub request_id_counter: AtomicU64,
    /// Per-node monotonic distributed-shuffle ID allocator. Starts at 1 (0 is
    /// a sentinel). Each coordinator-driven shuffle join (`ExchangeMode::Shuffle`)
    /// allocates one `shuffle_id` here, scoping producer fan-out inboxes and
    /// consumer barriers on every part-owner node. Kept distinct from
    /// `request_id_counter` so the shuffle keyspace never collides with SPSC.
    pub shuffle_id_counter: AtomicU64,
    /// System-wide metrics (Prometheus format).
    pub system_metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,
    /// Per-database quota usage counters for Prometheus scraping.
    pub database_metrics: Arc<crate::control::metrics::DatabaseMetricsRegistry>,
    /// Global per-cluster quota ceiling enforced when database quotas are
    /// written. Populated at startup from `[server]` config (`memory_limit`,
    /// `max_connections`); zero on a dimension means no ceiling there. Read by
    /// `ALTER DATABASE … SET QUOTA` to validate configured quotas stay within
    /// cluster resources. `RwLock`-wrapped for a future `ALTER SYSTEM` mutator.
    pub quota_ceiling: Arc<RwLock<crate::control::security::catalog::GlobalQuotaCeiling>>,
    /// Live retention settings. RwLock-wrapped for runtime ALTER SYSTEM mutation.
    pub retention_settings: Arc<std::sync::RwLock<crate::config::server::RetentionSettings>>,
    /// Memory governor for per-engine budget enforcement.
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    /// Per-database maintenance CPU budget tracker. Shared with every Data
    /// Plane `CoreLoop` so all cores draw from the same per-database window.
    /// Populated from `ALTER DATABASE … SET QUOTA (maintenance_cpu_pct = N)`.
    pub maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
    /// Durable producer registry for Lite client fencing.  `None` when the
    /// system catalog is unavailable (in-memory / test configurations).
    pub producer_registry:
        Option<Arc<crate::control::sync_producer::registry::SyncProducerRegistry>>,
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
    /// Warm-tier snapshot object store (defaults to local FS).
    pub snapshot_storage: Arc<dyn object_store::ObjectStore>,
    /// Quarantine archive object store (defaults to local FS).
    pub quarantine_storage: Arc<dyn object_store::ObjectStore>,
    /// Hybrid Logical Clock for metadata descriptor `modification_hlc` stamps.
    pub hlc_clock: Arc<nodedb_types::HlcClock>,
    /// Per-tenant monotonic HLC high-water used by RESTORE for write-order safety.
    pub tenant_write_hlc: Arc<std::sync::Mutex<std::collections::HashMap<u64, u64>>>,
    /// Serializes descriptor plan admission with local drain-start installation.
    ///
    /// Hold this std mutex while checking drain state and changing lease
    /// refcounts. Drain starts hold the same gate while publishing their local
    /// replicated state, so neither path can observe a partially admitted query.
    /// Lease grants happen later under `lease_grant_gate`.
    pub lease_admission_gate: Mutex<()>,
    /// Serializes raft/local descriptor lease grants and releases after
    /// admission has completed. It is independent of `lease_admission_gate`,
    /// so it may be held while a metadata proposal waits for apply.
    pub lease_grant_gate: Arc<Mutex<()>>,
    /// Replicated descriptor lease drain state (written by metadata applier).
    pub lease_drain: Arc<crate::control::lease::DescriptorDrainTracker>,
    /// Host-side refcount for descriptor leases (enables drain after last query).
    pub lease_refcount: Arc<crate::control::lease::LeaseRefCount>,
    /// Canonical shutdown watch — all background loops subscribe and exit on signal.
    pub shutdown: Arc<crate::control::shutdown::ShutdownWatch>,
    /// Registry of every background loop's join handle for graceful shutdown.
    pub loop_registry: Arc<crate::control::shutdown::LoopRegistry>,
    /// Latch the Data Plane drain sets once every core has finished its
    /// in-flight work. The response poller keeps routing until it is set.
    pub data_plane_drain: Arc<crate::control::shutdown::DataPlaneDrain>,
    /// Startup phase gate — listeners block until `GatewayEnable` phase.
    pub startup: Arc<crate::control::startup::StartupGate>,
    /// Calvin sequencer inbox for cross-shard transactions (empty in single-node mode).
    pub sequencer_inbox: std::sync::OnceLock<nodedb_cluster::calvin::sequencer::inbox::Inbox>,
    /// Calvin reservation inbox for hot-key read reservations (empty in single-node mode).
    pub reservation_inbox:
        std::sync::OnceLock<nodedb_cluster::calvin::sequencer::reservation_inbox::ReservationInbox>,
    /// Sequencer metrics for Prometheus `/metrics` route.
    pub sequencer_metrics: std::sync::OnceLock<
        std::sync::Arc<nodedb_cluster::calvin::sequencer::metrics::SequencerMetrics>,
    >,
    /// Calvin completion registry for sequencer submission and participant completion.
    pub calvin_completion_registry:
        std::sync::OnceLock<std::sync::Arc<nodedb_cluster::calvin::CalvinCompletionRegistry>>,
    /// OLLP orchestrator for dependent-read Calvin transactions (empty in single-node mode).
    pub ollp_orchestrator: std::sync::OnceLock<
        std::sync::Arc<
            crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator,
        >,
    >,
    /// Per-operation limits announced to clients in `HelloAckFrame`.
    pub limits: Limits,
    /// Performance tuning configuration.
    pub tuning: TuningConfig,
    /// Scheduler configuration (cron timezone offset, etc.).
    pub scheduler_config: crate::config::server::SchedulerConfig,
    /// On-disk data directory for host-side appliers (CA-trust, audit segments, etc.).
    pub data_dir: std::path::PathBuf,
    /// The Event Plane's trigger dead-letter queue, so operator statements can
    /// read and requeue failed actions. Set at boot; absent on a node that
    /// runs no Event Plane.
    pub trigger_dlq: OnceLock<Arc<Mutex<crate::event::trigger::TriggerDlq>>>,
    /// Where an operator's requeued actions wait for their consumer to collect
    /// them. Set by the Event Plane, which knows how many consumers it spawned;
    /// absent on a node that runs none.
    pub action_requeue: OnceLock<Arc<crate::event::action::ActionRequeueInbox>>,
    /// Test-only drop guard: owns the auto-cleaning temp directory the test
    /// constructor roots its CDC-offset / job-history / MV-persistence stores
    /// under, so they're removed on drop instead of leaking `/tmp/nodedb-test-*`
    /// dirs across a test session. `None` in production.
    pub _test_state_dir: Option<tempfile::TempDir>,
    /// Schema version counter — bumped on CREATE/DROP/ALTER DDL.
    pub schema_version: crate::control::server::shared::session::plan_cache::SchemaVersion,
    /// Materialized-sum bindings indexed by SOURCE collection, cached per
    /// schema version. Keeps the plan-time target resolution off the catalog
    /// scan that deriving it from the target-side declarations would otherwise
    /// need on every write.
    pub materialized_sum_index: crate::control::planner::materialized_sum::MaterializedSumIndex,
    /// In-memory sequence registry (nextval/currval/setval).
    pub sequence_registry: Arc<crate::control::sequence::SequenceRegistry>,
    /// Per-collection DML counter for auto-ANALYZE triggering.
    pub dml_counter:
        crate::control::server::shared::ddl::neutral::maintenance::auto_analyze::DmlCounter,
    /// Highest WAL LSN confirmed delivered to Data Plane for timeseries catch-up.
    pub wal_catchup_lsn: AtomicU64,
    /// Last globally-applied Calvin epoch, advanced by the per-vShard
    /// deterministic schedulers as they apply epochs. Read at `BEGIN` to anchor
    /// a session's cross-shard snapshot version (`tx_snapshot_epoch`). `Arc` so
    /// schedulers (holding `Arc<SharedState>`) advance the same counter the
    /// session reads. 0 in single-node / no-Calvin deployments.
    pub last_applied_calvin_epoch: Arc<AtomicU64>,
    /// Node-global Calvin observability counters (write versions recorded,
    /// read-set validation failures, commits flushed/dropped).
    pub calvin_counters: CalvinCounters,
    /// Local, in-process sidecar carrying the applied Data-Plane [`Response`]
    /// (affected-count and any RETURNING rows) of a completed Calvin
    /// transaction, keyed by its sequencer-assigned `TxnId`.
    ///
    /// RETURNING rows are a QUERY RESULT, not replicated state, so they MUST NOT
    /// ride the sequencer Raft log. The per-vShard scheduler deposits the applied
    /// `Response` here BEFORE proposing the replicated `CompletionAck`; the
    /// coordinator's completion path (static: `submit_and_await_calvin`;
    /// dependent: `dispatch_dependent_edge_recon`) drains it once completion
    /// fires. Every primary-write participant deposits (not RETURNING-only) — a
    /// multi-collection cross-shard COMMIT can have several plain-write
    /// participants, and those coalesce without conflict. Cross-node, the rows
    /// travel via the non-Raft routed-submit RPC response instead.
    ///
    /// Value is [`CalvinApplyResult`](super::CalvinApplyResult): `Single` for a
    /// deposited (possibly coalesced) participant, or `Conflict` only when two
    /// RETURNING-bearing participants deposit for the same `TxnId` — a
    /// cross-shard RETURNING union, drained as a loud error, never a silent
    /// partial. [`Response`](crate::bridge::envelope::Response) is Control-Plane
    /// `Send + Sync`; it never touches Raft.
    pub calvin_apply_results: Arc<
        Mutex<std::collections::HashMap<nodedb_cluster::calvin::TxnId, super::CalvinApplyResult>>,
    >,
    /// Per-vShard deterministic lock managers, lifted out of each Calvin
    /// `Scheduler` so the Control-Plane write-admission gate shares the SAME
    /// `Arc<Mutex<LockManager>>` the scheduler holds — a fast-path point write and
    /// a Calvin txn's lock validation contend on one OS mutex, no TOCTOU gap.
    /// Keyed by vShard id; empty in single-node / no-Calvin deployments.
    pub calvin_lock_managers: Arc<
        Mutex<
            std::collections::BTreeMap<
                u32,
                Arc<Mutex<crate::control::cluster::calvin::scheduler::lock_manager::LockManager>>,
            >,
        >,
    >,
    /// Global hot-key detector for Calvin read reservations (CP-local heuristic).
    pub hot_key_table: std::sync::Arc<
        std::sync::Mutex<crate::control::cluster::calvin::scheduler::lock::HotKeyTable>,
    >,
    /// Per-vShard promotion channels, parallel to `calvin_lock_managers`. When a
    /// fast-path write guard releases an uncontended key on drop, `LockManager`
    /// may promote a scheduler txn queued behind it; the guard (Control-Plane,
    /// not in the scheduler task) forwards the promoted `TxnId`s here for the
    /// scheduler to dispatch. Unbounded (low-volume, sent from a non-blocking
    /// `Drop`). Keyed by vShard id; empty in single-node / no-Calvin deployments.
    pub calvin_promotion_senders: Arc<
        Mutex<
            std::collections::BTreeMap<
                u32,
                tokio::sync::mpsc::UnboundedSender<
                    Vec<crate::control::cluster::calvin::scheduler::lock_manager::TxnId>,
                >,
            >,
        >,
    >,
    /// Monotonic `position` source for autocommit fast-path lock holders. Paired
    /// with [`TxnId::AUTOCOMMIT_EPOCH`] to mint holder identities that never
    /// collide with a real Calvin `(epoch, position)` schedule position.
    ///
    /// [`TxnId::AUTOCOMMIT_EPOCH`]: crate::control::cluster::calvin::scheduler::lock_manager::TxnId::AUTOCOMMIT_EPOCH
    pub autocommit_lock_seq: std::sync::atomic::AtomicU32,
    /// Single-node per-key write-ordering lock. When NO Calvin scheduler is
    /// registered for a write's vShard there is no lock table to fence against,
    /// yet concurrent same-key autocommit writes must still serialize so
    /// WAL-LSN order equals Data-Plane apply order per key. Hands out one
    /// FIFO-fair async mutex per lock key: same-key writers acquire in arrival
    /// order across [WAL append -> enqueue]; distinct keys never contend. Idle
    /// keys are reaped, so it never grows unbounded.
    pub write_order_locks:
        Arc<crate::control::server::shared::write_admission::KeyedWriteOrderLock>,
    /// Presence/Awareness manager: ephemeral user state broadcast channels.
    pub presence: Arc<tokio::sync::RwLock<crate::control::server::sync::presence::PresenceManager>>,
    /// Permission tree cache: in-memory resource hierarchy + permission grants.
    pub permission_cache:
        Arc<tokio::sync::RwLock<crate::control::security::permission_tree::PermissionCache>>,
    /// Gateway plan-cache invalidator; called after every DDL commit. None until `Gateway::new`.
    pub gateway_invalidator:
        std::sync::OnceLock<Arc<crate::control::gateway::PlanCacheInvalidator>>,
    /// The gateway: entry point for routing physical plans to the correct cluster node.
    pub gateway: std::sync::OnceLock<Arc<crate::control::gateway::Gateway>>,
    /// Per-backup KEK for wrapping DEKs. None = unencrypted backups.
    pub backup_kek: Option<Arc<[u8; 32]>>,
    /// In-process quarantine registry for corrupt segments.
    pub quarantine_registry: Arc<crate::storage::quarantine::QuarantineRegistry>,
    /// Per-database and per-tenant connection admission semaphores.
    pub admission_registry: Arc<crate::control::server::admission::AdmissionRegistry>,
    /// Per-database DML audit mode cache.
    /// Updated by `ALTER DATABASE SET AUDIT_DML`; consulted by the Event Plane consumer.
    pub audit_dml_cache: Arc<super::audit_dml_cache::AuditDmlCache>,
    /// Per-database idle session timeout cache.
    /// Updated by `ALTER DATABASE SET IDLE_TIMEOUT`; consulted by the idle-sweep loop.
    pub idle_timeout_cache: Arc<super::idle_timeout_cache::IdleTimeoutCache>,
    /// Collection-to-database reverse mapping for DML audit routing.
    /// Updated on `CREATE COLLECTION` / `DROP COLLECTION`.
    pub collection_to_database: Arc<super::collection_to_database::CollectionToDatabase>,
    /// LSN ↔ wall-clock millisecond interpolation map.
    ///
    /// Populated from WAL anchor records (`RecordType::LsnMsAnchor`) as they
    /// are replayed or emitted.  Used by the clone CoW resolver to convert
    /// `AS OF SYSTEM TIME <ms>` values to LSN for source-delegation clamping.
    /// Wrapped in `Mutex` because the Control Plane is `Send + Sync`.
    pub lsn_ms_map: Arc<Mutex<nodedb_types::temporal::LsnMsMap>>,
    /// Set of database ids temporarily frozen against new user writes because
    /// a clone materializer is reading from them as the source. Populated by
    /// `clone_materializer::walker` for one sweep over a dependent clone;
    /// concurrent materializers on different clones of the same source nest
    /// correctly via an internal reference count.
    pub materialize_freeze: Arc<crate::control::clone::MaterializeFreezeRegistry>,
    /// Cross-node streaming-shuffle receiver registry. Holds one bounded
    /// inbox per `(shuffle_id, part, side)` with a per-part build barrier. Fed
    /// by the cluster `ShufflePush` transport read-loop via
    /// `RegistryShuffleReceiver` and drained by the Data Plane. `Send + Sync`;
    /// the inbox uses std primitives only (no Tokio) so the `!Send` Data
    /// Plane can consume it.
    pub shuffle_registry: Arc<crate::control::server::shuffle::ShuffleReceiverRegistry>,
}
