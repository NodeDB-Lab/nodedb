// SPDX-License-Identifier: BUSL-1.1

//! The inputs of [`SharedState::assemble`](super::SharedState::assemble).
//!
//! Each constructor builds the values that differ between a test state and a
//! production state and hands them over in these structs. Everything built
//! the same way by both constructors lives in `assemble`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::metering::config::MeteringConfig;
use crate::control::security::metering::quota::QuotaManager;
use crate::control::security::ratelimit::limiter::RateLimiter;
use crate::control::security::session_handle::SessionHandleStore;
use crate::wal::WalManager;

/// Every input of `SharedState::assemble`.
pub(super) struct AssembleParts {
    pub(super) core: CoreParts,
    pub(super) registries: RegistryParts,
    pub(super) buses: BusParts,
    pub(super) auth: AuthInputs,
    pub(super) stores: StoreParts,
}

/// Handles, lifecycle gates, and directories.
pub(super) struct CoreParts {
    pub(super) dispatcher: Dispatcher,
    pub(super) wal: Arc<WalManager>,
    pub(super) quiesce: Arc<crate::bridge::quiesce::CollectionQuiesce>,
    pub(super) array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    pub(super) system_metrics: Arc<crate::control::metrics::SystemMetrics>,
    pub(super) governor: Arc<nodedb_mem::MemoryGovernor>,
    pub(super) tuning: TuningConfig,
    pub(super) credentials: Arc<CredentialStore>,
    pub(super) shutdown: Arc<crate::control::shutdown::ShutdownWatch>,
    pub(super) loop_registry: Arc<crate::control::shutdown::LoopRegistry>,
    pub(super) startup_gate: Arc<crate::control::startup::StartupGate>,
    /// The directory the state reports as its data directory.
    pub(super) data_dir: PathBuf,
    /// The directory shuffle inboxes stage under.
    pub(super) shuffle_dir: PathBuf,
    /// The auto-cleaning directory a test state roots its stores under.
    pub(super) test_state_dir: Option<tempfile::TempDir>,
}

/// Security, catalog, and event registries.
pub(super) struct RegistryParts {
    pub(super) api_keys: crate::control::security::apikey::ApiKeyStore,
    pub(super) roles: crate::control::security::role::RoleStore,
    pub(super) permissions: crate::control::security::permission::PermissionStore,
    pub(super) blacklist: crate::control::security::blacklist::store::BlacklistStore,
    pub(super) rls: crate::control::security::rls::RlsPolicyStore,
    pub(super) redaction: crate::control::security::redaction::RedactionStore,
    pub(super) trigger_registry: crate::control::trigger::TriggerRegistry,
    pub(super) stream_registry: Arc<crate::event::cdc::StreamRegistry>,
    pub(super) group_registry: crate::event::cdc::GroupRegistry,
    pub(super) schedule_registry: Arc<crate::event::scheduler::ScheduleRegistry>,
    pub(super) synonym_registry: Arc<crate::control::synonym::SynonymRegistry>,
    pub(super) custom_type_registry: Arc<crate::control::custom_type::CustomTypeRegistry>,
    pub(super) retention_policy_registry:
        Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
    pub(super) alert_registry: Arc<crate::event::alert::AlertRegistry>,
    pub(super) alert_hysteresis: Arc<crate::event::alert::hysteresis::HysteresisManager>,
    pub(super) ep_topic_registry: crate::event::topic::EpTopicRegistry,
    pub(super) mv_registry: Arc<crate::event::streaming_mv::MvRegistry>,
    pub(super) sequence_registry: Arc<crate::control::sequence::SequenceRegistry>,
    pub(super) database_registry: crate::control::database::DatabaseRegistry,
    pub(super) surrogate_registry: crate::control::surrogate::SurrogateRegistryHandle,
    pub(super) surrogate_assigner: Arc<crate::control::surrogate::SurrogateAssigner>,
    pub(super) producer_registry:
        Option<Arc<crate::control::sync_producer::registry::SyncProducerRegistry>>,
}

/// The audit log, session registry, security buses, and permission cache.
pub(super) struct BusParts {
    pub(super) audit: Arc<Mutex<AuditLog>>,
    pub(super) session_registry: Arc<crate::control::security::sessions::SessionRegistry>,
    pub(super) si_bus: crate::control::security::buses::SessionInvalidationBus,
    pub(super) uc_bus: crate::control::security::buses::UserChangeBus,
    pub(super) bus_consumer_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) permission_cache: crate::control::security::permission_tree::PermissionCache,
}

/// Values that come from the `[auth]` config, or from its defaults.
pub(super) struct AuthInputs {
    pub(super) metering_config: MeteringConfig,
    pub(super) siem: Arc<crate::control::security::siem::SiemExporter>,
    pub(super) risk_scorer: crate::control::security::risk::RiskScorer,
    pub(super) escalation: crate::control::security::escalation::EscalationEngine,
    pub(super) tls_policy: crate::control::security::tls_policy::TlsPolicy,
    pub(super) auth_users: crate::control::security::jit::auth_user::AuthUserStore,
    pub(super) scope_grants: crate::control::security::scope::grant::ScopeGrantStore,
    pub(super) quota_manager: QuotaManager,
    pub(super) rate_limiter: RateLimiter,
    pub(super) session_handles: SessionHandleStore,
    pub(super) remote_sources: crate::control::security::egress::RemoteSources,
    pub(super) audit_retention_days: u32,
    pub(super) audit_max_entries: u64,
    pub(super) idle_timeout_secs: u64,
    pub(super) session_absolute_timeout_secs: u64,
}

/// Security, metering, array-sync, and event stores.
pub(super) struct StoreParts {
    pub(super) orgs: crate::control::security::org::store::OrgStore,
    pub(super) scope_defs: crate::control::security::scope::store::ScopeStore,
    pub(super) usage_counter: Arc<crate::control::security::metering::counter::UsageCounter>,
    pub(super) usage_store: Arc<crate::control::security::metering::store::UsageStore>,
    pub(super) auth_api_keys: crate::control::security::auth_apikey::AuthApiKeyStore,
    pub(super) impersonation: crate::control::security::impersonation::ImpersonationStore,
    pub(super) emergency: crate::control::security::emergency::EmergencyState,
    pub(super) auth_metrics: crate::control::security::observability::AuthMetrics,
    pub(super) ceilings: crate::control::security::ceiling::CeilingStore,
    pub(super) array_sync_op_log: Arc<crate::control::array_sync::OriginOpLog>,
    pub(super) array_ack_registry: Arc<crate::control::array_sync::ArrayAckRegistry>,
    pub(super) array_snapshot_store: Arc<crate::control::array_sync::OriginSnapshotStore>,
    pub(super) array_sync_schemas: Arc<crate::control::array_sync::OriginSchemaRegistry>,
    pub(super) array_subscriber_cursors: Arc<crate::control::array_sync::SubscriberMap>,
    pub(super) offset_store: Arc<crate::event::cdc::OffsetStore>,
    pub(super) job_history: Arc<crate::event::scheduler::JobHistoryStore>,
    pub(super) mv_persistence: Arc<crate::event::streaming_mv::MvPersistence>,
}
