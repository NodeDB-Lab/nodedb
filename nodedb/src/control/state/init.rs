// SPDX-License-Identifier: BUSL-1.1

//! The shared test constructor of `SharedState`. The public test
//! constructors built on it live in [`super::init_variants`].

use std::sync::{Arc, Mutex};

use nodedb_types::config::TuningConfig;

use crate::bridge::dispatch::Dispatcher;
use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::metering::config::MeteringConfig;
use crate::control::security::metering::quota::QuotaManager;
use crate::control::security::permission::PermissionStore;
use crate::control::security::ratelimit::config::RateLimitConfig;
use crate::control::security::ratelimit::limiter::RateLimiter;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::security::role::RoleStore;
use crate::wal::WalManager;

use super::SharedState;
use super::init_assemble_parts::{
    AssembleParts, AuthInputs, BusParts, CoreParts, RegistryParts, StoreParts,
};
use super::init_prod::stores::{SecurityStores, security_stores};

impl SharedState {
    /// Construct the test state every constructor in [`super::init_variants`]
    /// builds on.
    pub(super) fn new_inner(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
    ) -> crate::Result<Arc<Self>> {
        // One auto-cleaning temp directory roots the on-disk stores, the
        // shuffle staging directory, and the reported data directory. The
        // state owns it, so dropping the state removes everything under it.
        let test_state_dir = tempfile::tempdir().map_err(|e| crate::Error::Storage {
            engine: "test_state".into(),
            detail: format!("create test state temp directory: {e}"),
        })?;
        let credentials = Arc::new(CredentialStore::new()?);
        let registries = test_registries(&credentials);
        let (auth, buses) = test_auth_and_buses(&wal)?;
        let stores = test_stores(test_state_dir.path(), &auth.metering_config)?;
        let core = CoreParts {
            dispatcher,
            wal,
            quiesce: crate::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: crate::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: Arc::new(crate::control::metrics::SystemMetrics::new()),
            governor: crate::data::executor::core_loop::test_governor(),
            tuning: TuningConfig::default(),
            credentials,
            shutdown: Arc::new(crate::control::shutdown::ShutdownWatch::new()),
            loop_registry: Arc::new(crate::control::shutdown::LoopRegistry::new()),
            // Test helpers get a pre-fired gate so listeners start accepting
            // immediately. Production replaces it with a real sequencer.
            startup_gate: crate::control::startup::StartupGate::pre_fired(),
            data_dir: test_state_dir.path().to_path_buf(),
            shuffle_dir: test_state_dir.path().join("shuffle"),
            test_state_dir: Some(test_state_dir),
        };
        let state = Self::assemble(AssembleParts {
            core,
            registries,
            buses,
            auth,
            stores,
        })?;
        Self::wire_session_handle_audit(&state);
        Ok(state)
    }
}

/// In-memory registries, with a surrogate assigner on `credentials`.
fn test_registries(credentials: &Arc<CredentialStore>) -> RegistryParts {
    let surrogate_registry: crate::control::surrogate::SurrogateRegistryHandle = Arc::new(
        std::sync::RwLock::new(crate::control::surrogate::SurrogateRegistry::new()),
    );
    let surrogate_assigner = Arc::new(crate::control::surrogate::SurrogateAssigner::new(
        Arc::clone(&surrogate_registry),
        Arc::clone(credentials),
        Arc::new(crate::control::surrogate::NoopWalAppender),
    ));
    RegistryParts {
        api_keys: ApiKeyStore::new(),
        roles: RoleStore::new(),
        permissions: PermissionStore::new(),
        blacklist: crate::control::security::blacklist::store::BlacklistStore::new(),
        rls: RlsPolicyStore::new(),
        redaction: crate::control::security::redaction::RedactionStore::new(),
        trigger_registry: crate::control::trigger::TriggerRegistry::new(),
        stream_registry: Arc::new(crate::event::cdc::StreamRegistry::new()),
        group_registry: crate::event::cdc::GroupRegistry::new(),
        schedule_registry: Arc::new(crate::event::scheduler::ScheduleRegistry::new()),
        synonym_registry: Arc::new(crate::control::synonym::SynonymRegistry::new()),
        custom_type_registry: Arc::new(crate::control::custom_type::CustomTypeRegistry::new()),
        retention_policy_registry: Arc::new(
            crate::engine::timeseries::retention_policy::RetentionPolicyRegistry::new(),
        ),
        alert_registry: Arc::new(crate::event::alert::AlertRegistry::new()),
        alert_hysteresis: Arc::new(crate::event::alert::hysteresis::HysteresisManager::new()),
        ep_topic_registry: crate::event::topic::EpTopicRegistry::new(),
        mv_registry: Arc::new(crate::event::streaming_mv::MvRegistry::new()),
        sequence_registry: Arc::new(crate::control::sequence::SequenceRegistry::new()),
        database_registry: crate::control::database::DatabaseRegistry::new(),
        surrogate_registry,
        surrogate_assigner,
        producer_registry: None,
    }
}

/// The auth inputs and security buses of a state with no `AuthConfig`.
fn test_auth_and_buses(wal: &Arc<WalManager>) -> crate::Result<(AuthInputs, BusParts)> {
    let permission_cache = crate::control::security::permission_tree::PermissionCache::new();
    // The strict egress policy: a test state reaches no private address.
    let remote_sources = crate::control::security::egress::RemoteSources::new(Arc::new(
        crate::control::security::egress::EgressPolicy::strict(),
    ))
    .map_err(|e| crate::Error::Config {
        detail: format!("outbound HTTP client: {e}"),
    })?;
    let siem = Arc::new(crate::control::security::siem::SiemExporter::with_client(
        crate::control::security::siem::SiemConfig::default(),
        Arc::clone(remote_sources.http()),
        Arc::clone(remote_sources.egress()),
    ));
    let audit = {
        let mut log = AuditLog::new(crate::control::security::audit::DEFAULT_FLUSH_BOUND);
        log.set_wal_sink(Arc::clone(wal) as Arc<dyn crate::control::security::audit::AuditWalSink>);
        log.set_export_sink(
            Arc::clone(&siem) as Arc<dyn crate::control::security::audit::AuditExportSink>
        );
        Arc::new(Mutex::new(log))
    };
    let session_registry = Arc::new(crate::control::security::sessions::SessionRegistry::new());
    let (si_bus, uc_bus, bus_consumer_handle) =
        super::buses_init::init_security_buses(Arc::clone(&audit), Arc::clone(&session_registry));
    // The default configs pin the effective bounds to the config types the
    // production constructor reads.
    let metering_config = MeteringConfig::default();
    let rate_limit_config = RateLimitConfig::default();
    let quota_manager = QuotaManager::with_bounds(metering_config.max_tracked_quota_grantees);
    let auth = AuthInputs {
        metering_config,
        siem,
        risk_scorer: crate::control::security::risk::RiskScorer::default(),
        escalation: crate::control::security::escalation::EscalationEngine::default(),
        tls_policy: crate::control::security::tls_policy::TlsPolicy::default(),
        auth_users: crate::control::security::jit::auth_user::AuthUserStore::new(),
        scope_grants: crate::control::security::scope::grant::ScopeGrantStore::new(),
        quota_manager,
        rate_limiter: RateLimiter::new(rate_limit_config),
        session_handles: crate::control::security::session_handle::SessionHandleStore::default(),
        remote_sources,
        audit_retention_days: 0,
        audit_max_entries: 0,
        idle_timeout_secs: 0,
        session_absolute_timeout_secs: 0,
    };
    let buses = BusParts {
        audit,
        session_registry,
        si_bus,
        uc_bus,
        bus_consumer_handle,
        permission_cache,
    };
    Ok((auth, buses))
}

/// In-memory array stores and event stores rooted under `state_dir`.
fn test_stores(
    state_dir: &std::path::Path,
    metering_config: &MeteringConfig,
) -> crate::Result<StoreParts> {
    let schema_db = Arc::new(
        redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("create test schema_registry db: {e}"),
            })?,
    );
    let replica_id = nodedb_array::sync::ReplicaId::new(0);
    let hlc_gen = Arc::new(nodedb_array::sync::HlcGenerator::new(replica_id));
    let array_sync_schemas = Arc::new(crate::control::array_sync::OriginSchemaRegistry::open(
        schema_db, replica_id, hlc_gen,
    )?);
    let array_subscriber_cursors = Arc::new(crate::control::array_sync::SubscriberMap::new(
        crate::control::array_sync::SubscriberStore::in_memory()?,
    ));
    let SecurityStores {
        orgs,
        scope_defs,
        usage_counter,
        usage_store,
        auth_api_keys,
        impersonation,
        emergency,
        auth_metrics,
        ceilings,
    } = security_stores(metering_config);
    Ok(StoreParts {
        orgs,
        scope_defs,
        usage_counter,
        usage_store,
        auth_api_keys,
        impersonation,
        emergency,
        auth_metrics,
        ceilings,
        array_sync_op_log: Arc::new(crate::control::array_sync::OriginOpLog::open_in_memory()?),
        array_ack_registry: crate::control::array_sync::ArrayAckRegistry::open_in_memory()?,
        array_snapshot_store: crate::control::array_sync::OriginSnapshotStore::open_in_memory()?,
        array_sync_schemas,
        array_subscriber_cursors,
        offset_store: Arc::new(crate::event::cdc::OffsetStore::open(
            &state_dir.join("offsets"),
        )?),
        job_history: Arc::new(crate::event::scheduler::JobHistoryStore::open(
            &state_dir.join("history"),
        )?),
        mv_persistence: Arc::new(crate::event::streaming_mv::MvPersistence::open(
            &state_dir.join("mvstate"),
        )?),
    })
}
