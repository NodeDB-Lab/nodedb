// SPDX-License-Identifier: BUSL-1.1

//! Catalog + registry bootstrap for [`super::SharedState::open`].
//!
//! Migrates the credential store, replays every persisted registry from the
//! system catalog, and creates the security event buses. Extracted out of the
//! top of `open()` so the returned [`ProdBootstrap`] bundles every value the
//! constructor's `Self { .. }` literal needs; `open()` destructures one value
//! instead of holding ~30 separate locals.

use std::sync::{Arc, Mutex};

use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::blacklist::store::BlacklistStore;
use crate::control::security::buses::{SessionInvalidationBus, UserChangeBus};
use crate::control::security::credential::CredentialStore;
use crate::control::security::permission::PermissionStore;
use crate::control::security::permission_tree::PermissionCache;
use crate::control::security::redaction::RedactionStore;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::security::role::RoleStore;
use crate::control::security::sessions::SessionRegistry;
use crate::control::shutdown::{LoopRegistry, ShutdownWatch};
use crate::control::startup::StartupGate;
use crate::control::surrogate::{SurrogateAssigner, SurrogateRegistry, SurrogateRegistryHandle};
use crate::control::sync_producer::registry::SyncProducerRegistry;
use crate::control::trigger::TriggerRegistry;

/// Every value computed before `SharedState`'s `Self { .. }` literal in
/// `SharedState::open`, bundled so the constructor can destructure one
/// return value instead of ~30 separate `let`s. Field-for-field, this is
/// the same set of locals `open()` otherwise builds directly.
pub(super) struct ProdBootstrap {
    pub(super) credentials: Arc<CredentialStore>,
    pub(super) producer_registry: Option<Arc<SyncProducerRegistry>>,
    pub(super) api_keys: ApiKeyStore,
    pub(super) roles: RoleStore,
    pub(super) permissions: PermissionStore,
    pub(super) blacklist: BlacklistStore,
    pub(super) trigger_registry: TriggerRegistry,
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
    pub(super) rls_store: RlsPolicyStore,
    pub(super) redaction_store: RedactionStore,
    pub(super) shared_audit: Arc<Mutex<AuditLog>>,
    pub(super) database_registry: crate::control::database::DatabaseRegistry,
    pub(super) surrogate_registry_handle: SurrogateRegistryHandle,
    pub(super) surrogate_assigner: Arc<SurrogateAssigner>,
    pub(super) permission_cache: PermissionCache,
    pub(super) shutdown: Arc<ShutdownWatch>,
    pub(super) loop_registry: Arc<LoopRegistry>,
    pub(super) startup_gate: Arc<StartupGate>,
    pub(super) prod_session_registry: Arc<SessionRegistry>,
    pub(super) si_bus: SessionInvalidationBus,
    pub(super) uc_bus: UserChangeBus,
    pub(super) bus_consumer_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Run the full catalog + registry bootstrap for a production
/// [`super::SharedState`]. Extracted from the first ~200 lines of
/// `SharedState::open`, with no behavior change.
pub(super) fn run(
    wal: &Arc<crate::wal::WalManager>,
    catalog_path: &std::path::Path,
    auth_config: &crate::config::auth::AuthConfig,
    is_cluster: bool,
) -> crate::Result<ProdBootstrap> {
    let mut credentials = CredentialStore::open(catalog_path)?;
    credentials
        .catalog()
        .configure_crdt_signing_root(wal.crdt_signing_root()?)?;

    // Share the credential store's already-open catalog (one redb file
    // handle). Opening a second `SystemCatalog` on the same path is rejected
    // by redb. The registry holds replicated state the metadata applier
    // writes, so an unreadable registry fails boot.
    let producer_registry = Some(Arc::new(SyncProducerRegistry::open(Arc::new(
        credentials.catalog().clone(),
    ))?));

    credentials.set_lockout_policy_with_grace(
        auth_config.max_failed_logins,
        auth_config.lockout_duration_secs,
        auth_config.password_expiry_days,
        auth_config.password_expiry_grace_days,
    );
    credentials.set_argon2_config(auth_config.argon2.clone());

    let api_keys = ApiKeyStore::new();
    let roles = RoleStore::new();
    let permissions = PermissionStore::new();
    let blacklist = BlacklistStore::new();
    let trigger_registry = TriggerRegistry::new();
    let stream_registry = Arc::new(crate::event::cdc::StreamRegistry::new());
    let group_registry = crate::event::cdc::GroupRegistry::new();
    let schedule_registry = Arc::new(crate::event::scheduler::ScheduleRegistry::new());
    let synonym_registry = Arc::new(crate::control::synonym::SynonymRegistry::new());
    let custom_type_registry = Arc::new(crate::control::custom_type::CustomTypeRegistry::new());
    let retention_policy_registry =
        Arc::new(crate::engine::timeseries::retention_policy::RetentionPolicyRegistry::new());
    let alert_registry = Arc::new(crate::event::alert::AlertRegistry::new());
    let alert_hysteresis = Arc::new(crate::event::alert::hysteresis::HysteresisManager::new());
    let ep_topic_registry = crate::event::topic::EpTopicRegistry::new();
    let mv_registry = Arc::new(crate::event::streaming_mv::MvRegistry::new());
    let sequence_registry = Arc::new(crate::control::sequence::SequenceRegistry::new());
    let rls_store = RlsPolicyStore::new();
    let redaction_store = RedactionStore::new();
    let audit_log = {
        let catalog = credentials.catalog();
        super::catalog_registries::CatalogRegistries {
            api_keys: &api_keys,
            roles: &roles,
            permissions: &permissions,
            blacklist: &blacklist,
            trigger_registry: &trigger_registry,
            stream_registry: &stream_registry,
            group_registry: &group_registry,
            schedule_registry: &schedule_registry,
            synonym_registry: &synonym_registry,
            custom_type_registry: &custom_type_registry,
            retention_policy_registry: &retention_policy_registry,
            alert_registry: &alert_registry,
            ep_topic_registry: &ep_topic_registry,
            mv_registry: &mv_registry,
            sequence_registry: &sequence_registry,
            rls_store: &rls_store,
            redaction_store: &redaction_store,
        }
        .load(catalog)?;
        crate::control::security::audit::restore_audit_log(catalog, wal)?
    };

    // Seed the database-id registry from the persisted hwm and the
    // applied-reservation cursor. Every issued id was persisted before it
    // left the registry, and metadata-log replay skips each reservation at
    // or below the cursor.
    let database_registry = {
        let catalog = credentials.catalog();
        crate::control::database::DatabaseRegistry::from_persisted(
            catalog.get_database_hwm()?,
            catalog.get_database_reserve_index()?,
        )
    };

    // Bootstrap the global surrogate registry from the persisted hwm. On
    // a fresh database this seeds `next = 1`; on restart it seeds `next
    // = persisted_hwm + 1` so post-restart allocations cannot collide
    // with pre-restart ones. `is_cluster` is the static, deployment-time
    // choice (from `[cluster]` section presence, not seed-list length)
    // between `Local` and `Cluster` mode — see `SurrogateRegistryMode`.
    let surrogate_registry_handle: SurrogateRegistryHandle = {
        let initial = {
            let catalog = credentials.catalog();
            let hwm = catalog.get_surrogate_hwm()?;
            // The singleton is flushed lazily and no engine contributes a
            // "surrogate durable through" floor to WAL truncation, so a
            // checkpoint can truncate the `SurrogateAlloc` / `SurrogateBindBatch`
            // records that cover a stale singleton. Take the
            // highest surrogate any live binding refers to as a floor the
            // allocator can never start below — re-issuing one already bound to
            // a live row corrupts cross-engine identity.
            let bound_floor = catalog.max_bound_surrogate()?.as_u32();
            let floor = hwm.max(bound_floor);
            if is_cluster {
                // Seed BOTH the global watermark `G` and the applied-reserve
                // cursor so metadata-log replay skips every `SurrogateReserve`
                // already folded into `G` (no restart double-count).
                let reserve_index = catalog.get_surrogate_reserve_index()?;
                SurrogateRegistry::from_persisted_cluster(floor, reserve_index)
            } else {
                SurrogateRegistry::from_persisted_hwm(floor)
            }
        };
        Arc::new(std::sync::RwLock::new(initial))
    };

    // Wrap the credential store in an Arc up front so the surrogate
    // assigner (and the SharedState field) can share the same handle.
    let credentials = Arc::new(credentials);
    let surrogate_wal_appender: Arc<dyn crate::control::surrogate::SurrogateWalAppender> = Arc::new(
        crate::control::surrogate::WalSurrogateAppender::new(Arc::clone(wal)),
    );
    let surrogate_assigner = Arc::new(SurrogateAssigner::new(
        Arc::clone(&surrogate_registry_handle),
        Arc::clone(&credentials),
        surrogate_wal_appender,
    ));

    // Pre-load permission tree definitions before wrapping in RwLock
    // (avoids blocking_write() which panics inside async runtimes).
    let permission_cache =
        crate::control::security::auth_fence::cache_from_catalog(credentials.catalog())?;

    let shutdown = Arc::new(ShutdownWatch::new());
    let loop_registry = Arc::new(LoopRegistry::new());
    // A pre-fired placeholder gate is installed here. `main.rs` replaces
    // it after `open()` returns by swapping via `Arc::get_mut`, installing
    // the real gate from the `StartupSequencer` it constructs.
    let startup_gate = StartupGate::pre_fired();
    let shared_audit = Arc::new(Mutex::new(audit_log));
    let prod_session_registry = Arc::new(SessionRegistry::new());
    let (si_bus, uc_bus, bus_consumer_task) =
        super::super::buses_init::init_security_buses(Arc::clone(&shared_audit));
    let bus_consumer_handle = bus_consumer_task;

    Ok(ProdBootstrap {
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
    })
}
