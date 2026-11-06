// SPDX-License-Identifier: BUSL-1.1

//! The one `SharedState` literal. Both the test and the production
//! constructor build their differing inputs and call [`SharedState::assemble`].

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use crate::control::security::tenant::{TenantIsolation, TenantQuota};
use crate::control::server::sync::dlq::{DlqConfig, SyncDlq};

use super::SharedState;
use super::init_assemble_parts::{
    AssembleParts, AuthInputs, BusParts, CoreParts, RegistryParts, StoreParts,
};

impl SharedState {
    /// Build a `SharedState` from `parts`.
    ///
    /// The returned `Arc` is uniquely owned: no clone is held anywhere, so a
    /// test constructor can call `Arc::get_mut` on it.
    pub(in crate::control::state) fn assemble(parts: AssembleParts) -> crate::Result<Arc<Self>> {
        let AssembleParts {
            core,
            registries,
            buses,
            auth,
            stores,
        } = parts;
        let CoreParts {
            mut dispatcher,
            wal,
            quiesce,
            array_catalog,
            system_metrics,
            governor,
            tuning,
            credentials,
            shutdown,
            loop_registry,
            startup_gate,
            data_dir,
            shuffle_dir,
            test_state_dir,
        } = core;
        let RegistryParts {
            api_keys,
            roles,
            permissions,
            blacklist,
            rls,
            redaction,
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
            database_registry,
            surrogate_registry,
            surrogate_assigner,
            producer_registry,
        } = registries;
        let BusParts {
            audit,
            session_registry,
            si_bus,
            uc_bus,
            bus_consumer_handle,
            permission_cache,
        } = buses;
        super::buses_init::connect_credential_store(
            &credentials,
            &si_bus,
            &uc_bus,
            &session_registry,
        );
        let AuthInputs {
            metering_config,
            siem,
            risk_scorer,
            escalation,
            tls_policy,
            auth_users,
            scope_grants,
            quota_manager,
            rate_limiter,
            session_handles,
            remote_sources,
            audit_retention_days,
            audit_max_entries,
            idle_timeout_secs,
            session_absolute_timeout_secs,
        } = auth;
        let StoreParts {
            orgs,
            scope_defs,
            usage_counter,
            usage_store,
            auth_api_keys,
            impersonation,
            emergency,
            auth_metrics,
            ceilings,
            array_sync_op_log,
            array_ack_registry,
            array_snapshot_store,
            array_sync_schemas,
            array_subscriber_cursors,
            offset_store,
            job_history,
            mv_persistence,
        } = stores;

        // The dispatcher is locked away below, so it adopts the metrics and
        // yields its outcome floor first.
        dispatcher.adopt_capacity_metrics(Arc::clone(&system_metrics.dispatch_capacity));
        let outcome_floor = dispatcher.outcome_floor();
        let redo_chunks = Arc::new(
            crate::control::wal_replication::transaction_redo::RedoChunkStore::new(
                Arc::clone(&wal),
                crate::control::wal_replication::transaction_redo::chunks::RedoChunkLimits::from_tuning(
                    &tuning.calvin,
                ),
            ),
        );
        wal.set_fsync_metrics(Arc::clone(&system_metrics));
        let hlc_clock = wal.hlc_clock();
        let tenant_marks = super::tenant_marks::TenantMarks::load(credentials.catalog())?;
        // The fence reads the cache's sources before the cache is wrapped.
        let authorization_fence = Arc::new(
            crate::control::security::auth_fence::AuthorizationFence::new(
                permission_cache.sources(),
            ),
        );
        // The router shares the registry DDL writes to and the metrics the
        // `/metrics` route renders.
        let cdc_router = Arc::new(
            crate::event::cdc::CdcRouter::new(Arc::clone(&stream_registry))
                .with_metrics(Arc::clone(&system_metrics)),
        );
        let webhook_manager = crate::event::webhook::WebhookManager::new(shutdown.raw_receiver());
        let kafka_manager = crate::event::kafka::KafkaManager::new(shutdown.raw_receiver());

        Ok(Arc::new(Self {
            outcome_floor,
            dispatcher: Mutex::new(dispatcher),
            tracker: crate::control::request_tracker::RequestTracker::new(),
            wal,
            quiesce,
            credentials,
            audit,
            audit_recorder: std::sync::OnceLock::new(),
            bearer_refusal_throttle: Default::default(),
            api_keys,
            roles,
            permissions,
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
            metadata_ddl: super::MetadataDdlState::new(),
            pending_ddl: crate::control::pending_ddl::PendingDdlTable::new(),
            metadata_apply_wedge: std::sync::Arc::default(),
            sequencer_halt: std::sync::Arc::default(),
            core_stall: std::sync::Arc::default(),
            metadata_raft: std::sync::OnceLock::new(),
            database_id_space: std::sync::OnceLock::new(),
            propose_tracker: std::sync::OnceLock::new(),
            raft_proposer: std::sync::OnceLock::new(),
            async_raft_proposer_pair: std::sync::OnceLock::new(),
            vshard_admission_sequencer: Arc::new(
                crate::control::vshard_admission::VShardAdmissionSequencer::new(),
            ),
            raft_compactor: std::sync::OnceLock::new(),
            raft_applied_index_sink: std::sync::OnceLock::new(),
            raft_apply_gates: std::sync::OnceLock::new(),
            raft_disk_halts: std::sync::OnceLock::new(),
            raft_read_gate: std::sync::OnceLock::new(),
            multi_raft: std::sync::OnceLock::new(),
            cluster_epoch: std::sync::OnceLock::new(),
            raft_status_fn: std::sync::OnceLock::new(),
            data_groups_replayed: std::sync::atomic::AtomicBool::new(false),
            cluster_observer: std::sync::OnceLock::new(),
            loop_metrics_registry: nodedb_cluster::LoopMetricsRegistry::new(),
            per_vshard_metrics: crate::control::metrics::PerVShardMetricsRegistry::new(),
            health_monitor: std::sync::OnceLock::new(),
            trace_exporter: crate::control::trace_export::TraceExporter::disabled(),
            debug_endpoints_enabled: false,
            migration_tracker: None,
            rls,
            blacklist,
            auth_users,
            orgs,
            scope_defs,
            scope_grants,
            rate_limiter,
            session_handles,
            session_registry,
            escalation,
            usage_counter,
            usage_store,
            quota_manager,
            metering_config,
            auth_api_keys,
            impersonation,
            emergency,
            auth_metrics,
            ceilings,
            redaction,
            risk_scorer,
            tls_policy,
            siem,
            jwks_registry: None,
            sync_dlq: Mutex::new(SyncDlq::new(DlqConfig::default())),
            audit_retention_days,
            audit_max_entries,
            idle_timeout_secs,
            session_absolute_timeout_secs,
            shape_registry: Arc::new(crate::control::server::sync::shape::ShapeRegistry::new()),
            change_stream: crate::control::change_stream::ChangeStream::new(4096),
            notify_bus: crate::control::notify_bus::NotifyBus::default(),
            trigger_registry,
            array_catalog,
            array_sync_op_log,
            array_ack_registry,
            array_snapshot_store,
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
            array_sync_schemas,
            array_delivery: std::sync::Arc::new(
                crate::control::array_sync::ArrayDeliveryRegistry::new(),
            ),
            array_subscriber_cursors,
            array_merger_registry: std::sync::Arc::new(
                crate::control::array_sync::MergerRegistry::new(),
            ),
            mirror: {
                let mirror = crate::control::mirror::MirrorState::default();
                surrogate_assigner.install_mirror_fence(Arc::clone(&mirror.fence));
                mirror
            },
            database_registry,
            surrogate_registry,
            surrogate_assigner,
            block_cache: crate::control::planner::procedural::executor::ProcedureBlockCache::new(
                4096,
            ),
            stream_registry,
            cdc_router,
            group_registry,
            offset_store,
            retention_policy_registry,
            bitemporal_retention_registry: Arc::new(
                crate::engine::bitemporal::BitemporalRetentionRegistry::new(),
            ),
            alert_registry,
            alert_hysteresis,
            schedule_registry,
            synonym_registry,
            custom_type_registry,
            job_history,
            ep_topic_registry,
            webhook_manager,
            mv_registry,
            consumer_assignments: crate::event::cdc::consumer_group::ConsumerAssignments::new(),
            watermark_tracker: Arc::new(crate::event::watermark_tracker::WatermarkTracker::new()),
            event_plane_budget: Arc::new(crate::event::budget::EventPlaneBudget::new()),
            cross_shard_dispatcher: None,
            cross_shard_dlq: None,
            cross_shard_metrics: None,
            cross_shard_dedup: std::sync::OnceLock::new(),
            redo_chunks,
            departure_fence: Default::default(),
            kafka_manager,
            definition_sync_fanout: std::sync::Arc::new(
                crate::control::server::sync::definition_fanout::DefinitionSyncFanout::new(),
            ),
            crdt_sync_delivery: Arc::new(crate::event::crdt_sync::CrdtSyncDelivery::new()),
            delta_packager: Arc::new(crate::event::crdt_sync::DeltaPackager::new()),
            mv_persistence,
            // Owns the temp dir a test state's stores live under. Production
            // stores live under real on-disk paths, so it holds `None`.
            _test_state_dir: test_state_dir,
            connections_rejected: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            raft_propose_leader_change_retries: AtomicU64::new(0),
            request_id_counter: AtomicU64::new(1),
            shuffle_id_counter: AtomicU64::new(1),
            system_metrics: Some(system_metrics),
            database_metrics: Arc::new(crate::control::metrics::DatabaseMetricsRegistry::new()),
            connection_tracker: Arc::new(crate::control::metrics::ConnectionTracker::new()),
            storage_shares: Arc::new(crate::control::metrics::StorageShares::default()),
            cluster_storage_totals: Arc::new(
                crate::control::metrics::ClusterStorageTotals::default(),
            ),
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
            cold_storage: None,
            snapshot_storage: Arc::new(object_store::memory::InMemory::new()),
            pitr: Default::default(),
            hlc_clock,
            tenant_write_hlc: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            tenant_marks,
            lease_admission_gate: Mutex::new(()),
            lease_grant_gate: Arc::new(tokio::sync::Mutex::new(())),
            lease_drain: Arc::new(crate::control::lease::DescriptorDrainTracker::new()),
            lease_refcount: Arc::new(crate::control::lease::LeaseRefCount::new()),
            lease_runtime: crate::control::lease::LeaseRuntime::new(),
            sequencer_inbox: std::sync::OnceLock::new(),
            reservation_inbox: std::sync::OnceLock::new(),
            sequencer_metrics: std::sync::OnceLock::new(),
            calvin_completion_registry: std::sync::OnceLock::new(),
            ollp_orchestrator: std::sync::OnceLock::new(),
            limits: nodedb_types::protocol::Limits::default(),
            tuning,
            scheduler_config: crate::config::server::SchedulerConfig::default(),
            data_dir,
            trigger_dlq: std::sync::OnceLock::new(),
            action_requeue: std::sync::OnceLock::new(),
            sink_ledgers: std::sync::OnceLock::new(),
            schema_version: crate::control::server::shared::session::plan_cache::SchemaVersion::new(
            ),
            materialized_sum_index:
                crate::control::planner::materialized_sum::MaterializedSumIndex::default(),
            sequence_registry,
            dml_counter:
                crate::control::server::shared::ddl::neutral::maintenance::auto_analyze::DmlCounter::new(),
            wal_catchup_lsn: AtomicU64::new(0),
            calvin: super::calvin_local::CalvinLocalState::new(),
            read_cut: crate::control::read_cut::ReadCutState::new(),
            write_order_locks: Arc::new(
                crate::control::server::shared::write_admission::KeyedWriteOrderLock::new(),
            ),
            write_order_fence:
                crate::control::server::shared::write_admission::WriteOrderFence::new(),
            presence: Arc::new(tokio::sync::RwLock::new(
                crate::control::server::sync::presence::PresenceManager::new(
                    crate::control::server::sync::presence::PresenceConfig::default(),
                ),
            )),
            permission_cache: Arc::new(tokio::sync::RwLock::new(permission_cache)),
            authorization_fence,
            gateway_invalidator: std::sync::OnceLock::new(),
            gateway: std::sync::OnceLock::new(),
            backup_kek: None,
            backup_storage: None,
            remote_sources,
            cut_captures: Arc::new(crate::control::backup::cut_capture::CutCaptures::new()),
            backup_schedules: Vec::new(),
            quarantine_registry: Arc::new(crate::storage::quarantine::QuarantineRegistry::new()),
            admission_registry: Arc::new(
                crate::control::server::admission::AdmissionRegistry::new(),
            ),
            audit_dml_cache: Arc::new(super::audit_dml_cache::AuditDmlCache::new()),
            idle_timeout_cache: Arc::new(super::idle_timeout_cache::IdleTimeoutCache::new()),
            collection_to_database: Arc::new(
                super::collection_to_database::CollectionToDatabase::new(),
            ),
            shuffle_registry: Arc::new(
                crate::control::server::shuffle::ShuffleReceiverRegistry::new(shuffle_dir),
            ),
            shutdown,
            loop_registry,
            data_plane_drain: crate::control::shutdown::DataPlaneDrain::new(),
            startup: startup_gate,
        }))
    }
}
