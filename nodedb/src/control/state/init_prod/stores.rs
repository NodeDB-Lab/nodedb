// SPDX-License-Identifier: BUSL-1.1

//! Stages of `SharedState::open` that build groups of fields: the on-disk
//! stores, the in-memory security stores, and the session controls.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::control::array_sync::{
    ArrayAckRegistry, OriginOpLog, OriginSchemaRegistry, OriginSnapshotStore, SubscriberMap,
    SubscriberStore,
};
use crate::control::security::auth_apikey::AuthApiKeyStore;
use crate::control::security::ceiling::CeilingStore;
use crate::control::security::emergency::EmergencyState;
use crate::control::security::impersonation::ImpersonationStore;
use crate::control::security::metering::config::MeteringConfig;
use crate::control::security::metering::counter::UsageCounter;
use crate::control::security::metering::store::UsageStore;
use crate::control::security::observability::AuthMetrics;
use crate::control::security::org::store::OrgStore;
use crate::control::security::ratelimit::config::RateLimitConfig;
use crate::control::security::ratelimit::limiter::RateLimiter;
use crate::control::security::scope::store::ScopeStore;
use crate::control::security::session_handle::SessionHandleStore;

/// Stores that live in redb files next to the catalog.
pub(super) struct DiskStores {
    pub(super) data_dir: PathBuf,
    pub(super) array_sync_op_log: Arc<OriginOpLog>,
    pub(super) array_ack_registry: Arc<ArrayAckRegistry>,
    pub(super) array_snapshot_store: Arc<OriginSnapshotStore>,
    pub(super) array_sync_schemas: Arc<OriginSchemaRegistry>,
    pub(super) array_subscriber_cursors: Arc<SubscriberMap>,
    pub(super) offset_store: Arc<crate::event::cdc::OffsetStore>,
    pub(super) job_history: Arc<crate::event::scheduler::JobHistoryStore>,
    pub(super) mv_persistence: Arc<crate::event::streaming_mv::MvPersistence>,
}

/// Open every on-disk store under the catalog's directory.
pub(super) fn open_disk_stores(catalog_path: &Path) -> crate::Result<DiskStores> {
    let data_dir = catalog_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    open_stores_under(data_dir)
}

impl crate::control::state::SharedState {
    /// Root this state at `data_dir` and replace its node-level stores with
    /// the on-disk stores a production node opens there.
    ///
    /// A snapshot capture reads each node-level store from its file under
    /// `data_dir`, and a restore writes those files back there. A test state
    /// keeps these stores in a private temp directory or in memory, so it
    /// calls this before it takes or boots from a snapshot. Call it before
    /// any task holds a clone of a replaced store.
    pub fn open_disk_stores_at(&mut self, data_dir: &Path) -> crate::Result<()> {
        let DiskStores {
            data_dir,
            array_sync_op_log,
            array_ack_registry,
            array_snapshot_store,
            array_sync_schemas,
            array_subscriber_cursors,
            offset_store,
            job_history,
            mv_persistence,
        } = open_stores_under(data_dir.to_path_buf())?;
        self.data_dir = data_dir;
        self.array_sync_op_log = array_sync_op_log;
        self.array_ack_registry = array_ack_registry;
        self.array_snapshot_store = array_snapshot_store;
        self.array_sync_schemas = array_sync_schemas;
        self.array_subscriber_cursors = array_subscriber_cursors;
        self.offset_store = offset_store;
        self.job_history = job_history;
        self.mv_persistence = mv_persistence;
        Ok(())
    }
}

/// Open every on-disk store under `data_dir`.
fn open_stores_under(data_dir: PathBuf) -> crate::Result<DiskStores> {
    let array_sync_op_log = Arc::new(OriginOpLog::open(&data_dir)?);
    let array_ack_registry = ArrayAckRegistry::open(&data_dir)?;
    let array_snapshot_store = OriginSnapshotStore::open(&data_dir)?;

    let schema_db = open_array_sync_db(
        &data_dir,
        crate::storage::snapshot_node::ARRAY_SCHEMA_DOCS_FILE,
        "schema_registry",
    )?;
    let replica_id = nodedb_array::sync::ReplicaId::new(0);
    let hlc_gen = Arc::new(nodedb_array::sync::HlcGenerator::new(replica_id));
    let array_sync_schemas = Arc::new(OriginSchemaRegistry::open(schema_db, replica_id, hlc_gen)?);

    let cursor_db = open_array_sync_db(
        &data_dir,
        crate::storage::snapshot_node::ARRAY_SUBSCRIBER_CURSORS_FILE,
        "subscriber_cursor",
    )?;
    let array_subscriber_cursors = Arc::new(SubscriberMap::new(SubscriberStore::open(cursor_db)?));

    Ok(DiskStores {
        array_sync_op_log,
        array_ack_registry,
        array_snapshot_store,
        array_sync_schemas,
        array_subscriber_cursors,
        offset_store: Arc::new(crate::event::cdc::OffsetStore::open(&data_dir)?),
        job_history: Arc::new(crate::event::scheduler::JobHistoryStore::open(&data_dir)?),
        mv_persistence: Arc::new(crate::event::streaming_mv::MvPersistence::open(&data_dir)?),
        data_dir,
    })
}

/// Create `{data_dir}/array_sync/` and open `file` in it as a redb database.
fn open_array_sync_db(
    data_dir: &Path,
    file: &str,
    what: &str,
) -> crate::Result<Arc<redb::Database>> {
    let dir = data_dir.join("array_sync");
    std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
        engine: "array_sync".into(),
        detail: format!("create array_sync dir for {what}: {e}"),
    })?;
    let db = redb::Database::create(dir.join(file)).map_err(|e| crate::Error::Storage {
        engine: "array_sync".into(),
        detail: format!("{what} db open: {e}"),
    })?;
    Ok(Arc::new(db))
}

/// In-memory security and metering stores.
pub(in crate::control::state) struct SecurityStores {
    pub(in crate::control::state) orgs: OrgStore,
    pub(in crate::control::state) scope_defs: ScopeStore,
    pub(in crate::control::state) usage_counter: Arc<UsageCounter>,
    pub(in crate::control::state) usage_store: Arc<UsageStore>,
    pub(in crate::control::state) auth_api_keys: AuthApiKeyStore,
    pub(in crate::control::state) impersonation: ImpersonationStore,
    pub(in crate::control::state) emergency: EmergencyState,
    pub(in crate::control::state) auth_metrics: AuthMetrics,
    pub(in crate::control::state) ceilings: CeilingStore,
}

/// Build the security stores. The usage store takes its bounds from the
/// metering config.
pub(in crate::control::state) fn security_stores(
    metering_config: &MeteringConfig,
) -> SecurityStores {
    SecurityStores {
        orgs: OrgStore::new(),
        scope_defs: ScopeStore::new(),
        usage_counter: Arc::new(UsageCounter::new()),
        usage_store: Arc::new(UsageStore::with_bounds(
            metering_config.max_usage_events,
            metering_config.max_tracked_scopes,
        )),
        auth_api_keys: AuthApiKeyStore::new(),
        impersonation: ImpersonationStore::default(),
        emergency: EmergencyState::default(),
        auth_metrics: AuthMetrics::new(),
        ceilings: CeilingStore::new(),
    }
}

/// Rate limits and session handles, both taken from the operator's auth
/// config.
pub(super) struct SessionControls {
    pub(super) rate_limiter: RateLimiter,
    pub(super) session_handles: SessionHandleStore,
}

/// Build the rate limiter from `rate_limit_config` and the session-handle
/// store from `[auth.session]`.
pub(super) fn session_controls(
    auth_config: &crate::config::auth::AuthConfig,
    rate_limit_config: &RateLimitConfig,
) -> SessionControls {
    SessionControls {
        rate_limiter: RateLimiter::new(rate_limit_config.clone()),
        session_handles: SessionHandleStore::from_config(&auth_config.session),
    }
}
