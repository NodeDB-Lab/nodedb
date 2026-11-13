// SPDX-License-Identifier: BUSL-1.1

//! Shared test utilities for Event Plane tests.

#![cfg(test)]

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::cdc::CdcRouter;
use crate::event::trigger::dlq::TriggerDlq;
use crate::event::watermark::WatermarkStore;
use crate::wal::WalManager;

/// All Event Plane test dependencies bundled together.
pub type EventTestDeps = (
    Arc<WalManager>,
    Arc<WatermarkStore>,
    Arc<SharedState>,
    Arc<std::sync::Mutex<TriggerDlq>>,
    Arc<CdcRouter>,
);

/// Create all Event Plane test dependencies from a temp directory.
pub fn event_test_deps(dir: &tempfile::TempDir) -> EventTestDeps {
    let watermark_store = Arc::new(WatermarkStore::open(dir.path()).unwrap());
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&wal_dir).unwrap());
    let trigger_dlq = Arc::new(std::sync::Mutex::new(TriggerDlq::open(dir.path()).unwrap()));
    let (dispatcher, _) = crate::bridge::dispatch::Dispatcher::new(1, 16);
    let catalog_path = dir.path().join("catalog.redb");
    let shared_state = SharedState::open(
        crate::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: crate::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: crate::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: Arc::new(crate::control::metrics::SystemMetrics::new()),
        },
        Arc::clone(&wal),
        &catalog_path,
        crate::control::state::OpenConfig::strict(&crate::config::auth::AuthConfig::default()),
        Default::default(),
        false,
        crate::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    // The gateway install production boot runs. This state runs no metadata
    // group, so a DDL refuses on it: a test that proposes boots
    // `control::cluster::test_one_node` instead.
    crate::bootstrap::state_wiring::install_gateway(&shared_state).unwrap();
    let cdc_router = Arc::clone(&shared_state.cdc_router);
    (wal, watermark_store, shared_state, trigger_dlq, cdc_router)
}

/// A commit HLC at the current wall time, in nanoseconds, for a test event
/// that must stay inside a stream's age retention.
pub fn test_commit_hlc() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
        })
}

/// A user Insert of row `row-1` into `collection` of `database_id`, named as
/// storage keys it: qualified outside the default database.
pub fn write_event_in(
    database_id: crate::types::DatabaseId,
    collection: &str,
) -> crate::event::types::WriteEvent {
    let row = nodedb_types::json_to_msgpack(&serde_json::json!({"id": "row-1", "status": "open"}))
        .unwrap_or_default();
    crate::event::types::WriteEvent {
        sequence: 1,
        collection: Arc::from(
            nodedb_types::QualifiedCollection::new(database_id, collection).as_str(),
        ),
        op: crate::event::types::WriteOp::Insert,
        row_id: crate::event::types::RowId::row(nodedb_types::RowIdentity::from_user_key("row-1")),
        lsn: crate::types::Lsn::new(10),
        record: None,
        database_id,
        tenant_id: crate::types::TenantId::new(1),
        vshard_id: crate::types::VShardId::new(0),
        source: crate::event::types::EventSource::User,
        new_value: Some(Arc::from(row.as_slice())),
        old_value: None,
        system_time_ms: None,
        valid_time_ms: None,
        user_id: None,
        statement_digest: None,
        commit_hlc: Some(test_commit_hlc()),
        image_fault: None,
    }
}
