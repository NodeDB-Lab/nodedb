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
        dispatcher,
        Arc::clone(&wal),
        &catalog_path,
        &crate::config::auth::AuthConfig::default(),
        Default::default(),
    )
    .unwrap();
    let cdc_router = Arc::clone(&shared_state.cdc_router);
    (wal, watermark_store, shared_state, trigger_dlq, cdc_router)
}
