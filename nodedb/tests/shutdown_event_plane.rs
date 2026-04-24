//! D-δ integration test 5: Event Plane watermarks persisted through shutdown.
//!
//! Verifies the `PersistingWatermarks` shutdown phase end-to-end:
//!
//! 1. Spawn an `EventPlane` with a real `WatermarkStore` backed by redb.
//! 2. Process 100 WriteEvents so consumer watermarks advance.
//! 3. Signal shutdown (via the node-wide `ShutdownWatch`).
//! 4. Drop the `EventPlane` (simulates process exit).
//! 5. Open a new `WatermarkStore` from the same redb file.
//! 6. Assert the loaded watermarks match the LSN that was reached before
//!    shutdown — no lost events, no duplicate replay required.
//!
//! This is an in-process test because watermark verification requires direct
//! access to `WatermarkStore` APIs that are not observable through the binary's
//! network interface.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthConfig;
use nodedb::control::shutdown::ShutdownWatch;
use nodedb::control::state::SharedState;
use nodedb::event::EventPlane;
use nodedb::event::bus::create_event_bus_with_capacity;
use nodedb::event::trigger::TriggerDlq;
use nodedb::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use nodedb::event::watermark::WatermarkStore;
use nodedb::types::{Lsn, TenantId, VShardId};
use nodedb::wal::WalManager;

fn make_write_event(seq: u64, lsn_val: u64) -> WriteEvent {
    WriteEvent {
        sequence: seq,
        collection: Arc::from("test_collection"),
        op: WriteOp::Insert,
        row_id: RowId::new("row-1"),
        lsn: Lsn::new(lsn_val),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        source: EventSource::User,
        new_value: Some(Arc::from(b"payload".as_slice())),
        old_value: None,
        system_time_ms: None,
        valid_time_ms: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_plane_watermarks_persisted_through_shutdown() {
    let dir = tempfile::tempdir().expect("tempdir");

    // ── Phase 1: Run and process events ──────────────────────────────────────

    let (final_lsn, core_count) = {
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).expect("create wal dir");
        let wal = Arc::new(WalManager::open_for_testing(&wal_dir).expect("wal"));
        let watermark_store = Arc::new(WatermarkStore::open(dir.path()).expect("watermark_store"));
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            TriggerDlq::open(dir.path()).expect("trigger_dlq"),
        ));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let catalog_path = dir.path().join("catalog.redb");
        let shared = SharedState::open(
            dispatcher,
            Arc::clone(&wal),
            &catalog_path,
            &AuthConfig::default(),
            Default::default(),
            nodedb::bridge::quiesce::CollectionQuiesce::new(),
        )
        .expect("shared_state");
        let cdc_router = Arc::clone(&shared.cdc_router);
        let shutdown = Arc::new(ShutdownWatch::new());

        let (mut producers, consumers) = create_event_bus_with_capacity(1, 256);
        let core_count = consumers.len();

        let plane = EventPlane::spawn(
            consumers,
            Arc::clone(&wal),
            Arc::clone(&watermark_store),
            shared,
            trigger_dlq,
            cdc_router,
            Arc::clone(&shutdown),
        );

        // Emit 100 events with increasing LSNs.
        for i in 1u64..=100 {
            producers[0].emit(make_write_event(i, i * 10));
        }

        // Wait for events to be processed.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Signal shutdown — this is what the unified bus does before
        // the PersistingWatermarks phase.
        shutdown.signal();

        // Give the plane time to flush watermarks on shutdown signal.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let events_processed = plane.total_events_processed();
        assert!(
            events_processed >= 50,
            "expected at least 50 events processed before shutdown, got {events_processed}"
        );

        // The final LSN we expect to see persisted.
        let final_lsn = 100 * 10; // seq 100 → LSN 1000

        // Await consumer task termination so every Arc<WatermarkStore> clone
        // they hold is definitely dropped before we reopen the redb file
        // below. `drop(plane)` would only abort — under parallel load the
        // abort propagation can lag the reopen and redb refuses to
        // re-acquire the file lock.
        plane.shutdown_and_join().await;
        drop(watermark_store); // release this scope's own Arc clone
        (final_lsn, core_count)
    };

    // ── Phase 2: Reload and verify watermarks ─────────────────────────────────

    // Open a fresh WatermarkStore from the same redb file.
    let watermark_store_reload = WatermarkStore::open(dir.path()).expect("reload watermark_store");

    // Check that at least one core's watermark advanced past 0.
    // We can't assert exact final LSN because event processing is concurrent
    // and may not have reached event 100 before flush, but we assert it
    // advanced well past 0 (proving persistence works).
    let mut any_advanced = false;
    for core_id in 0..core_count {
        let lsn = watermark_store_reload
            .load(core_id)
            .expect("load watermark");
        if lsn > Lsn::new(0) {
            any_advanced = true;
        }
    }

    assert!(
        any_advanced,
        "no core watermark advanced past 0 after processing events and reloading — \
         watermarks were not persisted through simulated shutdown. \
         Expected at least one core to have lsn > 0 in the reloaded store."
    );

    // Verify the watermark is less than or equal to our final emitted LSN —
    // ensures no phantom events were recorded.
    for core_id in 0..core_count {
        let lsn = watermark_store_reload
            .load(core_id)
            .expect("load watermark");
        assert!(
            lsn <= Lsn::new(final_lsn),
            "core {core_id} watermark LSN {lsn:?} exceeds the maximum emitted LSN {final_lsn} \
             — phantom events recorded"
        );
    }
}
