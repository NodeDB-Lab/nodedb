// SPDX-License-Identifier: BUSL-1.1

//! D-δ integration test 5: Event Plane watermarks persisted through shutdown.
//!
//! Verifies the `DrainingEventPlane` shutdown barrier end-to-end:
//!
//! 1. Spawn an `EventPlane` with a real `WatermarkStore` backed by redb.
//! 2. Process 100 WriteEvents so consumer watermarks advance.
//! 3. Hold an Event Plane auxiliary-loop drain guard beyond the 500ms phase
//!    budget and prove watermark persistence cannot start early.
//! 4. Release the guard, await `Closed`, and reload the final watermark.
//!
//! This is an in-process test because watermark verification requires direct
//! access to `WatermarkStore` APIs that are not observable through the binary's
//! network interface.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthConfig;
use nodedb::control::shutdown::{PHASE_BUDGET, ShutdownBus, ShutdownPhase, ShutdownWatch};
use nodedb::control::state::SharedState;
use nodedb::event::bus::create_event_bus_with_capacity;
use nodedb::event::trigger::TriggerDlq;
use nodedb::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use nodedb::event::watermark::WatermarkStore;
use nodedb::event::{EventPlane, EventPlaneConfig};
use nodedb::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb::wal::WalManager;

fn make_write_event(seq: u64, lsn_val: u64) -> WriteEvent {
    WriteEvent {
        sequence: seq,
        collection: Arc::from("test_collection"),
        op: WriteOp::Insert,
        row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("row-1")),
        lsn: Lsn::new(lsn_val),
        database_id: DatabaseId::DEFAULT,
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        source: EventSource::User,
        new_value: Some(Arc::from(b"payload".as_slice())),
        old_value: None,
        system_time_ms: None,
        valid_time_ms: None,
        user_id: None,
        statement_digest: None,
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
            nodedb::control::state::DataPlaneHandles {
                dispatcher,
                quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
                array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
                system_metrics: Arc::new(nodedb::control::metrics::SystemMetrics::new()),
            },
            Arc::clone(&wal),
            &catalog_path,
            &AuthConfig::default(),
            Default::default(),
            false,
            nodedb::data::executor::core_loop::test_governor(),
        )
        .expect("shared_state");
        let cdc_router = Arc::clone(&shared.cdc_router);
        let shutdown = Arc::new(ShutdownWatch::new());
        let (shutdown_bus, mut shutdown_handle) = ShutdownBus::new(Arc::clone(&shutdown));

        let (mut producers, consumers) = create_event_bus_with_capacity(1, 256);
        let core_count = consumers.len();

        let plane = EventPlane::spawn(EventPlaneConfig {
            consumers_rx: consumers,
            wal: Arc::clone(&wal),
            watermark_store: Arc::clone(&watermark_store),
            shared_state: shared,
            trigger_dlq,
            cdc_router,
            shutdown: Arc::clone(&shutdown),
            shutdown_bus: shutdown_bus.clone(),
        });

        // Emit 100 events with increasing LSNs.
        for i in 1u64..=100 {
            producers[0].emit(make_write_event(i, i * 10));
        }

        // Wait for every event so the final persisted watermark is
        // deterministic rather than dependent on scheduler timing.
        tokio::time::timeout(Duration::from_secs(2), async {
            while plane.total_events_processed() < 100 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Event Plane must process every emitted event before shutdown");
        assert_eq!(plane.total_events_processed(), 100);

        // The final LSN we expect to see persisted.
        let final_lsn = 100 * 10; // seq 100 → LSN 1000

        // Model an Event Plane auxiliary loop that cannot finish until its
        // current work is released. It is critical, so exceeding the normal
        // phase budget must hold the sequencer rather than permit watermark
        // persistence or WAL fsync to race it.
        let mut aux_guard = shutdown_bus.register_critical_task(
            ShutdownPhase::DrainingEventPlane,
            "event_plane::test_held_aux_loop",
        );
        let (release_aux_tx, release_aux_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            aux_guard.await_signal().await;
            let _ = release_aux_rx.await;
            aux_guard.report_drained();
        });

        let supervisor =
            plane.spawn_shutdown_supervisor(shutdown_bus.clone(), Duration::from_secs(2));
        let sequencer = shutdown_bus.initiate();
        shutdown_handle
            .await_phase(ShutdownPhase::DrainingEventPlane)
            .await;
        tokio::time::sleep(PHASE_BUDGET + Duration::from_millis(50)).await;
        assert_eq!(
            shutdown_bus.current_phase(),
            ShutdownPhase::DrainingEventPlane
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                shutdown_handle.await_phase(ShutdownPhase::PersistingWatermarks),
            )
            .await
            .is_err(),
            "watermark persistence advanced while an Event Plane auxiliary loop was held"
        );

        release_aux_tx
            .send(())
            .expect("release held auxiliary loop");
        shutdown_handle.await_phase(ShutdownPhase::Closed).await;
        sequencer.await.expect("shutdown sequencer");
        supervisor.await.expect("event plane supervisor");

        drop(watermark_store); // release this scope's own Arc clone
        (final_lsn, core_count)
    };

    // ── Phase 2: Reload and verify watermarks ─────────────────────────────────

    // Open a fresh WatermarkStore from the same redb file.
    let watermark_store_reload = WatermarkStore::open(dir.path()).expect("reload watermark_store");

    // The supervisor joined consumers only after their shutdown path flushed
    // its safe final watermark, so the exact final LSN must be observable.
    for core_id in 0..core_count {
        let lsn = watermark_store_reload
            .load(core_id)
            .expect("load watermark");
        assert_eq!(
            lsn,
            Lsn::new(final_lsn),
            "core {core_id} did not persist the final safe Event Plane watermark"
        );
    }
}
