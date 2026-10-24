// SPDX-License-Identifier: BUSL-1.1

//! Spawning one Data Plane core: arena pinning, engine open, boot recovery, and
//! handing the thread to the event loop.

use std::sync::Arc;
use std::thread::JoinHandle;

use tracing::{info, warn};

use crate::data::eventfd::{EventFd, EventFdNotifier};
use crate::data::executor::core_loop::CoreLoop;

use super::boot_replay::replay_wal_and_rebuild_indexes;
use super::boot_seed::seed_catalog_state;
use super::event_loop::run_event_loop;
use super::load_boot_checkpoints;
use super::params::SpawnCoreParams;

/// Spawn a Data Plane core on a dedicated OS thread with TPC isolation.
///
/// Returns the `JoinHandle` and the `EventFdNotifier` that the Control Plane
/// uses to wake this core after pushing a request into the SPSC queue.
///
/// If `wal_records` is non-empty, the core replays vector WAL records
/// during startup (before entering the event loop) to rebuild HNSW indexes.
pub fn spawn_core(
    params: SpawnCoreParams<'_>,
) -> std::io::Result<(JoinHandle<()>, EventFdNotifier)> {
    let SpawnCoreParams {
        core_id,
        node_id,
        request_rx,
        response_tx,
        data_dir,
        wal_records,
        tombstones,
        num_cores,
        compaction_config,
        system_metrics,
        event_producer,
        event_interest,
        governor,
        quiesce,
        hlc,
        array_catalog,
        quarantine_registry,
        maintenance_budget,
        doc_config_seed,
        vector_index_param_seed,
        columnar_schema_seed,
        crs_set_seed,
        replay_done,
    } = params;

    let data_dir = data_dir.to_path_buf();

    // The notifier shares ownership of the eventfd with the core thread.
    // The descriptor stays open after the thread exits. A late wake lands
    // in the dead core's eventfd, never in a reused descriptor number.
    let efd = EventFd::new().map_err(std::io::Error::other)?;
    let notifier = efd.notifier();

    let handle = std::thread::Builder::new()
        .name(format!("data-core-{core_id}"))
        .spawn(move || {
            // 1. Pin to dedicated jemalloc arena.
            match nodedb_mem::arena::pin_thread_arena(core_id as u32) {
                Ok(arena) => info!(core_id, arena, "pinned to jemalloc arena"),
                Err(e) => warn!(core_id, error = %e, "failed to pin jemalloc arena, continuing with default"),
            }

            // 2. Open engines.
            let mut core = CoreLoop::open_with_array_catalog(
                core_id,
                request_rx,
                response_tx,
                &data_dir,
                hlc,
                governor,
                array_catalog,
            )
            .expect("failed to open CoreLoop engines");
            core.set_fail_scope(node_id);

            wire_core_dependencies(
                &mut core,
                WiredDependencies {
                    maintenance_budget,
                    system_metrics,
                    event_producer,
                    event_interest,
                    quiesce,
                    quarantine_registry,
                },
            );

            // Capture before `.query`/`.graph`/`.timeseries` are moved out below
            // (Duration is Copy).
            let checkpoint_interval = compaction_config.checkpoint_interval;

            // 2b. The committed-redo apply routes records by `vshard % num_cores`.
            core.set_num_cores(num_cores);

            // 2c. Apply compaction config.
            core.set_compaction_config(
                compaction_config.interval,
                compaction_config.tombstone_threshold,
            );

            // 2c. Apply query tuning config.
            core.set_query_tuning(compaction_config.query);

            // 2d. Apply graph engine tuning (traversal limits + varlen caps).
            core.set_graph_tuning(compaction_config.graph);

            // 2e. Apply timeseries tuning. This must land before any ingest or
            // WAL replay: it is read when a collection's memtable is CREATED,
            // and a memtable built with the default budgets keeps them for its
            // whole life regardless of what the operator configured.
            core.set_timeseries_tuning(compaction_config.timeseries);

            // 2f. Apply vector tuning, also before the checkpoint restore:
            // restored collections take its seal threshold.
            core.set_vector_tuning(compaction_config.vector);

            // 3 → 3b → 4. Boot recovery runs in exactly this order and no other:
            // restore the checkpoints, THEN seed the catalog state, THEN replay
            // the WAL. Each checkpoint restores state as of the LSN it was
            // stamped with and installs the replay floor that makes replay resume
            // strictly ABOVE that LSN, so any other order overwrites newer state
            // with older rows (restore vs. replay) or leaves an empty seeded
            // engine in place of a restored one (restore vs. seed). Each stage's
            // doc comment states the constraint it rests on.
            load_boot_checkpoints(&mut core)
                .expect("boot checkpoint load failed: corrupt or unreadable checkpoint");
            seed_catalog_state(
                &mut core,
                &doc_config_seed,
                &vector_index_param_seed,
                &columnar_schema_seed,
                &crs_set_seed,
            );
            let replayed = replay_wal_and_rebuild_indexes(
                &mut core,
                &wal_records,
                num_cores,
                &tombstones,
                &vector_index_param_seed,
            );

            // Replay is complete: every in-memory index (HNSW, etc.) has been
            // rebuilt from the WAL. Signal boot so the client gateway is not
            // opened until this core is ready to serve fully-recovered results.
            // A failed replay signals its error, and boot refuses to start.
            // The core still enters its event loop: it is fail-stopped, so it
            // answers every request with a refusal instead of leaving boot's
            // dispatches unanswered until boot gives up.
            // A closed receiver (boot already gave up) is not actionable here.
            let _ = replay_done.send(replayed);

            info!(core_id, "data plane core started (eventfd-driven)");

            // 5. Event loop: poll → drain → tick → checkpoint → repeat.
            run_event_loop(&mut core, core_id, &efd, checkpoint_interval);
        })?;

    Ok((handle, notifier))
}

/// The shared handles a spawned core is wired with before it recovers.
struct WiredDependencies {
    maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
    system_metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,
    event_producer: Option<crate::event::bus::EventProducer>,
    event_interest: Arc<crate::event::interest::EventInterest>,
    quiesce: Option<Arc<crate::bridge::quiesce::CollectionQuiesce>>,
    quarantine_registry: Arc<crate::storage::quarantine::QuarantineRegistry>,
}

/// Attach the process-wide handles this core borrows. Nothing here reads or
/// writes engine state, so it carries no ordering relationship with boot
/// recovery beyond running before it.
fn wire_core_dependencies(core: &mut CoreLoop, deps: WiredDependencies) {
    let WiredDependencies {
        maintenance_budget,
        system_metrics,
        event_producer,
        event_interest,
        quiesce,
        quarantine_registry,
    } = deps;

    core.set_maintenance_budget(maintenance_budget);

    // 2b. Apply metrics reference.
    if let Some(m) = system_metrics {
        core.set_metrics(m);
    }

    // 2b. Wire Event Plane producer (Data Plane → Event Plane).
    if let Some(ep) = event_producer {
        core.set_event_producer(ep);
    }
    core.set_event_interest(event_interest);

    // 2b. Wire the shared scan-quiesce registry so scan
    // handlers can refuse new scans against a draining
    // collection (prerequisite for the safe hard-delete
    // unlink ordering).
    if let Some(q) = quiesce {
        core.set_quiesce(q);
    }

    // 2b. Wire the quarantine registry for corrupt-segment detection.
    core.set_quarantine_registry(quarantine_registry);
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::dispatch::test_requests::make_request;
    use crate::bridge::envelope::{ErrorCode, Status};

    /// A core that panics while it opens its engines drops its `EventFd`.
    /// Later Control Plane wakes for that core must not touch a file opened
    /// after the panic, and the request must fail with "core-0 is gone".
    #[test]
    fn wakes_after_a_core_panicked_at_open_leave_a_later_file_intact() {
        let dir = tempfile::tempdir().unwrap();
        // A regular file in place of the data directory makes every engine open fail.
        let data_dir = dir.path().join("not-a-directory");
        std::fs::write(&data_dir, b"").unwrap();

        let (mut dispatcher, data_sides) = Dispatcher::new(1, 64);
        let data_side = data_sides.into_iter().next().unwrap();
        // no-plane-separation: the one-shot readiness Sender `SpawnCoreParams` requires; no tokio runtime runs here.
        let (replay_done_tx, _replay_done_rx) = tokio::sync::oneshot::channel();
        let (handle, notifier) = spawn_core(SpawnCoreParams {
            core_id: 0,
            node_id: 1,
            request_rx: data_side.request_rx,
            response_tx: data_side.response_tx,
            data_dir: &data_dir,
            wal_records: Arc::from(Vec::new()),
            tombstones: nodedb_wal::TombstoneSet::default(),
            num_cores: 1,
            compaction_config: crate::data::runtime::CoreCompactionConfig::default(),
            system_metrics: None,
            event_producer: None,
            event_interest: crate::event::interest::EventInterest::new(),
            governor: crate::data::executor::core_loop::test_governor(),
            quiesce: None,
            hlc: Arc::new(nodedb_types::OrdinalClock::new()),
            array_catalog: crate::control::array_catalog::ArrayCatalog::handle(),
            quarantine_registry: Arc::new(crate::storage::quarantine::QuarantineRegistry::new()),
            maintenance_budget: Arc::new(
                crate::control::maintenance::MaintenanceBudgetTracker::new(),
            ),
            doc_config_seed: Arc::new(Vec::new()),
            vector_index_param_seed: Arc::new(Vec::new()),
            columnar_schema_seed: Arc::new(Vec::new()),
            crs_set_seed: Arc::new(nodedb_crs::CustomCrsSet::new()),
            replay_done: replay_done_tx,
        })
        .unwrap();
        assert!(
            handle.join().is_err(),
            "the core thread must panic while it opens its engines"
        );
        dispatcher.set_notifier(0, notifier);

        // The core is gone. A file opened now takes the lowest free number.
        let path = dir.path().join("system.redb");
        let mut header = b"redb\x1a\n\xa9\r\n".to_vec();
        header.resize(4096, 0);
        std::fs::write(&path, &header).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        dispatcher.wake_all_cores();
        dispatcher.dispatch(make_request(0)).unwrap();
        dispatcher.wake_all_cores();
        drop(file);

        let after = std::fs::read(&path).unwrap();
        assert_eq!(
            after, header,
            "a wake for the dead core modified a later file"
        );

        let responses = dispatcher.poll_responses();
        assert_eq!(
            responses.len(),
            1,
            "the request to the dead core must be answered"
        );
        assert_eq!(responses[0].status, Status::Error);
        match responses[0].error_code.as_deref() {
            Some(ErrorCode::Internal { detail }) => {
                assert!(
                    detail.contains("core-0 is gone"),
                    "unexpected detail: {detail}"
                );
            }
            other => panic!("expected a core-gone refusal, got {other:?}"),
        }
    }
}
