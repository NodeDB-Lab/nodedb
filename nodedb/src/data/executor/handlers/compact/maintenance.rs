// SPDX-License-Identifier: BUSL-1.1

//! Idle maintenance loop: checkpoint coordinator, KV expiry wheel, idle
//! flush of timeseries memtables, and the periodic compaction trigger.
//!
//! Driven by the runtime event loop on every idle wake; rate-limited via
//! `compaction_interval` so the heavy `run_compaction` path runs at most
//! once per interval.

use tracing::info;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Run maintenance tasks if enough time has elapsed.
    ///
    /// Called from the runtime event loop on every idle wake. Tracks the
    /// last maintenance time internally and skips if the interval hasn't
    /// elapsed. Returns `true` if maintenance was executed.
    pub fn maybe_run_maintenance(&mut self) -> bool {
        // Checkpoint coordinator tick: incremental dirty page flushing.
        // Runs on its own interval (independent from compaction interval).
        let flush_plan = self.checkpoint_coordinator.tick();
        for (engine, pages) in &flush_plan {
            match engine.as_str() {
                // A maintenance flush deliberately does NOT advance
                // `vector_durable_lsn` / `crdt_durable_lsn`, even on success.
                // Those fields are the floor the coordinated checkpoint clamps
                // to, and they may only record what a flush ordered against the
                // truncation it authorises has made durable. Raising them from
                // an unordered timer would let a later failed checkpoint clamp
                // to a point this path claimed — the exact "a flush that is not
                // ordered against the truncation it authorises is not a
                // checkpoint" mistake that moved the sparse-vector flush out of
                // `data/runtime.rs`. Leaving them alone costs nothing: the next
                // `execute_checkpoint` re-flushes and reports for itself.
                "vector" => match self.checkpoint_vector_indexes() {
                    Ok(outcome) => {
                        self.checkpoint_coordinator
                            .record_flush("vector", outcome.files_written.min(*pages));
                    }
                    Err(e) => {
                        tracing::warn!(
                            core = self.core_id,
                            error = %e,
                            "maintenance vector checkpoint failed; pages stay dirty for the \
                             next tick and the coordinated checkpoint will clamp its \
                             reported LSN if it fails there too"
                        );
                    }
                },
                "crdt" => match self.checkpoint_crdt_engines() {
                    Ok(outcome) => {
                        self.checkpoint_coordinator
                            .record_flush("crdt", outcome.files_written.min(*pages));
                    }
                    Err(e) => {
                        tracing::warn!(
                            core = self.core_id,
                            error = %e,
                            "maintenance CRDT checkpoint failed; pages stay dirty for the \
                             next tick and the coordinated checkpoint will clamp its \
                             reported LSN if it fails there too"
                        );
                    }
                },
                // Same rule as vector/crdt: the flushed point is deliberately
                // NOT recorded into `columnar_durable_lsn`. This flush is
                // ordered against a timer, not against the truncation the
                // coordinated checkpoint authorises, so it may not raise the
                // floor that checkpoint clamps to. It exists only to keep the
                // backlog from arriving at that checkpoint whole.
                "columnar" => match self.checkpoint_columnar_engines() {
                    Ok(_) => {
                        self.checkpoint_coordinator.record_flush("columnar", *pages);
                    }
                    Err(e) => {
                        tracing::warn!(
                            core = self.core_id,
                            error = %e,
                            "maintenance columnar checkpoint failed; pages stay dirty for the \
                             next tick and the coordinated checkpoint will clamp its \
                             reported LSN if it fails there too"
                        );
                    }
                },
                "sparse" => {
                    // redb is ACID — writes are already durable.
                    self.checkpoint_coordinator.record_flush("sparse", *pages);
                }
                "timeseries" => {
                    // A committed flush whose install failed is installed
                    // first, every tick, whether or not the core is idle.
                    let tick_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    self.roll_forward_ts_flushes(tick_ms);

                    // Idle flush: if no ingest for 5 seconds, flush all
                    // non-empty memtables so data becomes queryable.
                    let idle_threshold = std::time::Duration::from_secs(5);
                    let is_idle = self
                        .last_ts_ingest
                        .map(|t| t.elapsed() >= idle_threshold)
                        .unwrap_or(false);

                    if is_idle {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        let collections: Vec<(
                            nodedb_types::DatabaseId,
                            crate::types::TenantId,
                            String,
                        )> = self
                            .columnar_memtables
                            .iter()
                            .filter(|(_, mt)| !mt.is_empty())
                            .map(|(k, _)| k.clone())
                            .collect();
                        let mut flushed = 0usize;
                        for (db, tid, collection) in &collections {
                            match self.flush_ts_collection(*tid, *db, collection, now_ms) {
                                Ok(()) => flushed += 1,
                                Err(e) => {
                                    tracing::error!(
                                        collection = %collection,
                                        error = %e,
                                        "idle ts flush failed — segment write error; \
                                         collection skipped this maintenance cycle"
                                    );
                                    crate::diag::timeseries_idle_flush_failed(
                                        &e,
                                        db.as_u64(),
                                        tid.as_u64(),
                                        collection,
                                    );
                                }
                            }
                        }
                        if flushed > 0 {
                            info!(
                                core = self.core_id,
                                flushed, "idle flush: timeseries memtables flushed"
                            );
                        }
                        // Reset so we don't re-flush until next ingest.
                        self.last_ts_ingest = None;
                        self.checkpoint_coordinator
                            .record_flush("timeseries", flushed.max(*pages));
                    } else {
                        self.checkpoint_coordinator
                            .record_flush("timeseries", *pages);
                    }
                }
                // `tick()` only ever plans engines from `TRACKED_ENGINES`, so
                // reaching this arm means that list gained an entry without an
                // arm here. Silently ignoring it would leave the engine planned
                // every tick and flushed never, its dirty count only growing,
                // so it is reported rather than dropped.
                other => {
                    tracing::warn!(
                        core = self.core_id,
                        engine = other,
                        pages = *pages,
                        "checkpoint tick planned a flush for an engine with no maintenance \
                         flush path; its backlog cannot be worked off"
                    );
                }
            }
        }

        // Continuous aggregates: load images, backfill, and fold pending
        // refresh cuts. Bounded per call by the fold budget.
        {
            let now_ms = self.ingest_now_ms();
            let folded = self.fold_pending_cagg_partitions(now_ms);
            if folded > 0 {
                tracing::debug!(
                    core = self.core_id,
                    folded,
                    "continuous aggregate partitions folded"
                );
            }
        }

        // Timeseries partitions of one interval merge into one. Runs after
        // the fold, so groups whose sources the fold just completed merge in
        // the same call. Bounded per call by `merge_groups_per_tick`.
        {
            let pass = self.merge_ts_partitions(false);
            if pass.merged > 0 {
                tracing::debug!(
                    core = self.core_id,
                    merged = pass.merged,
                    "timeseries partitions merged"
                );
            }
        }

        // KV expiry wheel tick: hand every expired entry to the due set.
        // Bounded by the per-tick budget internally — safe for the reactor.
        // The core reaps nothing here: the data group's leader proposes each
        // reap through write admission, and every replica applies it at the
        // same log position (`engine::kv::engine::expiry_due`).
        {
            let now_ms = crate::engine::kv::current_ms();
            let moved = self.kv_engine.collect_due_expiries(now_ms);
            if moved > 0 {
                tracing::debug!(
                    core = self.core_id,
                    moved,
                    due = self.kv_engine.due_len(),
                    backlog = self.kv_engine.expiry_backlog(),
                    "kv expiry wheel tick"
                );
            }
        }

        // Compaction: periodic tombstone removal + segment merge.
        let now = std::time::Instant::now();
        if let Some(last) = self.maintenance.last_maintenance
            && now.duration_since(last) < self.maintenance.compaction_interval
        {
            return !flush_plan.is_empty();
        }
        self.maintenance.last_maintenance = Some(now);
        // Lease-GC abandoned per-transaction staging overlays (client vanished,
        // teardown dispatch failed, or vShard leader moved mid-txn). Bounded by
        // a per-tick budget internally. Rides the compaction interval.
        self.reap_expired_overlays();
        self.run_compaction(false);
        true
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn maintenance_respects_interval() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        // First call should run.
        assert!(core.maybe_run_maintenance());

        // Immediate second call should skip.
        assert!(!core.maybe_run_maintenance());
    }
}
