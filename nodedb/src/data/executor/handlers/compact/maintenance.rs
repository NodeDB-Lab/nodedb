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

        // Aside partition directories a committed timeseries truncate could
        // not remove at finalize: retry on every maintenance call.
        if !self.ts_truncate_backlog.is_empty() {
            let pending = self.retry_ts_truncate_backlog();
            if pending > 0 {
                tracing::warn!(
                    core = self.core_id,
                    pending,
                    "timeseries truncate backlog still pending after retry"
                );
            }
        }

        // KV expiry wheel tick: process expired keys on every maintenance call.
        // Bounded by the per-tick reap budget internally — safe for the reactor.
        // Expired keys are emitted as structured log events for CDC visibility.
        {
            let now_ms = crate::engine::kv::current_ms();
            let expired_keys = self.kv_engine.tick_expiry(now_ms);
            if !expired_keys.is_empty() {
                tracing::debug!(
                    core = self.core_id,
                    reaped = expired_keys.len(),
                    backlog = self.kv_engine.expiry_backlog(),
                    "kv expiry wheel tick"
                );

                for ek in &expired_keys {
                    info!(
                        target: "nodedb::kv::expired",
                        database_id = ek.database_id,
                        tenant_id = ek.tenant_id,
                        collection = %ek.collection,
                        key_len = ek.key.len(),
                        "kv key expired"
                    );
                }
            }
        }

        // Compaction: periodic tombstone removal + segment merge.
        let now = std::time::Instant::now();
        if let Some(last) = self.last_maintenance
            && now.duration_since(last) < self.compaction_interval
        {
            return !flush_plan.is_empty();
        }
        self.last_maintenance = Some(now);
        // Horizon-GC the per-core last-write-LSN version index: evict entries
        // far below the watermark and enforce the entry-count backstop. Rides
        // the compaction interval — no dedicated timer.
        self.gc_write_index();
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
