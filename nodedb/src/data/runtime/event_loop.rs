// SPDX-License-Identifier: BUSL-1.1

//! The steady-state core loop: poll → drain → tick → checkpoint → repeat.
//!
//! Entered only once boot recovery is complete and the core has signalled
//! readiness; nothing here participates in the boot ordering.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

use tracing::{error, warn};

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::core_health::{CoreHealthWatchdog, MAX_CONSECUTIVE_PANICS, PANIC_WINDOW_SECS};
use crate::data::eventfd::EventFd;
use crate::data::executor::core_loop::CoreLoop;

/// Maximum idle poll timeout in milliseconds.
///
/// Even without signals, cores wake periodically to run maintenance
/// (e.g., deferred retry polling, metrics flush).
const IDLE_POLL_TIMEOUT_MS: i32 = 100;

/// Maximum requests to process per event loop iteration before
/// yielding to maintenance tasks. Prevents maintenance starvation
/// under sustained high write load.
const MAX_TASKS_PER_ITERATION: usize = 256;

/// Run this core's event loop forever: poll → drain → tick → checkpoint.
///
/// Never returns — the core thread lives in here for the process's lifetime.
pub(super) fn run_event_loop(
    core: &mut CoreLoop,
    core_id: usize,
    efd: &EventFd,
    checkpoint_interval: std::time::Duration,
) {
    let mut watchdog = CoreHealthWatchdog::new();
    let mut last_checkpoint = Instant::now();
    let mut last_event_emit = Instant::now();
    let mut heartbeat_interval = heartbeat_interval_with_jitter();

    loop {
        // Block until signaled or timeout.
        efd.poll_wait(IDLE_POLL_TIMEOUT_MS);

        // Drain all accumulated signals.
        while efd.drain() > 0 {}

        // B4: per-core heartbeat. Recorded every iteration (before any work)
        // so a loop stuck inside a request still trips the stall detector
        // instead of wedging silently — the 12:23 wedge shape (2026-08-26).
        crate::bridge::runtime_health::record_core_tick(core_id);

        // If degraded, drain and reject all pending requests.
        if watchdog.is_degraded() {
            drain_and_reject(core, core_id);
            continue;
        }

        // Process pending requests with panic isolation.
        // Bounded to MAX_TASKS_PER_ITERATION to prevent maintenance
        // starvation under sustained high load.
        let mut tasks_processed = 0usize;
        loop {
            // catch_unwind requires FnOnce: &mut core is not UnwindSafe
            // by default, but we explicitly opt in. The CoreLoop state
            // may be partially inconsistent after a panic (e.g., a
            // half-inserted HNSW node), but:
            //   - Reads are safe: stale or partial data is better than dead core.
            //   - Writes: the WAL ensures crash consistency on replay;
            //     a panicked write was never acknowledged to the client.
            //   - The watchdog degrades the core before repeated panics
            //     can compound corruption.
            let result = catch_unwind(AssertUnwindSafe(|| core.tick()));

            match result {
                Ok(0) => break, // No more pending requests.
                Ok(_) => {
                    watchdog.record_success();
                    tasks_processed += 1;
                    if tasks_processed >= MAX_TASKS_PER_ITERATION {
                        break; // Yield to maintenance.
                    }
                }
                Err(panic_payload) => {
                    // Extract panic message for logging.
                    let msg = panic_message(&panic_payload);
                    error!(
                        core_id,
                        panic_count = watchdog.consecutive_panics + 1,
                        message = %msg,
                        "data plane core caught panic during tick"
                    );

                    let is_degraded = watchdog.record_panic();
                    if is_degraded {
                        error!(
                            core_id,
                            threshold = MAX_CONSECUTIVE_PANICS,
                            window_secs = PANIC_WINDOW_SECS,
                            "core entered DEGRADED mode — rejecting all requests"
                        );
                        drain_and_reject(core, core_id);
                    }

                    // The panicked tick may have consumed a request from
                    // the queue without sending a response. The in-flight
                    // request's oneshot channel in RequestTracker will
                    // time out on the Control Plane side (deadline expiry),
                    // which is the correct behavior — the client sees
                    // DEADLINE_EXCEEDED rather than hanging forever.

                    break; // Exit inner loop; re-enter poll_wait.
                }
            }
        }

        // Periodic vector checkpoint (when idle and interval elapsed).
        //
        // The sparse-vector flush used to ride along here. It no longer
        // does: this timer has no ordering relationship with the
        // coordinated checkpoint that authorises WAL truncation, so a
        // flush driven from here could land AFTER the truncation that
        // had already deleted the records it was meant to make
        // redundant. `execute_checkpoint` is the only place a flush can
        // both make state durable and report the LSN that authorises
        // deleting its WAL, so that is where it belongs.
        //
        // For the same reason this flush does not advance
        // `vector_durable_lsn` on success: only a flush ordered against
        // the truncation it authorises may raise the floor the
        // coordinated checkpoint clamps to. This one is an
        // opportunistic head start, so its only obligations are to make
        // the bytes durable and to say so when it cannot.
        if last_checkpoint.elapsed() >= checkpoint_interval {
            if let Err(e) = core.checkpoint_vector_indexes() {
                warn!(
                    core = core_id,
                    error = %e,
                    "periodic vector checkpoint failed; the coordinated checkpoint \
                     will clamp its reported LSN if it fails there too"
                );
            }
            last_checkpoint = Instant::now();
        }

        // Periodic compaction + maintenance (tombstone cleanup, CSR compact, edge sweep).
        core.maybe_run_maintenance();

        // Heartbeat: if no user writes for ~1 second (±100ms jitter),
        // emit a heartbeat to advance the Event Plane's partition
        // watermark. Without this, streaming MV global_watermark()
        // stalls on idle partitions. Jitter prevents multi-core
        // thundering herd when all cores go idle simultaneously.
        if tasks_processed > 0 {
            last_event_emit = Instant::now();
        } else if last_event_emit.elapsed() >= heartbeat_interval {
            core.emit_heartbeat();
            last_event_emit = Instant::now();
            heartbeat_interval = heartbeat_interval_with_jitter();
        }
    }
}

/// Drain all pending requests from a core's SPSC queue and send back
/// `CoreDegraded` error responses. Used when the watchdog has flagged
/// the core as unhealthy.
fn drain_and_reject(core: &mut CoreLoop, core_id: usize) {
    core.drain_requests();
    while let Some(task) = core.task_queue.pop_front() {
        let response = Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: core.watermark,
            error_code: Some(Box::new(ErrorCode::Internal {
                detail: format!("core-{core_id} is degraded after repeated panics"),
            })),
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        };
        if let Err(e) = core
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core_id, error = %e, "failed to send degraded-rejection response");
        }
    }
}

/// Compute heartbeat interval with ±100ms jitter.
///
/// Returns a Duration in the range [900ms, 1100ms]. The jitter spreads
/// heartbeat emissions across cores so they don't all fire in the same
/// poll iteration when the system goes idle.
///
/// Uses a fast splitmix64-style hash of the current timestamp nanos to
/// produce pseudo-random jitter without requiring the `rand` crate in
/// production code (it's dev-only).
fn heartbeat_interval_with_jitter() -> std::time::Duration {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    // splitmix64
    let mut x = seed;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    // Map to [0, 200] → offset by -100 → [-100, +100] ms.
    let jitter_ms = (x % 201) as i64 - 100;
    std::time::Duration::from_millis((1000 + jitter_ms) as u64)
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
