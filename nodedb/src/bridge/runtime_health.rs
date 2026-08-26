// SPDX-License-Identifier: BUSL-1.1

//! Runtime health registry — per-core heartbeat ticks + checkpoint stall flag.
//!
//! B4/B5 fix (2026-08-26): the 12:23 wedge showed zero ERROR logs while the
//! data-plane core loop and the coordinated checkpoint silently stalled. The
//! startup gate kept reporting `Ok` because it only tracks boot phases, so
//! `/healthz` and `STATUS` had no idea the node was wedged.
//!
//! Every event-loop iteration records a monotonic tick per core; the health
//! formatter compares ticks against a stall threshold. The checkpoint manager
//! raises a stall flag after consecutive failed cycles. Both signals are
//! surfaced through [`crate::control::startup::health::HealthState::Degraded`]
//! so HTTP `/healthz` degrades to 503 and the native `STATUS` reports
//! "Degraded" — a wedge becomes loud instead of silent.
//!
//! Lives in `bridge` so both the data plane (writer) and control plane
//! (reader) can reach it without a control→data or data→control dependency.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// A core whose last recorded tick is older than this is considered stalled.
///
/// The event loop polls at most every 100 ms (`IDLE_POLL_TIMEOUT_MS`), so a
/// healthy core ticks at least 10×/s. 60 s of silence means the loop is stuck
/// inside a request or maintenance work — the wedge shape we hit on 2026-08-26.
pub const CORE_STALL_THRESHOLD: Duration = Duration::from_secs(60);

/// Consecutive checkpoint cycles that failed (deferred / partial / timed out)
/// before the checkpoint stall flag is raised.
pub const CHECKPOINT_STALL_THRESHOLD: u32 = 3;

/// Per-core `Instant` of the last event-loop tick. Index = core id; extended
/// lazily as cores register on boot.
static CORE_LAST_TICK: LazyLock<Mutex<Vec<Instant>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Consecutive failed checkpoint cycles (reset on success).
static CHECKPOINT_FAILURES: AtomicU32 = AtomicU32::new(0);

/// Raised once [`CHECKPOINT_STALL_THRESHOLD`] consecutive cycles failed;
/// cleared on the next successful cycle.
static CHECKPOINT_STALLED: AtomicBool = AtomicBool::new(false);

/// Record one event-loop iteration for `core_id`. Called at the top of every
/// loop pass, before any work, so a stuck request cannot mask a live loop.
pub fn record_core_tick(core_id: usize) {
    let now = Instant::now();
    let mut ticks = CORE_LAST_TICK.lock().unwrap_or_else(|p| p.into_inner());
    if ticks.len() <= core_id {
        ticks.resize(core_id + 1, now);
    }
    ticks[core_id] = now;
}

/// Record the outcome of one checkpoint cycle.
///
/// `ok = true` (every core reported a fresh LSN) resets the consecutive
/// counter and clears the stall flag. `ok = false` increments the counter and
/// raises the stall flag once the threshold is crossed — with a single
/// escalation log at the crossing (the caller logs every failure already).
pub fn record_checkpoint_result(ok: bool) -> bool {
    if ok {
        CHECKPOINT_FAILURES.store(0, Ordering::Relaxed);
        CHECKPOINT_STALLED.store(false, Ordering::Relaxed);
        return false;
    }
    let failures = CHECKPOINT_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    if failures >= CHECKPOINT_STALL_THRESHOLD && !CHECKPOINT_STALLED.swap(true, Ordering::Relaxed) {
        return true; // just crossed the threshold — caller escalates
    }
    false
}

/// Core ids whose last tick is older than `threshold`. Only cores that have
/// registered (booted) are considered; unregistered ids are never "stalled".
pub fn stalled_core_ids(threshold: Duration) -> Vec<usize> {
    let now = Instant::now();
    let ticks = CORE_LAST_TICK.lock().unwrap_or_else(|p| p.into_inner());
    ticks
        .iter()
        .enumerate()
        .filter(|(_, last)| now.duration_since(**last) > threshold)
        .map(|(id, _)| id)
        .collect()
}

/// True when the coordinated checkpoint has been stalling.
pub fn checkpoint_stalled() -> bool {
    CHECKPOINT_STALLED.load(Ordering::Relaxed)
}

/// Aggregate runtime health: no stalled cores and no checkpoint stall.
pub fn runtime_healthy() -> bool {
    stalled_core_ids(CORE_STALL_THRESHOLD).is_empty() && !checkpoint_stalled()
}

/// Human-readable reason for the first unhealthy signal found, for
/// `HealthState::Degraded { reason }`.
pub fn unhealthy_reason() -> &'static str {
    let stalled = stalled_core_ids(CORE_STALL_THRESHOLD);
    if !stalled.is_empty() {
        // Static prefix; the core ids are attached by the caller if needed.
        "data plane core stalled (no event-loop tick)"
    } else if checkpoint_stalled() {
        "checkpoint stalled (consecutive failed cycles)"
    } else {
        "unknown runtime health degradation"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_cores_are_healthy() {
        record_core_tick(0);
        record_core_tick(1);
        assert!(runtime_healthy());
        assert!(stalled_core_ids(CORE_STALL_THRESHOLD).is_empty());
    }

    #[test]
    fn checkpoint_failures_raise_stall_after_threshold() {
        assert!(!record_checkpoint_result(false));
        assert!(!record_checkpoint_result(false));
        // Third consecutive failure crosses the threshold exactly once.
        assert!(record_checkpoint_result(false));
        assert!(checkpoint_stalled());
        // A success clears the flag and the counter.
        assert!(!record_checkpoint_result(true));
        assert!(!checkpoint_stalled());
        assert!(!record_checkpoint_result(false));
        assert!(!checkpoint_stalled()); // counter restarted at 1
    }

    #[test]
    fn unregistered_core_is_not_stalled() {
        // No tick ever recorded for core 7 — must not appear as stalled.
        assert!(stalled_core_ids(CORE_STALL_THRESHOLD).is_empty());
    }
}
