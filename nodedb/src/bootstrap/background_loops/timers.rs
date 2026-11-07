// SPDX-License-Identifier: BUSL-1.1

//! Fixed-interval Control Plane timers: tenant rate reset, audit flush,
//! and tenant memory estimation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::control::security::audit::AuditFlushControl;
use crate::control::shutdown::{ShutdownPhase, ShutdownReceiver, spawn_blocking_loop, spawn_loop};
use crate::control::state::SharedState;

/// Period of the tenant rate counter reset.
const TENANT_RATE_RESET_PERIOD: Duration = Duration::from_secs(1);
/// Period of the audit log flush to the catalog.
const AUDIT_FLUSH_PERIOD: Duration = Duration::from_secs(10);
/// Pause after a flush the catalog refused, before the next attempt.
const AUDIT_FLUSH_RETRY_DELAY: Duration = Duration::from_millis(200);
/// Longest time the flush executor waits before it looks at the shutdown signal again.
const AUDIT_FLUSH_SHUTDOWN_POLL: Duration = Duration::from_millis(100);
/// Period of the summary rows for throttled bearer refusals.
const BEARER_REFUSAL_SUMMARY_PERIOD: Duration = Duration::from_secs(60);
/// Period of the tenant memory estimate.
const TENANT_MEMORY_PERIOD: Duration = Duration::from_secs(30);

/// Spawn a Control Plane loop that runs `on_tick` once per `period` until
/// shutdown. The loop joins at the Control Plane drain.
pub fn spawn_tick_loop<F>(
    shared: &Arc<SharedState>,
    name: &'static str,
    period: Duration,
    on_tick: F,
) where
    F: Fn(&SharedState) + Send + 'static,
{
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        name,
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => on_tick(state.as_ref()),
                }
            }
        },
    );
}

/// Spawn the audit log flush loop.
///
/// The executor flushes every `AUDIT_FLUSH_PERIOD` and also when the buffer requests an early flush.
/// Recorders past the backpressure point wait for its flushes.
/// It runs on a blocking thread of its own for its whole life.
/// So no Tokio worker it needs can be parked by a recorder, and its catalog commits block no worker.
/// On shutdown it flushes once more, so a clean stop leaves no entry only in memory.
fn spawn_audit_flush_loop(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    let control = match shared.audit.lock() {
        Ok(log) => log.flush_control(),
        Err(poisoned) => poisoned.into_inner().flush_control(),
    };
    control.attach_executor();
    spawn_blocking_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "audit_log_flush",
        ShutdownPhase::DrainingControlPlane,
        move |shutdown| run_audit_flush_executor(&state, &control, &shutdown),
    );
}

/// The flush executor's body. It ends once shutdown is signalled.
fn run_audit_flush_executor(
    state: &SharedState,
    control: &AuditFlushControl,
    shutdown: &ShutdownReceiver,
) {
    let mut last_flush = Instant::now();
    while !shutdown.is_cancelled() {
        control.wait_for_flush_request(AUDIT_FLUSH_SHUTDOWN_POLL);
        let requested = control.take_flush_request();
        if !requested && last_flush.elapsed() < AUDIT_FLUSH_PERIOD {
            continue;
        }
        last_flush = Instant::now();
        if !state.flush_audit_log() {
            // Waiting recorders keep requesting flushes, so a failing catalog sets the pace.
            std::thread::sleep(AUDIT_FLUSH_RETRY_DELAY);
        }
    }
    state.flush_audit_log();
    control.detach_executor();
}

/// Spawn the loop that closes each bearer refusal window into its summary rows.
///
/// On shutdown it closes the last window, so a clean stop loses no count.
/// The rows reach the audit WAL, and boot replays what no flush committed.
fn spawn_bearer_refusal_summary_loop(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "bearer_refusal_summary",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(BEARER_REFUSAL_SUMMARY_PERIOD);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => state.close_bearer_refusal_window(),
                }
            }
            state.close_bearer_refusal_window();
        },
    );
}

/// Spawn the tenant rate reset, audit log flush, bearer refusal summary, and
/// tenant memory estimate timers.
pub fn spawn_tenant_timers(shared: &Arc<SharedState>) {
    spawn_tick_loop(
        shared,
        "tenant_rate_reset",
        TENANT_RATE_RESET_PERIOD,
        SharedState::reset_tenant_rate_counters,
    );
    spawn_audit_flush_loop(shared);
    spawn_bearer_refusal_summary_loop(shared);
    spawn_tick_loop(
        shared,
        "tenant_memory_estimate",
        TENANT_MEMORY_PERIOD,
        SharedState::update_tenant_memory_estimates,
    );
}
