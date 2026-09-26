// SPDX-License-Identifier: BUSL-1.1

//! Registry of every background loop on this node.
//!
//! Holds the join handles so shutdown can observe "did loop X exit?"
//! rather than hoping the watch signal was honored. Each handle carries
//! the [`ShutdownPhase`] at which it is joined: the canonical path calls
//! [`LoopRegistry::shutdown_phase_strict`] once per phase, and each call
//! retains and joins only that phase's loops. The bounded
//! [`LoopRegistry::shutdown_all`] API drains the whole registry at once
//! and reports laggards after its deadline.
//!
//! The phase governs WHEN a handle is joined, never when the loop is told
//! to stop. Every loop is signalled through the flat [`ShutdownWatch`] at
//! shutdown initiation, whatever phase it registered at.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use super::phase::ShutdownPhase;
use super::report::{LaggardReport, ShutdownReport};

/// Handle variant — async (tokio task) vs blocking (spawn_blocking).
#[derive(Debug)]
pub enum LoopHandle {
    /// Tokio task. Can be `.abort()`'d on laggard.
    Async(JoinHandle<()>),
    /// Tokio task that MUST NOT be `.abort()`'d — it advances a
    /// replicated / durable state machine and a mid-flight
    /// cancellation would leave it diverged from its peers or
    /// strand committed-but-unapplied work. A laggard is never
    /// force-cancelled. The bounded shutdown path reports it as
    /// still running; the strict canonical path retains and joins
    /// it at its safe termination boundary. The loop is responsible for observing
    /// shutdown promptly at a safe internal boundary (e.g. a
    /// Calvin scheduler breaking at an epoch boundary, or the
    /// raft apply loop finishing its current drain batch).
    AsyncNoAbort(JoinHandle<()>),
    /// `spawn_blocking` task. The join handle still exists,
    /// but aborting is a no-op — it only cancels scheduling,
    /// not the running thread. The bounded path reports laggards;
    /// the strict path waits for them to terminate.
    Blocking(JoinHandle<()>),
}

impl LoopHandle {
    fn take_handle(self) -> (JoinHandle<()>, bool) {
        match self {
            Self::Async(h) => (h, true),
            Self::AsyncNoAbort(h) => (h, false),
            Self::Blocking(h) => (h, false),
        }
    }
}

/// Error returned by [`LoopRegistry::register`] if a drain has
/// already been invoked. Prevents a race where a background spawn
/// completes registration after shutdown has started.
#[derive(Debug, thiserror::Error)]
#[error("loop registry is closed — cannot register \"{name}\"")]
pub struct RegistryClosed {
    /// Name of the loop that attempted to register too late.
    pub name: &'static str,
}

/// Per-handle record, carrying just enough context to produce
/// a useful shutdown report and to decide abort vs log.
#[derive(Debug)]
struct LoopEntry {
    name: &'static str,
    phase: ShutdownPhase,
    handle: LoopHandle,
    registered_at: Instant,
}

#[derive(Debug, Default)]
struct Inner {
    handles: Vec<LoopEntry>,
    closed: bool,
}

/// Shared registry. Held by `SharedState` in an `Arc`.
#[derive(Debug, Default)]
pub struct LoopRegistry {
    inner: Mutex<Inner>,
}

/// Lock the registry's inner state. On poisoning — which only
/// happens if a previous holder panicked while mutating the
/// registry — log loudly at `error!` before recovering the
/// guard. Shutdown must still proceed (aborting it on a panic
/// somewhere else would leak background loops), but operators
/// MUST see the signal.
fn lock_inner<'a>(inner: &'a Mutex<Inner>, site: &'static str) -> std::sync::MutexGuard<'a, Inner> {
    match inner.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!(
                site,
                "LoopRegistry mutex poisoned — a previous holder panicked while mutating \
                 the registry. Recovering the guard so shutdown can still proceed, but \
                 this is a bug and the panic source should be investigated."
            );
            poisoned.into_inner()
        }
    }
}

impl LoopRegistry {
    /// New empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a join handle under a stable name and the phase that
    /// joins it. Rejects once the first drain has run.
    ///
    /// `phase` selects the drain that joins this handle. It does NOT
    /// gate the shutdown signal: the loop is signalled with every other
    /// loop when the flat watch fires.
    pub fn register(
        &self,
        name: &'static str,
        phase: ShutdownPhase,
        handle: LoopHandle,
    ) -> Result<(), RegistryClosed> {
        let mut guard = lock_inner(&self.inner, "register");
        if guard.closed {
            return Err(RegistryClosed { name });
        }
        guard.handles.push(LoopEntry {
            name,
            phase,
            handle,
            registered_at: Instant::now(),
        });
        Ok(())
    }

    /// Number of registered handles currently alive. Useful
    /// for tests and for observability counters.
    pub fn live_count(&self) -> usize {
        lock_inner(&self.inner, "live_count").handles.len()
    }

    /// Number of registered handles that `phase` will join.
    pub fn live_count_at(&self, phase: ShutdownPhase) -> usize {
        lock_inner(&self.inner, "live_count_at")
            .handles
            .iter()
            .filter(|e| e.phase == phase)
            .count()
    }

    /// Close the registry and await every registered handle with a
    /// shared `deadline`, whatever phase it registered at. This is the
    /// bounded API for noncritical and test callers, which drain the
    /// whole registry in one call and run no phase sequencer. Handles
    /// that do not complete by the deadline:
    ///
    /// - Async handles are `.abort()`'d and recorded as
    ///   laggards with `aborted = true`.
    /// - Blocking handles are left running (there is no way
    ///   to force-kill them from tokio) and recorded as
    ///   laggards with `aborted = false`.
    ///
    /// The returned [`ShutdownReport`] lists every handle in
    /// exactly one of the two vectors.
    pub async fn shutdown_all(&self, deadline: Duration) -> ShutdownReport {
        let start = Instant::now();

        // Drain handles under the lock, then release it so
        // we're not holding a Mutex across `.await`.
        let entries: Vec<LoopEntry> = {
            let mut guard = lock_inner(&self.inner, "shutdown_all");
            guard.closed = true;
            std::mem::take(&mut guard.handles)
        };

        let mut exited_clean: Vec<&'static str> = Vec::with_capacity(entries.len());
        let mut laggards: Vec<LaggardReport> = Vec::new();

        for entry in entries {
            let LoopEntry {
                name,
                phase: _,
                handle,
                registered_at,
            } = entry;
            let (mut join, can_abort) = handle.take_handle();

            // Polled even with no budget left: `timeout` polls the handle
            // before its timer, so a loop that already exited while an
            // earlier one used up the budget is counted clean.
            let elapsed_budget = deadline.saturating_sub(start.elapsed());
            match tokio::time::timeout(elapsed_budget, &mut join).await {
                Ok(Ok(())) => exited_clean.push(name),
                Ok(Err(join_err)) => {
                    // Task panicked or was previously
                    // cancelled. Treat as exited — shutdown
                    // doesn't care whether the body
                    // completed normally; a panic is already
                    // a bug that surfaced elsewhere.
                    tracing::warn!(
                        loop_name = name,
                        error = %join_err,
                        "background loop exited with error during shutdown"
                    );
                    exited_clean.push(name);
                }
                Err(_) => {
                    laggards.push(
                        abort_and_report_laggard(&mut join, name, registered_at, start, can_abort)
                            .await,
                    );
                }
            }
        }

        ShutdownReport {
            exited_clean,
            laggards,
            total: start.elapsed(),
        }
    }

    /// Signal every registered loop, then close the registry and drain the
    /// loops registered at `phase` for the canonical shutdown path.
    ///
    /// Loops registered at another phase stay registered and are joined by
    /// that phase's own call. The signal is flat: this call signals EVERY
    /// loop, including the ones it does not join, so a loop drained late has
    /// still been told to stop at the first drain.
    ///
    /// The configured deadline bounds only abortable async work. An
    /// [`LoopHandle::AsyncNoAbort`] or [`LoopHandle::Blocking`] loop that
    /// passes the deadline is recorded as a laggard but remains owned and is
    /// awaited to actual termination. This makes those loops correctness
    /// barriers: callers must not report a later shutdown phase as drained
    /// while either can still hold durable or replicated state. A second OS
    /// signal is the process-level force-exit path.
    pub async fn shutdown_phase_strict(
        &self,
        shutdown: &super::ShutdownWatch,
        phase: ShutdownPhase,
        deadline: Duration,
    ) -> ShutdownReport {
        shutdown.signal();
        let start = Instant::now();
        let entries: Vec<LoopEntry> = {
            let mut guard = lock_inner(&self.inner, "shutdown_phase_strict");
            // The registry closes at the FIRST drain: a loop registering
            // after shutdown starts is the race `RegistryClosed` rejects.
            guard.closed = true;
            let mut drained = Vec::new();
            let mut kept = Vec::new();
            for entry in std::mem::take(&mut guard.handles) {
                if entry.phase == phase {
                    drained.push(entry);
                } else {
                    kept.push(entry);
                }
            }
            guard.handles = kept;
            drained
        };

        let mut exited_clean = Vec::with_capacity(entries.len());
        let mut laggards = Vec::new();

        for entry in entries {
            let LoopEntry {
                name,
                phase: _,
                handle,
                registered_at,
            } = entry;
            let (mut join, can_abort) = handle.take_handle();
            let elapsed_budget = deadline.saturating_sub(start.elapsed());

            // Polled even with no budget left: `timeout` polls the handle
            // before its timer, so a loop that already exited while an
            // earlier one used up the budget is counted clean, not aborted.
            let joined = tokio::time::timeout(elapsed_budget, &mut join).await.ok();
            match joined {
                Some(Ok(())) => exited_clean.push(name),
                Some(Err(join_err)) => {
                    tracing::warn!(
                        loop_name = name,
                        error = %join_err,
                        "background loop exited with error during strict shutdown"
                    );
                    exited_clean.push(name);
                }
                None => {
                    if can_abort {
                        join.abort();
                    }
                    // This await is intentionally unbounded. For abortable
                    // work it confirms cancellation; for no-abort and
                    // blocking work it preserves ownership until the loop has
                    // reached its own safe termination boundary.
                    let _ = join.await;
                    laggards.push(LaggardReport {
                        name,
                        uptime: registered_at.elapsed(),
                        wait_elapsed: start.elapsed(),
                        aborted: can_abort,
                    });
                }
            }
        }

        ShutdownReport {
            exited_clean,
            laggards,
            total: start.elapsed(),
        }
    }
}

async fn abort_and_report_laggard(
    handle: &mut JoinHandle<()>,
    name: &'static str,
    registered_at: Instant,
    shutdown_start: Instant,
    can_abort: bool,
) -> LaggardReport {
    if can_abort {
        handle.abort();
        let _ = handle.await;
    }
    LaggardReport {
        name,
        uptime: registered_at.elapsed(),
        wait_elapsed: shutdown_start.elapsed(),
        aborted: can_abort,
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ShutdownPhase, ShutdownWatch};
    use super::*;
    use std::sync::Arc;

    /// Phase used by tests that do not care which phase joins the loop.
    const PHASE: ShutdownPhase = ShutdownPhase::DrainingEventPlane;

    #[tokio::test]
    async fn clean_exit_all_handles_finish() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();

        for name in ["a", "b", "c"] {
            let mut rx = watch.subscribe();
            let handle = tokio::spawn(async move {
                rx.wait_cancelled().await;
            });
            registry
                .register(name, PHASE, LoopHandle::Async(handle))
                .expect("register");
        }

        assert_eq!(registry.live_count(), 3);
        watch.signal();

        let report = registry.shutdown_all(Duration::from_millis(200)).await;
        assert!(report.is_clean(), "{report}");
        assert_eq!(report.exited_clean.len(), 3);
        assert!(report.total < Duration::from_millis(150));
    }

    #[tokio::test]
    async fn laggard_detected_and_aborted() {
        let registry = LoopRegistry::new();
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        registry
            .register("sleepy", PHASE, LoopHandle::Async(handle))
            .expect("register");

        let report = registry.shutdown_all(Duration::from_millis(50)).await;
        assert!(!report.is_clean());
        assert_eq!(report.laggards.len(), 1);
        assert_eq!(report.laggards[0].name, "sleepy");
        assert!(report.laggards[0].aborted);
    }

    #[tokio::test]
    async fn register_after_close_rejected() {
        let registry = LoopRegistry::new();
        let _ = registry.shutdown_all(Duration::from_millis(10)).await;

        let late = tokio::spawn(async {});
        let err = registry
            .register("late", PHASE, LoopHandle::Async(late))
            .unwrap_err();
        assert_eq!(err.name, "late");
    }

    #[tokio::test]
    async fn empty_registry_returns_empty_report() {
        let registry = LoopRegistry::new();
        let report = registry.shutdown_all(Duration::from_millis(10)).await;
        assert!(report.is_clean());
        assert_eq!(report.loop_count(), 0);
    }

    #[tokio::test]
    async fn blocking_loop_laggard_is_reported_not_aborted() {
        let registry = LoopRegistry::new();
        // spawn_blocking with a short sleep that will exceed
        // our deadline — the report must flag it as a
        // laggard but NOT mark it aborted, because
        // `LoopHandle::Blocking` leaves it running.
        let handle = tokio::task::spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(500));
        });
        registry
            .register("blocking", PHASE, LoopHandle::Blocking(handle))
            .expect("register");

        let report = registry.shutdown_all(Duration::from_millis(30)).await;
        assert_eq!(report.laggards.len(), 1);
        assert_eq!(report.laggards[0].name, "blocking");
        assert!(!report.laggards[0].aborted);
    }

    #[tokio::test]
    async fn mixed_clean_and_laggard_accounting() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();

        let mut r1 = watch.subscribe();
        let quick = tokio::spawn(async move { r1.wait_cancelled().await });
        registry
            .register("quick", PHASE, LoopHandle::Async(quick))
            .unwrap();

        let slow = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        registry
            .register("slow", PHASE, LoopHandle::Async(slow))
            .unwrap();

        watch.signal();
        let report = registry.shutdown_all(Duration::from_millis(100)).await;
        assert_eq!(report.exited_clean, vec!["quick"]);
        assert_eq!(report.laggards.len(), 1);
        assert_eq!(report.laggards[0].name, "slow");
    }

    #[tokio::test]
    async fn strict_shutdown_keeps_event_plane_blocked_for_no_abort_laggard() {
        use tokio::sync::{Notify, oneshot};

        let watch = Arc::new(ShutdownWatch::new());
        let registry = Arc::new(LoopRegistry::new());
        let (bus, mut shutdown_handle) = super::super::ShutdownBus::new(Arc::clone(&watch));
        let guard =
            bus.register_critical_task(ShutdownPhase::DrainingEventPlane, "strict-no-abort");
        let release = Arc::new(Notify::new());
        let (started_tx, started_rx) = oneshot::channel();
        let mut loop_shutdown = watch.subscribe();
        let loop_release = Arc::clone(&release);
        registry
            .register(
                "no-abort",
                ShutdownPhase::DrainingEventPlane,
                LoopHandle::AsyncNoAbort(tokio::spawn(async move {
                    loop_shutdown.wait_cancelled().await;
                    let _ = started_tx.send(());
                    loop_release.notified().await;
                })),
            )
            .expect("register no-abort loop");

        let sequencer = bus.initiate();
        let strict_registry = Arc::clone(&registry);
        let strict_watch = Arc::clone(&watch);
        let strict = tokio::spawn(async move {
            let report = strict_registry
                .shutdown_phase_strict(
                    &strict_watch,
                    ShutdownPhase::DrainingEventPlane,
                    Duration::from_millis(20),
                )
                .await;
            guard.report_drained();
            report
        });
        started_rx
            .await
            .expect("loop should receive shutdown signal");

        let blocked = tokio::time::timeout(
            Duration::from_millis(100),
            shutdown_handle.await_phase(ShutdownPhase::PersistingWatermarks),
        )
        .await
        .is_err();
        release.notify_one();
        let report = strict.await.expect("strict shutdown task should not panic");
        sequencer
            .await
            .expect("shutdown sequencer should not panic");

        assert!(
            blocked,
            "later shutdown phases advanced before no-abort exit"
        );
        assert_eq!(report.laggards.len(), 1);
        assert!(!report.laggards[0].aborted);
    }

    #[tokio::test]
    async fn strict_shutdown_keeps_event_plane_blocked_for_blocking_laggard() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::sync::oneshot;

        let watch = Arc::new(ShutdownWatch::new());
        let registry = Arc::new(LoopRegistry::new());
        let (bus, mut shutdown_handle) = super::super::ShutdownBus::new(Arc::clone(&watch));
        let guard =
            bus.register_critical_task(ShutdownPhase::DrainingEventPlane, "strict-blocking");
        let release = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = oneshot::channel();
        let loop_shutdown = watch.subscribe();
        let loop_release = Arc::clone(&release);
        registry
            .register(
                "blocking",
                ShutdownPhase::DrainingEventPlane,
                LoopHandle::Blocking(tokio::task::spawn_blocking(move || {
                    while !loop_shutdown.is_cancelled() {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    let _ = started_tx.send(());
                    while !loop_release.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                })),
            )
            .expect("register blocking loop");

        let sequencer = bus.initiate();
        let strict_registry = Arc::clone(&registry);
        let strict_watch = Arc::clone(&watch);
        let strict = tokio::spawn(async move {
            let report = strict_registry
                .shutdown_phase_strict(
                    &strict_watch,
                    ShutdownPhase::DrainingEventPlane,
                    Duration::from_millis(20),
                )
                .await;
            guard.report_drained();
            report
        });
        started_rx
            .await
            .expect("blocking loop should receive shutdown signal");

        let blocked = tokio::time::timeout(
            Duration::from_millis(100),
            shutdown_handle.await_phase(ShutdownPhase::PersistingWatermarks),
        )
        .await
        .is_err();
        release.store(true, Ordering::SeqCst);
        let report = strict.await.expect("strict shutdown task should not panic");
        sequencer
            .await
            .expect("shutdown sequencer should not panic");

        assert!(
            blocked,
            "later shutdown phases advanced before blocking loop exit"
        );
        assert_eq!(report.laggards.len(), 1);
        assert!(!report.laggards[0].aborted);
    }

    /// A loop that overruns the budget must not make every loop joined after
    /// it a laggard: those exited on the signal, within the budget.
    #[tokio::test]
    async fn a_slow_loop_does_not_make_later_loops_laggards() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();
        registry
            .register(
                "slow",
                ShutdownPhase::DrainingControlPlane,
                LoopHandle::Async(tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                })),
            )
            .expect("register slow loop");
        let mut prompt_rx = watch.subscribe();
        registry
            .register(
                "prompt",
                ShutdownPhase::DrainingControlPlane,
                LoopHandle::Async(tokio::spawn(
                    async move { prompt_rx.wait_cancelled().await },
                )),
            )
            .expect("register prompt loop");

        let report = registry
            .shutdown_phase_strict(
                &watch,
                ShutdownPhase::DrainingControlPlane,
                Duration::from_millis(50),
            )
            .await;
        assert_eq!(report.exited_clean, vec!["prompt"]);
        let laggards: Vec<&str> = report.laggards.iter().map(|l| l.name).collect();
        assert_eq!(laggards, vec!["slow"]);
    }

    #[tokio::test]
    async fn control_plane_drain_joins_only_its_own_phase() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();

        let mut cp_rx = watch.subscribe();
        registry
            .register(
                "cp",
                ShutdownPhase::DrainingControlPlane,
                LoopHandle::Async(tokio::spawn(async move { cp_rx.wait_cancelled().await })),
            )
            .expect("register control plane loop");

        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        registry
            .register(
                "ep",
                ShutdownPhase::DrainingEventPlane,
                LoopHandle::Async(tokio::spawn(async move {
                    let _ = release_rx.await;
                })),
            )
            .expect("register event plane loop");

        let cp_report = registry
            .shutdown_phase_strict(
                &watch,
                ShutdownPhase::DrainingControlPlane,
                Duration::from_millis(200),
            )
            .await;
        assert_eq!(cp_report.exited_clean, vec!["cp"]);
        assert_eq!(
            registry.live_count_at(ShutdownPhase::DrainingControlPlane),
            0
        );

        // The Data Plane drain runs next and finds nothing to join: the
        // Control Plane loop is already gone and the Event Plane loop is
        // not its business.
        let dp_report = registry
            .shutdown_phase_strict(
                &watch,
                ShutdownPhase::DrainingDataPlane,
                Duration::from_millis(50),
            )
            .await;
        assert_eq!(dp_report.loop_count(), 0);

        release_tx.send(()).expect("release event plane loop");
        let ep_report = registry
            .shutdown_phase_strict(
                &watch,
                ShutdownPhase::DrainingEventPlane,
                Duration::from_millis(200),
            )
            .await;
        assert_eq!(ep_report.exited_clean, vec!["ep"]);
        assert_eq!(registry.live_count(), 0);
    }

    #[tokio::test]
    async fn later_phase_loop_survives_an_earlier_drain() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();

        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        registry
            .register(
                "late-phase",
                ShutdownPhase::DrainingEventPlane,
                LoopHandle::Async(tokio::spawn(async move {
                    let _ = release_rx.await;
                })),
            )
            .expect("register");

        let early = registry
            .shutdown_phase_strict(
                &watch,
                ShutdownPhase::DrainingControlPlane,
                Duration::from_millis(30),
            )
            .await;
        assert_eq!(early.loop_count(), 0, "earlier phase must join nothing");
        assert_eq!(registry.live_count_at(ShutdownPhase::DrainingEventPlane), 1);

        release_tx.send(()).expect("release loop");
        let late = registry
            .shutdown_phase_strict(
                &watch,
                ShutdownPhase::DrainingEventPlane,
                Duration::from_millis(200),
            )
            .await;
        assert_eq!(late.exited_clean, vec!["late-phase"]);
    }

    #[tokio::test]
    async fn register_after_first_phase_drain_rejected() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();

        let _ = registry
            .shutdown_phase_strict(
                &watch,
                ShutdownPhase::DrainingControlPlane,
                Duration::from_millis(10),
            )
            .await;

        let late = tokio::spawn(async {});
        let err = registry
            .register(
                "late",
                ShutdownPhase::DrainingEventPlane,
                LoopHandle::Async(late),
            )
            .unwrap_err();
        assert_eq!(err.name, "late");
    }
}
