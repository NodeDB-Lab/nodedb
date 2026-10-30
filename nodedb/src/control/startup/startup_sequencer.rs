// SPDX-License-Identifier: BUSL-1.1

//! Gate-based startup sequencer.
//!
//! [`StartupSequencer`] is the coordination hub for deterministic node
//! startup. Every subsystem that must complete before a phase transition
//! calls [`register_gate`] to obtain a [`ReadyGate`]; when it finishes its
//! work it calls [`ReadyGate::fire`]. The sequencer advances to the next
//! phase only when *all* registered gates for the current phase have fired.
//!
//! Observers — listeners, health checks, the SPSC bridge init path — hold
//! an [`Arc<StartupGate>`] and call [`StartupGate::await_phase`] to block
//! until a specific phase is reached. The gate is cancel-safe.
//!
//! On any subsystem failure (via [`ReadyGate::fail`] or an unfired drop),
//! the sequencer immediately transitions to `Failed` and every waiter wakes
//! with the stored [`StartupError`].
//!
//! [`register_gate`]: StartupSequencer::register_gate

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use super::error::StartupError;
use super::gate::{GateId, ReadyGate, SequencerSnapshot, StartupGate};
use super::phase::StartupPhase;

// ---------------------------------------------------------------------------
// SequencerState — internal, Mutex-protected
// ---------------------------------------------------------------------------

/// Mutable interior of the [`StartupSequencer`]. Held under a
/// `Mutex<SequencerState>` so gate fires from multiple subsystems
/// (potentially concurrent) are serialized.
///
/// All phase-advance logic lives here so it can be called from both
/// [`StartupSequencer`] and the gate drop impl without circular
/// dependencies.
pub struct SequencerState {
    /// Phase the sequencer is currently in.
    pub(super) current: StartupPhase,
    /// Set to `Some` on the first call to [`set_failed`], never cleared.
    pub(super) failed: Option<Arc<StartupError>>,
    /// Gates that must fire before the sequencer advances past their
    /// phase. Keyed by target phase. When all gates for `current` have
    /// fired, the entry is removed and `current` advances.
    pub(super) pending_gates: BTreeMap<StartupPhase, Vec<GateId>>,
    /// Metadata about every registered gate, keyed by `GateId`. Used to
    /// produce helpful error messages when a gate is dropped unfired.
    gate_meta: BTreeMap<GateId, GateMeta>,
    /// Monotonically increasing gate counter.
    pub(super) next_gate_id: u64,
}

/// Metadata stored for each registered gate. Fields are retained for
/// future observability (snapshots, health reports).
#[allow(dead_code)]
struct GateMeta {
    phase: StartupPhase,
    subsystem: String,
    fired: bool,
}

impl SequencerState {
    fn new() -> Self {
        Self {
            current: StartupPhase::Boot,
            failed: None,
            pending_gates: BTreeMap::new(),
            gate_meta: BTreeMap::new(),
            next_gate_id: 0,
        }
    }

    /// Register a new gate for `phase`. Returns the assigned [`GateId`].
    ///
    /// If the sequencer has already advanced past `phase`, the gate is
    /// considered immediately fired: no entry is added to
    /// `pending_gates`, and the caller's `ReadyGate::fire` becomes a
    /// no-op. This prevents late-registering subsystems from deadlocking
    /// the sequencer.
    pub(super) fn register(
        &mut self,
        phase: StartupPhase,
        subsystem: impl Into<String>,
    ) -> (GateId, bool /* already_passed */) {
        let id = GateId(self.next_gate_id);
        self.next_gate_id += 1;
        let subsystem = subsystem.into();

        // If the sequencer has already passed this phase (or failed),
        // mark the gate as pre-fired so the ReadyGate is a no-op.
        let already_passed = self.failed.is_some() || self.current > phase;
        if !already_passed {
            self.pending_gates.entry(phase).or_default().push(id);
        }
        self.gate_meta.insert(
            id,
            GateMeta {
                phase,
                subsystem,
                fired: already_passed,
            },
        );
        (id, already_passed)
    }

    /// Mark gate `id` as fired. If all gates for `phase` have now fired,
    /// advance `current` (possibly in a chain if subsequent phases have
    /// no pending gates either).
    pub(super) fn fire_gate(
        &mut self,
        id: GateId,
        phase: StartupPhase,
        tx: &Arc<watch::Sender<SequencerSnapshot>>,
    ) {
        // Ignore if already in a terminal state.
        if self.failed.is_some() {
            return;
        }

        // Mark meta as fired.
        if let Some(meta) = self.gate_meta.get_mut(&id) {
            meta.fired = true;
        }

        // Remove this gate from pending set for its phase.
        if let Some(gates) = self.pending_gates.get_mut(&phase) {
            gates.retain(|g| g != &id);
            if gates.is_empty() {
                self.pending_gates.remove(&phase);
            }
        }

        // Try to advance: while the next phase either (a) has no pending
        // gates or (b) is not the current+1, keep advancing.
        self.try_advance(tx);
    }

    /// Attempt to advance `current` as far as gates allow. Called after
    /// every `fire_gate` and after initial construction.
    fn try_advance(&mut self, tx: &Arc<watch::Sender<SequencerSnapshot>>) {
        loop {
            // If in a terminal state, stop.
            if self.failed.is_some() {
                return;
            }
            if self.current == StartupPhase::Serving {
                return;
            }
            let Some(next) = self.current.next() else {
                return;
            };
            if next == StartupPhase::Failed {
                return;
            }
            // Only advance if there are no pending gates blocking `next`.
            if self.pending_gates.contains_key(&next) {
                // Gates still pending for the next phase — wait.
                return;
            }
            // No gates registered (or all already fired) for `next`.
            // Check if `current` itself still has pending gates that must
            // fire first (gates registered for `current`). If they have
            // all fired (or none were registered), advance.
            if self.pending_gates.contains_key(&self.current) {
                // Gates still pending for the CURRENT phase.
                return;
            }
            self.current = next;
            tracing::info!(phase = ?next, "StartupSequencer phase advanced");
            tx.send_replace(SequencerSnapshot {
                phase: next,
                failed: None,
            });
        }
    }

    /// Transition to `Failed` with the given error. Idempotent: if
    /// already failed, the first error is preserved.
    pub(super) fn set_failed(
        &mut self,
        err: StartupError,
        tx: &Arc<watch::Sender<SequencerSnapshot>>,
    ) {
        if self.failed.is_some() {
            // Already failed — preserve the first error.
            return;
        }
        let err_arc = Arc::new(err);
        self.failed = Some(Arc::clone(&err_arc));
        tracing::error!(error = %err_arc, "StartupSequencer transitioned to Failed");
        tx.send_replace(SequencerSnapshot {
            phase: self.current,
            failed: Some(err_arc),
        });
    }
}

// ---------------------------------------------------------------------------
// StartupSequencer
// ---------------------------------------------------------------------------

/// Gate-based startup sequencer.
///
/// Construct with [`StartupSequencer::new`], which returns the sequencer
/// together with an [`Arc<StartupGate>`] suitable for sharing with any
/// observer. Register subsystem gates with [`register_gate`]; each
/// subsystem fires its gate when ready. The sequencer advances
/// automatically when all gates for a phase have fired.
///
/// [`register_gate`]: StartupSequencer::register_gate
pub struct StartupSequencer {
    state: Arc<Mutex<SequencerState>>,
    phase_tx: Arc<watch::Sender<SequencerSnapshot>>,
}

impl StartupSequencer {
    /// Create a new sequencer at `StartupPhase::Boot`.
    ///
    /// Returns the sequencer and a shared [`StartupGate`] handle.
    /// Clone the gate freely — all clones observe the same channel.
    pub fn new() -> (Self, Arc<StartupGate>) {
        let (tx, rx) = watch::channel(SequencerSnapshot {
            phase: StartupPhase::Boot,
            failed: None,
        });
        let phase_tx = Arc::new(tx);
        let state = Arc::new(Mutex::new(SequencerState::new()));
        let gate = Arc::new(StartupGate::new(rx));
        let sequencer = Self { state, phase_tx };
        (sequencer, gate)
    }

    /// Register a gate that must fire before the sequencer can advance
    /// past `required_at`.
    ///
    /// If the sequencer has already advanced past `required_at` (e.g.
    /// a late-registering subsystem), the returned `ReadyGate` is
    /// pre-fired: calling `fire()` on it is a no-op and drop does not
    /// trigger auto-fail.
    ///
    /// # Arguments
    ///
    /// - `required_at` — the phase this gate blocks. The sequencer will
    ///   not leave this phase until the gate fires (or fails).
    /// - `subsystem` — human-readable name used in error messages and
    ///   logs (e.g. `"raft"`, `"catalog-hydration"`).
    pub fn register_gate(
        &self,
        required_at: StartupPhase,
        subsystem: impl Into<String>,
    ) -> ReadyGate {
        let subsystem: String = subsystem.into();
        let mut state = lock_state(&self.state);
        let (id, already_passed) = state.register(required_at, subsystem.clone());

        ReadyGate {
            id,
            phase: required_at,
            subsystem,
            sequencer: Arc::downgrade(&self.state),
            fired: std::sync::atomic::AtomicBool::new(already_passed),
            phase_tx: Arc::clone(&self.phase_tx),
        }
    }

    /// Immediately transition the sequencer to `Failed` with the given
    /// error. Useful when the startup driver detects an error outside of
    /// any registered gate (e.g. a fatal config parse error before any
    /// subsystem has been registered).
    ///
    /// Idempotent: the first call wins; subsequent calls are no-ops.
    pub fn fail(&self, err: StartupError) {
        let mut state = lock_state(&self.state);
        state.set_failed(err, &self.phase_tx);
    }

    /// Lightweight snapshot of the current sequencer state.
    pub fn current(&self) -> SequencerSnapshot {
        self.phase_tx.borrow().clone()
    }
}

impl Default for StartupSequencer {
    fn default() -> Self {
        let (s, _) = Self::new();
        s
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn lock_state(mu: &Mutex<SequencerState>) -> std::sync::MutexGuard<'_, SequencerState> {
    match mu.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!(
                "StartupSequencer state mutex poisoned — recovering guard. \
                 A previous holder panicked; this is a bug."
            );
            poisoned.into_inner()
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ── Helpers ─────────────────────────────────────────────────────────────

    fn make() -> (StartupSequencer, Arc<StartupGate>) {
        StartupSequencer::new()
    }

    // ── 1. Phase ordering ───────────────────────────────────────────────────

    /// Register gates across three consecutive phases plus a sentinel gate
    /// at the next phase to stop the chain, fire them in order, and assert
    /// that `current_phase()` advances in lock-step.
    ///
    /// Without the sentinel gate the sequencer would advance all the way to
    /// `Serving` after the last registered gate fires, because no
    /// pending gates block the remaining phases. The sentinel makes the
    /// stopping point explicit and deterministic.
    #[tokio::test]
    async fn phase_ordering_fires_in_lock_step() {
        let (seq, gate) = make();

        let g1 = seq.register_gate(StartupPhase::WalRecovery, "wal");
        let g2 = seq.register_gate(StartupPhase::ClusterCatalogOpen, "catalog");
        let g3 = seq.register_gate(StartupPhase::RaftMetadataReplay, "raft");
        // Sentinel: blocks SchemaCacheWarmup so the sequencer stops at
        // RaftMetadataReplay after g3 fires.
        let sentinel = seq.register_gate(StartupPhase::SchemaCacheWarmup, "sentinel");

        // Sequencer is still at Boot because gates are pending.
        assert_eq!(gate.current_phase(), StartupPhase::Boot);

        g1.fire();
        // WalRecovery gate fired; sequencer should advance to WalRecovery
        // then stop at ClusterCatalogOpen (gate pending).
        assert_eq!(gate.current_phase(), StartupPhase::WalRecovery);

        g2.fire();
        // `DataPlaneReplay` has no gate here, so the sequencer passes it and
        // stops before `RaftMetadataReplay`, whose gate is pending.
        assert_eq!(gate.current_phase(), StartupPhase::DataPlaneReplay);

        g3.fire();
        // After g3 fires, sequencer advances to RaftMetadataReplay and then
        // would continue — but the sentinel gate blocks SchemaCacheWarmup, so
        // it stops at RaftMetadataReplay.
        assert_eq!(gate.current_phase(), StartupPhase::RaftMetadataReplay);

        // Clean up: fire the sentinel so its Drop doesn't trigger auto-fail.
        sentinel.fire();
    }

    // ── 2. Failure propagation ───────────────────────────────────────────────

    /// Two concurrent waiters on GatewayEnable should both wake with an
    /// error when `fail()` is called.
    #[tokio::test]
    async fn failure_wakes_all_waiters() {
        let (seq, gate) = make();

        let g1 = gate.clone();
        let g2 = gate.clone();

        let h1 = tokio::spawn(async move { g1.await_phase(StartupPhase::GatewayEnable).await });
        let h2 = tokio::spawn(async move { g2.await_phase(StartupPhase::GatewayEnable).await });

        // Give tasks time to start waiting.
        tokio::time::sleep(Duration::from_millis(5)).await;

        seq.fail(StartupError::SubsystemFailed {
            phase: StartupPhase::Boot,
            subsystem: "test".into(),
            reason: "intentional test failure".into(),
        });

        let r1 = tokio::time::timeout(Duration::from_millis(100), h1)
            .await
            .expect("waiter 1 timed out")
            .expect("task panicked");
        let r2 = tokio::time::timeout(Duration::from_millis(100), h2)
            .await
            .expect("waiter 2 timed out")
            .expect("task panicked");

        assert!(r1.is_err(), "waiter 1 should have received an error");
        assert!(r2.is_err(), "waiter 2 should have received an error");

        // Both errors should be identical (same Arc contents).
        let e1 = r1.unwrap_err();
        let e2 = r2.unwrap_err();
        assert_eq!(e1.to_string(), e2.to_string());
    }

    // ── 3. Idempotent double-fire ────────────────────────────────────────────

    /// Firing the same gate twice must not panic, double-advance, or
    /// produce any error.
    #[test]
    fn idempotent_double_fire() {
        let (seq, gate) = make();
        let g = seq.register_gate(StartupPhase::WalRecovery, "wal");

        g.fire();
        let phase_after_first = gate.current_phase();

        // Second fire — must be a no-op.
        g.fire();
        assert_eq!(
            gate.current_phase(),
            phase_after_first,
            "double-fire must not advance the phase again"
        );
    }

    // ── 4. Late registration ─────────────────────────────────────────────────

    /// A gate registered for a phase the sequencer has already passed
    /// should be considered immediately fired. Calling `fire()` on it is a
    /// no-op; dropping it without firing must NOT trigger auto-fail.
    ///
    /// A sentinel gate at `ClusterCatalogOpen` ensures the sequencer stops
    /// at `WalRecovery` after `g` fires, so the assertion is deterministic.
    #[test]
    fn late_registration_is_pre_fired() {
        let (seq, gate) = make();

        let g = seq.register_gate(StartupPhase::WalRecovery, "wal");
        // Sentinel stops the sequencer at WalRecovery after g fires.
        let sentinel = seq.register_gate(StartupPhase::ClusterCatalogOpen, "sentinel");

        // Register and fire a gate for WalRecovery so the sequencer advances.
        g.fire();
        assert_eq!(gate.current_phase(), StartupPhase::WalRecovery);

        // Now register a gate for Boot — already passed.
        let late_gate = seq.register_gate(StartupPhase::Boot, "boot-late");

        // Drop without firing — must NOT trigger auto-fail.
        drop(late_gate);

        // Sequencer must remain healthy.
        assert!(
            gate.is_failed().is_none(),
            "late gate drop should not fail the sequencer"
        );

        // Clean up sentinel.
        sentinel.fire();
    }

    // ── 5. Drop-without-fire auto-fail ───────────────────────────────────────

    /// Dropping a ReadyGate without firing it should automatically
    /// transition the sequencer to Failed with a descriptive error.
    #[tokio::test]
    async fn drop_without_fire_triggers_auto_fail() {
        let (seq, gate) = make();

        // Register a gate but never fire it.
        let g = seq.register_gate(StartupPhase::WalRecovery, "wal-never-fires");
        drop(g);

        // Sequencer must be in Failed state.
        let err = gate.is_failed().expect("sequencer should have failed");
        assert!(
            err.to_string().contains("wal-never-fires"),
            "error message must name the dropped subsystem: {err}"
        );
        assert!(
            matches!(*err, StartupError::GateDroppedWithoutFire { .. }),
            "wrong error variant: {err:?}"
        );

        // await_phase must return Err immediately.
        let result = tokio::time::timeout(
            Duration::from_millis(10),
            gate.await_phase(StartupPhase::GatewayEnable),
        )
        .await
        .expect("await_phase should not block after failure");
        assert!(
            result.is_err(),
            "await_phase should return Err after failure"
        );
    }

    // ── 6. Matchstick: StartupPhase::next() is exhaustive ───────────────────

    /// Every non-terminal phase must return `Some(_)` from `next()`, and
    /// the chain must terminate exactly at `Serving`. If a new
    /// variant is added without a branch in `next()`, the compiler rejects
    /// the match — catching the omission at compile time.
    #[test]
    fn phase_next_chain_is_exhaustive_and_monotonic() {
        // Walk the full chain and assert monotonic ordering.
        let mut prev = StartupPhase::Boot;
        let mut cur = StartupPhase::Boot;
        let mut count = 0;
        while let Some(next) = cur.next() {
            if next == StartupPhase::Failed {
                break;
            }
            assert!(next > prev, "next() is not monotonic: {prev:?} -> {next:?}");
            prev = cur;
            cur = next;
            count += 1;
            assert!(count < 64, "phase chain appears infinite");
        }
        assert_eq!(
            cur,
            StartupPhase::Serving,
            "chain must terminate at Serving"
        );

        // Exhaustive match — compile error if a variant is added without
        // being handled here.
        let _: Option<StartupPhase> = match StartupPhase::Boot {
            StartupPhase::Boot => StartupPhase::Boot.next(),
            StartupPhase::WalRecovery => StartupPhase::WalRecovery.next(),
            StartupPhase::ClusterCatalogOpen => StartupPhase::ClusterCatalogOpen.next(),
            StartupPhase::DataPlaneReplay => StartupPhase::DataPlaneReplay.next(),
            StartupPhase::RaftMetadataReplay => StartupPhase::RaftMetadataReplay.next(),
            StartupPhase::SchemaCacheWarmup => StartupPhase::SchemaCacheWarmup.next(),
            StartupPhase::CatalogSanityCheck => StartupPhase::CatalogSanityCheck.next(),
            StartupPhase::DataGroupsReplay => StartupPhase::DataGroupsReplay.next(),
            StartupPhase::TransportBind => StartupPhase::TransportBind.next(),
            StartupPhase::WarmPeers => StartupPhase::WarmPeers.next(),
            StartupPhase::HealthLoopStart => StartupPhase::HealthLoopStart.next(),
            StartupPhase::GatewayEnable => StartupPhase::GatewayEnable.next(),
            StartupPhase::Serving => StartupPhase::Serving.next(),
            StartupPhase::Failed => StartupPhase::Failed.next(),
        };
    }

    // ── Bonus: multiple gates per phase ──────────────────────────────────────

    /// Two gates registered for the same phase — sequencer must NOT
    /// advance past Boot until both have fired. A sentinel gate blocks
    /// the phase after WalRecovery so the final assertion is deterministic.
    #[test]
    fn two_gates_same_phase_require_both() {
        let (seq, gate) = make();

        let g1 = seq.register_gate(StartupPhase::WalRecovery, "wal-a");
        let g2 = seq.register_gate(StartupPhase::WalRecovery, "wal-b");
        // Sentinel blocks ClusterCatalogOpen so the sequencer stops at
        // WalRecovery after both WalRecovery gates fire.
        let sentinel = seq.register_gate(StartupPhase::ClusterCatalogOpen, "sentinel");

        // Only one fired — must not advance past Boot.
        g1.fire();
        assert_eq!(gate.current_phase(), StartupPhase::Boot);

        // Second fired — now advances to WalRecovery and stops at
        // ClusterCatalogOpen (sentinel pending).
        g2.fire();
        assert_eq!(gate.current_phase(), StartupPhase::WalRecovery);

        sentinel.fire();
    }

    // ── Bonus: no gates registered advances through unblocked phases ─────────

    /// If no gates are registered for any phase, the sequencer should
    /// remain at Boot (it only advances when gates fire).
    #[test]
    fn no_gates_stays_at_boot() {
        let (_seq, gate) = make();
        // No gates registered — sequencer stays at Boot (nothing fires it).
        assert_eq!(gate.current_phase(), StartupPhase::Boot);
    }

    // ── Bonus: fail() is idempotent ──────────────────────────────────────────

    /// Two calls to `fail()` preserve the first error.
    #[tokio::test]
    async fn fail_is_idempotent() {
        let (seq, gate) = make();

        let err1 = StartupError::SubsystemFailed {
            phase: StartupPhase::Boot,
            subsystem: "first".into(),
            reason: "first error".into(),
        };
        let err2 = StartupError::SubsystemFailed {
            phase: StartupPhase::Boot,
            subsystem: "second".into(),
            reason: "second error".into(),
        };

        seq.fail(err1);
        seq.fail(err2);

        let stored = gate.is_failed().expect("should be failed");
        assert!(
            stored.to_string().contains("first"),
            "first error should be preserved: {stored}"
        );
    }
}
