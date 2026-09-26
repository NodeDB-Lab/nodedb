// SPDX-License-Identifier: BUSL-1.1

//! New-txn processing, dependent-read barrier setup, and txn-completion
//! bookkeeping for the Calvin scheduler.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use nodedb_cluster::calvin::types::{
    LockKeyWire, ReleaseReason, SchedulerInput, SequencedTxn, TxnIdWire,
};

use super::super::barrier::PendingDependentBarrier;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::{AcquireOutcome, TxnId};

/// Epochs a read reservation may live before the scheduler reaps it as orphaned.
/// At the default 20ms epoch tick this is ~5s of wall-clock — far longer than any
/// real think-time between reservation install and commit, yet short enough that a
/// crashed coordinator's reservation is reclaimed promptly. Expressed in epochs
/// (logical, replicated), NOT seconds — deterministic across replicas.
const LEASE_EPOCHS: u64 = 250;

impl Scheduler {
    /// Route a fanned-out scheduler input to its handler.
    ///
    /// Every replica applies the sequencer-ordered inputs in identical order, so
    /// the resulting `process`/`acquire_shared`/`release` calls are identical
    /// across replicas — the determinism contract. No wall clock or local state
    /// enters the routing decision.
    ///
    /// Before dispatching, advances `max_input_epoch` on epoch increase and reaps
    /// any shared reservation whose owner epoch has fallen behind the lease
    /// window (`max_input_epoch - LEASE_EPOCHS`) — a deterministic function of
    /// replicated input order, so every replica reaps identically.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn process_scheduler_input(
        &mut self,
        input: SchedulerInput,
    ) {
        let epoch = Self::input_epoch(&input);
        if epoch > self.max_input_epoch {
            self.max_input_epoch = epoch;
            let threshold = self.max_input_epoch.saturating_sub(LEASE_EPOCHS);
            let promoted = {
                let mut lm = self.lock_manager.lock().unwrap_or_else(|p| p.into_inner());
                lm.reap_expired_shared(threshold)
            };
            self.dispatch_promoted(promoted);
        }

        match input {
            SchedulerInput::Txn(txn) => self.process_new_txn(txn),
            SchedulerInput::Reserve { owner, key } => self.install_reservation(owner, key),
            SchedulerInput::Release { owner, reason } => self.release_reservation(owner, reason),
            SchedulerInput::CutMarker { hlc } => self.receive_cut_marker(hlc),
        }
    }

    /// The replicated epoch an input is stamped with — the monotonic logical
    /// clock the lease reap advances on. A cut marker carries no epoch, so it
    /// reports `0`, which never advances the clock.
    fn input_epoch(input: &SchedulerInput) -> u64 {
        match input {
            SchedulerInput::Txn(txn) => txn.epoch,
            SchedulerInput::Reserve { owner, .. } => owner.epoch,
            SchedulerInput::Release { owner, .. } => owner.epoch,
            SchedulerInput::CutMarker { .. } => 0,
        }
    }

    /// Install a SHARED reservation on `key` for interactive txn `owner`.
    ///
    /// The `AcquireOutcome` is intentionally discarded: promotion of a
    /// reservation that blocks behind an exclusive holder is handled by the
    /// existing FIFO waiter mechanics, and lock-promotion wiring for reservations
    /// lands in a later change.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn install_reservation(
        &mut self,
        owner: TxnIdWire,
        key: LockKeyWire,
    ) {
        let txn_id: TxnId = owner.into();
        let lock_key = super::super::helpers::decode_lock_key(&key);
        let mut lm = self.lock_manager.lock().unwrap_or_else(|p| p.into_inner());
        let _outcome = lm.acquire_shared(txn_id, lock_key);
    }

    /// Release ALL shared reservations held by `owner`, promoting any waiters that
    /// become ready — the same promotion -> dispatch path `on_txn_complete` uses.
    ///
    /// `reason` is carried for observability only; the release is identical for
    /// every reason.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn release_reservation(
        &mut self,
        owner: TxnIdWire,
        reason: ReleaseReason,
    ) {
        let txn_id: TxnId = owner.into();
        tracing::debug!(
            vshard = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            ?reason,
            "calvin: releasing shared reservation"
        );
        let newly_unblocked = {
            let lm = Arc::clone(&self.lock_manager);
            let mut guard = lm.lock().unwrap_or_else(|p| p.into_inner());
            guard.release(txn_id)
        };
        self.dispatch_promoted(newly_unblocked);
    }

    /// Process a newly arrived sequenced transaction.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn process_new_txn(
        &mut self,
        txn: SequencedTxn,
    ) {
        let txn_id = TxnId::new(txn.epoch, txn.position);
        // Lock-table owner: distinct from the apply-slot only when a producer
        // set `lock_owner` (e.g. a read reservation). Defaults to the apply-slot
        // id, so today's behavior is unchanged.
        let lock_owner = txn.lock_owner.map(TxnId::from).unwrap_or(txn_id);

        // Record this delivery with the sequencer's per-`(epoch, vShard)` count.
        // A count >= 1 is the authoritative expected total (every position of the
        // epoch carries the same value, so this is idempotent); a count of 0
        // marks a batch encoded before the count field existed, and the position
        // is tracked so the epoch can fold via in-order delivery instead.
        self.applied
            .note_expected(txn.epoch, txn.position, txn.epoch_vshard_txn_count);

        // Exact per-position skip: never re-apply a position that already
        // committed (its CalvinApplied marker is durable), and never re-run a
        // whole epoch that has fully folded into the watermark. Re-running an
        // applied position would re-fire its side effects — this gate IS the
        // exactly-once mechanism. Skipping a whole epoch on its first completing
        // position (the previous per-epoch gate) dropped every other position of
        // that epoch across a restart: a torn transaction.
        if self.applied.is_applied(txn.epoch, txn.position) {
            // Learning the count for an already-applied position may complete a
            // historical epoch's applied set (during restart re-fan-out), folding
            // it into the watermark and pruning its tail — bounding memory.
            if let Some(watermark) = self.applied.advance() {
                self.publish_watermark(watermark);
            }
            return;
        }

        // In-flight guard (catch-up-replay idempotency). Skip a txn that is
        // already in-flight on this scheduler — dispatched-and-awaiting-response
        // (`pending`), blocked on locks (`blocked`), or parked on a dependent-read
        // barrier (`dependent_barrier`). Re-running any of these would dispatch a
        // SECOND copy and double-execute the transaction.
        //
        // This is a strict NO-OP for LIVE inputs: the sequencer delivers each
        // `(epoch, position)` to a given vShard exactly once, so a live txn can
        // never already be in-flight on its first arrival. The guard fires ONLY
        // when the catch-up drain replays a committed log range that overlaps an
        // input already delivered live and still in-flight — the exact overlap
        // the drain cannot avoid (it replays from the earliest dropped index
        // forward, which may re-cover inputs that were NOT dropped). Reserve /
        // Release replay is already idempotent in the lock manager, so only Txn
        // needs this guard.
        if self.pending.contains_key(&txn_id)
            || self.blocked.contains_key(&lock_owner)
            || self.dependent_barrier.contains_key(&txn_id)
        {
            return;
        }

        let keys = super::super::helpers::expand_rw_set(&txn);
        let keys_count = keys.len();
        let _acquire_span = tracing::info_span!(
            "scheduler_acquire_locks",
            epoch = txn.epoch,
            position = txn.position,
            vshard = self.vshard_id,
            keys_count,
        )
        .entered();
        let outcome = {
            let mut lm = self.lock_manager.lock().unwrap_or_else(|p| p.into_inner());
            lm.acquire(lock_owner, keys.clone())
        };

        match outcome {
            AcquireOutcome::Ready => {
                self.dispatch_or_barrier(txn, txn_id, lock_owner);
            }
            AcquireOutcome::Blocked => {
                self.metrics.record_blocked();
                self.blocked.insert(
                    lock_owner,
                    super::super::types::BlockedTxn {
                        txn,
                        keys,
                        // no-determinism: blocked_at is scheduler observability, not Calvin WAL data
                        blocked_at: Instant::now(),
                    },
                );
            }
        }
    }

    /// Route a ready txn to either a static dispatch or a dependent barrier.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_or_barrier(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
    ) {
        let is_dependent = txn.tx_class.dependent_reads.is_some();
        if is_dependent {
            self.insert_dependent_barrier(txn, txn_id, lock_owner);
        } else {
            self.dispatch_txn(txn, txn_id, lock_owner);
        }
    }

    /// Insert a dependent-read barrier for an active vshard.
    fn insert_dependent_barrier(&mut self, txn: SequencedTxn, txn_id: TxnId, lock_owner: TxnId) {
        let spec = match &txn.tx_class.dependent_reads {
            Some(s) => s,
            None => {
                // Shouldn't happen; fall through to static dispatch.
                self.dispatch_txn(txn, txn_id, lock_owner);
                return;
            }
        };

        let waiting_for: std::collections::BTreeSet<u32> =
            spec.passive_reads.keys().copied().collect();
        // no-determinism: passive barrier timeout is scheduler observability, not Calvin WAL data
        let timeout_at = Instant::now() + self.config.passive_timeout();

        let barrier = PendingDependentBarrier {
            txn,
            lock_owner,
            waiting_for,
            received: BTreeMap::new(),
            timeout_at,
        };

        self.dependent_barrier.insert(txn_id, barrier);
    }

    /// Complete an in-flight txn (success or infrastructure error).
    ///
    /// Releases the lock-table owner recorded in its `pending` entry. A txn
    /// with no `pending` entry has already completed, so this logs and
    /// releases nothing. A txn that fails before it enters `pending` uses
    /// [`Self::on_unpending_txn_complete`] instead.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn on_txn_complete(
        &mut self,
        txn_id: TxnId,
    ) {
        let Some(pending) = self.pending.remove(&txn_id) else {
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: completion for a txn with no pending entry; nothing to release"
            );
            return;
        };
        // The flush applied the txn's redo record.
        if let Some(records) = pending.redo_records {
            records.settle();
        }
        self.release_and_mark_applied(txn_id, pending.lock_owner);
    }

    /// Complete a txn that failed before it entered `pending`, releasing the
    /// locks held under `lock_owner`.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn on_unpending_txn_complete(
        &mut self,
        txn_id: TxnId,
        lock_owner: TxnId,
    ) {
        self.release_and_mark_applied(txn_id, lock_owner);
    }

    /// Release `lock_owner`'s locks, dispatch the promoted waiters, and mark
    /// `txn_id`'s position applied.
    fn release_and_mark_applied(&mut self, txn_id: TxnId, lock_owner: TxnId) {
        // Release this txn's locks. `release` promotes any waiter queued behind
        // each freed key to holder (moving it pending -> held) and returns the
        // fully-promoted ids. Those ids are already holders in the table the
        // moment `release` returns, so a concurrent gate probe on the same key
        // sees the promoted holder and cannot steal it — the subsequent dispatch
        // is safe outside this critical section.
        let newly_unblocked = {
            let lm = Arc::clone(&self.lock_manager);
            let mut guard = lm.lock().unwrap_or_else(|p| p.into_inner());
            guard.release(lock_owner)
        };
        self.dispatch_promoted(newly_unblocked);

        // Mark this EXACT position applied. The watermark folds an epoch only
        // once ALL of its positions for this vShard have terminally completed,
        // so any advertised watermark reflects a FULLY-applied epoch — the value
        // `BEGIN` needs for a torn-free cross-shard snapshot anchor.
        let folded = self.applied.mark_applied(txn_id.epoch, txn_id.position);
        self.applied_mirror.mark(txn_id.epoch, txn_id.position);
        if let Some(watermark) = folded {
            self.publish_watermark(watermark);
        }
    }

    /// Dispatch transactions that a `LockManager::release` promoted to holder.
    ///
    /// Shared by both promotion entry points:
    /// - `on_txn_complete`, where THIS scheduler released a completed txn's locks;
    /// - the `promotion_rx` `select!` arm, where a Control-Plane fast-path
    ///   [`WriteAdmissionGuard`] drop released an uncontended key that one of this
    ///   scheduler's blocked txns had queued behind.
    ///
    /// In both cases `release` has already installed each promoted txn as holder
    /// on all its keys and moved it into `held_locks`. This method confirms
    /// readiness (an idempotent re-acquire — a no-op that also guards against a
    /// promoted id that is somehow not yet ready), clears the `blocked` entry,
    /// records the wait, and routes the txn to static dispatch or a dependent
    /// barrier. Collect-under-guard then dispatch-after-drop keeps the lock-table
    /// critical section minimal and re-entrancy-safe (`dispatch_or_barrier` does
    /// not touch the lock table).
    ///
    /// [`WriteAdmissionGuard`]: crate::control::server::shared::write_admission::WriteAdmissionGuard
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_promoted(
        &mut self,
        promoted: Vec<TxnId>,
    ) {
        if promoted.is_empty() {
            return;
        }

        let lm = Arc::clone(&self.lock_manager);
        let mut to_dispatch: Vec<(SequencedTxn, TxnId)> = Vec::new();
        {
            let mut guard = lm.lock().unwrap_or_else(|p| p.into_inner());
            for waiter_id in promoted {
                let Some(blocked) = self.blocked.get(&waiter_id) else {
                    // A promotion can only name a waiter this scheduler enqueued
                    // (its key set lives in `blocked`), so a miss should not
                    // happen. Skip defensively rather than panic — the txn holds
                    // no dispatch state here to act on.
                    tracing::debug!(
                        vshard = self.vshard_id,
                        epoch = waiter_id.epoch,
                        position = waiter_id.position,
                        "calvin: promoted txn absent from blocked map; skipping dispatch"
                    );
                    continue;
                };
                if !guard.is_ready(waiter_id, &blocked.keys) {
                    continue;
                }
                let keys = blocked.keys.clone();
                let outcome = guard.acquire(waiter_id, keys);
                debug_assert_eq!(
                    outcome,
                    AcquireOutcome::Ready,
                    "is_ready returned true but acquire returned Blocked"
                );

                if let Some(blocked_txn) = self.blocked.remove(&waiter_id) {
                    let wait_ms = blocked_txn.blocked_at.elapsed().as_millis() as u64;
                    self.metrics.record_lock_wait_ms(wait_ms);
                    to_dispatch.push((blocked_txn.txn, waiter_id));
                }
            }
        }

        for (txn, waiter_id) in to_dispatch {
            // `waiter_id` is the lock-table owner — a reservation id when the txn
            // holds a read reservation, otherwise the apply-slot itself. The
            // watermark / completion apply-slot is ALWAYS the txn's own
            // `(epoch, position)`; recover it here so a reservation-owned commit
            // dispatches under the correct apply-slot while releasing the lock
            // under `waiter_id`.
            let apply_slot = TxnId::new(txn.epoch, txn.position);
            self.dispatch_or_barrier(txn, apply_slot, waiter_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;
    use nodedb_types::TenantId;

    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        await_data_plane_request, build_test_scheduler, build_test_scheduler_with_data_side,
        fill_tenant_inflight, make_sequenced_txn, make_validate_only_txn, release_filler,
        spawn_scheduler_loop, test_coll_vshard,
    };

    /// A refused stage dispatch leaves the txn unapplied and publishes no
    /// watermark for its epoch.
    #[tokio::test]
    async fn stage_dispatch_refused_at_capacity_leaves_txn_unapplied() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        let watermark_before = shared.calvin.last_applied_epoch.load(Ordering::Acquire);

        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(3, 0)));

        assert!(
            !scheduler.applied.is_applied(3, 0),
            "a capacity refusal must not mark the position applied"
        );
        assert_eq!(
            shared.calvin.last_applied_epoch.load(Ordering::Acquire),
            watermark_before,
            "a capacity refusal must not publish a watermark for the txn's epoch"
        );
    }

    /// A refused stage dispatch keeps the txn's key locks: a later txn on the
    /// same key queues behind it.
    #[tokio::test]
    async fn stage_dispatch_refused_at_capacity_keeps_key_locks() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));

        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(3, 0)));
        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(4, 0)));

        assert!(
            scheduler.blocked.contains_key(&TxnId::new(4, 0)),
            "a txn on the same key must block behind the refused txn's held locks"
        );
    }

    /// A refused stage dispatch leaves no request-tracker entry behind.
    #[tokio::test]
    async fn stage_dispatch_refused_at_capacity_leaves_no_tracker_entry() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        let tracked_before = shared.tracker.in_flight();

        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(3, 0)));

        assert_eq!(
            shared.tracker.in_flight(),
            tracked_before,
            "a refused dispatch must not leave a registered tracker entry"
        );
    }

    /// Once a Data Plane response frees tenant capacity, the refused txn's
    /// stage request reaches the Data Plane.
    #[tokio::test]
    async fn stage_dispatch_refused_at_capacity_is_retried_after_capacity_frees() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        let fillers = fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));

        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(3, 0)));
        let running = spawn_scheduler_loop(scheduler);
        release_filler(&shared, &mut data_side, fillers[0]);

        let arrived = await_data_plane_request(&mut data_side, |plan| {
            matches!(
                plan,
                PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
                    epoch: 3,
                    position: 0,
                    ..
                })
            )
        })
        .await;
        running.stop().await;

        assert!(
            arrived,
            "the refused stage request must reach the Data Plane once capacity frees"
        );
    }

    #[tokio::test]
    async fn in_flight_guard_skips_replayed_txn_already_in_flight() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        let txn = make_sequenced_txn(5, 0);
        let txn_id = TxnId::new(5, 0);

        // Stand in for the LIVE delivery: the txn is already in-flight (here,
        // blocked on locks — keyed by its lock_owner == apply-slot). Inserting the
        // map entry directly avoids driving the full dispatch machinery.
        scheduler.blocked.insert(
            txn_id,
            crate::control::cluster::calvin::scheduler::driver::types::BlockedTxn {
                txn: txn.clone(),
                keys: BTreeSet::new(),
                // no-determinism: test-only blocked_at timestamp for a fabricated BlockedTxn fixture.
                blocked_at: Instant::now(),
            },
        );

        let dispatched_before = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);

        // Now REPLAY the same (epoch, position) through the live processing path —
        // exactly what `drain_catch_up` does for a dropped-then-recovered input that
        // overlaps an already-in-flight live one. The in-flight guard must turn it
        // into a no-op: no second dispatch, no duplicate in-flight entry.
        scheduler.process_scheduler_input(SchedulerInput::Txn(txn));

        let dispatched_after = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);
        assert_eq!(
            dispatched_before, dispatched_after,
            "in-flight guard must prevent a second dispatch of an already-in-flight txn"
        );
        assert_eq!(
            scheduler.blocked.len(),
            1,
            "guard must not add a duplicate in-flight entry"
        );
        assert!(
            scheduler.pending.is_empty(),
            "guard must not have dispatched (no pending entry created)"
        );
    }
}
