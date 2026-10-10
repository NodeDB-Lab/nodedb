// SPDX-License-Identifier: BUSL-1.1

//! The scheduler event loop: per-pass sweeps and the `select!` over every input.

use std::sync::Arc;

use tracing::info;

use super::catch_up::CatchUpDrain;
use super::scheduler::Scheduler;
use crate::control::shutdown::ShutdownReceiver;

/// How often a new data-group leader checks whether its term's no-op
/// applied, while its stage gate is closed.
const ROLE_POLL: std::time::Duration = std::time::Duration::from_millis(5);

impl Scheduler {
    /// Run the scheduler event loop until shutdown is signaled.
    pub async fn run(mut self, mut shutdown: ShutdownReceiver) {
        info!(
            vshard_id = self.vshard_id,
            fully_applied_epoch = self.applied.fully_applied_epoch(),
            rebuild_target_epoch = self.rebuild_target_epoch,
            "calvin scheduler starting"
        );

        // Low-frequency liveness timer so the top-of-loop stall/barrier sweeps
        // run even on an otherwise-idle vShard. Without it, a dropped verdict
        // push plus zero further events for this vShard will leave a parked txn
        // never re-probing the durable verdict. A fraction of the stall-warn
        // window re-probes well within it.
        let mut stall_tick = tokio::time::interval(self.config.verdict_stall_warn() / 4);
        stall_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Woken when a routed Data-Plane response frees dispatcher capacity.
        let capacity_freed = Arc::clone(&self.capacity_freed);
        // Woken when the data-group apply loop concludes a stamped redo.
        let inbox = Arc::clone(self.inbox.inbox());
        // Woken when the data-group apply loop buffers a barrier entry.
        let read_results = Arc::clone(&self.read_results);
        // Woken when a cut's barrier is proposed or applied on this node.
        let shared = Arc::clone(&self.shared);
        // Set when a tick left armed catch-up unreplayed. The next open-gate
        // pass fires the tick at once to resume it.
        let mut catch_up_resume = false;

        loop {
            // Register for the capacity wake BEFORE the re-send pass. A
            // response routed after a refusal but before this point is
            // covered by the pass itself. One routed after it wakes the arm.
            let capacity_notified = capacity_freed.notified();
            tokio::pin!(capacity_notified);
            capacity_notified.as_mut().enable();
            if self.resends_deferred() {
                self.redispatch_deferred();
            }
            // Registered before the hold check below, for the same reason.
            let cut_changed = shared.calvin.cut_barriers.notified();
            tokio::pin!(cut_changed);
            cut_changed.as_mut().enable();

            // Every pass reads the role first: a promotion stages held txns,
            // a demotion stops staging.
            self.refresh_role();
            self.drain_inbox();
            // A cut's barrier applied here: propose the redo it held.
            if self.release_cut_holds() {
                self.retry_redo_proposals();
            }
            self.drain_read_results();
            self.propose_due_read_timeouts();
            self.check_awaiting_verdict_stalls();
            self.restage_due();
            self.resume_metadata_hold();

            // Open only once the inputs a closed gate held are processed. A
            // gate closed for the backlog still takes awaited parts and
            // releases (see `super::parts_lane`).
            let intake_open = self.pass_intake_lane();
            if intake_open && catch_up_resume {
                catch_up_resume = false;
                stall_tick.reset_immediately();
            }
            // The backoff end of the next restage, when one waits.
            let restage_at = self.next_restage_at().map(tokio::time::Instant::from_std);
            // When this leader next proposes a barrier's timeout entry.
            let read_timeout_at = self
                .next_read_timeout_due()
                .map(tokio::time::Instant::from_std);

            tokio::select! {
                biased;

                _ = shutdown.wait_cancelled() => {
                    info!(vshard_id = self.vshard_id, "calvin scheduler shutting down");
                    break;
                }

                // A closed channel below means this node retired the vShard or
                // started a new scheduler for it. A closed receiver yields at
                // once on every poll, so the loop must leave rather than spin.
                maybe_completion = self.completion_rx.recv() => {
                    let Some((txn_id, request_id, resp_opt)) = maybe_completion else {
                        self.log_superseded("completion");
                        break;
                    };
                    self.handle_completion(txn_id, request_id, resp_opt);
                }

                // The apply loop concluded a stamped redo. Drained in every
                // state, halted included, so the apply loop never waits on
                // this scheduler.
                () = inbox.ready() => {
                    self.drain_inbox();
                }

                maybe_verdict = self.verdict_rx.recv() => {
                    let Some(signal) = maybe_verdict else {
                        self.log_superseded("verdict");
                        break;
                    };
                    // A durable global verdict landed: resume the matching
                    // txn.
                    self.handle_verdict_signal(signal);
                }

                // The apply loop buffered a barrier entry. Drained in every
                // state, so a held txn keeps every entry of its log.
                () = read_results.pushed() => {
                    self.drain_read_results();
                }

                maybe_promoted = self.promotion_rx.recv() => {
                    let Some(promoted) = maybe_promoted else {
                        self.log_superseded("promotion");
                        break;
                    };
                    // A fast-path write-admission guard released an uncontended
                    // key that one of this scheduler's txns had queued behind;
                    // `release` already promoted it to holder. Run the normal
                    // promotion -> dispatch path so it stops being a stalled
                    // holder in `blocked` and actually executes.
                    self.dispatch_promoted(promoted);
                }

                _ = tokio::time::sleep(super::metadata_hold::HOLD_POLL),
                    if self.metadata_hold.is_some() => {
                    // The next loop pass re-checks the held txn's floor.
                }

                _ = tokio::time::sleep_until(
                    restage_at.unwrap_or_else(tokio::time::Instant::now)
                ), if restage_at.is_some() => {
                    // The next loop pass stages again every txn whose
                    // restage backoff ended.
                }

                _ = tokio::time::sleep_until(
                    read_timeout_at.unwrap_or_else(tokio::time::Instant::now)
                ), if read_timeout_at.is_some() => {
                    // The next loop pass proposes the timeout entry of every
                    // barrier whose timeout passed.
                }

                _ = tokio::time::sleep(ROLE_POLL), if self.role.awaits_term_start() => {
                    // The next loop pass checks whether the term's no-op
                    // applied, and stages the held txns once it has.
                }

                _ = &mut cut_changed, if !self.cut_holds.is_empty() => {
                    // The next loop pass ends the hold of every cut whose
                    // barrier applied here.
                }

                _ = &mut capacity_notified, if self.resends_deferred() => {
                    // Capacity freed: the next loop pass re-sends deferred
                    // requests in FIFO order.
                }

                maybe_txn = self.receiver.recv(), if intake_open => {
                    match maybe_txn {
                        Some(input) => self.process_scheduler_input(input),
                        None => {
                            info!(
                                vshard_id = self.vshard_id,
                                "calvin scheduler: receiver channel closed; exiting"
                            );
                            break;
                        }
                    }
                }

                _ = stall_tick.tick() => {
                    // Replay any sequencer-fan-out inputs dropped on this replica
                    // (channel Full/Closed) so a missed `SchedulerInput` never
                    // permanently diverges this vShard's lock table from its peers.
                    // O(1) common case (no pending catch-up). See `drain_catch_up`.
                    // A closed intake gate skips the drain until it opens.
                    catch_up_resume =
                        !intake_open || self.drain_catch_up() == CatchUpDrain::Remaining;
                    // Propose again every owed sequencer entry not yet applied.
                    self.retry_owed_sequencer_entries();
                    // Propose again every committed slice's redo that is not
                    // in the log. Apply events are drained first, so an
                    // install already reported is never proposed again.
                    self.drain_inbox();
                    self.retry_redo_proposals();
                    // A barrier entry the apply loop buffered after the
                    // scheduler finished its txn is spent.
                    let ledger = Arc::clone(&self.ledger);
                    self.read_results.discard_finished(|txn| {
                        ledger.is_applied(txn.epoch, txn.position)
                    });
                    // So is a stored row of a finished txn its finish did
                    // not remove.
                    self.sweep_finished_barrier_rows();
                    // The top-of-loop check_awaiting_verdict_stalls /
                    // propose_due_read_timeouts and the deferred re-send
                    // pass run on every wake; this arm guarantees the loop wakes
                    // to run them (and the drain) when no other event arrives.
                }
            }
        }
        // The next scheduler of the vShard starts from the barrier entries
        // this one took.
        self.return_barrier_logs();
    }

    /// Log that a scheduler channel closed and the loop exits.
    fn log_superseded(&self, channel: &str) {
        info!(
            vshard_id = self.vshard_id,
            channel,
            "calvin scheduler: a channel closed; the vShard left this node or a new scheduler took it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, spawn_scheduler_loop,
    };

    /// Retiring a vShard drops its verdict sender. The scheduler then leaves
    /// its loop. A closed receiver is always ready, so a loop that ignored the
    /// close would spin and starve the runtime.
    #[tokio::test]
    async fn a_closed_verdict_channel_ends_the_loop() {
        let (scheduler, _dir) = build_test_scheduler(0);
        let registry = Arc::clone(&scheduler.registry);
        let running = spawn_scheduler_loop(scheduler);
        registry.unregister_verdict_signal_sender(0);
        assert!(running.exits_unprompted().await);
    }
}
