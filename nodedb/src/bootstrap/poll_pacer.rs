// SPDX-License-Identifier: BUSL-1.1

//! Pacing for the response poller.

use std::time::Duration;

/// How long the response poller waits before its next poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PollPace {
    /// Poll again without waiting.
    Fast,
    /// Wait one tick. A response that lands meanwhile waits at most this long.
    Tick(Duration),
}

/// Pacing for the response poller.
///
/// The loop has two jobs that pull against each other: route a response as soon
/// as it lands, and avoid spending a worker asking a question the cores have not
/// answered yet. Polling at yield speed serves the first and loses the second,
/// so the fast path is bounded here rather than granted on the strength of the
/// last poll.
///
/// Granting it on the strength of the last poll is what made the loop's own wait
/// unreachable. Any routed response cleared the idle counter, and a poll of this
/// loop is expensive - the dispatcher mutex plus a `flush_wfq` per core - so the
/// 256 empty polls needed to reach the first wait take about a second on a live
/// node. One routed response inside that window clears the counter, so any
/// response rate above roughly one per second holds `idle_iters` below the first
/// wait for the life of the process; the loop then runs at fast-path speed,
/// which is a pinned worker.
///
/// The two counters are therefore deliberately asymmetric: `idle` counts polls
/// that routed nothing and widens the wait, while `fast` counts the fast polls
/// taken since the last wait and is cleared by the wait itself, not by work.
/// Work narrows the wait but cannot buy another fast poll, which bounds the loop
/// for every response rate rather than for the ones we predicted.
pub(super) struct PollPacer {
    /// Fast polls taken since the last wait.
    fast: u32,
    /// Consecutive polls that routed nothing.
    idle: u32,
}

/// Share of a worker the poller may spend polling, in percent.
///
/// The wait is derived from what a poll actually cost, so this holds whatever
/// that turns out to be: a poll locks the dispatcher and flushes the WFQ once
/// per core, which is milliseconds on a debug build and much less on a release
/// one. A fixed wait cannot bound CPU for both.
const MAX_DUTY_PERCENT: u32 = 10;

/// Fast polls allowed back-to-back before the pacer has to wait.
///
/// One: the poll after a drain is what catches a response that landed just
/// behind the batch that was just routed. Every further fast poll is CPU spent
/// re-reading a queue that has already said it is empty.
const FAST_POLLS: u32 = 1;

/// Polls that routed nothing before the wait widens to [`IDLE_TICK`].
///
/// 1024 is the threshold the loop already used, kept so a node that goes quiet
/// settles on the same timeline it did before this change.
const QUIET_POLLS: u32 = 1024;

/// Wait while responses are still arriving. One millisecond is the latency the
/// loop already accepted once its idle counter passed 256.
const TICK: Duration = Duration::from_millis(1);

/// Wait once they stop: 100 polls/second, off a core.
const IDLE_TICK: Duration = Duration::from_millis(10);

impl PollPacer {
    pub(super) const fn new() -> Self {
        Self { fast: 0, idle: 0 }
    }

    /// Wait that holds this loop to [`MAX_DUTY_PERCENT`] of a worker given what
    /// a poll just cost.
    ///
    /// Spending `cost` in every `cost + wait` bounds the share at
    /// `cost / (cost + wait)`, so the wait scales with the cost. A cheap poll
    /// falls back to [`TICK`], and an expensive one is paced out far enough that
    /// it cannot pin a worker however costly the poll turns out to be.
    fn wait_for(cost: Duration) -> Duration {
        // A cycle is `FAST_POLLS + 1` polls followed by this wait, so the loop's
        // share of a worker is `per_cycle / (per_cycle + wait)`.
        let per_cycle = cost * (FAST_POLLS + 1);
        let scaled = per_cycle * (100 / MAX_DUTY_PERCENT - 1);
        if scaled > TICK { scaled } else { TICK }
    }

    /// Record one poll outcome and its cost, and say how long to wait before the
    /// next poll.
    pub(super) fn step(&mut self, routed: usize, cost: Duration) -> PollPace {
        if routed > 0 {
            self.idle = 0;
            if self.fast < FAST_POLLS {
                self.fast = self.fast.saturating_add(1);
                return PollPace::Fast;
            }
        } else {
            self.idle = self.idle.saturating_add(1);
        }
        self.fast = 0;
        let wait = Self::wait_for(cost);
        if self.idle > QUIET_POLLS {
            PollPace::Tick(if wait > IDLE_TICK { wait } else { IDLE_TICK })
        } else {
            PollPace::Tick(wait)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nominal cost of one poll in the simulations below.
    const POLL_COST: Duration = Duration::from_micros(200);

    /// Longest run of `Fast` paces the pacer hands out for `outcomes`.
    ///
    /// The run, not the poll count, is the quantity that matters: a fast poll
    /// hands the runtime a task that is ready again immediately, so an
    /// unbounded run is the pin.
    fn longest_fast_run(outcomes: impl Iterator<Item = usize>) -> u32 {
        let mut pacer = PollPacer::new();
        let (mut run, mut longest) = (0u32, 0u32);
        for routed in outcomes {
            match pacer.step(routed, POLL_COST) {
                PollPace::Fast => {
                    run += 1;
                    longest = longest.max(run);
                }
                PollPace::Tick(_) => run = 0,
            }
        }
        longest
    }

    /// Share of a worker this loop spends polling when a poll costs `cost` and
    /// every poll routes work - the worst case, and the one that pinned a
    /// worker before: the counter was cleared by work, so the loop yielded
    /// again and again and its share of the worker was whatever a poll cost.
    fn duty_percent(cost: Duration) -> u128 {
        let mut pacer = PollPacer::new();
        let (mut polling, mut wall) = (Duration::ZERO, Duration::ZERO);
        for _ in 0..1_000 {
            match pacer.step(1, cost) {
                PollPace::Fast => {
                    polling += cost;
                    wall += cost;
                }
                PollPace::Tick(wait) => {
                    polling += cost;
                    wall += cost + wait;
                }
            }
        }
        polling.as_micros() * 100 / wall.as_micros()
    }

    /// The bound this pacer exists for: for any cost a poll turns out to have, a
    /// poll on every iteration must stay inside the duty budget.
    ///
    /// The wait is derived from the measured cost rather than fixed, so the
    /// bound does not depend on how expensive a poll is on a given build - which
    /// is the mistake a fixed 1 ms wait makes, and why this is asserted across
    /// three orders of magnitude.
    #[test]
    fn a_poll_on_every_iteration_stays_within_the_duty_budget() {
        for micros in [10u64, 100, 1_000, 4_300, 10_000] {
            let duty = duty_percent(Duration::from_micros(micros));
            assert!(
                duty <= MAX_DUTY_PERCENT as u128,
                "a {micros} us poll held {duty}% of a worker"
            );
        }
    }

    /// The old shape at the same rate: work cleared the counter and the loop
    /// took the fast path again, so there was no wait to bound anything.
    #[test]
    fn work_on_every_poll_cannot_hold_the_fast_path() {
        let longest = longest_fast_run(std::iter::repeat_n(1, 10_000));
        assert!(
            longest <= FAST_POLLS,
            "fast path ran {longest} polls without a wait"
        );
    }

    /// The same defect at the rate a live node actually reaches: one response
    /// per few polls keeps clearing the counter.
    #[test]
    fn a_trickle_of_responses_cannot_hold_the_fast_path() {
        let longest = longest_fast_run((0..10_000).map(|i| usize::from(i % 3 == 0)));
        assert!(
            longest <= FAST_POLLS,
            "trickle held the fast path for {longest} polls"
        );
    }

    /// An idle node still has to reach the 10 ms wait, or a pinned worker is
    /// traded for a permanently hot one.
    #[test]
    fn an_idle_poller_still_widens_to_the_idle_tick() {
        let mut pacer = PollPacer::new();
        let widened = (0..10_000).any(|_| pacer.step(0, POLL_COST) == PollPace::Tick(IDLE_TICK));
        assert!(widened, "idle poller never reached the idle tick");
    }

    /// Work must still narrow the wait, or a single response after a quiet
    /// spell would leave the loop on the wide idle wait while a burst follows.
    #[test]
    fn work_narrows_the_wait_again() {
        let mut pacer = PollPacer::new();
        let mut idle_wait = Duration::ZERO;
        for _ in 0..10_000 {
            if let PollPace::Tick(wait) = pacer.step(0, POLL_COST) {
                idle_wait = wait;
            }
        }
        assert!(
            idle_wait >= IDLE_TICK,
            "an idle poller must reach the idle wait"
        );
        assert_eq!(
            pacer.step(1, POLL_COST),
            PollPace::Fast,
            "the response just routed earns one fast poll"
        );
        let after_work = match pacer.step(1, POLL_COST) {
            PollPace::Tick(wait) => wait,
            PollPace::Fast => panic!("work must not hold the fast path open"),
        };
        assert!(
            after_work < idle_wait,
            "work narrowed the wait to {after_work:?}, which is not below {idle_wait:?}"
        );
    }

    /// The latency the fast path exists for: a response that landed just behind
    /// the batch just routed is taken without a wait.
    #[test]
    fn the_poll_after_a_batch_is_still_fast() {
        let mut pacer = PollPacer::new();
        assert_eq!(pacer.step(1, POLL_COST), PollPace::Fast);
    }
}
