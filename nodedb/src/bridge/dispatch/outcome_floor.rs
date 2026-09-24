// SPDX-License-Identifier: BUSL-1.1

//! The outcome floor: the highest WAL LSN at or below which every record sent
//! to a Data Plane core has a final outcome.
//!
//! A record's outcome is final when the core applied it, or refused it and its
//! `WriteAborted` marker is durable. A watermark that says "restart may skip
//! every record at or below me" is sound only at or below this floor: a record
//! above it can still be on its way to a core, or still be applying.
//!
//! ## Windows
//!
//! A write opens a [`WriteWindow`] before it mints its LSN, notes each LSN it
//! mints, and settles the window once its outcome is final. The dispatcher
//! also opens a window for every accepted request that carries a WAL LSN, and
//! settles it when the core's final response arrives. A record minted outside
//! any window must never reach a core.
//!
//! Each open window has a horizon, a lower bound on every LSN it holds back:
//!
//! - A window opened before its mint takes `max_noted + 1`. WAL LSNs strictly
//!   increase, so an LSN minted after the open exceeds every LSN noted before
//!   it.
//! - A dispatcher window takes the request's LSN.
//!
//! ## The floor
//!
//! F is the smallest open horizon minus one, or the highest noted LSN when no
//! window is open. F never decreases: each computed value is raised to the
//! last published one.
//!
//! F never passes an open mint window:
//!
//! 1. Every computed value is at most `max_noted`, so every published F is at
//!    most `max_noted`.
//! 2. A mint window opens with horizon `max_noted + 1`, above every F published
//!    before it.
//! 3. While it stays open, every computed value is at most its horizon minus
//!    one, so the published F stays below its horizon too.
//!
//! A dispatcher window whose LSN is at or below the published F cannot hold F
//! back. Its record was minted outside a window, and the floor passed it
//! before it reached the dispatcher. The open logs that record.
//!
//! ## Owned records
//!
//! A window owns every LSN it records with [`WriteWindow::own`]. A record
//! sent to a core again through [`OutcomeFloor::open_existing`] must have no
//! owner and no final outcome: an owner carries its record to an outcome, and
//! a closed owner already did. Both refuse the resend, as the floor does.
//!
//! ## Closing a window
//!
//! [`WriteWindow::settle`] states that the outcome is final.
//! [`WriteWindow::hold`] states that the record has no final outcome in this
//! process: restart replay must reach it, so the window stays open until the
//! process exits. A window dropped without either is a leak. It stays open
//! too, and the drop counts it, logs an error, and files a report.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tracing::{error, warn};

use super::closed_lsns::ClosedLsns;
use crate::types::Lsn;

/// The node's registry of open write windows.
#[derive(Debug, Default)]
pub struct OutcomeFloor {
    windows: Mutex<Windows>,
    /// Windows dropped without a settle or a hold.
    leaked: AtomicU64,
}

#[derive(Debug)]
struct OpenWindow {
    horizon: u64,
    opened_at: Instant,
    /// Held until restart: the floor stays below it by design.
    held: bool,
    /// LSNs this window owns.
    lsns: Vec<u64>,
}

#[derive(Debug, Default)]
struct Windows {
    next_ticket: u64,
    /// Each open window, by ticket. Tickets increase, so the first entry is
    /// the oldest open window.
    open: BTreeMap<u64, OpenWindow>,
    /// Number of open windows at each horizon.
    horizons: BTreeMap<u64, usize>,
    /// Highest LSN any window noted.
    max_noted: u64,
    /// Highest floor handed out.
    published: u64,
    /// Number of held windows.
    held: usize,
    /// Open windows owning each LSN.
    owners: BTreeMap<u64, usize>,
    /// LSNs above the published floor whose last owner closed. Bounded by
    /// the number of live owned LSNs, whatever the floor does.
    closed: ClosedLsns,
}

impl Windows {
    fn open(&mut self, horizon: u64) -> u64 {
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.open.insert(
            ticket,
            OpenWindow {
                horizon,
                opened_at: Instant::now(),
                held: false,
                lsns: Vec::new(),
            },
        );
        *self.horizons.entry(horizon).or_insert(0) += 1;
        ticket
    }

    fn close(&mut self, ticket: u64) {
        let Some(window) = self.open.remove(&ticket) else {
            return;
        };
        if let Some(count) = self.horizons.get_mut(&window.horizon) {
            *count -= 1;
            if *count == 0 {
                self.horizons.remove(&window.horizon);
            }
        }
        let mut released = Vec::new();
        for lsn in window.lsns {
            if let Some(count) = self.owners.get_mut(&lsn) {
                *count -= 1;
                if *count == 0 {
                    self.owners.remove(&lsn);
                    released.push(lsn);
                }
            }
        }
        for lsn in released {
            self.closed.insert(lsn, &self.owners);
        }
    }

    /// Record that window `ticket` owns `lsn`.
    fn own(&mut self, ticket: u64, lsn: u64) {
        self.note(lsn);
        let Some(window) = self.open.get_mut(&ticket) else {
            return;
        };
        if !window.lsns.contains(&lsn) {
            window.lsns.push(lsn);
            *self.owners.entry(lsn).or_insert(0) += 1;
        }
    }

    /// Why `lsn` is claimed: a live window owns it, or its last owner
    /// closed. `None` when it is free.
    fn claim(&self, lsn: u64) -> Option<ResendRefusal> {
        if self.owners.contains_key(&lsn) {
            Some(ResendRefusal::Owned)
        } else if self.closed.contains(lsn) {
            Some(ResendRefusal::Closed)
        } else {
            None
        }
    }

    /// Mark a window held. Returns its horizon and age.
    fn mark_held(&mut self, ticket: u64) -> Option<(u64, Duration)> {
        let window = self.open.get_mut(&ticket)?;
        if !window.held {
            window.held = true;
            self.held += 1;
        }
        Some((window.horizon, window.opened_at.elapsed()))
    }

    /// The oldest window that is not held.
    fn oldest_unheld(&self) -> Option<&OpenWindow> {
        self.open.values().find(|window| !window.held)
    }

    fn note(&mut self, lsn: u64) {
        self.max_noted = self.max_noted.max(lsn);
    }

    fn floor(&mut self) -> u64 {
        let computed = match self.horizons.first_key_value() {
            Some((&horizon, _)) => horizon.saturating_sub(1).min(self.max_noted),
            None => self.max_noted,
        };
        self.published = self.published.max(computed);
        // A closed LSN at or below the floor is refused by the floor itself.
        self.closed.prune_through(self.published);
        self.published
    }
}

/// Why [`OutcomeFloor::open_existing`] refused to send a record again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResendRefusal {
    /// The floor passed the record: its outcome is final.
    BelowFloor,
    /// A live or held window owns the record and carries it to its outcome.
    Owned,
    /// The record's last owner closed: its outcome is final.
    Closed,
}

/// The oldest window that holds the floor, and how long it has held it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StuckFloor {
    /// The current floor.
    pub floor: Lsn,
    /// The oldest open window's horizon: the floor stays below it.
    pub horizon: Lsn,
    /// How long the oldest open window has been open.
    pub open_for: Duration,
    /// Number of open windows that are not held.
    pub open_windows: usize,
}

impl OutcomeFloor {
    /// An empty registry. Its floor starts at zero.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> MutexGuard<'_, Windows> {
        self.windows.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Open a window before the write mints its LSN.
    pub fn open_write(self: &Arc<Self>) -> WriteWindow {
        let ticket = {
            let mut windows = self.lock();
            let horizon = windows.max_noted.saturating_add(1);
            windows.open(horizon)
        };
        WriteWindow::new(Arc::clone(self), ticket)
    }

    /// Open a window for a request that carries `lsn` as it enters the
    /// dispatcher.
    pub fn open_dispatched(self: &Arc<Self>, lsn: Lsn) -> WriteWindow {
        let ticket = {
            let mut windows = self.lock();
            if lsn.as_u64() <= windows.published {
                warn!(
                    lsn = lsn.as_u64(),
                    floor = windows.published,
                    "a record reached the dispatcher after the outcome floor passed it; \
                     it was minted outside a write window"
                );
            }
            let ticket = windows.open(lsn.as_u64());
            windows.own(ticket, lsn.as_u64());
            ticket
        };
        WriteWindow::new(Arc::clone(self), ticket)
    }

    /// Open a window for an existing record at `lsn` that is sent to a core
    /// again. Refused, with the reason, when the record must not be sent:
    ///
    /// - the floor passed `lsn`, so its outcome is final;
    /// - a live or held window owns it, and carries it to its outcome;
    /// - a window that owned it closed, so its outcome is final.
    pub fn open_existing(self: &Arc<Self>, lsn: Lsn) -> Result<WriteWindow, ResendRefusal> {
        let ticket = {
            let mut windows = self.lock();
            if lsn.as_u64() <= windows.floor() {
                return Err(ResendRefusal::BelowFloor);
            }
            if let Some(refusal) = windows.claim(lsn.as_u64()) {
                return Err(refusal);
            }
            let ticket = windows.open(lsn.as_u64());
            windows.own(ticket, lsn.as_u64());
            ticket
        };
        Ok(WriteWindow::new(Arc::clone(self), ticket))
    }

    /// The current floor. Never lower than a value returned before.
    pub fn floor(&self) -> Lsn {
        Lsn::new(self.lock().floor())
    }

    /// Windows dropped without a settle or a hold since the process started.
    pub fn leaked_windows(&self) -> u64 {
        self.leaked.load(Ordering::Relaxed)
    }

    /// Windows held until restart. The floor stays below each by design.
    pub fn held_windows(&self) -> usize {
        self.lock().held
    }

    /// The oldest open window that is not held, when it has held the floor
    /// for longer than `bound`. A held window is expected to stay open, so
    /// it never makes the floor stuck.
    pub fn stuck(&self, bound: Duration) -> Option<StuckFloor> {
        let mut windows = self.lock();
        let floor = windows.floor();
        let open_windows = windows.open.len() - windows.held;
        let oldest = windows.oldest_unheld()?;
        let open_for = oldest.opened_at.elapsed();
        (open_for > bound).then_some(StuckFloor {
            floor: Lsn::new(floor),
            horizon: Lsn::new(oldest.horizon),
            open_for,
            open_windows,
        })
    }

    /// How long the oldest open window that is not held has been open, or
    /// zero when none is.
    pub fn oldest_open_for(&self) -> Duration {
        self.lock()
            .oldest_unheld()
            .map_or(Duration::ZERO, |oldest| oldest.opened_at.elapsed())
    }

    fn close(&self, ticket: u64) {
        self.lock().close(ticket);
    }

    /// Count a leaked window and report it. The window stays open.
    fn leak(&self, ticket: u64) {
        self.leaked.fetch_add(1, Ordering::Relaxed);
        let (horizon, open_for) = self
            .lock()
            .open
            .get(&ticket)
            .map_or((0, Duration::ZERO), |window| {
                (window.horizon, window.opened_at.elapsed())
            });
        error!(
            ticket,
            horizon,
            open_for_ms = u64::try_from(open_for.as_millis()).unwrap_or(u64::MAX),
            "a write window was dropped before its outcome was final; the outcome \
             floor stays below it until restart"
        );
        crate::diag::write_window_leaked(ticket, horizon, open_for);
    }
}

/// One write's hold on the outcome floor. Settle it once the write's outcome
/// is final, or hold it when the record has no final outcome in this process.
/// Dropped without either, it leaks: it holds the floor until the process
/// exits, and the drop reports it.
#[derive(Debug)]
pub struct WriteWindow {
    owner: Arc<OutcomeFloor>,
    ticket: u64,
    closed: bool,
}

impl WriteWindow {
    fn new(owner: Arc<OutcomeFloor>, ticket: u64) -> Self {
        Self {
            owner,
            ticket,
            closed: false,
        }
    }

    /// Record an LSN this window minted. Call it before the window settles.
    pub fn note_minted(&self, lsn: Lsn) {
        self.owner.lock().note(lsn.as_u64());
    }

    /// Record that this window owns the record at `lsn`. Call it when the
    /// record is appended.
    pub fn own(&self, lsn: Lsn) {
        self.owner.lock().own(self.ticket, lsn.as_u64());
    }

    /// Close the window: the write's outcome is final.
    pub fn settle(mut self) {
        self.closed = true;
        self.owner.close(self.ticket);
    }

    /// Keep the window open until the process exits: the record has no final
    /// outcome here, and restart replay must reach it. Files a report naming
    /// the caller.
    #[track_caller]
    pub fn hold(mut self) {
        self.closed = true;
        let site = std::panic::Location::caller();
        let (horizon, open_for) = self
            .owner
            .lock()
            .mark_held(self.ticket)
            .unwrap_or((0, Duration::ZERO));
        warn!(
            ticket = self.ticket,
            horizon,
            site = %site,
            "a write window is held until restart; the outcome floor stays below it"
        );
        crate::diag::write_window_held(site, self.ticket, horizon, open_for);
    }
}

impl Drop for WriteWindow {
    fn drop(&mut self) {
        if !self.closed {
            self.owner.leak(self.ticket);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[test]
    fn an_empty_registry_has_floor_zero() {
        assert_eq!(OutcomeFloor::new().floor(), Lsn::ZERO);
    }

    #[test]
    fn the_floor_never_passes_an_open_window() {
        let floor = OutcomeFloor::new();
        let early = floor.open_write();
        early.note_minted(Lsn::new(10));
        let late = floor.open_write();
        late.note_minted(Lsn::new(11));
        late.settle();
        assert!(
            floor.floor() < Lsn::new(10),
            "the open window at 10 holds F"
        );
        early.settle();
        assert_eq!(floor.floor(), Lsn::new(11));
    }

    #[test]
    fn the_floor_advances_when_windows_settle() {
        let floor = OutcomeFloor::new();
        let first = floor.open_write();
        first.note_minted(Lsn::new(5));
        first.settle();
        assert_eq!(floor.floor(), Lsn::new(5));
        let second = floor.open_write();
        second.note_minted(Lsn::new(9));
        assert_eq!(floor.floor(), Lsn::new(5));
        second.settle();
        assert_eq!(floor.floor(), Lsn::new(9));
    }

    #[test]
    fn a_window_opened_before_its_mint_holds_the_floor_below_the_mint() {
        let floor = OutcomeFloor::new();
        let done = floor.open_write();
        done.note_minted(Lsn::new(3));
        done.settle();
        let pending = floor.open_write();
        assert_eq!(floor.floor(), Lsn::new(3));
        pending.note_minted(Lsn::new(4));
        assert_eq!(floor.floor(), Lsn::new(3));
        pending.settle();
    }

    #[test]
    fn a_dropped_window_holds_the_floor() {
        let floor = OutcomeFloor::new();
        let before = floor.open_write();
        before.note_minted(Lsn::new(6));
        before.settle();
        let lost = floor.open_write();
        lost.note_minted(Lsn::new(7));
        drop(lost);
        let later = floor.open_write();
        later.note_minted(Lsn::new(8));
        later.settle();
        assert_eq!(floor.floor(), Lsn::new(6));
    }

    #[test]
    fn a_dropped_window_counts_as_a_leak() {
        let floor = OutcomeFloor::new();
        assert_eq!(floor.leaked_windows(), 0);
        drop(floor.open_write());
        assert_eq!(floor.leaked_windows(), 1);
        floor.open_write().settle();
        floor.open_write().hold();
        assert_eq!(
            floor.leaked_windows(),
            1,
            "a settled or held window is not a leak"
        );
    }

    #[test]
    fn a_held_window_holds_the_floor() {
        let floor = OutcomeFloor::new();
        let before = floor.open_write();
        before.note_minted(Lsn::new(4));
        before.settle();
        let held = floor.open_write();
        held.note_minted(Lsn::new(5));
        held.hold();
        let later = floor.open_write();
        later.note_minted(Lsn::new(6));
        later.settle();
        assert_eq!(floor.floor(), Lsn::new(4));
    }

    #[test]
    fn a_window_open_past_the_bound_reports_a_stuck_floor() {
        let floor = OutcomeFloor::new();
        assert!(floor.stuck(Duration::ZERO).is_none(), "no open window");
        let window = floor.open_write();
        window.note_minted(Lsn::new(3));
        std::thread::sleep(Duration::from_millis(2));
        let stuck = floor
            .stuck(Duration::ZERO)
            .expect("the window is older than a zero bound");
        assert_eq!(stuck.horizon, Lsn::new(1));
        assert_eq!(stuck.floor, Lsn::ZERO);
        assert_eq!(stuck.open_windows, 1);
        assert!(floor.oldest_open_for() > Duration::ZERO);
        assert!(
            floor.stuck(Duration::from_secs(3600)).is_none(),
            "a young window is inside the bound"
        );
        window.settle();
        assert!(floor.stuck(Duration::ZERO).is_none());
        assert_eq!(floor.oldest_open_for(), Duration::ZERO);
    }

    /// A held window stays open by design. It never makes the floor stuck,
    /// and the held count reports it.
    #[test]
    fn a_held_window_is_counted_and_never_stuck() {
        let floor = OutcomeFloor::new();
        let held = floor.open_write();
        held.note_minted(Lsn::new(3));
        held.hold();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(floor.held_windows(), 1);
        assert!(floor.stuck(Duration::ZERO).is_none());
        assert_eq!(floor.oldest_open_for(), Duration::ZERO);

        let open = floor.open_write();
        std::thread::sleep(Duration::from_millis(2));
        let stuck = floor
            .stuck(Duration::ZERO)
            .expect("the window that is not held is older than a zero bound");
        assert_eq!(stuck.open_windows, 1);
        open.settle();
        assert!(floor.stuck(Duration::ZERO).is_none());
    }

    #[test]
    fn an_existing_record_the_floor_passed_opens_no_window() {
        let floor = OutcomeFloor::new();
        let window = floor.open_write();
        window.note_minted(Lsn::new(8));
        window.settle();
        assert_eq!(
            floor.open_existing(Lsn::new(8)).err(),
            Some(ResendRefusal::BelowFloor)
        );
        let resent = floor
            .open_existing(Lsn::new(9))
            .expect("the floor has not passed 9");
        assert_eq!(floor.floor(), Lsn::new(8));
        resent.settle();
        assert_eq!(floor.floor(), Lsn::new(9));
    }

    /// A held window keeps the floor down for the rest of the process. The
    /// closed LSNs above it stay a bounded number of ranges however many
    /// records close, and each stays refused.
    #[test]
    fn closed_lsns_stay_bounded_while_a_window_is_held() {
        let floor = OutcomeFloor::new();
        floor.open_dispatched(Lsn::new(10)).hold();
        for lsn in 11..5_011u64 {
            floor.open_dispatched(Lsn::new(lsn)).settle();
        }
        assert_eq!(
            floor.floor(),
            Lsn::new(9),
            "the held window keeps the floor down"
        );
        assert!(
            floor.lock().closed.range_count() <= 2,
            "closed LSNs must stay bounded while a window is held"
        );
        assert_eq!(
            floor.open_existing(Lsn::new(2_500)).err(),
            Some(ResendRefusal::Closed)
        );
        assert_eq!(
            floor.open_existing(Lsn::new(10)).err(),
            Some(ResendRefusal::Owned),
            "the held window still owns its record"
        );
    }

    #[test]
    fn a_dispatched_window_holds_the_floor_below_its_lsn() {
        let floor = OutcomeFloor::new();
        let dispatched = floor.open_dispatched(Lsn::new(20));
        assert_eq!(floor.floor(), Lsn::new(19));
        dispatched.settle();
        assert_eq!(floor.floor(), Lsn::new(20));
    }

    #[test]
    fn a_dispatched_window_below_the_floor_does_not_lower_it() {
        let floor = OutcomeFloor::new();
        let window = floor.open_write();
        window.note_minted(Lsn::new(30));
        window.settle();
        assert_eq!(floor.floor(), Lsn::new(30));
        let late = floor.open_dispatched(Lsn::new(12));
        assert_eq!(floor.floor(), Lsn::new(30), "the floor never decreases");
        late.settle();
    }

    /// Writers mint from a shared counter the way the WAL does, each inside a
    /// window. A reader samples the floor while they run. No sampled floor may
    /// reach an LSN whose window was still open when the sample was taken.
    #[test]
    fn concurrent_writers_never_see_the_floor_pass_their_open_window() {
        let floor = OutcomeFloor::new();
        let wal = Arc::new(AtomicU64::new(1));
        let writers: Vec<_> = (0..4)
            .map(|_| {
                let floor = Arc::clone(&floor);
                let wal = Arc::clone(&wal);
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let window = floor.open_write();
                        let lsn = Lsn::new(wal.fetch_add(1, Ordering::SeqCst));
                        window.note_minted(lsn);
                        let seen = floor.floor();
                        assert!(seen < lsn, "floor {seen:?} passed open lsn {lsn:?}");
                        window.settle();
                    }
                })
            })
            .collect();
        let mut last = Lsn::ZERO;
        for _ in 0..2000 {
            let seen = floor.floor();
            assert!(
                seen >= last,
                "the floor went back from {last:?} to {seen:?}"
            );
            last = seen;
        }
        for writer in writers {
            writer.join().expect("writer thread");
        }
        let minted = wal.load(Ordering::SeqCst) - 1;
        assert_eq!(floor.floor(), Lsn::new(minted), "every window settled");
    }
}
