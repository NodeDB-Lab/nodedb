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
//! ## Settling
//!
//! [`WriteWindow::settle`] states that the outcome is final. A window dropped
//! without it stays open for the rest of the process. Its record can still
//! need restart replay, so F must never pass it. The drop logs an error.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use tracing::{error, warn};

use crate::types::Lsn;

/// The node's registry of open write windows.
#[derive(Debug, Default)]
pub struct OutcomeFloor {
    windows: Mutex<Windows>,
}

#[derive(Debug, Default)]
struct Windows {
    next_ticket: u64,
    /// Horizon of each open window, by ticket.
    open: HashMap<u64, u64>,
    /// Number of open windows at each horizon.
    horizons: BTreeMap<u64, usize>,
    /// Highest LSN any window noted.
    max_noted: u64,
    /// Highest floor handed out.
    published: u64,
}

impl Windows {
    fn open(&mut self, horizon: u64) -> u64 {
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.open.insert(ticket, horizon);
        *self.horizons.entry(horizon).or_insert(0) += 1;
        ticket
    }

    fn close(&mut self, ticket: u64) {
        let Some(horizon) = self.open.remove(&ticket) else {
            return;
        };
        if let Some(count) = self.horizons.get_mut(&horizon) {
            *count -= 1;
            if *count == 0 {
                self.horizons.remove(&horizon);
            }
        }
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
        self.published
    }
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
            windows.note(lsn.as_u64());
            windows.open(lsn.as_u64())
        };
        WriteWindow::new(Arc::clone(self), ticket)
    }

    /// The current floor. Never lower than a value returned before.
    pub fn floor(&self) -> Lsn {
        Lsn::new(self.lock().floor())
    }
}

/// One write's hold on the outcome floor. Settle it once the write's outcome
/// is final. Dropped unsettled, it holds the floor for the rest of the
/// process.
#[derive(Debug)]
pub struct WriteWindow {
    owner: Arc<OutcomeFloor>,
    ticket: u64,
    settled: bool,
}

impl WriteWindow {
    fn new(owner: Arc<OutcomeFloor>, ticket: u64) -> Self {
        Self {
            owner,
            ticket,
            settled: false,
        }
    }

    /// Record an LSN this window minted. Call it before the window settles.
    pub fn note_minted(&self, lsn: Lsn) {
        self.owner.lock().note(lsn.as_u64());
    }

    /// Close the window: the write's outcome is final.
    pub fn settle(mut self) {
        self.settled = true;
        self.owner.lock().close(self.ticket);
    }
}

impl Drop for WriteWindow {
    fn drop(&mut self) {
        if !self.settled {
            error!(
                ticket = self.ticket,
                "a write window was dropped before its outcome was final; the outcome \
                 floor stays below it until restart"
            );
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
