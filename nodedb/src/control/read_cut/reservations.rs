// SPDX-License-Identifier: BUSL-1.1

//! The system-time ordinals this node's writes reserved and have not
//! applied yet.
//!
//! A write fixes its ordinal before its record is durable and applies later
//! (`wal_dispatch::stamp_system_ordinals`, the data-group leader's write
//! gate). A read at a cut `S` taken in between misses the write, and a later
//! read at `S` sees it. Every write that reserves an ordinal therefore holds
//! an [`OrdinalReservation`] from the draw until its apply completed or it
//! failed. Dropping the guard releases it, so every error path and every
//! cancelled future releases it too.
//!
//! [`OrdinalReservations::close_through`] closes a cut on this node: it folds
//! `S` into the ordinal clock under the registry lock, so every later draw
//! stamps above `S`, then waits until no reservation at or below `S` is
//! open. A draw takes the same lock ([`OrdinalReservations::reserve_with`]),
//! so a draw is either registered before the fold or stamps above `S`.

use std::collections::BTreeMap;
use std::panic::Location;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_types::OrdinalClock;

/// The age past which an open reservation that blocks a read is reported as
/// leaked. Every step a write holds its reservation through (WAL append,
/// enqueue, apply) is bounded by its statement deadline or drains with its
/// core's queue, so a reservation this old outlived its write.
const LEAK_AGE: Duration = Duration::from_secs(120);

/// One open reservation, keyed by `(first ordinal, id)`.
struct Open {
    site: &'static Location<'static>,
    reserved_at: Instant,
}

#[derive(Default)]
struct Inner {
    next_id: u64,
    open: BTreeMap<(i64, u64), Open>,
}

/// Every open ordinal reservation of this node.
#[derive(Default)]
pub struct OrdinalReservations {
    inner: Mutex<Inner>,
    released: tokio::sync::Notify,
}

/// An open reservation. Dropping it releases it.
#[must_use = "dropping the reservation releases it before the write applied"]
pub struct OrdinalReservation {
    registry: Arc<OrdinalReservations>,
    key: (i64, u64),
}

impl std::fmt::Debug for OrdinalReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrdinalReservation")
            .field("ordinal", &self.key.0)
            .finish()
    }
}

impl OrdinalReservation {
    /// The first ordinal the write reserved.
    pub fn ordinal(&self) -> i64 {
        self.key.0
    }
}

impl Drop for OrdinalReservation {
    fn drop(&mut self) {
        self.registry.release(self.key);
    }
}

/// The open reservation that kept a cut from closing.
#[derive(Debug, Clone, Copy)]
pub struct BlockingReservation {
    pub ordinal: i64,
    pub site: &'static Location<'static>,
    pub open_for: Duration,
}

impl OrdinalReservations {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Run `draw` under the registry lock and register the first ordinal it
    /// returns. `draw` returns `None` for a write that reserves no ordinal,
    /// and then nothing is registered.
    ///
    /// `draw` must only read or advance the ordinal clock: it runs under the
    /// registry lock.
    #[track_caller]
    pub fn reserve_with(
        self: &Arc<Self>,
        draw: impl FnOnce() -> Option<i64>,
    ) -> (Option<i64>, Option<OrdinalReservation>) {
        let site = Location::caller();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let Some(ordinal) = draw() else {
            return (None, None);
        };
        let reservation = self.register(&mut inner, ordinal, site);
        (Some(ordinal), Some(reservation))
    }

    fn register(
        self: &Arc<Self>,
        inner: &mut Inner,
        ordinal: i64,
        site: &'static Location<'static>,
    ) -> OrdinalReservation {
        let id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1);
        let key = (ordinal, id);
        inner.open.insert(
            key,
            Open {
                site,
                reserved_at: Instant::now(),
            },
        );
        OrdinalReservation {
            registry: Arc::clone(self),
            key,
        }
    }

    /// Run `draw` under the registry lock and register the first ordinal it
    /// returns (see [`Self::reserve_with`]).
    #[track_caller]
    pub fn reserve(self: &Arc<Self>, draw: impl FnOnce() -> i64) -> (i64, OrdinalReservation) {
        let site = Location::caller();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let ordinal = draw();
        let reservation = self.register(&mut inner, ordinal, site);
        (ordinal, reservation)
    }

    fn release(&self, key: (i64, u64)) {
        let removed = {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            inner.open.remove(&key).is_some()
        };
        if removed {
            self.released.notify_waiters();
        }
    }

    /// Fold `cut` into `clock` under the registry lock: every draw after it
    /// stamps above `cut`, and every draw before it is registered.
    pub fn fold(&self, clock: &OrdinalClock, cut: i64) {
        let _inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        clock.update_from_remote(cut);
    }

    /// The oldest open reservation at or below `cut`.
    pub fn blocking(&self, cut: i64) -> Option<BlockingReservation> {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner
            .open
            .range(..=(cut, u64::MAX))
            .next()
            .map(|((ordinal, _), open)| BlockingReservation {
                ordinal: *ordinal,
                site: open.site,
                open_for: open.reserved_at.elapsed(),
            })
    }

    /// Close `cut` on this node: fold it into `clock`, then wait until no
    /// reservation at or below it is open, or until `deadline`. Returns the
    /// reservation still open at the deadline.
    pub async fn close_through(
        &self,
        clock: &OrdinalClock,
        cut: i64,
        deadline: tokio::time::Instant,
    ) -> Result<(), BlockingReservation> {
        self.fold(clock, cut);
        loop {
            // Registered before the check, so a release between the check
            // and the wait still wakes it.
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if self.blocking(cut).is_none() {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                return match self.blocking(cut) {
                    Some(blocker) => Err(blocker),
                    None => Ok(()),
                };
            }
        }
    }

    /// The number of open reservations.
    #[cfg(test)]
    pub fn open_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .open
            .len()
    }
}

/// Whether `blocker` outlived any write that holds it (see [`LEAK_AGE`]).
pub fn is_leaked(blocker: &BlockingReservation) -> bool {
    blocker.open_for >= LEAK_AGE
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deadline_in(ms: u64) -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_millis(ms)
    }

    #[test]
    fn a_reservation_releases_on_drop_after_success_or_error() {
        let registry = OrdinalReservations::new();
        let clock = OrdinalClock::new();
        let applied = || -> Result<(), crate::Error> {
            let (_, _guard) = registry.reserve_with(|| Some(clock.next_ordinal()));
            Ok(())
        };
        applied().expect("applied");
        assert_eq!(registry.open_count(), 0, "a write that applied releases");
        let failed = || -> Result<(), crate::Error> {
            let (_, _guard) = registry.reserve_with(|| Some(clock.next_ordinal()));
            Err(crate::Error::Internal {
                detail: "refused".into(),
            })
        };
        assert!(failed().is_err());
        assert_eq!(registry.open_count(), 0, "a write that failed releases");
        let (ordinal, none) = registry.reserve_with(|| None);
        assert!(ordinal.is_none() && none.is_none());
        assert_eq!(registry.open_count(), 0, "no draw registers nothing");
    }

    /// A write future dropped mid-flight releases its reservation.
    #[tokio::test]
    async fn a_cancelled_write_releases() {
        let registry = OrdinalReservations::new();
        let clock = OrdinalClock::new();
        let held = Arc::clone(&registry);
        let write = tokio::spawn(async move {
            let (_, _guard) = held.reserve_with(|| Some(clock.next_ordinal()));
            std::future::pending::<()>().await;
        });
        while registry.open_count() == 0 {
            tokio::task::yield_now().await;
        }
        write.abort();
        let _ = write.await;
        assert_eq!(registry.open_count(), 0);
    }

    /// The close waits for an in-flight reservation below the cut, and a
    /// draw after the close stamps above the cut.
    #[tokio::test]
    async fn the_close_waits_for_a_reservation_below_the_cut() {
        let registry = OrdinalReservations::new();
        let clock = Arc::new(OrdinalClock::new());
        let (ordinal, guard) = registry.reserve_with(|| Some(clock.next_ordinal()));
        let ordinal = ordinal.expect("drawn");
        let cut = ordinal + 10;
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(guard);
        });
        let started = Instant::now();
        registry
            .close_through(&clock, cut, deadline_in(5_000))
            .await
            .expect("closed once the write applied");
        assert!(started.elapsed() >= Duration::from_millis(40));
        releaser.await.expect("join");
        assert!(
            clock.next_ordinal() > cut,
            "a later draw stamps above the cut"
        );
    }

    #[tokio::test]
    async fn a_reservation_above_the_cut_does_not_block() {
        let registry = OrdinalReservations::new();
        let clock = OrdinalClock::new();
        let (ordinal, _guard) = registry.reserve_with(|| Some(clock.next_ordinal()));
        let cut = ordinal.expect("drawn") - 1;
        registry
            .close_through(&clock, cut, deadline_in(10))
            .await
            .expect("nothing at or below the cut");
    }

    #[tokio::test]
    async fn an_open_reservation_holds_the_close_until_the_deadline() {
        let registry = OrdinalReservations::new();
        let clock = OrdinalClock::new();
        let (ordinal, _guard) = registry.reserve_with(|| Some(clock.next_ordinal()));
        let ordinal = ordinal.expect("drawn");
        let blocked = registry
            .close_through(&clock, ordinal, deadline_in(30))
            .await
            .expect_err("the write never applied");
        assert_eq!(blocked.ordinal, ordinal);
        assert!(!is_leaked(&blocked), "a fresh reservation is not a leak");
    }
}
