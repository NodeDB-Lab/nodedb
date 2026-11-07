// SPDX-License-Identifier: BUSL-1.1

//! Early-flush signal, flush serialization, and recorder backpressure for the audit buffer.
//!
//! Only the flush executor writes the buffer to the catalog.
//! A recorder never runs a flush itself, so a recorder never opens a catalog transaction.
//! A recorder past the backpressure point requests a flush and waits for it.
//! The wait has a bound, and it holds no lock.
//! So a recorder that holds any lock or transaction the flush needs cannot deadlock it.
//!
//! Only a flush that committed to the catalog releases a waiter.
//! A failed flush leaves its waiters waiting, up to their bound, so a catalog that keeps failing throttles every recorder.
//!
//! The executor runs on a blocking thread of its own, and an async recorder hands off its worker before it waits.
//! So recorders cannot starve the flush they wait for.
//!
//! At the hard bound (see [`HARD_BOUND_MULTIPLE`]) the buffer takes no more entries:
//! - A recorder with an error path refuses: the Event Plane batch stays unrecorded and its watermark holds,
//!   and a strict recorder returns an error, so its caller refuses the audited action.
//! - A best-effort recorder has no error path, so the audited action goes on.
//!   Its entry stays in the audit WAL only, and the next flush that reaches the catalog commits it.
//!   A WAL that refuses that entry too loses it, and the error is reported.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

/// Buffer length at which a server's audit log requests an early flush.
pub const DEFAULT_FLUSH_BOUND: usize = 10_000;

/// Multiple of the flush bound at which a recorder waits for a flush.
///
/// Below it, the flush executor drains the buffer after an early-flush request.
/// At or above it, a burst outruns the executor, so the recorder waits.
pub const BACKPRESSURE_MULTIPLE: usize = 2;

/// Multiple of the flush bound at which the buffer takes no more entries.
pub const HARD_BOUND_MULTIPLE: usize = 8;

/// Longest time one recorder waits for a flush.
///
/// After it a best-effort recorder continues. Its entry stays in the log or the audit WAL.
pub const DEFAULT_BACKPRESSURE_WAIT: Duration = Duration::from_secs(1);

/// Shared between an `AuditLog`, its recorders, and the flush executor.
pub struct AuditFlushControl {
    /// Set by a flush request, cleared by the executor when it wakes.
    requested: Mutex<bool>,
    request_changed: Condvar,
    /// Held for the whole of one flush, so two flushes never interleave.
    gate: Mutex<()>,
    /// Count of flushes that committed to the catalog. Recorders wait for it to advance.
    completed: Mutex<u64>,
    completed_changed: Condvar,
    /// Whether a flush executor runs. With none, a recorder has nothing to wait for.
    executor_attached: AtomicBool,
    /// Longest backpressure wait, in milliseconds.
    backpressure_wait_ms: AtomicU64,
    /// Test support: every catalog commit of a flush fails while this is set.
    #[cfg(test)]
    catalog_fault: AtomicBool,
}

impl Default for AuditFlushControl {
    fn default() -> Self {
        Self {
            requested: Mutex::new(false),
            request_changed: Condvar::new(),
            gate: Mutex::new(()),
            completed: Mutex::new(0),
            completed_changed: Condvar::new(),
            executor_attached: AtomicBool::new(false),
            backpressure_wait_ms: AtomicU64::new(
                u64::try_from(DEFAULT_BACKPRESSURE_WAIT.as_millis()).unwrap_or(u64::MAX),
            ),
            #[cfg(test)]
            catalog_fault: AtomicBool::new(false),
        }
    }
}

impl AuditFlushControl {
    fn requested_flag(&self) -> MutexGuard<'_, bool> {
        match self.requested.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Ask the flush executor to flush now. The call does not wait for the flush.
    pub fn request_flush(&self) {
        *self.requested_flag() = true;
        self.request_changed.notify_one();
    }

    /// Clear a pending flush request and return whether one was pending.
    pub fn take_flush_request(&self) -> bool {
        std::mem::take(&mut *self.requested_flag())
    }

    /// Block the executor until a flush is requested or `timeout` ends.
    pub fn wait_for_flush_request(&self, timeout: Duration) {
        let requested = self.requested_flag();
        let waited = self
            .request_changed
            .wait_timeout_while(requested, timeout, |requested| !*requested);
        // The lock is released here. A poisoned wait still ends the wait.
        drop(waited);
    }

    /// Test support: make every catalog commit of a flush fail, or succeed again.
    #[cfg(test)]
    pub(crate) fn inject_catalog_fault(&self, fault: bool) {
        self.catalog_fault.store(fault, Ordering::Release);
    }

    /// Test support: whether the next catalog commit of a flush must fail.
    #[cfg(test)]
    pub(crate) fn catalog_fault_injected(&self) -> bool {
        self.catalog_fault.load(Ordering::Acquire)
    }

    /// Serialize one flush against every other flush.
    ///
    /// The caller must not hold the `AuditLog` lock while it waits here.
    pub fn serialize(&self) -> MutexGuard<'_, ()> {
        match self.gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Mark a flush executor as running.
    pub fn attach_executor(&self) {
        self.executor_attached.store(true, Ordering::Release);
    }

    /// Mark the flush executor as stopped and release every waiting recorder.
    pub fn detach_executor(&self) {
        self.executor_attached.store(false, Ordering::Release);
        // Taking the lock orders this wake after a waiter's check of the flag.
        drop(self.completed_count());
        self.completed_changed.notify_all();
    }

    /// Whether a flush executor runs.
    pub fn executor_attached(&self) -> bool {
        self.executor_attached.load(Ordering::Acquire)
    }

    /// Set the longest backpressure wait.
    pub fn set_backpressure_wait(&self, wait: Duration) {
        let ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        self.backpressure_wait_ms.store(ms, Ordering::Release);
    }

    fn completed_count(&self) -> MutexGuard<'_, u64> {
        match self.completed.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Record that one flush committed to the catalog, and wake every waiting recorder.
    ///
    /// The flush path calls this only after a commit, never after a failed flush.
    pub fn note_flush_completed(&self) {
        *self.completed_count() += 1;
        self.completed_changed.notify_all();
    }

    /// Request a flush and wait for the next one to commit, up to the backpressure wait.
    ///
    /// Returns `true` when a flush committed in time.
    /// Returns `false` on timeout, or when no executor runs.
    /// A failed flush does not end the wait.
    /// The caller must hold no `AuditLog` lock and no flush gate.
    pub fn await_flush(&self) -> bool {
        if !self.executor_attached() {
            return false;
        }
        let wait = Duration::from_millis(self.backpressure_wait_ms.load(Ordering::Acquire));
        let completed = self.completed_count();
        let start = *completed;
        self.request_flush();
        let (completed, _) =
            match self
                .completed_changed
                .wait_timeout_while(completed, wait, |count| {
                    *count == start && self.executor_attached()
                }) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
        *completed != start
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use super::*;

    #[test]
    fn request_is_taken_once() {
        let control = AuditFlushControl::default();
        assert!(!control.take_flush_request());
        control.request_flush();
        assert!(control.take_flush_request());
        assert!(!control.take_flush_request());
    }

    #[test]
    fn request_before_wait_wakes_the_waiter() {
        let control = AuditFlushControl::default();
        control.request_flush();
        let start = Instant::now();
        // A request made before the wait is not lost.
        control.wait_for_flush_request(Duration::from_secs(30));
        assert!(start.elapsed() < Duration::from_secs(5));
        assert!(control.take_flush_request());
    }

    #[test]
    fn a_request_wakes_the_executor() {
        let control = Arc::new(AuditFlushControl::default());
        let executor = {
            let control = Arc::clone(&control);
            std::thread::spawn(move || {
                let start = Instant::now();
                control.wait_for_flush_request(Duration::from_secs(30));
                start.elapsed()
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        control.request_flush();
        let waited = executor.join().expect("executor thread");
        assert!(waited < Duration::from_secs(5), "waited {waited:?}");
    }

    #[test]
    fn await_without_executor_returns_at_once() {
        let control = AuditFlushControl::default();
        control.set_backpressure_wait(Duration::from_secs(30));
        let start = Instant::now();
        assert!(!control.await_flush());
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn await_returns_when_the_executor_flushes() {
        let control = Arc::new(AuditFlushControl::default());
        control.attach_executor();
        control.set_backpressure_wait(Duration::from_secs(30));
        let executor = {
            let control = Arc::clone(&control);
            std::thread::spawn(move || {
                while !control.take_flush_request() {
                    std::thread::yield_now();
                }
                control.note_flush_completed();
            })
        };
        assert!(
            control.await_flush(),
            "the waiter must see the flush finish"
        );
        executor.join().expect("executor thread");
    }

    #[test]
    fn await_times_out_when_the_executor_is_stuck() {
        let control = AuditFlushControl::default();
        control.attach_executor();
        control.set_backpressure_wait(Duration::from_millis(50));
        let start = Instant::now();
        assert!(
            !control.await_flush(),
            "a stuck executor must not hold the recorder"
        );
        assert!(start.elapsed() >= Duration::from_millis(50));
        assert!(
            control.take_flush_request(),
            "the wait must still request a flush"
        );
    }
}
