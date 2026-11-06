// SPDX-License-Identifier: BUSL-1.1

//! Send-rate throttle for cross-cluster mirror observers.
//!
//! The source side tracks bytes-in-flight for every connected mirror observer.
//! When the in-flight byte count exceeds [`SendThrottle::cap`] the source
//! stops pushing new snapshot chunks or log entries for that mirror until the
//! observer drains below [`SendThrottle::resume_threshold`].
//!
//! # Design
//!
//! The throttle is intentionally simple: a per-mirror `AtomicU64` counter.
//! The source increments it before writing a chunk over the QUIC stream and
//! decrements it when the observer's ack arrives.  If the ack is never
//! received (e.g. the observer crashes mid-stream) the counter stays high
//! until the link is torn down, at which point [`SendThrottle::reset`] brings
//! it back to zero.
//!
//! This matches the `ObserverState::MAX_PENDING` entry-count cap in
//! `nodedb-raft` but operates at the byte level so large snapshot chunks do
//! not slip through.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default bytes-in-flight cap per mirror observer: 64 MiB.
pub const DEFAULT_CAP_BYTES: u64 = 64 * 1024 * 1024;

/// Resume threshold as a fraction of the cap (resume when below 75% of cap).
const RESUME_FRACTION: f64 = 0.75;

/// Per-mirror send throttle shared between the sender task and the ack handler.
///
/// Clone-cheap: backed by `Arc<AtomicU64>`.
#[derive(Debug, Clone)]
pub struct SendThrottle {
    /// Current bytes in flight to this mirror (acknowledged bytes have been
    /// decremented).
    in_flight: Arc<AtomicU64>,
    /// Maximum bytes in flight before the source pauses sends.
    cap: u64,
    /// Threshold below which sends resume after being paused.
    resume_threshold: u64,
}

impl SendThrottle {
    /// Create a new throttle with the given cap.
    pub fn new(cap: u64) -> Self {
        let resume_threshold = (cap as f64 * RESUME_FRACTION) as u64;
        Self {
            in_flight: Arc::new(AtomicU64::new(0)),
            cap,
            resume_threshold,
        }
    }

    /// Create a throttle with [`DEFAULT_CAP_BYTES`].
    pub fn default_cap() -> Self {
        Self::new(DEFAULT_CAP_BYTES)
    }

    /// Current bytes in flight (approximate, relaxed ordering).
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Whether the source should currently send to this mirror.
    ///
    /// Returns `true` when `in_flight < cap`.
    pub fn can_send(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) < self.cap
    }

    /// Whether the source may resume after being throttled.
    ///
    /// Returns `true` when `in_flight <= resume_threshold`.
    pub fn can_resume(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) <= self.resume_threshold
    }

    /// Record that `bytes` have been dispatched toward the mirror.
    ///
    /// Call this before writing a chunk to the QUIC stream.
    pub fn charge(&self, bytes: u64) {
        self.in_flight.fetch_add(bytes, Ordering::AcqRel);
    }

    /// Record that `bytes` have been acknowledged by the mirror.
    ///
    /// Saturates at zero to guard against double-acks.
    pub fn ack(&self, bytes: u64) {
        // One atomic step: a charge that lands during the ack stays counted.
        let mut held = self.in_flight.load(Ordering::Acquire);
        loop {
            let next = held.saturating_sub(bytes);
            match self.in_flight.compare_exchange_weak(
                held,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => held = current,
            }
        }
    }

    /// Reset the counter to zero (called on link teardown).
    pub fn reset(&self) {
        self.in_flight.store(0, Ordering::Release);
    }

    /// The configured cap in bytes.
    pub fn cap(&self) -> u64 {
        self.cap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_charge_and_ack() {
        let t = SendThrottle::new(100);
        assert!(t.can_send());
        t.charge(80);
        assert!(t.can_send());
        t.charge(30);
        // 110 >= cap 100 → cannot send
        assert!(!t.can_send());
        t.ack(30);
        // 80 <= resume 75 → false; 80 > 75 → still above resume
        assert!(!t.can_resume());
        t.ack(10);
        // 70 <= 75 → can resume
        assert!(t.can_resume());
    }

    #[test]
    fn throttle_reset_zeroes_counter() {
        let t = SendThrottle::new(100);
        t.charge(99);
        t.reset();
        assert_eq!(t.in_flight(), 0);
        assert!(t.can_send());
    }

    #[test]
    fn ack_does_not_underflow() {
        let t = SendThrottle::new(100);
        t.charge(10);
        t.ack(50); // ack more than in flight
        assert_eq!(t.in_flight(), 0);
    }

    /// Charges and acks race on many threads and none is lost. The balance
    /// ends where the operations say.
    #[test]
    fn concurrent_charges_and_acks_lose_nothing() {
        const ROUNDS: u64 = 20_000;
        let throttle = SendThrottle::new(u64::MAX);
        throttle.charge(4 * ROUNDS);
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let charger = throttle.clone();
                let acker = throttle.clone();
                (
                    std::thread::spawn(move || {
                        for _ in 0..ROUNDS {
                            charger.charge(3);
                        }
                    }),
                    std::thread::spawn(move || {
                        for _ in 0..ROUNDS {
                            acker.ack(3);
                        }
                    }),
                )
            })
            .collect();
        for (charger, acker) in workers {
            charger.join().expect("charger");
            acker.join().expect("acker");
        }
        assert_eq!(throttle.in_flight(), 4 * ROUNDS);
    }

    #[test]
    fn clone_shares_state() {
        let t = SendThrottle::new(100);
        let t2 = t.clone();
        t.charge(60);
        assert_eq!(t2.in_flight(), 60);
    }
}
