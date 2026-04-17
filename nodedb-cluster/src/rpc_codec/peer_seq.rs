//! Per-peer monotonic sequence counters and a 64-entry sliding-window
//! replay detector.
//!
//! # Outbound: [`PeerSeqSender`]
//!
//! Each local-node-to-peer direction has a distinct counter. Sent frames
//! carry strictly-increasing sequence numbers starting from 1. `0` is
//! reserved as a sentinel meaning "never sent".
//!
//! # Inbound: [`PeerSeqWindow`]
//!
//! A 64-bit bitmap anchored at `last_accepted_seq`. Frame with sequence
//! `n` is:
//! - accepted and window advanced if `n > last_accepted_seq`
//! - accepted and bit set if `last_accepted_seq - 63 <= n < last_accepted_seq`
//!   and the bit was previously unset
//! - rejected as replay if the bit was already set, or if `n` is older
//!   than the window
//!
//! The window model is identical to IPsec AH/ESP (RFC 4303 §3.4.3).
//! 64 entries is the standard default — large enough that legitimate
//! reordering in-flight (rare over one QUIC stream but possible across
//! QUIC streams) is tolerated, and small enough to fit in a single
//! `u64`.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{ClusterError, Result};

/// Size of the inbound replay-detection window.
pub const REPLAY_WINDOW: u64 = 64;

/// Per-peer outbound monotonic counter. One counter per (local_node,
/// remote_peer) pair. Thread-safe via atomics.
#[derive(Default, Debug)]
pub struct PeerSeqSender {
    counters: RwLock<HashMap<u64, AtomicU64>>,
}

impl PeerSeqSender {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserve and return the next sequence number for frames sent to
    /// `peer_id`. Sequence starts at 1 and is strictly increasing.
    pub fn next(&self, peer_id: u64) -> u64 {
        // Fast path: counter already exists.
        {
            let guard = self.counters.read().unwrap_or_else(|p| p.into_inner());
            if let Some(counter) = guard.get(&peer_id) {
                return counter.fetch_add(1, Ordering::Relaxed) + 1;
            }
        }
        // Slow path: create counter under the write lock.
        let mut guard = self.counters.write().unwrap_or_else(|p| p.into_inner());
        let counter = guard.entry(peer_id).or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Current counter value (0 if no frames have been sent). Test-only.
    #[cfg(test)]
    pub fn peek(&self, peer_id: u64) -> u64 {
        let guard = self.counters.read().unwrap_or_else(|p| p.into_inner());
        guard
            .get(&peer_id)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// Per-peer inbound sliding-window replay detector. One window per
/// (local_node, remote_peer) pair.
#[derive(Default, Debug)]
pub struct PeerSeqWindow {
    windows: RwLock<HashMap<u64, WindowState>>,
}

/// Sliding-window state for one peer.
#[derive(Default, Debug, Clone, Copy)]
struct WindowState {
    /// Highest accepted sequence seen from this peer. 0 if none yet.
    high: u64,
    /// Bitmap of accepted sequences in `[high - 63, high]`. Bit 0 is
    /// `high`, bit 63 is `high - 63`.
    mask: u64,
}

impl PeerSeqWindow {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept `seq` from `peer_id`, rejecting replays and out-of-window
    /// stale frames. Returns `Err(ClusterError::Codec)` on rejection.
    ///
    /// Sequence `0` is always rejected — a well-formed sender starts at
    /// 1, so `0` means "nothing sent", which is not a valid inbound frame.
    pub fn accept(&self, peer_id: u64, seq: u64) -> Result<()> {
        if seq == 0 {
            return Err(ClusterError::Codec {
                detail: format!("peer {peer_id} sent reserved sequence 0"),
            });
        }

        let mut guard = self.windows.write().unwrap_or_else(|p| p.into_inner());
        let state = guard.entry(peer_id).or_default();

        if seq > state.high {
            // Frame advances the window. Shift by the delta and set bit 0.
            let delta = seq - state.high;
            state.mask = if delta >= REPLAY_WINDOW {
                1
            } else {
                (state.mask << delta) | 1
            };
            state.high = seq;
            return Ok(());
        }

        // Frame is `state.high - seq` positions back in the window.
        let offset = state.high - seq;
        if offset >= REPLAY_WINDOW {
            return Err(ClusterError::Codec {
                detail: format!(
                    "peer {peer_id} sent stale sequence {seq}, window high is {}",
                    state.high
                ),
            });
        }
        let bit = 1u64 << offset;
        if state.mask & bit != 0 {
            return Err(ClusterError::Codec {
                detail: format!(
                    "peer {peer_id} replayed sequence {seq} (window high {})",
                    state.high
                ),
            });
        }
        state.mask |= bit;
        Ok(())
    }

    #[cfg(test)]
    pub fn highest(&self, peer_id: u64) -> u64 {
        let guard = self.windows.read().unwrap_or_else(|p| p.into_inner());
        guard.get(&peer_id).map(|w| w.high).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_counter_starts_at_one() {
        let s = PeerSeqSender::new();
        assert_eq!(s.next(1), 1);
        assert_eq!(s.next(1), 2);
        assert_eq!(s.next(1), 3);
    }

    #[test]
    fn outbound_counters_are_independent_per_peer() {
        let s = PeerSeqSender::new();
        assert_eq!(s.next(1), 1);
        assert_eq!(s.next(2), 1);
        assert_eq!(s.next(1), 2);
        assert_eq!(s.next(2), 2);
    }

    #[test]
    fn window_accepts_monotonic_sequence() {
        let w = PeerSeqWindow::new();
        for seq in 1..=10 {
            w.accept(7, seq).unwrap();
        }
        assert_eq!(w.highest(7), 10);
    }

    #[test]
    fn window_rejects_immediate_replay() {
        let w = PeerSeqWindow::new();
        w.accept(1, 1).unwrap();
        let err = w.accept(1, 1).unwrap_err();
        assert!(err.to_string().contains("replayed"));
    }

    #[test]
    fn window_rejects_zero_sequence() {
        let w = PeerSeqWindow::new();
        let err = w.accept(1, 0).unwrap_err();
        assert!(err.to_string().contains("reserved sequence 0"));
    }

    #[test]
    fn window_accepts_in_order_gap_within_window() {
        let w = PeerSeqWindow::new();
        // 1 ... 5 arrive out of order but within window.
        w.accept(1, 5).unwrap();
        w.accept(1, 3).unwrap();
        w.accept(1, 1).unwrap();
        w.accept(1, 2).unwrap();
        w.accept(1, 4).unwrap();
        assert_eq!(w.highest(1), 5);
    }

    #[test]
    fn window_rejects_replay_within_window() {
        let w = PeerSeqWindow::new();
        w.accept(1, 5).unwrap();
        w.accept(1, 3).unwrap();
        let err = w.accept(1, 3).unwrap_err();
        assert!(err.to_string().contains("replayed"));
    }

    #[test]
    fn window_rejects_stale_outside_window() {
        let w = PeerSeqWindow::new();
        w.accept(1, 100).unwrap();
        // Window is [37, 100]. seq=36 is stale.
        let err = w.accept(1, 36).unwrap_err();
        assert!(err.to_string().contains("stale sequence 36"));
        // seq=37 is inside the window edge and acceptable.
        w.accept(1, 37).unwrap();
    }

    #[test]
    fn window_advances_beyond_window_clears_mask() {
        let w = PeerSeqWindow::new();
        w.accept(1, 1).unwrap();
        w.accept(1, 2).unwrap();
        w.accept(1, 100).unwrap();
        // Sequences 1, 2 are now outside the window anchored at 100 and
        // must be rejected on replay (not accepted as fresh within mask).
        let err = w.accept(1, 1).unwrap_err();
        assert!(err.to_string().contains("stale sequence 1"));
    }

    #[test]
    fn windows_are_independent_per_peer() {
        let w = PeerSeqWindow::new();
        w.accept(1, 10).unwrap();
        w.accept(2, 10).unwrap();
        w.accept(1, 9).unwrap();
        w.accept(2, 9).unwrap();
        // Independent — neither is a replay.
        assert_eq!(w.highest(1), 10);
        assert_eq!(w.highest(2), 10);
    }
}
