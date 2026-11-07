// SPDX-License-Identifier: BUSL-1.1

//! Per-peer bound on the audit rows bearer refusals write.
//!
//! A refusal skips the lockout, the login rate limiter and the escalation ladder, by design.
//! No per-IP limiter runs on the bearer routes: HTTP, native `OidcBearer`, OTLP and sync handshakes.
//! So an unauthenticated client could force one audit WAL record per bad token.
//!
//! Each peer gets [`REFUSAL_ROWS_PER_WINDOW`] rows per window.
//! All peers together get [`MAX_ROWS_PER_WINDOW`] rows per window.
//! Past either limit a refusal is counted, and the window's close writes one row with the count.
//! The close writes at most [`MAX_SUMMARY_ROWS_PER_WINDOW`] rows.
//! Its smaller counts fold into the [`OTHER_PEERS`] row, so the sum stays exact.
//! The map holds at most [`MAX_TRACKED_PEERS`] peers, and later peers count in that row too.
//! The throttle changes audit rows only: the client's answer and the auth metrics stay the same.
//! An `AuthSuccess` row never passes through here.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Mutex, MutexGuard};

use crate::control::security::blacklist::ip::normalize_peer_ip;

/// Refusal rows one peer writes in one window.
pub const REFUSAL_ROWS_PER_WINDOW: u32 = 20;

/// Refusal rows all peers write in one window.
pub const MAX_ROWS_PER_WINDOW: u32 = 1024;

/// Summary rows one window's close writes.
pub const MAX_SUMMARY_ROWS_PER_WINDOW: usize = 64;

/// Peers one window tracks. A source address is attacker-chosen, so this map must stay bounded.
pub const MAX_TRACKED_PEERS: usize = 4096;

/// The audit source of the summary row for refusals no named row carries.
pub const OTHER_PEERS: &str = "<other peers>";

/// Longest text kept for a peer that is not an address.
const MAX_LABEL_CHARS: usize = 64;

/// Bits of an IPv6 address that name one peer. One client can hold a whole /64.
const IPV6_PEER_PREFIX_BITS: u32 = 64;

/// The refusals of one peer that one window did not record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusalSummary {
    /// The audit source of the summary row.
    pub source: String,
    /// Refusals with no row of their own.
    pub unrecorded: u64,
}

impl RefusalSummary {
    /// The detail of the summary row.
    pub fn detail(&self) -> String {
        format!(
            "bearer login refused: {} more refusals in the last window, past the limits of \
             {REFUSAL_ROWS_PER_WINDOW} rows per peer and {MAX_ROWS_PER_WINDOW} rows in total",
            self.unrecorded
        )
    }
}

/// One peer's refusals in one window.
#[derive(Default)]
struct PeerCount {
    refusals: u64,
    /// Refusals of this peer that wrote their own row.
    rows: u32,
}

#[derive(Default)]
struct Window {
    /// Counts per peer in this window.
    refusals: HashMap<String, PeerCount>,
    /// Refusals of peers past the map's bound.
    other_peers: u64,
    /// Rows written in this window, across every peer.
    rows: u32,
}

/// Decides which bearer refusals write their own audit row.
#[derive(Default)]
pub struct BearerRefusalThrottle {
    window: Mutex<Window>,
}

impl BearerRefusalThrottle {
    fn lock(&self) -> MutexGuard<'_, Window> {
        match self.window.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Count one refusal from `peer_addr`. Returns whether it writes its own audit row.
    pub fn admit(&self, peer_addr: &str) -> bool {
        let key = peer_key(peer_addr);
        let mut guard = self.lock();
        let window = &mut *guard;
        if !window.refusals.contains_key(&key) && window.refusals.len() >= MAX_TRACKED_PEERS {
            window.other_peers = window.other_peers.saturating_add(1);
            return false;
        }
        let peer = window.refusals.entry(key).or_default();
        peer.refusals = peer.refusals.saturating_add(1);
        if peer.rows >= REFUSAL_ROWS_PER_WINDOW || window.rows >= MAX_ROWS_PER_WINDOW {
            return false;
        }
        peer.rows += 1;
        window.rows += 1;
        true
    }

    /// End this window and start the next.
    ///
    /// Returns at most [`MAX_SUMMARY_ROWS_PER_WINDOW`] summaries, largest counts first.
    /// Smaller counts fold into the [`OTHER_PEERS`] summary, so the sum of `unrecorded` stays exact.
    pub fn close_window(&self) -> Vec<RefusalSummary> {
        let window = std::mem::take(&mut *self.lock());
        let mut summaries: Vec<RefusalSummary> = window
            .refusals
            .into_iter()
            .filter_map(|(source, peer)| {
                let unrecorded = peer.refusals.saturating_sub(u64::from(peer.rows));
                (unrecorded > 0).then_some(RefusalSummary { source, unrecorded })
            })
            .collect();
        summaries.sort_unstable_by(|a, b| {
            b.unrecorded
                .cmp(&a.unrecorded)
                .then_with(|| a.source.cmp(&b.source))
        });
        let mut other_peers = window.other_peers;
        // The row of other peers takes one of the summary rows when anything folds into it.
        let named = if other_peers > 0 || summaries.len() > MAX_SUMMARY_ROWS_PER_WINDOW {
            MAX_SUMMARY_ROWS_PER_WINDOW - 1
        } else {
            MAX_SUMMARY_ROWS_PER_WINDOW
        };
        if summaries.len() > named {
            for folded in summaries.drain(named..) {
                other_peers = other_peers.saturating_add(folded.unrecorded);
            }
        }
        if other_peers > 0 {
            summaries.push(RefusalSummary {
                source: OTHER_PEERS.to_string(),
                unrecorded: other_peers,
            });
        }
        summaries
    }
}

/// The key one peer counts under: its IP address, or its IPv6 /64.
fn peer_key(peer_addr: &str) -> String {
    match normalize_peer_ip(peer_addr) {
        Some(IpAddr::V4(ip)) => ip.to_string(),
        Some(IpAddr::V6(ip)) => {
            let network = u128::from(ip) & (u128::MAX << (128 - IPV6_PEER_PREFIX_BITS));
            format!(
                "{}/{IPV6_PEER_PREFIX_BITS}",
                std::net::Ipv6Addr::from(network)
            )
        }
        None => peer_addr.chars().take(MAX_LABEL_CHARS).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_peer_past_its_limit_is_counted_once_in_a_summary() {
        let throttle = BearerRefusalThrottle::default();
        for _ in 0..REFUSAL_ROWS_PER_WINDOW {
            assert!(throttle.admit("203.0.113.7:9000"));
        }
        for _ in 0..5 {
            assert!(
                !throttle.admit("203.0.113.7:9001"),
                "the port is not the peer"
            );
        }
        assert!(
            throttle.admit("203.0.113.8:9000"),
            "another peer has its own limit"
        );

        assert_eq!(
            throttle.close_window(),
            vec![RefusalSummary {
                source: "203.0.113.7".to_string(),
                unrecorded: 5,
            }]
        );
        assert!(throttle.close_window().is_empty(), "a window closes once");
        assert!(
            throttle.admit("203.0.113.7:9000"),
            "a new window starts over"
        );
    }

    #[test]
    fn an_ipv6_slash_64_counts_as_one_peer() {
        assert_eq!(
            peer_key("[2001:db8:1:2:aaaa::1]:443"),
            peer_key("[2001:db8:1:2:bbbb::2]:80")
        );
        assert_ne!(
            peer_key("[2001:db8:1:2::1]:443"),
            peer_key("[2001:db8:1:3::1]:443")
        );
    }

    #[test]
    fn many_peers_stay_bounded_and_share_one_count() {
        let throttle = BearerRefusalThrottle::default();
        for i in 0..MAX_TRACKED_PEERS {
            throttle.admit(&format!("peer-{i}"));
        }
        for i in 0..10 {
            assert!(!throttle.admit(&format!("late-{i}")));
        }
        assert_eq!(throttle.lock().refusals.len(), MAX_TRACKED_PEERS);
        assert_eq!(throttle.lock().other_peers, 10);
    }

    /// Every peer stays under its own limit, and the total limit still stops the rows.
    #[test]
    fn the_total_row_limit_holds_across_peers() {
        let throttle = BearerRefusalThrottle::default();
        let refusals = MAX_ROWS_PER_WINDOW as usize + 100;
        let rows = (0..refusals)
            .filter(|i| throttle.admit(&format!("peer-{i}")))
            .count();
        assert_eq!(rows, MAX_ROWS_PER_WINDOW as usize);

        let unrecorded: u64 = throttle
            .close_window()
            .iter()
            .map(|summary| summary.unrecorded)
            .sum();
        assert_eq!(unrecorded, 100, "a refusal past the limit stays counted");
    }

    /// A close writes a bounded number of rows, and the largest counts keep their names.
    #[test]
    fn summary_rows_stay_bounded_and_the_sum_stays_exact() {
        let throttle = BearerRefusalThrottle::default();
        // The first peer takes its rows and passes its limit by the most.
        for _ in 0..REFUSAL_ROWS_PER_WINDOW + 9 {
            throttle.admit("loud-peer");
        }
        // Spend the total limit, so each later peer's one refusal has no row.
        let spent = MAX_ROWS_PER_WINDOW - REFUSAL_ROWS_PER_WINDOW;
        for i in 0..spent {
            throttle.admit(&format!("filler-{i}"));
        }
        let quiet = MAX_SUMMARY_ROWS_PER_WINDOW * 3;
        for i in 0..quiet {
            assert!(!throttle.admit(&format!("quiet-{i}")));
        }

        let summaries = throttle.close_window();
        assert_eq!(summaries.len(), MAX_SUMMARY_ROWS_PER_WINDOW);
        assert_eq!(summaries[0].source, "loud-peer");
        assert_eq!(summaries[0].unrecorded, 9);
        let last = summaries.last().expect("a summary");
        assert_eq!(last.source, OTHER_PEERS);
        let total: u64 = summaries.iter().map(|summary| summary.unrecorded).sum();
        assert_eq!(total, 9 + quiet as u64, "folding keeps the sum exact");
    }

    #[test]
    fn exactly_the_summary_limit_folds_nothing() {
        let throttle = BearerRefusalThrottle::default();
        for i in 0..MAX_ROWS_PER_WINDOW {
            throttle.admit(&format!("filler-{i}"));
        }
        for i in 0..MAX_SUMMARY_ROWS_PER_WINDOW {
            throttle.admit(&format!("quiet-{i}"));
        }
        let summaries = throttle.close_window();
        assert_eq!(summaries.len(), MAX_SUMMARY_ROWS_PER_WINDOW);
        assert!(
            summaries
                .iter()
                .all(|summary| summary.source != OTHER_PEERS)
        );
    }

    #[test]
    fn a_long_label_is_capped() {
        let key = peer_key(&"x".repeat(MAX_LABEL_CHARS * 4));
        assert_eq!(key.chars().count(), MAX_LABEL_CHARS);
    }
}
