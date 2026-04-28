//! Metrics for unknown or mismatched wire versions observed on inbound frames.
//!
//! # Wiring
//!
//! This module defines a struct holding per-key atomic counters. An instance
//! should be stored in the cluster subsystem's shared state (e.g. alongside
//! `LoopMetricsRegistry`) and incremented whenever `decode_versioned` returns
//! `WireVersionError::UnsupportedVersion`.
//!
//! PHASE2-WIRE: Expose `WireVersionMetrics` from the cluster subsystem and
//! register its counters with the HTTP metrics endpoint (same pattern as
//! `LoopMetrics`). Until then, the counters accumulate in memory and can be
//! read programmatically by tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A single counter bucket keyed by `(peer_node_id, message_type_tag, version)`.
#[derive(Debug)]
pub struct WireVersionCounter {
    /// Node ID of the peer that sent the unsupported version.
    pub peer_node_id: u64,
    /// Human-readable message type tag (e.g. "MetadataEntry", "RaftRpc",
    /// "SyncFrame"). Stored as a `&'static str` to avoid allocation on the
    /// hot path.
    pub message_type: &'static str,
    /// The unsupported version number observed.
    pub version: u16,
    /// Cumulative count of frames with this key.
    pub count: AtomicU64,
}

impl WireVersionCounter {
    fn new(peer_node_id: u64, message_type: &'static str, version: u16) -> Self {
        Self {
            peer_node_id,
            message_type,
            version,
            count: AtomicU64::new(0),
        }
    }
}

/// Registry of unknown wire-version counters, keyed by
/// `(peer_node_id, message_type, version)`.
///
/// Designed for low-frequency events (version mismatches are rare after a
/// successful negotiation handshake). Uses a `parking_lot::RwLock`-free
/// append-only `Vec` behind an `Arc` for simplicity; contention is negligible.
#[derive(Debug, Default, Clone)]
pub struct WireVersionMetrics {
    inner: Arc<std::sync::Mutex<Vec<Arc<WireVersionCounter>>>>,
}

impl WireVersionMetrics {
    /// Increment the counter for `(peer_node_id, message_type, version)`,
    /// creating a new bucket if this is the first observation.
    pub fn increment_unknown_version(
        &self,
        peer_node_id: u64,
        message_type: &'static str,
        version: u16,
    ) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());

        // Linear scan — buckets accumulate slowly (one per distinct
        // unsupported version per peer type), so O(n) is acceptable.
        for bucket in guard.iter() {
            if bucket.peer_node_id == peer_node_id
                && bucket.message_type == message_type
                && bucket.version == version
            {
                bucket.count.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // First observation: create bucket and immediately record.
        let bucket = Arc::new(WireVersionCounter::new(peer_node_id, message_type, version));
        bucket.count.fetch_add(1, Ordering::Relaxed);
        guard.push(bucket);
    }

    /// Return a snapshot of all counter values, sorted by
    /// `(peer_node_id, message_type, version)`.
    pub fn snapshot(&self) -> Vec<(u64, &'static str, u16, u64)> {
        let guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<(u64, &'static str, u16, u64)> = guard
            .iter()
            .map(|b| {
                (
                    b.peer_node_id,
                    b.message_type,
                    b.version,
                    b.count.load(Ordering::Relaxed),
                )
            })
            .collect();
        out.sort_by_key(|(peer, msg, ver, _)| (*peer, *msg, *ver));
        out
    }

    /// Total number of unknown-version frames seen across all buckets.
    pub fn total(&self) -> u64 {
        let guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        guard.iter().map(|b| b.count.load(Ordering::Relaxed)).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_accumulates() {
        let m = WireVersionMetrics::default();
        m.increment_unknown_version(1, "MetadataEntry", 9999);
        m.increment_unknown_version(1, "MetadataEntry", 9999);
        m.increment_unknown_version(2, "RaftRpc", 9999);
        assert_eq!(m.total(), 3);
        let snap = m.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0], (1, "MetadataEntry", 9999, 2));
        assert_eq!(snap[1], (2, "RaftRpc", 9999, 1));
    }
}
