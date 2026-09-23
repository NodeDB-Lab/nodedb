// SPDX-License-Identifier: Apache-2.0

//! Hybrid Logical Clock (HLC) for array CRDT sync.
//!
//! Each array op carries an [`Hlc`] that encodes a wall-clock millisecond
//! timestamp (`physical_ms`), a per-millisecond monotonic counter (`logical`),
//! and the originating [`ReplicaId`]. The total order
//! `(physical_ms, logical, replica_id)` provides deterministic tiebreaks under
//! any clock skew without requiring synchronised clocks.
//!
//! `physical_ms` is stored as `u64` but only values up to
//! [`MAX_PHYSICAL_MS`] (2^48 − 1 ≈ year 10 889) are valid. This matches the
//! spec's u48 intent while keeping byte alignment simple.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{ArrayError, ArrayResult};
use crate::sync::replica_id::ReplicaId;

/// Maximum value for `physical_ms` (2^48 − 1).
///
/// Values above this are rejected by [`Hlc::new`] and [`HlcGenerator::next`].
/// This cap corresponds to roughly the year 10 889 CE, so it is not a
/// practical limit.
pub const MAX_PHYSICAL_MS: u64 = (1u64 << 48) - 1;

/// A Hybrid Logical Clock timestamp.
///
/// Total order: `(physical_ms, logical, replica_id.0)` lexicographic.
/// This order is also the byte order produced by [`Hlc::to_bytes`].
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Hlc {
    /// Wall-clock milliseconds since Unix epoch at the originating replica.
    /// Valid range: `0..=MAX_PHYSICAL_MS`.
    pub physical_ms: u64,
    /// Monotonic counter within a single millisecond on one replica.
    pub logical: u16,
    /// Stable identity of the replica that generated this timestamp.
    pub replica_id: ReplicaId,
}

impl Hlc {
    /// The bottom of the total order; useful as a "no ops seen yet" sentinel.
    pub const ZERO: Self = Self {
        physical_ms: 0,
        logical: 0,
        replica_id: ReplicaId(0),
    };

    /// Construct a new [`Hlc`], returning an error if `physical_ms` exceeds
    /// [`MAX_PHYSICAL_MS`].
    pub fn new(physical_ms: u64, logical: u16, replica_id: ReplicaId) -> ArrayResult<Self> {
        if physical_ms > MAX_PHYSICAL_MS {
            return Err(ArrayError::InvalidHlc {
                detail: format!(
                    "physical_ms {physical_ms} exceeds MAX_PHYSICAL_MS {MAX_PHYSICAL_MS}"
                ),
            });
        }
        Ok(Self {
            physical_ms,
            logical,
            replica_id,
        })
    }

    /// True when `self` is strictly later than `other` on the clock alone —
    /// comparing `(physical_ms, logical)` and ignoring `replica_id`.
    ///
    /// [`Ord`] breaks same-instant ties with `replica_id` so that the order is
    /// *total*, which is what deterministic op ordering needs. A freshness test
    /// must not use it: two replicas that independently stamp the same instant
    /// would be ranked by an arbitrary identity, so any gate built on `>` would
    /// admit or reject the same pair of events depending on which replica id
    /// happened to be larger.
    pub fn is_later_than(&self, other: &Self) -> bool {
        (self.physical_ms, self.logical) > (other.physical_ms, other.logical)
    }

    /// Encode as 18 bytes in big-endian order for stable byte-comparable sort.
    ///
    /// Layout: `physical_ms` (8 bytes) | `logical` (2 bytes) | `replica_id` (8 bytes).
    pub fn to_bytes(&self) -> [u8; 18] {
        let mut out = [0u8; 18];
        out[0..8].copy_from_slice(&self.physical_ms.to_be_bytes());
        out[8..10].copy_from_slice(&self.logical.to_be_bytes());
        out[10..18].copy_from_slice(&self.replica_id.0.to_be_bytes());
        out
    }

    /// Decode from a 18-byte big-endian slice produced by [`Hlc::to_bytes`].
    ///
    /// No range validation is performed; the caller should use [`Hlc::new`]
    /// if validation is required.
    pub fn from_bytes(b: &[u8; 18]) -> Self {
        let physical_ms = u64::from_be_bytes(
            b[0..8]
                .try_into()
                .expect("invariant: b is [u8; 18], slice [0..8] is exactly 8 bytes"),
        );
        let logical = u16::from_be_bytes(
            b[8..10]
                .try_into()
                .expect("invariant: b is [u8; 18], slice [8..10] is exactly 2 bytes"),
        );
        let replica_id =
            ReplicaId(u64::from_be_bytes(b[10..18].try_into().expect(
                "invariant: b is [u8; 18], slice [10..18] is exactly 8 bytes",
            )));
        Self {
            physical_ms,
            logical,
            replica_id,
        }
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.physical_ms, self.logical, self.replica_id.0).cmp(&(
            other.physical_ms,
            other.logical,
            other.replica_id.0,
        ))
    }
}

/// Monotonic HLC generator for a single replica.
///
/// Thread-safe via an internal `Mutex`. Lock contention at HLC-generation
/// rates (typically hundreds per second per replica) is negligible.
pub struct HlcGenerator {
    replica_id: ReplicaId,
    /// Guarded state: `(last_physical_ms, last_logical)`.
    state: Mutex<(u64, u16)>,
}

impl HlcGenerator {
    /// Create a new generator for the given replica.
    pub fn new(replica_id: ReplicaId) -> Self {
        Self {
            replica_id,
            state: Mutex::new((0, 0)),
        }
    }

    /// Return the current wall-clock milliseconds since Unix epoch.
    fn now_ms() -> ArrayResult<u64> {
        nodedb_types::clock::since_epoch()
            .map(|d| d.as_millis() as u64)
            .ok_or_else(|| ArrayError::InvalidHlc {
                detail: "system clock before Unix epoch".to_owned(),
            })
    }

    /// Generate the next [`Hlc`], guaranteeing strict monotonicity.
    ///
    /// Implements the standard HLC advancement algorithm:
    /// - `new_physical = max(now_ms, last_physical)`
    /// - if `new_physical == last_physical`: `new_logical = last_logical + 1`
    /// - else: `new_logical = 0`
    pub fn next(&self) -> ArrayResult<Hlc> {
        let now_ms = Self::now_ms()?;
        if now_ms > MAX_PHYSICAL_MS {
            return Err(ArrayError::InvalidHlc {
                detail: format!("system clock {now_ms} exceeds MAX_PHYSICAL_MS"),
            });
        }

        let mut guard = self.state.lock().map_err(|_| ArrayError::HlcLockPoisoned)?;
        let (last_physical, last_logical) = *guard;

        let new_physical = now_ms.max(last_physical);
        let new_logical = if new_physical == last_physical {
            last_logical
                .checked_add(1)
                .ok_or_else(|| ArrayError::InvalidHlc {
                    detail: "logical counter overflow within one millisecond".into(),
                })?
        } else {
            0
        };

        *guard = (new_physical, new_logical);
        drop(guard);

        Hlc::new(new_physical, new_logical, self.replica_id)
    }

    /// Advance local state after observing a remote [`Hlc`].
    ///
    /// Implements the standard HLC merge rule so that subsequent calls to
    /// [`next`](HlcGenerator::next) return timestamps strictly greater than
    /// any observed remote timestamp.
    pub fn observe(&self, remote: Hlc) -> ArrayResult<()> {
        let now_ms = Self::now_ms()?.min(MAX_PHYSICAL_MS);

        let mut guard = self.state.lock().map_err(|_| ArrayError::HlcLockPoisoned)?;
        let (last_physical, last_logical) = *guard;

        let new_physical = now_ms.max(last_physical).max(remote.physical_ms);
        let new_logical = if new_physical == last_physical && new_physical == remote.physical_ms {
            // All three agree on physical; advance logical past both.
            last_logical
                .max(remote.logical)
                .checked_add(1)
                .ok_or_else(|| ArrayError::InvalidHlc {
                    detail: "logical counter overflow during observe".into(),
                })?
        } else if new_physical == last_physical {
            last_logical
                .checked_add(1)
                .ok_or_else(|| ArrayError::InvalidHlc {
                    detail: "logical counter overflow during observe (local wins)".into(),
                })?
        } else if new_physical == remote.physical_ms {
            remote
                .logical
                .checked_add(1)
                .ok_or_else(|| ArrayError::InvalidHlc {
                    detail: "logical counter overflow during observe (remote wins)".into(),
                })?
        } else {
            // now_ms is strictly larger than both; reset logical.
            0
        };

        *guard = (new_physical, new_logical);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_replica() -> ReplicaId {
        ReplicaId::new(42)
    }

    #[test]
    fn monotonic_under_fast_calls() {
        let g = HlcGenerator::new(test_replica());
        let mut prev = g.next().unwrap();
        for _ in 0..999 {
            let curr = g.next().unwrap();
            assert!(
                curr > prev,
                "HLC must be strictly increasing: {curr:?} <= {prev:?}"
            );
            prev = curr;
        }
    }

    #[test]
    fn survives_clock_skew_injection() {
        let g = HlcGenerator::new(test_replica());

        // Get a baseline HLC.
        let baseline = g.next().unwrap();

        // Synthesise a "remote" HLC far in the future (but within MAX_PHYSICAL_MS).
        let future_physical = baseline.physical_ms + 100_000; // +100 seconds
        let future_hlc = Hlc::new(future_physical, 50, ReplicaId::new(99)).unwrap();

        // Observe the future HLC.
        g.observe(future_hlc).unwrap();

        // Next local HLC must be > the observed future HLC.
        let next = g.next().unwrap();
        assert!(
            next > future_hlc,
            "next {next:?} should be > observed future {future_hlc:?}"
        );
    }

    #[test]
    fn to_bytes_roundtrip() {
        let hlc = Hlc::new(123_456_789, 7, ReplicaId::new(0xabcd)).unwrap();
        let bytes = hlc.to_bytes();
        let back = Hlc::from_bytes(&bytes);
        assert_eq!(hlc, back);
    }

    #[test]
    fn byte_order_matches_lexicographic() {
        use std::collections::BTreeMap;

        // Build 100 HLCs with varied fields.
        let replica = ReplicaId::new(1);
        let mut hlcs: Vec<Hlc> = (0u64..100)
            .map(|i| Hlc {
                physical_ms: i / 10,
                logical: (i % 10) as u16,
                replica_id: replica,
            })
            .collect();

        // Sort by Ord.
        let mut by_ord = hlcs.clone();
        by_ord.sort();

        // Sort by to_bytes().
        hlcs.sort_by_key(|h| h.to_bytes());

        assert_eq!(by_ord, hlcs, "byte sort must match Ord sort");

        // Also verify via BTreeMap (key = bytes).
        let mut map: BTreeMap<[u8; 18], Hlc> = BTreeMap::new();
        for h in &by_ord {
            map.insert(h.to_bytes(), *h);
        }
        let map_order: Vec<Hlc> = map.into_values().collect();
        assert_eq!(by_ord, map_order);
    }

    #[test]
    fn serialize_roundtrip() {
        let hlc = Hlc::new(9_999, 3, ReplicaId::new(77)).unwrap();
        let bytes = zerompk::to_msgpack_vec(&hlc).expect("serialize");
        let back: Hlc = zerompk::from_msgpack(&bytes).expect("deserialize");
        assert_eq!(hlc, back);
    }

    #[test]
    fn physical_ms_overflow_errors() {
        let result = Hlc::new(MAX_PHYSICAL_MS + 1, 0, ReplicaId::new(1));
        assert!(
            matches!(result, Err(ArrayError::InvalidHlc { .. })),
            "expected InvalidHlc, got: {result:?}"
        );
    }

    #[test]
    fn hlc_zero_is_minimum() {
        let non_zero = Hlc::new(1, 0, ReplicaId::new(0)).unwrap();
        assert!(Hlc::ZERO < non_zero);
    }
}
