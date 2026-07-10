// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a session transaction block. Keys the per-transaction staging
/// overlay. Globally unique across the cluster: the minting node's id is
/// packed into the high bits (see [`TxnId::from_origin`]), so two coordinators
/// that each mint the same per-node sequence never collide on the same
/// staging-overlay key of a shared owner shard.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TxnId(u64);

impl TxnId {
    /// Low bits reserved for the per-node monotonic sequence; the remaining
    /// high bits carry the origin node id. 48 bits of sequence is ~281e12
    /// transactions per process lifetime; 16 bits of node id is 65_536 nodes.
    const SEQ_BITS: u32 = 48;
    const SEQ_MASK: u64 = (1u64 << Self::SEQ_BITS) - 1;

    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Mint a globally-unique id from the origin `node_id` and a per-node
    /// monotonic `seq`uence. The node id occupies the high 16 bits, the
    /// sequence the low 48.
    ///
    /// `node_id == 0` (single-node / no-cluster deployments, where
    /// `SharedState.node_id` is never assigned) yields `TxnId(seq)` —
    /// byte-identical to the pre-cluster scheme, so single-node behavior and
    /// value-sensitive tests are unchanged. A non-zero origin disambiguates
    /// otherwise-identical sequences from different coordinators, which is
    /// what makes cross-node read-your-own-write staging collision-free.
    pub const fn from_origin(node_id: u64, seq: u64) -> Self {
        Self((node_id << Self::SEQ_BITS) | (seq & Self::SEQ_MASK))
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::TxnId;

    #[test]
    fn node_zero_is_identity() {
        // Single-node / no-cluster: the id equals the bare sequence, keeping
        // the pre-cluster scheme (and value-sensitive tests) byte-identical.
        assert_eq!(TxnId::from_origin(0, 1).as_u64(), 1);
        assert_eq!(TxnId::from_origin(0, 42).as_u64(), 42);
    }

    #[test]
    fn distinct_origins_never_collide_on_equal_sequence() {
        // The exact flaky cross-node RYOW collision: two coordinators each
        // minting sequence 1. Packing the origin in keeps them distinct, so
        // they never share a staging-overlay key on a common owner shard.
        assert_ne!(TxnId::from_origin(1, 1), TxnId::from_origin(2, 1));
        assert_ne!(TxnId::from_origin(1, 1), TxnId::from_origin(3, 1));
    }

    #[test]
    fn same_origin_distinct_sequence_stays_unique() {
        assert_ne!(TxnId::from_origin(3, 1), TxnId::from_origin(3, 2));
    }

    #[test]
    fn origin_is_recoverable_from_high_bits() {
        // Sanity on the layout: sequence in the low 48, origin in the high 16.
        let id = TxnId::from_origin(7, 99).as_u64();
        assert_eq!(id >> 48, 7);
        assert_eq!(id & ((1u64 << 48) - 1), 99);
    }
}
