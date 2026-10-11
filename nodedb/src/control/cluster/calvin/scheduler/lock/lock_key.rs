// SPDX-License-Identifier: BUSL-1.1

//! Deterministic identity types for the Calvin lock table.
//!
//! [`LockKey`] names one lockable unit; [`TxnId`] names one transaction. Both
//! derive a total order so that `BTreeMap`/`BTreeSet` iteration is stable and
//! reproducible across replicas — a correctness requirement, not a style
//! preference.

use std::sync::Arc;

// ── LockKey ───────────────────────────────────────────────────────────────────

/// A deterministic key identifying one lockable unit in the Calvin lock table.
///
/// Keys are totally ordered by their `Ord` impl so `BTreeMap` and `BTreeSet`
/// over `LockKey` produce a stable, portable ordering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockKey {
    /// Document / Vector engine: a single row identified by its surrogate.
    Surrogate {
        /// Collection name (interned string for cheap clone).
        collection: Arc<str>,
        /// Global surrogate identifier for this row.
        surrogate: u32,
    },
    /// Key-Value engine: a single row identified by raw bytes.
    Kv {
        collection: Arc<str>,
        /// Raw byte key (interned slice for cheap clone).
        key: Arc<[u8]>,
    },
    /// Graph edge: directed edge identified by (src, dst) surrogate pair.
    Edge {
        collection: Arc<str>,
        src: u32,
        dst: u32,
    },
    /// A whole collection. Writers of single rows hold it `Intent`. Writers
    /// of the whole collection hold it `Exclusive`. Predicate readers hold it
    /// `Shared`.
    Collection { collection: Arc<str> },
    /// One claimed value of one UNIQUE index. Writers that claim the value
    /// hold it `Exclusive`, so two claims of one value serialize.
    Unique {
        collection: Arc<str>,
        index: Arc<str>,
        value: Arc<[u8]>,
    },
}

// ── TxnId ─────────────────────────────────────────────────────────────────────

/// A globally unique, totally ordered transaction identifier.
///
/// `(epoch, position)` is the Calvin schedule position.  `BTreeMap<TxnId, _>`
/// and `BTreeSet<TxnId>` iterate in `(epoch, position)` order, which is the
/// deterministic dispatch order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId {
    /// Sequencer epoch.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
}

impl TxnId {
    /// Reserved epoch band for autocommit fast-path lock holders. A holder in
    /// this band never collides with a real Calvin `(epoch, position)` schedule
    /// position, so the fast path and the deterministic scheduler can share one
    /// lock table without identity aliasing.
    pub const AUTOCOMMIT_EPOCH: u64 = u64::MAX;

    pub fn new(epoch: u64, position: u32) -> Self {
        Self { epoch, position }
    }

    /// Whether this id belongs to the reserved autocommit fast-path band. FIFO
    /// waiter ordering is insertion-order (`VecDeque`), and `BTreeMap` iteration
    /// merely sorts the reserved band last — neither is affected by the band.
    pub fn is_autocommit(&self) -> bool {
        self.epoch == Self::AUTOCOMMIT_EPOCH
    }

    /// Start of the reservation position band. A read-reservation owner always
    /// has `position >= this`; a real Calvin batch position never does (batch
    /// positions run `0..max_txns_per_epoch`, far below this band). Mirrors the
    /// sequencer's reservation-position band — the two MUST stay in sync.
    pub const RESERVATION_POSITION_BAND: u32 = 1 << 31;

    /// Whether this id owns a read reservation (its position is in the
    /// reservation band) rather than a real Calvin batch transaction.
    pub fn is_reservation(&self) -> bool {
        self.position >= Self::RESERVATION_POSITION_BAND
    }
}
