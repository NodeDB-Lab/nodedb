// SPDX-License-Identifier: Apache-2.0

//! The decided mutation set a governed KV write resolved to.
//!
//! A KV write whose stored image depends on the stored row — an atomic
//! increment, a CAS, a field merge, a transfer — cannot cross the Raft wire
//! carrying a live write predicate: a follower has no writing identity to
//! decide it against. The Control Plane resolves the write against the rows
//! the Data Plane holds, decides the policy there, and ships the mutations
//! themselves.

use nodedb_types::{QualifiedCollection, Surrogate};

/// One storage mutation a resolved KV write applies.
///
/// `collection` is per-mutation, not hoisted onto the write: `TransferItem`
/// moves a row between two collections in one resolved write.
///
/// `precondition` is the apply's drift check, not `KvOp::Cas`'s `expected`
/// semantics: `None` requires the key absent, `Some(bytes)` requires it hold
/// exactly `bytes` (`==`), never conflating absent with present-and-empty.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvResolvedMutation {
    /// Write `value` under `key`, replacing whatever is there. `surrogate`
    /// is the row's bound identity and is never `Surrogate::ZERO`.
    Put {
        collection: QualifiedCollection,
        key: Vec<u8>,
        value: Vec<u8>,
        /// TTL the originating op requested, carried for the durable record's
        /// shape only. `expire_at_ms` is what the apply installs.
        ttl_ms: u64,
        /// Absolute expiry instant (ms since epoch) to install verbatim, `0`
        /// for none. Resolved once and applied identically on every replica —
        /// separate from `ttl_ms` because `ttl_ms == 0` means "clear TTL" on
        /// `KvEngine::put` but "preserve TTL" on the atomic path.
        expire_at_ms: u64,
        surrogate: Surrogate,
        precondition: Option<Vec<u8>>,
    },
    /// Replace the value of the row `key` already holds, keeping its bound
    /// identity. The row must be present and hold exactly `precondition`. A
    /// predicate update resolves to this: it rewrites existing rows and
    /// allocates no identity.
    Rewrite {
        collection: QualifiedCollection,
        key: Vec<u8>,
        value: Vec<u8>,
        /// See `Put::ttl_ms`.
        ttl_ms: u64,
        /// See `Put::expire_at_ms`.
        expire_at_ms: u64,
        precondition: Vec<u8>,
    },
    /// Remove `key`.
    Delete {
        collection: QualifiedCollection,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
    /// Install a TTL on `key`, leaving its body untouched.
    Expire {
        collection: QualifiedCollection,
        key: Vec<u8>,
        ttl_ms: u64,
        /// The wall-clock instant the resolving node read. The apply installs
        /// `resolved_now_ms + ttl_ms`, so every replica reaches the same
        /// absolute expiry instead of reading its own clock.
        resolved_now_ms: u64,
        precondition: Option<Vec<u8>>,
    },
    /// Clear `key`'s TTL, leaving its body untouched.
    Persist {
        collection: QualifiedCollection,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
    /// Reap `key` because its TTL expired at `expire_at_ms`. The apply checks
    /// the stored entry's own expiry, blind to any clock: it reaps only an
    /// entry whose `expire_at_ms` is exactly this one, and ends that version
    /// at the last ordinal of the expiry millisecond. Any other state is a
    /// no-op, never an error: a write replaced the TTL before the reap.
    Reap {
        collection: QualifiedCollection,
        key: Vec<u8>,
        expire_at_ms: u64,
    },
}

impl KvResolvedMutation {
    /// The collection this one mutation targets.
    pub fn collection(&self) -> &QualifiedCollection {
        match self {
            KvResolvedMutation::Put { collection, .. }
            | KvResolvedMutation::Rewrite { collection, .. }
            | KvResolvedMutation::Delete { collection, .. }
            | KvResolvedMutation::Expire { collection, .. }
            | KvResolvedMutation::Persist { collection, .. }
            | KvResolvedMutation::Reap { collection, .. } => collection,
        }
    }

    /// The key this one mutation targets.
    pub fn key(&self) -> &[u8] {
        match self {
            KvResolvedMutation::Put { key, .. }
            | KvResolvedMutation::Rewrite { key, .. }
            | KvResolvedMutation::Delete { key, .. }
            | KvResolvedMutation::Expire { key, .. }
            | KvResolvedMutation::Persist { key, .. }
            | KvResolvedMutation::Reap { key, .. } => key.as_slice(),
        }
    }

    /// The state this mutation requires the key to be in before it applies.
    /// A `Reap` checks the stored expiry instead, so it names none.
    pub fn precondition(&self) -> Option<&[u8]> {
        match self {
            KvResolvedMutation::Reap { .. } => None,
            KvResolvedMutation::Put { precondition, .. }
            | KvResolvedMutation::Delete { precondition, .. }
            | KvResolvedMutation::Expire { precondition, .. }
            | KvResolvedMutation::Persist { precondition, .. } => precondition.as_deref(),
            KvResolvedMutation::Rewrite { precondition, .. } => Some(precondition.as_slice()),
        }
    }
}

/// What `KvOp::ResolveWrite` reports back: every mutation the intercepted
/// write applies, plus the exact response payload the statement returns once
/// they all apply cleanly.
///
/// An empty `mutations` list is a legitimate outcome — a `CAS` whose expected
/// value did not match writes nothing and still owes the caller its
/// `{"success": false, ...}` reply.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvResolveOutcome {
    pub mutations: Vec<KvResolvedMutation>,
    pub response_payload: Vec<u8>,
}
