// SPDX-License-Identifier: BUSL-1.1

//! Plain-data wire twins of the scheduler's lock identity types.
//!
//! The sequencer crate cannot depend on `nodedb`'s `scheduler::lock::LockKey` /
//! `TxnId`, so these owned, zerompk-serializable twins mirror their field shapes
//! for transport inside replicated [`super::super::sequencer::entry::SequencerEntry`]
//! variants. They are decoded back into the real `LockKey` / `TxnId` on the
//! scheduler side (`nodedb`), where both types are in scope.

use serde::{Deserialize, Serialize};

// ── TxnIdWire ─────────────────────────────────────────────────────────────────

/// Wire twin of the scheduler's `TxnId` — a Calvin `(epoch, position)` lock id.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TxnIdWire {
    /// Sequencer epoch.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
}

// ── LockKeyWire ───────────────────────────────────────────────────────────────

/// Wire twin of the scheduler's `LockKey`. Owned (`String` / `Vec<u8>`) so it is
/// self-contained on the replicated log; mirrors the real `LockKey` variants
/// field-for-field.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum LockKeyWire {
    /// Document / Vector engine: a single row identified by its surrogate.
    Surrogate { collection: String, surrogate: u32 },
    /// Key-Value engine: a single row identified by raw bytes.
    Kv { collection: String, key: Vec<u8> },
    /// Graph edge: directed edge identified by a `(src, dst)` surrogate pair.
    Edge {
        collection: String,
        src: u32,
        dst: u32,
    },
    /// A whole collection.
    Collection { collection: String },
    /// One claimed value of one UNIQUE index.
    Unique {
        collection: String,
        index: String,
        value: Vec<u8>,
    },
}

// ── ReleaseReason ─────────────────────────────────────────────────────────────

/// Why a shared reservation is being released. Carried for observability; the
/// scheduler applies the same `release` for every reason.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReleaseReason {
    /// The owning interactive txn committed.
    Commit,
    /// The owning interactive txn aborted.
    Abort,
    /// The reservation lease expired without commit/abort.
    Timeout,
    /// The reservation was reclaimed by lease garbage collection.
    LeaseGc,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_id_wire_msgpack_roundtrip() {
        let w = TxnIdWire {
            epoch: 42,
            position: 7,
        };
        let bytes = zerompk::to_msgpack_vec(&w).expect("encode");
        let decoded: TxnIdWire = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(w, decoded);
    }

    #[test]
    fn lock_key_wire_msgpack_roundtrip_all_variants() {
        let variants = [
            LockKeyWire::Surrogate {
                collection: "users".to_owned(),
                surrogate: 9,
            },
            LockKeyWire::Kv {
                collection: "sessions".to_owned(),
                key: b"abc".to_vec(),
            },
            LockKeyWire::Edge {
                collection: "follows".to_owned(),
                src: 1,
                dst: 2,
            },
            LockKeyWire::Collection {
                collection: "orders".to_owned(),
            },
            LockKeyWire::Unique {
                collection: "users".to_owned(),
                index: "users_email".to_owned(),
                value: b"a@x".to_vec(),
            },
        ];
        for v in variants {
            let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
            let decoded: LockKeyWire = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(v, decoded);
        }
    }

    #[test]
    fn release_reason_msgpack_roundtrip_all_variants() {
        for r in [
            ReleaseReason::Commit,
            ReleaseReason::Abort,
            ReleaseReason::Timeout,
            ReleaseReason::LeaseGc,
        ] {
            let bytes = zerompk::to_msgpack_vec(&r).expect("encode");
            let decoded: ReleaseReason = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(r, decoded);
        }
    }
}
