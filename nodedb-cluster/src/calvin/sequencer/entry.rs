// SPDX-License-Identifier: BUSL-1.1

//! The canonical wire-type for entries proposed to the sequencer Raft group.
//!
//! [`SequencerEntry`] mirrors the [`crate::metadata_group::entry::MetadataEntry`]
//! pattern: a group-local typed enum with zerompk derives. Future variants can
//! be added here without coupling to the metadata group's apply path.

use serde::{Deserialize, Serialize};

use crate::calvin::types::{EpochBatch, LockKeyWire, ReleaseReason, TxnIdWire};

/// Why a staged cross-shard txn aborted. Carried on the abort-only entry
/// variants so the coordinator reports the actual cause, not a blanket
/// serialization conflict.
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
pub enum AbortReason {
    /// A participant's read-set was stale at validation.
    SerializationConflict,
    /// A participant returned an error, so its read-set was never validated.
    ParticipantError,
}

/// An entry in the replicated sequencer log.
///
/// Every epoch batch committed by the sequencer is encoded as one of these
/// variants, proposed to the sequencer Raft group, and applied on every replica
/// by [`super::state_machine::SequencerStateMachine`].
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
pub enum SequencerEntry {
    /// A validated epoch batch ready for global-order assignment.
    ///
    /// The state machine fans the batch out to per-vshard output channels after
    /// applying it to local state.
    EpochBatch { batch: EpochBatch },
    /// Completion acknowledgement from one participating vshard.
    CompletionAck {
        epoch: u64,
        position: u32,
        vshard_id: u32,
    },
    /// OLLP predicate-mismatch signal. Proposed by the per-vShard scheduler when
    /// the active executor returns `OllpRetryRequired`. Applied on ALL sequencer-group
    /// replicas so every node's `CalvinCompletionRegistry` fires `note_ollp_mismatch`,
    /// waking the coordinator's retry-loop waiter wherever it is (including remote
    /// nodes). Mirrors `CompletionAck` — no `vshard_id` because mismatch is keyed
    /// only by `(epoch, position)` like the registry's `TxnId`.
    OllpMismatch { epoch: u64, position: u32 },
    /// Local-routing failure signal. Proposed by the per-vShard scheduler when
    /// `local_calvin_plans` rejects a plan as `Unroutable`, `ControlPlaneOnly`,
    /// or `NotAWrite` — a scheduler-side bug or malformed plan, never a normal
    /// retry condition. Applied on ALL sequencer-group replicas so every node's
    /// `CalvinCompletionRegistry` fires `note_routing_failed`, waking the
    /// coordinator's completion waiter wherever it is with the terminal,
    /// NON-retryable `AttemptOutcome::Failed` outcome. Mirrors `OllpMismatch` —
    /// no `vshard_id` because the failure is keyed only by `(epoch, position)`.
    TxnRoutingFailed {
        epoch: u64,
        position: u32,
        detail: String,
    },
    /// One participant vShard's durable commit vote for a staged cross-shard txn.
    /// Generalizes `OllpMismatch` (an implicit vote=abort). Unlike
    /// `OllpMismatch`/`CompletionAck` which key only on `(epoch, position)`, `Vote`
    /// carries `vshard` because the verdict aggregator must attribute exactly one
    /// vote per participant to know when the tally is complete. `commit: true`
    /// is the read-set-valid vote; `commit: false` appears only in log entries
    /// written before `AbortVote` existed, and carries no reason.
    Vote {
        epoch: u64,
        position: u32,
        vshard: u32,
        commit: bool,
    },
    /// The global commit/abort verdict for a staged cross-shard txn, proposed by
    /// the sequencer leader once every participant has voted (see `Vote`).
    /// `commit: true` means every participant voted commit. Every replica applies
    /// this to store the authoritative decision; participants later flush (commit)
    /// or drop (abort) their staged buffer on it. `commit: false` appears only in
    /// log entries written before `AbortVerdict` existed.
    Verdict {
        epoch: u64,
        position: u32,
        commit: bool,
    },
    /// Install a SHARED reservation on `key` for interactive txn `owner` (its
    /// stable Calvin `(epoch, position)` lock id). Leader-proposed; applied on
    /// every replica so each installs an identical shared lock.
    ReserveRead {
        owner: TxnIdWire,
        vshard: u32,
        key: LockKeyWire,
    },
    /// Release ALL of `owner`'s shared reservations on `vshard`.
    ReleaseReservation {
        owner: TxnIdWire,
        vshard: u32,
        reason: ReleaseReason,
    },
    /// One participant vShard's durable ABORT vote, carrying why it aborted.
    /// `Vote` stays the commit-only vote: adding a field to it would break
    /// decode of already-durable log entries, so the reason rides a new variant.
    AbortVote {
        epoch: u64,
        position: u32,
        vshard: u32,
        reason: AbortReason,
    },
    /// The global ABORT verdict for a staged cross-shard txn, carrying the
    /// winning participant reason. `Verdict` stays the commit-only verdict, for
    /// the same wire-compatibility reason as `AbortVote`.
    AbortVerdict {
        epoch: u64,
        position: u32,
        reason: AbortReason,
    },
    /// A backup's consistent-cut marker, carrying the backup's watermark
    /// `hlc`. Every replica fans it out to each of its vShard schedulers in
    /// log order. A scheduler reports the marker once every transaction
    /// delivered to it before the marker finished, and gives every
    /// transaction delivered after it a commit HLC above `hlc`.
    CutMarker { hlc: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SequencedTxn, SortedVec, TxClass};
    use nodedb_types::{
        TenantId,
        id::{DatabaseId, VShardId},
    };

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    fn make_epoch_batch() -> EpochBatch {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![1, 2]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![3]),
            },
        ]);
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![0xAB],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass");

        EpochBatch {
            epoch: 7,
            txns: vec![SequencedTxn {
                epoch: 7,
                position: 0,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn epoch_batch_msgpack_roundtrip() {
        let entry = SequencerEntry::EpochBatch {
            batch: make_epoch_batch(),
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        match (entry, decoded) {
            (SequencerEntry::EpochBatch { batch: a }, SequencerEntry::EpochBatch { batch: b }) => {
                assert_eq!(a.epoch, b.epoch);
                assert_eq!(a.txns.len(), b.txns.len());
                assert_eq!(a.txns[0].position, b.txns[0].position);
            }
            _ => panic!("decoded wrong sequencer entry variant"),
        }
    }

    #[test]
    fn epoch_batch_roundtrip_preserves_txn_count() {
        let batch = make_epoch_batch();
        let entry = SequencerEntry::EpochBatch { batch };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        let SequencerEntry::EpochBatch { batch } = decoded else {
            panic!("decoded wrong sequencer entry variant");
        };
        assert_eq!(batch.txns.len(), 1);
        assert_eq!(batch.epoch, 7);
    }

    #[test]
    fn txn_routing_failed_msgpack_roundtrip() {
        let entry = SequencerEntry::TxnRoutingFailed {
            epoch: 42,
            position: 3,
            detail: "unroutable plan: Vector".to_owned(),
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        let SequencerEntry::TxnRoutingFailed {
            epoch,
            position,
            detail,
        } = decoded
        else {
            panic!("decoded wrong sequencer entry variant");
        };
        assert_eq!(epoch, 42);
        assert_eq!(position, 3);
        assert_eq!(detail, "unroutable plan: Vector");
    }

    #[test]
    fn vote_msgpack_roundtrip() {
        let entry = SequencerEntry::Vote {
            epoch: 5,
            position: 2,
            vshard: 9,
            commit: true,
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn verdict_msgpack_roundtrip() {
        let entry = SequencerEntry::Verdict {
            epoch: 5,
            position: 2,
            commit: false,
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn abort_vote_msgpack_roundtrip() {
        let entry = SequencerEntry::AbortVote {
            epoch: 5,
            position: 2,
            vshard: 9,
            reason: AbortReason::ParticipantError,
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn abort_verdict_msgpack_roundtrip() {
        let entry = SequencerEntry::AbortVerdict {
            epoch: 5,
            position: 2,
            reason: AbortReason::SerializationConflict,
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn vote_and_verdict_keep_their_pre_existing_wire_shape() {
        // Guards the compat claim behind adding `AbortVote`/`AbortVerdict`:
        // already-durable log entries must still decode. Variant payloads are
        // positional arrays under a strict length check, so the name tag and the
        // field count are what a field addition would have broken.
        let vote = zerompk::to_msgpack_vec(&SequencerEntry::Vote {
            epoch: 5,
            position: 2,
            vshard: 9,
            commit: true,
        })
        .expect("encode");
        assert_eq!(vote[0], 0x92, "enum stays a 2-element [tag, payload] array");
        assert_eq!(&vote[1..6], b"\xA4Vote", "fixstr(4) name tag");
        assert_eq!(vote[6], 0x94, "Vote payload must stay a 4-element array");

        let verdict = zerompk::to_msgpack_vec(&SequencerEntry::Verdict {
            epoch: 5,
            position: 2,
            commit: false,
        })
        .expect("encode");
        assert_eq!(&verdict[1..9], b"\xA7Verdict");
        assert_eq!(
            verdict[9], 0x93,
            "Verdict payload must stay a 3-element array"
        );

        // And both still round-trip through the widened enum.
        let decoded: SequencerEntry = zerompk::from_msgpack(&vote).expect("decode legacy Vote");
        assert_eq!(
            decoded,
            SequencerEntry::Vote {
                epoch: 5,
                position: 2,
                vshard: 9,
                commit: true,
            }
        );
        let decoded: SequencerEntry =
            zerompk::from_msgpack(&verdict).expect("decode legacy Verdict");
        assert_eq!(
            decoded,
            SequencerEntry::Verdict {
                epoch: 5,
                position: 2,
                commit: false,
            }
        );
    }

    #[test]
    fn reserve_read_msgpack_roundtrip() {
        let entry = SequencerEntry::ReserveRead {
            owner: TxnIdWire {
                epoch: 11,
                position: 4,
            },
            vshard: 7,
            key: LockKeyWire::Kv {
                collection: "sessions".to_owned(),
                key: b"hot".to_vec(),
            },
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn release_reservation_msgpack_roundtrip() {
        let entry = SequencerEntry::ReleaseReservation {
            owner: TxnIdWire {
                epoch: 11,
                position: 4,
            },
            vshard: 7,
            reason: ReleaseReason::Commit,
        };
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: SequencerEntry = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }
}
