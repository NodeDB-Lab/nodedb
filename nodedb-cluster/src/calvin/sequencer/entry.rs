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
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum AbortReason {
    /// A participant's read-set was stale at validation.
    SerializationConflict,
    /// A participant returned an error, so its read-set was never validated.
    ParticipantError,
    /// A collection the transaction names no longer holds the incarnation
    /// its coordinator planned against: a purge and a same-name create
    /// replaced it.
    CollectionSuperseded,
    /// A participant found state other than the reconnaissance its
    /// coordinator planned against, and wrote nothing. The coordinator reads
    /// again and resubmits.
    PredictionDrift,
    /// A multi-part transaction lost its parts: the sequencer leader that
    /// held them changed before it proposed them all. No participant staged
    /// the whole transaction, and the coordinator resubmits it.
    PartsLost,
    /// A participant could not decode or route its slice of the plans, or
    /// found no local work in them. Every replica rejects the same plans
    /// alike, and a resubmit of them fails the same way.
    PlanRejected,
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
        /// The participant's apply result for the coordinator, opaque to the
        /// sequencer: the host crate encodes and decodes it. Every replica
        /// applies the ack, so the coordinator reads it whether or not its
        /// node hosts a replica of this vShard. Empty when the apply has no
        /// result to report.
        result: Vec<u8>,
        /// The node whose scheduler applied the slice and proposed the ack:
        /// the vShard's data-group leader when it proposed. For tracing; the
        /// first ack of a vShard in log order answers for it.
        from_node: u64,
    },
    /// One participant vShard's durable COMMIT vote for a staged cross-shard
    /// txn: its read-set is valid. An abort vote is `AbortVote`. `Vote`
    /// carries `vshard` because the verdict aggregator must attribute exactly one
    /// vote per participant to know when the tally is complete.
    Vote {
        epoch: u64,
        position: u32,
        vshard: u32,
    },
    /// The global COMMIT verdict for a staged cross-shard txn, proposed by the
    /// sequencer leader once every participant voted commit (see `Vote`). An
    /// abort verdict is `AbortVerdict`. Every replica applies it to store the
    /// authoritative decision. Participants then flush their staged buffer.
    Verdict { epoch: u64, position: u32 },
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
    AbortVote {
        epoch: u64,
        position: u32,
        vshard: u32,
        reason: AbortReason,
    },
    /// The global ABORT verdict for a staged cross-shard txn, carrying the
    /// winning participant reason. Participants drop their staged buffer.
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
    ///
    /// `restore_point` names the cluster restore point the cut takes, `0` for
    /// a backup's cut. Every replica records the sequencer's place at the
    /// point when it applies the marker.
    CutMarker { hlc: u64, restore_point: u64 },
    /// The first entry of a sequencer log a cluster restore rebuilt. It sets
    /// the next epoch the sequencer proposes to `next_epoch`, the epoch that
    /// followed the restore point, so no restored epoch is minted again. It
    /// sets the applied epoch instant to `epoch_system_ms`, the highest one
    /// applied before the point, so every later epoch instant is above it.
    /// `0` when no epoch applied before the point.
    EpochFloor {
        next_epoch: u64,
        epoch_system_ms: i64,
    },
    /// Part `index` of the plans of the multi-part transaction sequenced at
    /// `(epoch, position)`. The leader proposes every part after the header's
    /// epoch batch, in part order. `targets` are the vShards the part's tasks
    /// route to, sorted. Only their schedulers receive it. `chunk` is set on
    /// a part that holds one byte range of one task. A part of a transaction
    /// that is not open (unknown, complete, or abandoned) is ignored.
    TxnPart {
        epoch: u64,
        position: u32,
        index: u32,
        first_task: u32,
        targets: Vec<u32>,
        plans: Vec<u8>,
        chunk: Option<crate::calvin::types::TaskChunk>,
    },
    /// The leader's claim that the multi-part transaction at
    /// `(epoch, position)` lost parts no leader holds any more. Applied only
    /// while the transaction is open: it then aborts with
    /// [`AbortReason::PartsLost`]. A transaction whose last part applied
    /// before this entry ignores it.
    TxnPartsAbandoned { epoch: u64, position: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SequencedTxn, SortedVec, TxClass};
    use nodedb_types::{
        TenantId,
        id::{CollectionKey, DatabaseId},
    };

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
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
    fn vote_msgpack_roundtrip() {
        let entry = SequencerEntry::Vote {
            epoch: 5,
            position: 2,
            vshard: 9,
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
