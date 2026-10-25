// SPDX-License-Identifier: BUSL-1.1

//! The state machine's side of multi-part transactions.
//!
//! A multi-part transaction is open from its header's epoch batch until its
//! last part applies, or until a `TxnPartsAbandoned` for it applies first.
//! Every replica decides both from the log alone, so every replica opens and
//! closes the same transactions at the same entries:
//!
//! - A part of an open transaction fans out to the schedulers of its targets
//!   this node hosts, as a [`SchedulerInput::TxnPart`]. A part of a
//!   transaction that is not open is ignored.
//! - An abandonment of an open transaction aborts it with
//!   `AbortReason::PartsLost` in the completion registry, and fans a
//!   [`SchedulerInput::PartsAbandoned`] out to every participant this node
//!   hosts. An abandonment of a transaction that is not open is ignored.
//!
//! The log compactor keeps every open transaction's header (see
//! [`SequencerStateMachine::min_open_parts_index`]), so a replica that
//! replays the log meets the header before its parts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tracing::{debug, error, warn};

use crate::calvin::TxnId;
use crate::calvin::types::{EpochBatch, SchedulerInput, TxnIdWire};

use super::core::{Delivery, SequencerStateMachine};
use super::snapshot::OpenTxnImage;

/// One open multi-part transaction.
#[derive(Debug)]
struct OpenTxn {
    /// Raft index of the header's epoch batch.
    header_index: u64,
    /// Every participating vShard, for the abandonment fan-out.
    participants: Vec<u32>,
    /// How many parts the header announced.
    count: u32,
    /// The parts applied so far.
    seen: BTreeSet<u32>,
}

/// The open multi-part transactions of a state machine.
#[derive(Debug, Default)]
pub struct OpenParts {
    txns: BTreeMap<TxnId, OpenTxn>,
}

impl OpenParts {
    /// Every open transaction, as a snapshot carries it.
    pub(super) fn images(&self) -> Vec<OpenTxnImage> {
        self.txns
            .iter()
            .map(|(txn, open)| OpenTxnImage {
                epoch: txn.epoch,
                position: txn.position,
                header_index: open.header_index,
                participants: open.participants.clone(),
                count: open.count,
                seen: open.seen.iter().copied().collect(),
            })
            .collect()
    }

    /// The open transactions a snapshot carries.
    pub(super) fn from_images(images: Vec<OpenTxnImage>) -> Self {
        let txns = images
            .into_iter()
            .map(|image| {
                (
                    image.txn(),
                    OpenTxn {
                        header_index: image.header_index,
                        participants: image.participants,
                        count: image.count,
                        seen: image.seen.into_iter().collect(),
                    },
                )
            })
            .collect();
        Self { txns }
    }
}

impl SequencerStateMachine {
    /// Open every multi-part transaction of `batch`, the epoch batch at Raft
    /// index `index`.
    pub(super) fn open_multi_parts(&mut self, index: u64, batch: &EpochBatch) {
        for txn in &batch.txns {
            let Some(manifest) = &txn.tx_class.multi_part else {
                continue;
            };
            self.open_parts.txns.insert(
                TxnId::new(batch.epoch, txn.position),
                OpenTxn {
                    header_index: index,
                    participants: txn
                        .tx_class
                        .participating_vshards()
                        .iter()
                        .map(|vshard| vshard.as_u32())
                        .collect(),
                    count: manifest.part_count,
                    seen: BTreeSet::new(),
                },
            );
        }
    }

    /// Apply part `part` of the transaction `txn`, at Raft index `index`.
    pub(super) fn apply_txn_part(&mut self, index: u64, txn: TxnId, part: PartEntry) {
        let Some(open) = self.open_parts.txns.get_mut(&txn) else {
            debug!(
                epoch = txn.epoch,
                position = txn.position,
                part = part.index,
                "sequencer apply: part of a transaction that is not open; ignored"
            );
            return;
        };
        if part.index >= open.count {
            error!(
                epoch = txn.epoch,
                position = txn.position,
                part = part.index,
                count = open.count,
                raft_index = index,
                "sequencer apply: part index past the header's part count; ignored"
            );
            return;
        }
        if !open.seen.insert(part.index) {
            return;
        }
        let complete = open.seen.len() == open.count as usize;
        if complete {
            self.open_parts.txns.remove(&txn);
        }
        let plans = Arc::new(part.plans);
        for vshard in part.targets {
            self.send_part_input(
                index,
                vshard,
                SchedulerInput::TxnPart {
                    txn: TxnIdWire {
                        epoch: txn.epoch,
                        position: txn.position,
                    },
                    index: part.index,
                    first_task: part.first_task,
                    plans: Arc::clone(&plans),
                    chunk: part.chunk,
                },
            );
        }
    }

    /// Apply the abandonment of the transaction `txn`, at Raft index `index`.
    pub(super) fn apply_parts_abandoned(&mut self, index: u64, txn: TxnId) {
        let Some(open) = self.open_parts.txns.remove(&txn) else {
            debug!(
                epoch = txn.epoch,
                position = txn.position,
                "sequencer apply: abandonment of a transaction that is not open; ignored"
            );
            return;
        };
        warn!(
            epoch = txn.epoch,
            position = txn.position,
            parts_seen = open.seen.len(),
            parts = open.count,
            "sequencer apply: multi-part transaction lost its parts; aborting it"
        );
        self.completion_registry.note_parts_abandoned(txn);
        for vshard in open.participants {
            self.send_part_input(
                index,
                vshard,
                SchedulerInput::PartsAbandoned {
                    txn: TxnIdWire {
                        epoch: txn.epoch,
                        position: txn.position,
                    },
                },
            );
        }
    }

    /// Deliver `input` to `vshard`'s scheduler when this node hosts it. An
    /// armed vShard, or a full or closed channel, leaves it to the replay,
    /// as for every other input.
    fn send_part_input(&self, index: u64, vshard: u32, input: SchedulerInput) {
        match self.deliver(index, vshard, input) {
            Delivery::NotHosted | Delivery::Sent | Delivery::Deferred => {}
            Delivery::DroppedFull => warn!(
                vshard,
                raft_index = index,
                "sequencer apply: vshard channel full (backpressure); part input left to replay"
            ),
            Delivery::DroppedClosed => warn!(
                vshard,
                "sequencer apply: vshard sender gone; scheduler may have exited (part input)"
            ),
        }
    }

    /// Every open multi-part transaction, in sequence order. The sequencer
    /// leader abandons the ones whose parts it does not hold.
    pub fn open_multi_part_txns(&self) -> Vec<TxnId> {
        self.open_parts.txns.keys().copied().collect()
    }

    /// The lowest Raft index of an open transaction's header, or `None`
    /// when none is open. The log compactor keeps it.
    pub fn min_open_parts_index(&self) -> Option<u64> {
        self.open_parts
            .txns
            .values()
            .map(|open| open.header_index)
            .min()
    }
}

/// The fields of a `SequencerEntry::TxnPart` the apply path reads.
#[derive(Debug)]
pub(super) struct PartEntry {
    pub index: u32,
    pub first_task: u32,
    pub targets: Vec<u32>,
    pub plans: Vec<u8>,
    pub chunk: Option<crate::calvin::types::TaskChunk>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::TenantId;
    use nodedb_types::id::{CollectionKey, DatabaseId};
    use tokio::sync::mpsc;

    use super::*;
    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::sequencer::entry::SequencerEntry;
    use crate::calvin::types::{
        EngineKeySet, MultiPartPlans, PartStreamId, ReadWriteSet, SequencedTxn, SortedVec,
        TaskChunk, TxClass, VShardParts, VersionedReadSet,
    };

    /// Two collections homed on distinct vShards, with those vShards.
    fn two_homes() -> ((String, u32), (String, u32)) {
        let home = |name: &str| {
            CollectionKey::from_bare(DatabaseId::DEFAULT, name)
                .vshard()
                .as_u32()
        };
        let first = ("col_0".to_owned(), home("col_0"));
        let second = (1u32..512)
            .map(|i| format!("col_{i}"))
            .find(|name| home(name) != first.1)
            .map(|name| {
                let vshard = home(&name);
                (name, vshard)
            })
            .expect("two distinct homes in 512 names");
        (first, second)
    }

    /// The epoch-0 batch whose one txn is a two-part header: part 0 targets
    /// `va`, part 1 targets `vb`.
    fn header_batch() -> (EpochBatch, u32, u32) {
        header_batch_of(2)
    }

    /// The epoch-0 batch whose one txn is the header of `parts` parts.
    fn header_batch_of(parts: u32) -> (EpochBatch, u32, u32) {
        let ((col_a, va), (col_b, vb)) = two_homes();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![1]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![2]),
            },
        ]);
        let mut tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        let mut per_vshard = vec![
            VShardParts {
                vshard: va,
                parts: parts - 1,
            },
            VShardParts {
                vshard: vb,
                parts: 1,
            },
        ];
        per_vshard.sort_by_key(|entry| entry.vshard);
        tx_class.multi_part = Some(MultiPartPlans {
            stream: PartStreamId { node: 1, seq: 1 },
            part_count: parts,
            total_tasks: parts,
            user_write: true,
            client_write: true,
            per_vshard,
        });
        let batch = EpochBatch {
            epoch: 0,
            txns: vec![SequencedTxn {
                epoch: 0,
                position: 0,
                tx_class: std::sync::Arc::new(tx_class),
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
                local_plans: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        };
        (batch, va, vb)
    }

    fn encode(entry: &SequencerEntry) -> Vec<u8> {
        zerompk::to_msgpack_vec(entry).expect("encode")
    }

    fn part_entry(index: u32, target: u32) -> SequencerEntry {
        sized_part_entry(index, target, 1)
    }

    fn sized_part_entry(index: u32, target: u32, bytes: usize) -> SequencerEntry {
        SequencerEntry::TxnPart {
            epoch: 0,
            position: 0,
            index,
            first_task: index,
            targets: vec![target],
            plans: vec![0x90; bytes],
            chunk: None,
        }
    }

    fn abandon_entry() -> SequencerEntry {
        SequencerEntry::TxnPartsAbandoned {
            epoch: 0,
            position: 0,
        }
    }

    struct Fixture {
        sm: SequencerStateMachine,
        registry: Arc<CalvinCompletionRegistry>,
        rx_a: mpsc::Receiver<SchedulerInput>,
        rx_b: mpsc::Receiver<SchedulerInput>,
        va: u32,
        vb: u32,
    }

    /// A state machine that applied the header at Raft index 1, with the
    /// header's `Txn` input already taken from both channels.
    fn fixture() -> Fixture {
        fixture_of(2)
    }

    /// [`fixture`] for a header of `parts` parts.
    fn fixture_of(parts: u32) -> Fixture {
        let (batch, va, vb) = header_batch_of(parts);
        let depth = usize::try_from(parts).unwrap_or(usize::MAX) + 16;
        let (tx_a, mut rx_a) = mpsc::channel(depth);
        let (tx_b, mut rx_b) = mpsc::channel(depth);
        let registry = CalvinCompletionRegistry::new_detached();
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, registry.clone());
        sm.apply(1, &encode(&SequencerEntry::EpochBatch { batch }));
        assert!(matches!(rx_a.try_recv(), Ok(SchedulerInput::Txn(_))));
        assert!(matches!(rx_b.try_recv(), Ok(SchedulerInput::Txn(_))));
        Fixture {
            sm,
            registry,
            rx_a,
            rx_b,
            va,
            vb,
        }
    }

    /// A part reaches only its targets. An abandonment while a part is
    /// missing aborts the txn, reaches every participant, and ignores the
    /// parts that follow it.
    #[test]
    fn an_abandonment_while_a_part_is_missing_aborts_the_whole_txn() {
        let mut f = fixture();
        let txn = TxnId::new(0, 0);
        assert_eq!(f.sm.open_multi_part_txns(), [txn]);
        assert_eq!(f.sm.min_open_parts_index(), Some(1));

        f.sm.apply(2, &encode(&part_entry(0, f.va)));
        assert!(matches!(
            f.rx_a.try_recv(),
            Ok(SchedulerInput::TxnPart { index: 0, .. })
        ));
        assert!(f.rx_b.try_recv().is_err(), "part 0 does not target vb");

        f.sm.apply(3, &encode(&abandon_entry()));
        assert_eq!(f.registry.verdict(txn), Some(false));
        assert!(matches!(
            f.rx_a.try_recv(),
            Ok(SchedulerInput::PartsAbandoned { .. })
        ));
        assert!(matches!(
            f.rx_b.try_recv(),
            Ok(SchedulerInput::PartsAbandoned { .. })
        ));
        assert!(f.sm.open_multi_part_txns().is_empty());
        assert_eq!(f.sm.min_open_parts_index(), None);

        f.sm.apply(4, &encode(&part_entry(1, f.vb)));
        assert!(f.rx_b.try_recv().is_err(), "a part after the abandonment");
    }

    /// The last part closes the txn. An abandonment after it is ignored, and
    /// a part delivered twice fans out once.
    #[test]
    fn an_abandonment_after_the_last_part_is_ignored() {
        let mut f = fixture();
        let txn = TxnId::new(0, 0);
        f.sm.apply(2, &encode(&part_entry(0, f.va)));
        f.sm.apply(3, &encode(&part_entry(0, f.va)));
        f.sm.apply(4, &encode(&part_entry(1, f.vb)));
        assert!(f.sm.open_multi_part_txns().is_empty());
        assert!(matches!(
            f.rx_a.try_recv(),
            Ok(SchedulerInput::TxnPart { index: 0, .. })
        ));
        assert!(f.rx_a.try_recv().is_err(), "the duplicate part is dropped");
        assert!(matches!(
            f.rx_b.try_recv(),
            Ok(SchedulerInput::TxnPart { index: 1, .. })
        ));

        f.sm.apply(5, &encode(&abandon_entry()));
        assert_eq!(f.registry.verdict(txn), None);
        assert!(f.rx_a.try_recv().is_err());
        assert!(f.rx_b.try_recv().is_err());
    }

    /// A txn of 72 one-MiB parts, over the 64 MiB RPC limit, stays open and
    /// uncommitted until its last part applies. Every part reaches its
    /// target whole, a chunk part keeps its place in its task, and nothing
    /// aborts it.
    #[test]
    fn a_txn_over_64_mib_of_parts_applies_whole() {
        const PARTS: u32 = 72;
        let mut f = fixture_of(PARTS);
        let txn = TxnId::new(0, 0);
        for index in 0..PARTS - 1 {
            let mut entry = sized_part_entry(index, f.va, 1 << 20);
            if index == 5
                && let SequencerEntry::TxnPart { chunk, .. } = &mut entry
            {
                *chunk = Some(TaskChunk {
                    offset: 0,
                    total_len: 1 << 20,
                });
            }
            f.sm.apply(u64::from(index) + 2, &encode(&entry));
            assert_eq!(
                f.sm.open_multi_part_txns(),
                [txn],
                "open before the last part"
            );
        }
        f.sm.apply(u64::from(PARTS) + 1, &encode(&part_entry(PARTS - 1, f.vb)));
        assert!(
            f.sm.open_multi_part_txns().is_empty(),
            "the last part closes it"
        );
        assert_eq!(f.registry.verdict(txn), None, "nothing aborted it");

        let mut bytes = 0usize;
        let mut chunks = 0;
        while let Ok(input) = f.rx_a.try_recv() {
            let SchedulerInput::TxnPart { plans, chunk, .. } = input else {
                panic!("only parts after the header");
            };
            bytes += plans.len();
            chunks += usize::from(chunk.is_some());
        }
        assert_eq!(bytes, 71 << 20);
        assert_eq!(chunks, 1);
        assert!(matches!(
            f.rx_b.try_recv(),
            Ok(SchedulerInput::TxnPart { index: 71, .. })
        ));
    }

    /// A leader change mid-stream: the next leader's abandonment lands after
    /// 40 of 72 parts. The whole txn aborts, every participant is told, and
    /// the parts the old leader proposed after it change nothing.
    #[test]
    fn a_leader_change_mid_stream_aborts_the_whole_txn() {
        const PARTS: u32 = 72;
        let mut f = fixture_of(PARTS);
        let txn = TxnId::new(0, 0);
        for index in 0..40 {
            f.sm.apply(
                u64::from(index) + 2,
                &encode(&sized_part_entry(index, f.va, 1 << 10)),
            );
        }
        f.sm.apply(42, &encode(&abandon_entry()));
        assert_eq!(f.registry.verdict(txn), Some(false));
        assert!(f.sm.open_multi_part_txns().is_empty());
        for index in 40..PARTS - 1 {
            f.sm.apply(u64::from(index) + 3, &encode(&part_entry(index, f.va)));
        }
        f.sm.apply(99, &encode(&part_entry(PARTS - 1, f.vb)));

        let mut parts = 0;
        let mut abandoned = 0;
        while let Ok(input) = f.rx_a.try_recv() {
            match input {
                SchedulerInput::TxnPart { .. } => parts += 1,
                SchedulerInput::PartsAbandoned { .. } => abandoned += 1,
                other => panic!("unexpected input {other:?}"),
            }
        }
        assert_eq!((parts, abandoned), (40, 1));
        assert!(matches!(
            f.rx_b.try_recv(),
            Ok(SchedulerInput::PartsAbandoned { .. })
        ));
        assert!(f.rx_b.try_recv().is_err(), "no part after the abandonment");
    }

    /// A part dropped at a full channel arms the catch-up. The next part is
    /// deferred to the replay too, though the channel has room again, so the
    /// scheduler never receives part `I + 1` before part `I`.
    #[test]
    fn a_part_after_a_dropped_part_is_never_sent_live() {
        let (batch, va, _vb) = header_batch_of(3);
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        // The header fills the one-slot channel.
        sm.apply(1, &encode(&SequencerEntry::EpochBatch { batch }));
        sm.apply(2, &encode(&part_entry(0, va)));
        assert_eq!(sm.peek_catch_up_from(va), Some(2), "part 0 dropped");

        assert!(matches!(rx_a.try_recv(), Ok(SchedulerInput::Txn(_))));
        sm.apply(3, &encode(&part_entry(1, va)));
        assert!(rx_a.try_recv().is_err(), "part 1 waits for the replay");
        sm.clear_catch_up_up_to(va, 2);
        assert_eq!(
            sm.peek_catch_up_from(va),
            Some(3),
            "the replay still owes part 1"
        );
    }

    /// Catch-up replay hands a vShard the header, the parts that target it,
    /// and every abandonment, in log order.
    #[test]
    fn replay_hands_a_vshard_only_the_parts_that_target_it() {
        let (batch, va, vb) = header_batch();
        let sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        let log: Vec<nodedb_raft::LogEntry> = [
            SequencerEntry::EpochBatch { batch },
            part_entry(0, va),
            abandon_entry(),
        ]
        .iter()
        .zip(1u64..)
        .map(|(entry, index)| nodedb_raft::LogEntry {
            term: 1,
            index,
            data: encode(entry),
        })
        .collect();

        let for_a = sm.replay_epochs_for_vshard(&log, va, 0, 0);
        assert!(matches!(
            for_a.as_slice(),
            [
                SchedulerInput::Txn(_),
                SchedulerInput::TxnPart { index: 0, .. },
                SchedulerInput::PartsAbandoned { .. },
            ]
        ));
        let for_b = sm.replay_epochs_for_vshard(&log, vb, 0, 0);
        assert!(matches!(
            for_b.as_slice(),
            [
                SchedulerInput::Txn(_),
                SchedulerInput::PartsAbandoned { .. }
            ]
        ));
    }
}
