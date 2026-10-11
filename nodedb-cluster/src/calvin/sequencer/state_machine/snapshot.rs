// SPDX-License-Identifier: BUSL-1.1

//! The sequencer state machine's Raft snapshot.
//!
//! A replica rebuilds the state machine by applying the sequencer log. A
//! follower that installs a snapshot never applies the entries the snapshot
//! covers, so the snapshot carries the state they built:
//!
//! - the last applied epoch and its epoch instant, so a later leader never
//!   mints an epoch or an instant at or below a committed one;
//! - every open multi-part transaction, with the parts already applied, so
//!   the follower closes it on the same entry as every other replica, and
//!   never abandons one the others committed;
//! - whether the captured state holds every committed entry's effect, so a
//!   follower seeds its epoch from it only when it does.
//!
//! The leader captures the snapshot at its applied index, on the Raft tick
//! thread between apply batches, so it holds exactly the entries through
//! that index. The halt flag is local to a replica and is not carried.

use crate::calvin::TxnId;
use crate::error::ClusterError;

use super::core::{NOT_YET_APPLIED, SequencerStateMachine};
use super::history::HistoryOrigin;

/// The state the sequencer log built through `applied_index`.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct SequencerSnapshot {
    applied_index: u64,
    last_applied_epoch: Option<u64>,
    last_epoch_system_ms: Option<i64>,
    open_txns: Vec<OpenTxnImage>,
    history_known: bool,
}

/// One open multi-part transaction in a [`SequencerSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(super) struct OpenTxnImage {
    pub epoch: u64,
    pub position: u32,
    pub header_index: u64,
    pub participants: Vec<u32>,
    pub count: u32,
    pub seen: Vec<u32>,
}

impl OpenTxnImage {
    pub(super) fn txn(&self) -> TxnId {
        TxnId::new(self.epoch, self.position)
    }
}

impl SequencerSnapshot {
    /// The sequencer log index the snapshot holds the state through.
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    /// Encode the snapshot as a Raft snapshot payload.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        zerompk::to_msgpack_vec(self).map_err(|e| ClusterError::Codec {
            detail: format!("sequencer snapshot encode: {e}"),
        })
    }

    /// Decode a Raft snapshot payload.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        zerompk::from_msgpack(bytes).map_err(|e| ClusterError::Codec {
            detail: format!("sequencer snapshot decode: {e}"),
        })
    }
}

impl SequencerStateMachine {
    /// Capture the state this replica built through `applied_index`, the
    /// group's applied index.
    ///
    /// The caller holds the state machine's lock on the Raft tick thread,
    /// between apply batches. An entry with no payload changes no state, so
    /// the state machine's own applied index can trail `applied_index`.
    pub fn capture_snapshot(&self, applied_index: u64) -> SequencerSnapshot {
        SequencerSnapshot {
            applied_index,
            last_applied_epoch: self.last_applied_epoch(),
            last_epoch_system_ms: self.last_epoch_system_ms,
            open_txns: self.open_parts.images(),
            history_known: self.history.is_known(),
        }
    }

    /// Replace the state the log built with `snapshot`, installed as the
    /// group's log boundary. Entries after the snapshot apply on top.
    ///
    /// The history origin becomes the snapshot, or stays unknown when the
    /// capturing replica's history was unknown.
    pub fn restore_snapshot(&mut self, snapshot: SequencerSnapshot) {
        self.last_applied_epoch = snapshot.last_applied_epoch.unwrap_or(NOT_YET_APPLIED);
        self.last_epoch_system_ms = snapshot.last_epoch_system_ms;
        self.last_committed_index = snapshot.applied_index;
        self.open_parts = super::parts::OpenParts::from_images(snapshot.open_txns);
        self.history = if snapshot.history_known {
            HistoryOrigin::Snapshot {
                through: snapshot.applied_index,
            }
        } else {
            HistoryOrigin::Unknown
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::mpsc;

    use super::*;
    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::sequencer::entry::SequencerEntry;
    use crate::calvin::types::{
        EngineKeySet, EpochBatch, MultiPartPlans, PartStreamId, ReadWriteSet, SchedulerInput,
        SequencedTxn, SortedVec, TxClass, VShardParts, VersionedReadSet,
    };
    use nodedb_types::TenantId;
    use nodedb_types::id::{CollectionKey, DatabaseId};

    fn encode(entry: &SequencerEntry) -> Vec<u8> {
        zerompk::to_msgpack_vec(entry).expect("encode entry")
    }

    /// The epoch-0 batch whose one txn is a two-part header writing `col_0`
    /// and a collection on another vShard, and `col_0`'s vShard, which both
    /// parts target. A multi-vShard write set makes the class valid.
    fn header_batch() -> (EpochBatch, u32) {
        let home = |name: &str| {
            CollectionKey::from_bare(DatabaseId::DEFAULT, name)
                .vshard()
                .as_u32()
        };
        let vshard = home("col_0");
        let other = (1u32..512)
            .map(|i| format!("col_{i}"))
            .find(|name| home(name) != vshard)
            .expect("a second home in 512 names");
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: "col_0".to_owned(),
                surrogates: SortedVec::new(vec![1]),
            },
            EngineKeySet::Document {
                collection: other,
                surrogates: SortedVec::new(vec![2]),
            },
        ]);
        let mut tx_class = TxClass::new(
            ReadWriteSet::new(Vec::new()),
            write_set,
            Vec::new(),
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("tx class");
        tx_class.multi_part = Some(MultiPartPlans {
            stream: PartStreamId { node: 1, seq: 1 },
            part_count: 2,
            total_tasks: 2,
            user_write: true,
            client_write: true,
            per_vshard: vec![VShardParts { vshard, parts: 2 }],
        });
        let batch = EpochBatch {
            epoch: 0,
            txns: vec![SequencedTxn {
                epoch: 0,
                position: 0,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        };
        (batch, vshard)
    }

    fn part(index: u32, target: u32) -> SequencerEntry {
        SequencerEntry::TxnPart {
            epoch: 0,
            position: 0,
            index,
            first_task: index,
            targets: vec![target],
            plans: vec![0x90],
            chunk: None,
        }
    }

    /// A follower that installs a snapshot taken between a txn's parts holds
    /// the txn open with its applied parts. The last part closes it on the
    /// follower as on every other replica, and reaches the follower's
    /// scheduler. A snapshot round-trips through its payload.
    #[test]
    fn a_snapshot_carries_an_open_txn_across_an_install() {
        let (batch, vshard) = header_batch();
        let mut leader =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        leader.apply(1, &encode(&SequencerEntry::EpochBatch { batch }));
        leader.apply(2, &encode(&part(0, vshard)));
        let snapshot = leader.capture_snapshot(3);
        let bytes = snapshot.encode().expect("encode");
        let decoded = SequencerSnapshot::decode(&bytes).expect("decode");
        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.applied_index(), 3);

        let (tx, mut rx) = mpsc::channel(8);
        let mut senders = HashMap::new();
        senders.insert(vshard, tx);
        let mut follower =
            SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        follower.restore_snapshot(decoded);
        assert_eq!(follower.open_multi_part_txns(), [TxnId::new(0, 0)]);
        assert_eq!(follower.min_open_parts_index(), Some(1));
        assert_eq!(follower.last_applied_epoch(), Some(0));
        assert_eq!(follower.current_committed_index(), Some(3));
        assert_eq!(
            follower.history_origin(),
            HistoryOrigin::Snapshot { through: 3 }
        );

        follower.apply(4, &encode(&part(1, vshard)));
        assert!(
            follower.open_multi_part_txns().is_empty(),
            "the last part closes it"
        );
        assert!(matches!(
            rx.try_recv(),
            Ok(SchedulerInput::TxnPart { index: 1, .. })
        ));
    }

    /// A snapshot captured with unknown history restores as unknown, so the
    /// follower never treats it as complete.
    #[test]
    fn a_snapshot_of_unknown_history_restores_as_unknown() {
        let mut leader =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        leader.mark_history_unknown();
        let snapshot = leader.capture_snapshot(7);

        let mut follower =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        follower.restore_snapshot(snapshot);
        assert_eq!(follower.history_origin(), HistoryOrigin::Unknown);
    }
}
