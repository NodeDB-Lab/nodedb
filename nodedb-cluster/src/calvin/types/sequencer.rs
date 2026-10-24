// SPDX-License-Identifier: BUSL-1.1

//! Sequencer output types: [`SequencedTxn`] and [`EpochBatch`].
//!
//! These are the Raft-replicated entries produced by the Calvin sequencer.
//! Every replica applies the same `EpochBatch` in the same order, guaranteeing
//! determinism.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::lock_wire::TxnIdWire;
use super::transaction::TxClass;

// ── SequencedTxn ──────────────────────────────────────────────────────────────

/// A transaction that has been assigned a global position by the sequencer.
///
/// The `(epoch, position)` pair is globally unique and totally ordered across
/// all vShards. Every shard that participates in the transaction will see this
/// txn at the same `(epoch, position)` in its scheduler input stream.
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
#[msgpack(map)]
pub struct SequencedTxn {
    /// Sequencer epoch in which this transaction was admitted.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
    /// The fully-declared transaction class.
    ///
    /// Shared: the sequencer fans one class out to every participating
    /// vShard, so each copy clones a pointer. It encodes as the inner class.
    pub tx_class: Arc<TxClass>,
    /// Wall-clock ms at epoch creation (read once on the sequencer leader).
    ///
    /// This is the single deterministic timestamp source for all Calvin write
    /// paths. Engine handlers that need a "current time" (bitemporal sys_from,
    /// KV TTL expire_at, timeseries system_ms) MUST use this value instead of
    /// reading the wall clock independently, ensuring byte-identical state
    /// across all replicas. Wire-additive: zerompk returns default (0) when
    /// decoding older entries that lack this field.
    #[serde(default)]
    #[msgpack(default)]
    pub epoch_system_ms: i64,
    /// Number of positions in this epoch that target the vShard this copy is
    /// delivered to.
    ///
    /// Stamped per-vShard by the sequencer state machine at fan-out time (like
    /// `epoch_system_ms`), NOT at sequencing time — the value is `0` on the
    /// replicated log and is filled in when the batch is fanned out to each
    /// participating vShard's scheduler. It lets a scheduler know, without a
    /// priori knowledge, how many positions of an epoch it must apply before
    /// that epoch is fully applied on its vShard — the input to the scheduler's
    /// exactly-once per-`(epoch, position)` applied gate and fully-applied
    /// watermark. Wire-additive: zerompk returns default (0) when decoding
    /// entries that predate this field.
    #[serde(default)]
    #[msgpack(default)]
    pub epoch_vshard_txn_count: u32,
    /// Optional lock-table identity distinct from this txn's `(epoch, position)`
    /// apply-slot. `None` (the default) means the lock owner is the apply slot —
    /// the established behavior. `Some(id)` lets a different identity (e.g. a
    /// read-reservation id from a separate position band) own the exclusive lock
    /// while this txn keeps its own fresh apply-slot for the watermark and
    /// completion path. Wire-additive: decodes to `None` on older log entries.
    #[serde(default)]
    #[msgpack(default)]
    pub lock_owner: Option<TxnIdWire>,
    /// The plans this vShard runs, once a multi-part txn's parts assembled.
    ///
    /// Local to one vShard's copy, so the shared class stays one allocation.
    /// Never encoded: `#[msgpack(ignore)]` decodes it as `None`.
    #[serde(skip)]
    #[msgpack(ignore)]
    pub local_plans: Option<Arc<LocalPlans>>,
}

/// The local plans of one vShard's copy of a multi-part txn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPlans {
    /// The local tasks as a plan batch, in their order in the whole txn.
    pub plans: Vec<u8>,
    /// Indexes into `plans` of the tasks a trigger body buffered. Sorted, no repeats.
    pub body_plans: Vec<u32>,
}

impl SequencedTxn {
    /// The plan batch this vShard runs: the local plans once assembled, else the class's.
    pub fn plans(&self) -> &[u8] {
        match &self.local_plans {
            Some(local) => &local.plans,
            None => &self.tx_class.plans,
        }
    }

    /// The trigger-body indexes into [`Self::plans`]. Sorted, no repeats.
    pub fn body_plans(&self) -> &[u32] {
        match &self.local_plans {
            Some(local) => &local.body_plans,
            None => &self.tx_class.body_plans,
        }
    }
}

// ── EpochBatch ────────────────────────────────────────────────────────────────

/// A fully-ordered batch of transactions for one sequencer epoch.
///
/// This is the Raft-replicated entry emitted by the sequencer. Every replica
/// applies the same `EpochBatch` in the same order, guaranteeing determinism.
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
pub struct EpochBatch {
    /// The epoch number. Monotonically increasing across all batches.
    pub epoch: u64,
    /// Ordered transactions in this epoch. Position is the index in this vec.
    pub txns: Vec<SequencedTxn>,
    /// Wall-clock ms read ONCE on the sequencer leader at epoch creation.
    ///
    /// This single timestamp is the deterministic time anchor for every
    /// transaction in this epoch. When the state machine fans `SequencedTxn`s
    /// out to per-shard channels it copies this value into each txn's
    /// `epoch_system_ms` field, making it available to engine handlers without
    /// threading it through every intermediate layer.
    pub epoch_system_ms: i64,
}

#[cfg(test)]
mod tests {
    use nodedb_types::TenantId;
    use nodedb_types::id::{CollectionKey, DatabaseId};

    use super::super::primitives::{EngineKeySet, SortedVec, VersionedReadSet};
    use super::super::read_write_set::ReadWriteSet;
    use super::*;

    fn doc_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Document {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn multi_vshard_write_set() -> ReadWriteSet {
        // Use two different collections that hash to different vShards.
        // We can't pick known-distinct names without running the hash, so we
        // scan at test time.
        let (a, b) = find_two_distinct_collections();
        ReadWriteSet::new(vec![doc_set(&a, vec![1, 2]), doc_set(&b, vec![3])])
    }

    /// Find two collection names whose vShards differ.
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

    fn make_tx_class(write_set: ReadWriteSet) -> TxClass {
        TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![0x01, 0x02],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass")
    }

    #[test]
    fn sequenced_txn_msgpack_roundtrip() {
        let tx_class = make_tx_class(multi_vshard_write_set());
        let st = SequencedTxn {
            epoch: 42,
            position: 7,
            tx_class: std::sync::Arc::new(tx_class),
            epoch_system_ms: 1_700_000_000_000,
            epoch_vshard_txn_count: 3,
            lock_owner: None,
            local_plans: Some(std::sync::Arc::new(LocalPlans {
                plans: vec![0x09],
                body_plans: vec![0],
            })),
        };
        assert_eq!(st.plans(), &[0x09]);
        assert_eq!(st.body_plans(), &[0]);
        let bytes = zerompk::to_msgpack_vec(&st).unwrap();
        let mut decoded: SequencedTxn = zerompk::from_msgpack(&bytes).unwrap();
        std::sync::Arc::make_mut(&mut decoded.tx_class)
            .restore_derived()
            .expect("restore derived");
        assert_eq!(st.epoch, decoded.epoch);
        assert_eq!(st.position, decoded.position);
        assert_eq!(st.epoch_system_ms, decoded.epoch_system_ms);
        assert_eq!(st.tx_class.write_set, decoded.tx_class.write_set);
        assert!(decoded.local_plans.is_none(), "local plans never encode");
        assert_eq!(decoded.plans(), st.tx_class.plans.as_slice());
    }

    #[test]
    fn epoch_batch_msgpack_roundtrip() {
        let tc = make_tx_class(multi_vshard_write_set());
        let batch = EpochBatch {
            epoch: 1,
            txns: vec![
                SequencedTxn {
                    epoch: 1,
                    position: 0,
                    tx_class: std::sync::Arc::new(tc.clone()),
                    epoch_system_ms: 1_700_000_000_000,
                    epoch_vshard_txn_count: 2,
                    lock_owner: None,
                    local_plans: None,
                },
                SequencedTxn {
                    epoch: 1,
                    position: 1,
                    tx_class: std::sync::Arc::new(tc),
                    epoch_system_ms: 1_700_000_000_000,
                    epoch_vshard_txn_count: 2,
                    lock_owner: None,
                    local_plans: None,
                },
            ],
            epoch_system_ms: 1_700_000_000_000,
        };
        let bytes = zerompk::to_msgpack_vec(&batch).unwrap();
        let mut decoded: EpochBatch = zerompk::from_msgpack(&bytes).unwrap();
        for txn in &mut decoded.txns {
            std::sync::Arc::make_mut(&mut txn.tx_class)
                .restore_derived()
                .expect("restore derived");
        }
        assert_eq!(batch.epoch, decoded.epoch);
        assert_eq!(batch.epoch_system_ms, decoded.epoch_system_ms);
        assert_eq!(batch.txns.len(), decoded.txns.len());
        assert_eq!(
            batch.txns[0].epoch_system_ms,
            decoded.txns[0].epoch_system_ms
        );
        assert_eq!(
            batch.txns[0].tx_class.write_set,
            decoded.txns[0].tx_class.write_set
        );
    }
}
