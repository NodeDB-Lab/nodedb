// SPDX-License-Identifier: BUSL-1.1

//! Pre-validation pass for a candidate epoch batch.
//!
//! The validator is a pure deterministic function: it takes the candidate
//! transactions admitted this epoch and produces a deterministically-ordered
//! list of admitted (positioned) and rejected transactions. It performs no
//! I/O, reads no wall clock, and touches no global state, so it re-runs
//! byte-identically on a failover leader.
//!
//! # Ordering
//!
//! Transactions are first sorted by `(inbox_seq, tenant_id, hash(plans))`.
//! `inbox_seq` is assigned by the sequencer leader at admit time and is the
//! primary tiebreaker. `xxh3_64` (from `xxhash-rust`) is used for `hash(plans)`
//! because it is byte-stable across processes and architectures. This sort
//! makes the sorted index order identical to the sort-key order, so "smallest
//! sort key" is "smallest index" and "largest sort key" is "largest index".
//!
//! # Conflict handling (read-aware)
//!
//! The scheduler locks a txn's write keys exclusive and its read keys shared,
//! so a reader and a writer of one key serialize by `position`. Per-key last-write-LSN
//! validation means a reader `R` of key `K` aborts at commit only if a
//! *lower-position* writer `W` of `K` committed first (its write LSN exceeds
//! `R`'s read version). Giving `R` a lower position than `W` converts that
//! read-after-write abort into a write-after-read that commits.
//!
//! So the validator:
//!
//! - Builds, per key `K`, the set of transactions that read `K` and the set
//!   that write `K`.
//! - For every `(reader r, writer w)` pair on the same key with `r != w`, adds
//!   a directed edge `r -> w` ("`r` must get a lower position than `w`").
//! - Emits positions in a deterministic topological order of those edges,
//!   which places every reader ahead of the conflicting writers it can — the
//!   read-after-write → write-after-read reorder that avoids the abort.
//!
//! Write-write pairs add no edge: two writers of `K` both commit (they apply in
//! position order, last write wins) and are simply ordered by the sort-key
//! tiebreak. Read-read pairs add no edge either.
//!
//! Only a cyclic read/write dependency (which cannot be satisfied by any single
//! serial order) forces a rejection: the cycle is broken deterministically by
//! rejecting the member with the largest sort key (highest `inbox_seq`),
//! mirroring "the later transaction loses". Positions on the admitted set are
//! dense `0..(n - num_rejected)` in emission order.

use std::collections::{BTreeMap, BTreeSet};

use xxhash_rust::xxh3::xxh3_64;

use crate::calvin::sequencer::error::SequencerError;
use crate::calvin::sequencer::inbox::{AdmittedTx, RejectedTx};
use crate::calvin::sequencer::metrics::ConflictKey;
use crate::calvin::types::{EngineKeySet, ReadWriteSet, SequencedTxn};

/// Canonical, order-comparable identity of a single key across engines:
/// `(engine_discriminant, collection, key_bytes)`. Used as the `BTreeMap` key
/// for the reader/writer maps so all iteration feeding output is deterministic.
type KeyId = (u8, String, Vec<u8>);

/// A single flattened key drawn from one transaction's read or write set.
#[derive(Debug)]
struct FlatKey {
    /// Discriminant tag for the key-set variant (Document=0, Vector=1, Kv=2,
    /// Edge=3, Array=4, Collection=5, Unique=6).
    discriminant: u8,
    /// Static engine name used when building conflict-context keys.
    engine_name: &'static str,
    /// Collection name.
    collection: String,
    /// Serialized key bytes.
    key_bytes: Vec<u8>,
}

impl FlatKey {
    /// The order-comparable identity of this key.
    fn key_id(&self) -> KeyId {
        (
            self.discriminant,
            self.collection.clone(),
            self.key_bytes.clone(),
        )
    }
}

/// Flatten one read-or-write set into its constituent per-key entries.
fn flatten_set(set: &ReadWriteSet) -> Vec<FlatKey> {
    let mut out = Vec::new();
    for key_set in &set.0 {
        match key_set {
            EngineKeySet::Document {
                collection,
                surrogates,
            } => {
                for &s in surrogates.as_slice() {
                    out.push(FlatKey {
                        discriminant: 0,
                        engine_name: "document",
                        collection: collection.clone(),
                        key_bytes: s.to_le_bytes().to_vec(),
                    });
                }
            }
            EngineKeySet::Vector {
                collection,
                surrogates,
            } => {
                for &s in surrogates.as_slice() {
                    out.push(FlatKey {
                        discriminant: 1,
                        engine_name: "vector",
                        collection: collection.clone(),
                        key_bytes: s.to_le_bytes().to_vec(),
                    });
                }
            }
            EngineKeySet::Kv { collection, keys } => {
                for k in keys.as_slice() {
                    out.push(FlatKey {
                        discriminant: 2,
                        engine_name: "kv",
                        collection: collection.clone(),
                        key_bytes: k.clone(),
                    });
                }
            }
            EngineKeySet::Edge {
                collection, edges, ..
            } => {
                for &(src, dst) in edges.as_slice() {
                    let mut key_bytes = src.to_le_bytes().to_vec();
                    key_bytes.extend_from_slice(&dst.to_le_bytes());
                    out.push(FlatKey {
                        discriminant: 3,
                        engine_name: "edge",
                        collection: collection.clone(),
                        key_bytes,
                    });
                }
            }
            // The whole array on each vShard it names.
            EngineKeySet::Array {
                collection,
                vshards,
            } => {
                for &vshard in vshards.as_slice() {
                    out.push(FlatKey {
                        discriminant: 4,
                        engine_name: "array",
                        collection: collection.clone(),
                        key_bytes: vshard.to_le_bytes().to_vec(),
                    });
                }
            }
            // The whole collection: one key with no row bytes.
            EngineKeySet::Collection { collection, .. } => {
                out.push(FlatKey {
                    discriminant: 5,
                    engine_name: "collection",
                    collection: collection.clone(),
                    key_bytes: Vec::new(),
                });
            }
            // One key per claimed value: the index name, a zero byte, the value.
            EngineKeySet::Unique {
                collection,
                index,
                values,
            } => {
                for value in values.as_slice() {
                    let mut key_bytes = Vec::with_capacity(index.len() + 1 + value.len());
                    key_bytes.extend_from_slice(index.as_bytes());
                    key_bytes.push(0);
                    key_bytes.extend_from_slice(value);
                    out.push(FlatKey {
                        discriminant: 6,
                        engine_name: "unique",
                        collection: collection.clone(),
                        key_bytes,
                    });
                }
            }
        }
    }
    out
}

/// Sort key for admitted transactions: `(inbox_seq, tenant_id, hash(plans))`.
fn admitted_sort_key(tx: &AdmittedTx) -> (u64, u64, u64) {
    let plan_hash = xxh3_64(&tx.tx_class.plans);
    (tx.inbox_seq, tx.tx_class.tenant_id.as_u64(), plan_hash)
}

/// Validate a candidate batch of admitted transactions.
///
/// Returns `(Vec<(u64, SequencedTxn)>, Vec<RejectedTx>)`:
/// - `SequencedTxn.position` is 0-based and dense among the admitted
///   transactions only.
/// - Each admitted entry is paired with its `inbox_seq`.
/// - `RejectedTx.reason` is `SequencerError::Conflict { position_admitted }`;
///   rejections occur only when a read/write dependency cycle cannot be
///   serialized.
///
/// The function is pure — no I/O, no wall clock, no global state, deterministic.
pub fn validate_batch_with_assignments(
    epoch: u64,
    mut candidates: Vec<AdmittedTx>,
) -> (Vec<(u64, SequencedTxn)>, Vec<RejectedTx>) {
    if candidates.is_empty() {
        return (vec![], vec![]);
    }

    // Sort by (inbox_seq, tenant_id, hash(plans)). After this, the sorted index
    // order equals the sort-key order: index `i < j` implies `sort_key(i) <=
    // sort_key(j)`. The topological pick and cycle-break rely on this.
    candidates.sort_by_key(admitted_sort_key);
    let n = candidates.len();

    // Flatten both sets of every transaction and build the reader/writer maps
    // keyed by canonical key identity. BTreeMap + ascending index vectors keep
    // every iteration that feeds output deterministic.
    let mut readers_of: BTreeMap<KeyId, Vec<usize>> = BTreeMap::new();
    let mut writers_of: BTreeMap<KeyId, Vec<usize>> = BTreeMap::new();
    let mut tx_reads: Vec<Vec<FlatKey>> = Vec::with_capacity(n);
    let mut tx_writes: Vec<Vec<FlatKey>> = Vec::with_capacity(n);

    for (i, tx) in candidates.iter().enumerate() {
        let reads = flatten_set(&tx.tx_class.read_set);
        let writes = flatten_set(&tx.tx_class.write_set);
        for fk in &reads {
            let entry = readers_of.entry(fk.key_id()).or_default();
            if entry.last() != Some(&i) {
                entry.push(i);
            }
        }
        for fk in &writes {
            let entry = writers_of.entry(fk.key_id()).or_default();
            if entry.last() != Some(&i) {
                entry.push(i);
            }
        }
        tx_reads.push(reads);
        tx_writes.push(writes);
    }

    // Build RAW edges `reader -> writer` (reader gets the lower position). A
    // BTreeSet of successors deduplicates edges arising from multiple shared
    // keys and keeps successor iteration deterministic.
    let mut out: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
    let mut in_degree: Vec<usize> = vec![0; n];
    for (key, readers) in &readers_of {
        if let Some(writers) = writers_of.get(key) {
            for &r in readers {
                for &w in writers {
                    if r != w && out[r].insert(w) {
                        in_degree[w] += 1;
                    }
                }
            }
        }
    }

    // Deterministic topological emission with cycle-breaking. `ready` is a
    // BTreeSet so `pop_first` always yields the smallest index — i.e. the
    // smallest admitted sort key among currently-schedulable nodes.
    let mut rejected = vec![false; n];
    let mut emitted_position: Vec<Option<u32>> = vec![None; n];
    let mut ready: BTreeSet<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut next_position: u32 = 0;

    loop {
        while let Some(node) = ready.pop_first() {
            emitted_position[node] = Some(next_position);
            next_position += 1;
            for &succ in &out[node] {
                in_degree[succ] -= 1;
                if in_degree[succ] == 0 && !rejected[succ] && emitted_position[succ].is_none() {
                    ready.insert(succ);
                }
            }
        }
        // A cycle remains iff some node is neither emitted nor rejected. Break
        // it by rejecting the largest sort key (largest index = highest
        // inbox_seq): the later transaction loses. A rejected node imposes no
        // ordering, so drop its outgoing edges.
        match (0..n)
            .rev()
            .find(|&i| !rejected[i] && emitted_position[i].is_none())
        {
            Some(victim) => {
                rejected[victim] = true;
                for &succ in &out[victim] {
                    in_degree[succ] -= 1;
                    if in_degree[succ] == 0 && !rejected[succ] && emitted_position[succ].is_none() {
                        ready.insert(succ);
                    }
                }
            }
            None => break,
        }
    }

    // Build output in sorted-index order.
    let mut admitted_out: Vec<(u64, SequencedTxn)> = Vec::new();
    let mut rejected_out: Vec<RejectedTx> = Vec::new();

    for (idx, tx) in candidates.into_iter().enumerate() {
        match emitted_position[idx] {
            Some(position) => {
                let inbox_seq = tx.inbox_seq;
                // Copy the submitted class's reservation owner before the class
                // is moved into the SequencedTxn: `Some(R)` acquires the commit
                // batch's keys as `R` so it self-upgrades the session's shared
                // reservations.
                let lock_owner = tx.tx_class.lock_owner;
                admitted_out.push((
                    inbox_seq,
                    SequencedTxn {
                        epoch,
                        position,
                        tx_class: tx.tx_class,
                        // epoch_system_ms is filled in by the service tick()
                        // when the EpochBatch is constructed; 0 is a safe
                        // placeholder here.
                        epoch_system_ms: 0,
                        // epoch_vshard_txn_count is stamped per-vShard by the
                        // state machine at fan-out time; 0 is a safe placeholder.
                        epoch_vshard_txn_count: 0,
                        lock_owner,
                    },
                ));
            }
            None => {
                let tenant = tx.tx_class.tenant_id.as_u64();
                let (position_admitted, conflict_context) = reject_context(
                    idx,
                    tenant,
                    &tx_reads,
                    &tx_writes,
                    &readers_of,
                    &writers_of,
                    &emitted_position,
                );
                rejected_out.push(RejectedTx {
                    admitted: tx,
                    reason: SequencerError::Conflict { position_admitted },
                    conflict_context,
                });
            }
        }
    }

    (admitted_out, rejected_out)
}

pub fn validate_batch(
    epoch: u64,
    candidates: Vec<AdmittedTx>,
) -> (Vec<SequencedTxn>, Vec<RejectedTx>) {
    let (admitted, rejected) = validate_batch_with_assignments(epoch, candidates);
    (admitted.into_iter().map(|(_, txn)| txn).collect(), rejected)
}

/// Build the `(position_admitted, conflict_context)` for a cycle-rejected txn.
///
/// A rejected txn conflicts with any admitted txn that writes a key it reads or
/// reads a key it writes. We select the conflict with the smallest admitted
/// position (ties broken by key identity) so both the reported position and the
/// [`ConflictKey`] are deterministic. If no conflicting txn was admitted, the
/// position defaults to `0` and the context to `None`.
fn reject_context(
    idx: usize,
    tenant: u64,
    tx_reads: &[Vec<FlatKey>],
    tx_writes: &[Vec<FlatKey>],
    readers_of: &BTreeMap<KeyId, Vec<usize>>,
    writers_of: &BTreeMap<KeyId, Vec<usize>>,
    emitted_position: &[Option<u32>],
) -> (u32, Option<ConflictKey>) {
    // Best conflict so far, ordered by (admitted position, key identity).
    let mut best: Option<(u32, KeyId, &'static str)> = None;
    let mut consider = |fk: &FlatKey, others: Option<&Vec<usize>>| {
        let Some(others) = others else { return };
        for &other in others {
            if other == idx {
                continue;
            }
            let Some(pos) = emitted_position[other] else {
                continue;
            };
            let key = fk.key_id();
            let candidate = (pos, key, fk.engine_name);
            let better = match &best {
                None => true,
                Some((bpos, bkey, _)) => (pos, &candidate.1) < (*bpos, bkey),
            };
            if better {
                best = Some(candidate);
            }
        }
    };

    // A writer of a key this txn reads.
    for fk in &tx_reads[idx] {
        consider(fk, writers_of.get(&fk.key_id()));
    }
    // A reader of a key this txn writes.
    for fk in &tx_writes[idx] {
        consider(fk, readers_of.get(&fk.key_id()));
    }

    match best {
        Some((pos, key, engine)) => (
            pos,
            Some(ConflictKey {
                tenant,
                engine,
                collection: key.1,
            }),
        ),
        None => (0, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::sequencer::inbox::AdmittedTx;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
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

    /// Build a two-collection write-only transaction (multi-vshard).
    fn make_tx(
        inbox_seq: u64,
        col_a: &str,
        surrogates_a: Vec<u32>,
        col_b: &str,
        surrogates_b: Vec<u32>,
    ) -> AdmittedTx {
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a.to_owned(),
                surrogates: SortedVec::new(surrogates_a),
            },
            EngineKeySet::Document {
                collection: col_b.to_owned(),
                surrogates: SortedVec::new(surrogates_b),
            },
        ]);
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![inbox_seq as u8],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        AdmittedTx {
            inbox_seq,
            tx_class,
        }
    }

    /// Build a transaction with a distinct read set and write set. The write set
    /// is a single collection, so this uses the single-vshard opt-in
    /// constructor (the validator operates purely on set contents). An empty
    /// `read_surrogates` yields a read set with no keys.
    fn make_rw_tx(
        inbox_seq: u64,
        read_col: &str,
        read_surrogates: Vec<u32>,
        write_col: &str,
        write_surrogates: Vec<u32>,
    ) -> AdmittedTx {
        let read_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: read_col.to_owned(),
            surrogates: SortedVec::new(read_surrogates),
        }]);
        let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: write_col.to_owned(),
            surrogates: SortedVec::new(write_surrogates),
        }]);
        let tx_class = TxClass::new_single_vshard(
            read_set,
            write_set,
            vec![inbox_seq as u8],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        AdmittedTx {
            inbox_seq,
            tx_class,
        }
    }

    #[test]
    fn empty_input_produces_empty_output() {
        let (admitted, rejected) = validate_batch(1, vec![]);
        assert!(admitted.is_empty());
        assert!(rejected.is_empty());
    }

    #[test]
    fn single_txn_admitted_at_position_zero() {
        let (col_a, col_b) = find_two_distinct_collections();
        let tx = make_tx(0, &col_a, vec![1], &col_b, vec![2]);
        let (admitted, rejected) = validate_batch(1, vec![tx]);
        assert_eq!(admitted.len(), 1);
        assert!(rejected.is_empty());
        assert_eq!(admitted[0].position, 0);
        assert_eq!(admitted[0].epoch, 1);
    }

    #[test]
    fn two_non_conflicting_txns_both_admitted_in_inbox_seq_order() {
        let (col_a, col_b) = find_two_distinct_collections();
        let tx0 = make_tx(0, &col_a, vec![1], &col_b, vec![10]);
        let tx1 = make_tx(1, &col_a, vec![2], &col_b, vec![20]);
        let (admitted, rejected) = validate_batch(2, vec![tx0, tx1]);
        assert_eq!(admitted.len(), 2);
        assert!(rejected.is_empty());
        // No RAW edges → inbox_seq order preserved.
        assert_eq!(admitted[0].position, 0);
        assert_eq!(admitted[1].position, 1);
    }

    #[test]
    fn two_same_key_writers_both_admitted() {
        let (col_a, col_b) = find_two_distinct_collections();
        // Both txns WRITE surrogate 42 in col_a. Write-write is not a conflict:
        // they serialize by position (last write wins) and both commit.
        let tx0 = make_tx(0, &col_a, vec![42], &col_b, vec![1]);
        let tx1 = make_tx(1, &col_a, vec![42], &col_b, vec![2]);
        let (admitted, rejected) = validate_batch(3, vec![tx0, tx1]);
        assert_eq!(admitted.len(), 2);
        assert!(rejected.is_empty());
        // Ordered by inbox_seq (no RAW edge between two writers).
        assert_eq!(admitted[0].position, 0);
        assert_eq!(admitted[1].position, 1);
    }

    #[test]
    fn raw_reader_ordered_before_writer() {
        let (col_k, col_other) = find_two_distinct_collections();
        // tx0 (inbox_seq 0) WRITES key K.
        let tx0 = make_rw_tx(0, &col_k, vec![], &col_k, vec![7]);
        // tx1 (inbox_seq 1) READS key K and writes something else.
        let tx1 = make_rw_tx(1, &col_k, vec![7], &col_other, vec![99]);
        let (admitted, _rejected) = validate_batch_with_assignments(4, vec![tx0, tx1]);
        assert_eq!(admitted.len(), 2);

        // Reader (inbox_seq 1) must get a LOWER position than writer (inbox_seq 0)
        // — the reorder that protects the read from a read-after-write abort,
        // overriding inbox_seq order.
        let writer_pos = admitted
            .iter()
            .find(|(seq, _)| *seq == 0)
            .map(|(_, t)| t.position)
            .expect("writer admitted");
        let reader_pos = admitted
            .iter()
            .find(|(seq, _)| *seq == 1)
            .map(|(_, t)| t.position)
            .expect("reader admitted");
        assert!(
            reader_pos < writer_pos,
            "reader (seq 1) at {reader_pos} must precede writer (seq 0) at {writer_pos}"
        );
    }

    #[test]
    fn raw_cycle_rejects_later_txn_with_winner_position() {
        let (col_a, col_b) = find_two_distinct_collections();
        // tx0 READS A, WRITES B; tx1 READS B, WRITES A → a RAW cycle that no
        // serial order can satisfy.
        let tx0 = make_rw_tx(0, &col_a, vec![1], &col_b, vec![1]);
        let tx1 = make_rw_tx(1, &col_b, vec![1], &col_a, vec![1]);
        let (admitted, rejected) = validate_batch_with_assignments(5, vec![tx0, tx1]);
        assert_eq!(admitted.len(), 1);
        assert_eq!(rejected.len(), 1);
        // Later txn (inbox_seq 1) loses the cycle break.
        assert_eq!(rejected[0].admitted.inbox_seq, 1);
        // Winner admitted at position 0.
        assert_eq!(
            rejected[0].reason,
            SequencerError::Conflict {
                position_admitted: 0
            }
        );
        assert_eq!(admitted[0].1.position, 0);
        assert_eq!(admitted[0].1.tx_class.tenant_id.as_u64(), 1);
    }

    #[test]
    fn raw_cycle_reject_carries_conflict_context() {
        let (col_a, col_b) = find_two_distinct_collections();
        let tx0 = make_rw_tx(0, &col_a, vec![1], &col_b, vec![1]);
        let tx1 = make_rw_tx(1, &col_b, vec![1], &col_a, vec![1]);
        let (_admitted, rejected) = validate_batch(10, vec![tx0, tx1]);
        assert_eq!(rejected.len(), 1);

        let ctx = rejected[0]
            .conflict_context
            .as_ref()
            .expect("conflict_context must be Some for a Conflict rejection");
        assert_eq!(ctx.tenant, 1, "tenant should match tx tenant_id");
        assert_eq!(
            ctx.engine, "document",
            "engine should be 'document' for Document key set"
        );
        assert!(
            ctx.collection == col_a || ctx.collection == col_b,
            "collection {} should be one of the cycle's conflicting keys",
            ctx.collection
        );
    }

    #[test]
    fn positions_are_dense_and_reorder_is_deterministic() {
        let (col_k, col_other) = find_two_distinct_collections();
        let build = || {
            vec![
                // tx0 WRITES K.
                make_rw_tx(0, &col_k, vec![], &col_k, vec![5]),
                // tx1 READS K, writes elsewhere → RAW edge tx1 -> tx0.
                make_rw_tx(1, &col_k, vec![5], &col_other, vec![6]),
                // tx2 independent write.
                make_rw_tx(2, &col_other, vec![], &col_other, vec![7]),
            ]
        };

        let (admitted1, rejected1) = validate_batch_with_assignments(7, build());
        assert!(rejected1.is_empty());
        assert_eq!(admitted1.len(), 3);
        // Positions are exactly 0..len with no gaps.
        let mut positions: Vec<u32> = admitted1.iter().map(|(_, t)| t.position).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![0, 1, 2]);

        // Second run yields identical position-per-inbox_seq assignment.
        let (admitted2, _rejected2) = validate_batch_with_assignments(7, build());
        for (seq, txn) in &admitted1 {
            let other = admitted2
                .iter()
                .find(|(s, _)| s == seq)
                .map(|(_, t)| t.position)
                .expect("same txn present in second run");
            assert_eq!(
                txn.position, other,
                "position for inbox_seq {seq} must be stable"
            );
        }
    }

    #[test]
    fn deterministic_ordering_across_repeated_runs() {
        let (col_a, col_b) = find_two_distinct_collections();
        // Two same-key writers (write-write both admitted).
        let tx0 = make_tx(0, &col_a, vec![1], &col_b, vec![10]);
        let tx1 = make_tx(1, &col_a, vec![1], &col_b, vec![20]);

        let (admitted1, rejected1) = validate_batch(1, vec![tx0.clone(), tx1.clone()]);
        let (admitted2, rejected2) = validate_batch(1, vec![tx0, tx1]);

        assert_eq!(admitted1.len(), admitted2.len());
        assert_eq!(rejected1.len(), rejected2.len());
        for (a, b) in admitted1.iter().zip(admitted2.iter()) {
            assert_eq!(a.position, b.position);
        }
    }
}
