// SPDX-License-Identifier: BUSL-1.1

//! Primitive Calvin type definitions.
//!
//! [`SortedVec`], [`EngineKeySet`], and [`PassiveReadKey`] live in
//! `nodedb-types` so the physical-plan IR can reference them without
//! pulling in the distributed scheduler. [`DependentReadSpec`] stays
//! here because it is scheduler-internal.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use nodedb_types::calvin::{
    EngineKeySet, EngineTag, PassiveKey, PassiveReadKey, PassiveReadKeyId, ReadKeyIdent, SortedVec,
    VersionedReadEntry, VersionedReadSet,
};

/// Describes the passive-read participants for a dependent-read Calvin txn.
///
/// `passive_reads` maps each passive vShard to the keys it reads at its lock
/// grant and broadcasts to every active participant before any write stages.
///
/// `expected` holds the value the coordinator read for each passive row
/// before it planned the writes: the stored bytes, `None` for an absent row.
/// An active participant whose broadcast values differ votes
/// `PredictionDrift`, and the coordinator reads again.
///
/// `BTreeMap` is mandatory here: the sequencer and scheduler must iterate
/// vshards in a deterministic order (determinism contract).
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
pub struct DependentReadSpec {
    /// Passive participants: vshard → keys to read.
    pub passive_reads: BTreeMap<u32, Vec<PassiveReadKey>>,
    /// The value the coordinator read for each passive row.
    pub expected: BTreeMap<PassiveReadKeyId, Option<Vec<u8>>>,
}

impl DependentReadSpec {
    /// Total estimated serialized bytes across all passive read keys and
    /// the expected values the coordinator read.
    ///
    /// Used by the sequencer admission check to enforce
    /// `max_dependent_read_bytes_per_txn`.  This is an O(1)-per-key
    /// estimate, not an exact serialized size.
    pub fn total_bytes(&self) -> usize {
        let keys: usize = self
            .passive_reads
            .values()
            .flat_map(|ks| ks.iter())
            .map(|k| k.engine_key.serialized_size_hint())
            .sum();
        let expected: usize = self
            .expected
            .iter()
            .map(|(id, value)| id.serialized_size_hint() + value.as_ref().map_or(0, Vec::len))
            .sum();
        keys + expected
    }

    /// The passive vShards, in id order.
    pub fn passive_vshards(&self) -> impl Iterator<Item = u32> + '_ {
        self.passive_reads.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Document {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn vec_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Vector {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn kv_set(collection: &str, keys: Vec<Vec<u8>>) -> EngineKeySet {
        EngineKeySet::Kv {
            collection: collection.to_owned(),
            keys: SortedVec::new(keys),
        }
    }

    fn edge_set(collection: &str, edges: Vec<(u32, u32)>) -> EngineKeySet {
        EngineKeySet::Edge {
            collection: collection.to_owned(),
            edges: SortedVec::new(edges),
            home_vshards: SortedVec::new(Vec::new()),
        }
    }

    // ── SortedVec ─────────────────────────────────────────────────────────────

    #[test]
    fn sorted_vec_sort_and_dedup() {
        let v: SortedVec<u32> = SortedVec::new(vec![5, 1, 3, 1, 2, 5]);
        assert_eq!(v.as_slice(), &[1, 2, 3, 5]);
    }

    #[test]
    fn sorted_vec_already_sorted() {
        let v: SortedVec<u32> = SortedVec::new(vec![1, 2, 3]);
        assert_eq!(v.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn sorted_vec_empty() {
        let v: SortedVec<u32> = SortedVec::new(vec![]);
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn sorted_vec_bytes_deterministic_regardless_of_insertion_order() {
        let a: SortedVec<u32> = SortedVec::new(vec![3, 1, 4, 1, 5]);
        let b: SortedVec<u32> = SortedVec::new(vec![5, 4, 3, 1, 1]);
        let a_bytes = sonic_rs::to_vec(&a).unwrap();
        let b_bytes = sonic_rs::to_vec(&b).unwrap();
        assert_eq!(a_bytes, b_bytes);
    }

    // ── EngineKeySet ──────────────────────────────────────────────────────────

    #[test]
    fn engine_key_set_collection_name() {
        let d = doc_set("users", vec![1]);
        assert_eq!(d.collection(), "users");

        let v = vec_set("embeddings", vec![2]);
        assert_eq!(v.collection(), "embeddings");

        let k = kv_set("sessions", vec![b"key1".to_vec()]);
        assert_eq!(k.collection(), "sessions");

        let e = edge_set("follows", vec![(1, 2)]);
        assert_eq!(e.collection(), "follows");
    }

    #[test]
    fn engine_key_set_is_empty() {
        assert!(doc_set("users", vec![]).is_empty());
        assert!(!doc_set("users", vec![1]).is_empty());
    }

    // ── DependentReadSpec ─────────────────────────────────────────────────────

    #[test]
    fn dependent_read_spec_msgpack_roundtrip() {
        let spec = DependentReadSpec {
            passive_reads: {
                let mut m = BTreeMap::new();
                m.insert(
                    1u32,
                    vec![PassiveReadKey {
                        engine_key: doc_set("users", vec![10, 20]),
                    }],
                );
                m.insert(
                    2u32,
                    vec![PassiveReadKey {
                        engine_key: kv_set("sessions", vec![b"abc".to_vec()]),
                    }],
                );
                m
            },
            expected: BTreeMap::from([(
                PassiveReadKeyId::kv(
                    nodedb_types::QualifiedCollection::from_stored("sessions".to_owned()),
                    b"abc".to_vec(),
                ),
                Some(b"stored".to_vec()),
            )]),
        };
        let bytes = zerompk::to_msgpack_vec(&spec).unwrap();
        let decoded: DependentReadSpec = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded, spec);
    }

    /// The byte estimate counts the expected values the coordinator read.
    #[test]
    fn total_bytes_counts_expected_values() {
        let mut spec = DependentReadSpec {
            passive_reads: BTreeMap::from([(
                1u32,
                vec![PassiveReadKey {
                    engine_key: kv_set("s", vec![b"k".to_vec()]),
                }],
            )]),
            expected: BTreeMap::new(),
        };
        let keys_only = spec.total_bytes();
        spec.expected.insert(
            PassiveReadKeyId::kv(
                nodedb_types::QualifiedCollection::from_stored("s".to_owned()),
                b"k".to_vec(),
            ),
            Some(vec![0u8; 100]),
        );
        assert!(spec.total_bytes() >= keys_only + 100);
    }
}
