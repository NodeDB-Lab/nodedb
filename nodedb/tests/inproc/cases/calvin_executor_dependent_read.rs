// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the Calvin dependent-read barrier's public surface:
//! the dependent transaction class, the barrier log a vShard folds from its
//! data-group log, and the check of the read values against the values the
//! coordinator read.

use std::collections::{BTreeMap, BTreeSet};

use nodedb::control::cluster::calvin::scheduler::driver::barrier::expected_reads_drift;
use nodedb::control::cluster::calvin::scheduler::{BarrierEvent, BarrierLog, BarrierOutcome};
use nodedb_cluster::calvin::types::{
    DependentReadSpec, EngineKeySet, PassiveReadKey, ReadWriteSet, SortedVec, TxClass,
    VersionedReadSet,
};
use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::{DatabaseId, QualifiedCollection, TenantId, Value};

fn two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("col_{i}");
        let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
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

fn row(collection: &str, surrogate: u32) -> PassiveReadKeyId {
    PassiveReadKeyId::surrogate(
        QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        surrogate,
    )
}

fn read(passive_vshard: u32, collection: &str, value: &[u8]) -> BarrierEvent {
    BarrierEvent::Read {
        passive_vshard,
        values: vec![(row(collection, 1), Value::Bytes(value.to_vec()))],
    }
}

/// A dependent class names every passive vShard as a participant, and its
/// write vShards as its active participants.
#[test]
fn a_dependent_class_participates_its_passive_and_active_vshards() {
    let (col_a, col_b) = two_distinct_collections();
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a.clone(),
            surrogates: SortedVec::new(vec![1]),
        },
        EngineKeySet::Document {
            collection: col_b.clone(),
            surrogates: SortedVec::new(vec![2]),
        },
    ]);
    let passive_vshard = 977u32;
    let spec = DependentReadSpec {
        passive_reads: BTreeMap::from([(
            passive_vshard,
            vec![PassiveReadKey {
                engine_key: EngineKeySet::Document {
                    collection: "passive_col".to_owned(),
                    surrogates: SortedVec::new(vec![42u32]),
                },
            }],
        )]),
        expected: BTreeMap::from([(row("passive_col", 42), Some(b"v".to_vec()))]),
    };
    let tx_class = TxClass::new_dependent(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        DatabaseId::DEFAULT,
        spec,
        VersionedReadSet::default(),
    )
    .expect("valid dependent TxClass");

    let participants: Vec<u32> = tx_class
        .participating_vshards()
        .iter()
        .map(|v| v.as_u32())
        .collect();
    assert!(participants.contains(&passive_vshard));
    let active = tx_class.active_vshards().expect("active vShards");
    assert_eq!(active.len(), 2);
    assert!(!active.contains(&passive_vshard));
}

/// The barrier completes once every passive vShard's result is in, and the
/// values it injects are the ones the log carried.
#[test]
fn a_barrier_completes_once_every_passive_result_is_in() {
    let passive: BTreeSet<u32> = [10, 20].into_iter().collect();
    let mut log = BarrierLog::default();
    log.note(read(10, "coll_10", b"a"));
    assert_eq!(log.outcome(&passive), BarrierOutcome::Waiting);
    log.note(read(20, "coll_20", b"b"));
    assert_eq!(log.outcome(&passive), BarrierOutcome::Complete);
    let injected = log.injected_reads();
    assert_eq!(
        injected.get(&row("coll_10", 1)),
        Some(&Value::Bytes(b"a".to_vec()))
    );
    assert_eq!(injected.len(), 2);
}

/// A timeout entry that comes before a passive result times the barrier
/// out, whatever arrives after it.
#[test]
fn a_logged_timeout_before_a_result_times_the_barrier_out() {
    let passive: BTreeSet<u32> = [10].into_iter().collect();
    let mut log = BarrierLog::default();
    log.note(BarrierEvent::Timeout);
    log.note(read(10, "coll_10", b"a"));
    assert_eq!(log.outcome(&passive), BarrierOutcome::TimedOut);
}

/// Values that match the coordinator's read pass. A moved value drifts.
#[test]
fn read_values_are_checked_against_the_coordinators_read() {
    let spec = DependentReadSpec {
        passive_reads: BTreeMap::new(),
        expected: BTreeMap::from([(row("coll", 1), Some(b"v1".to_vec()))]),
    };
    let same = BTreeMap::from([(row("coll", 1), Value::Bytes(b"v1".to_vec()))]);
    let moved = BTreeMap::from([(row("coll", 1), Value::Bytes(b"v2".to_vec()))]);
    assert_eq!(expected_reads_drift(&spec, &same), None);
    assert!(expected_reads_drift(&spec, &moved).is_some());
}
