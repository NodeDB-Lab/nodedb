// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for Calvin executor admission-cap rejections.
//!
//! Verifies that `Inbox::submit` correctly enforces:
//! - `max_dependent_read_bytes_per_txn` → `DependentReadTooLarge`
//! - `max_dependent_read_passives_per_txn` → `DependentReadFanoutTooWide`

use std::collections::BTreeMap;

use nodedb_cluster::calvin::sequencer::config::SequencerConfig;
use nodedb_cluster::calvin::sequencer::error::SequencerError;
use nodedb_cluster::calvin::sequencer::inbox::new_inbox;
use nodedb_cluster::calvin::types::{
    DependentReadSpec, EngineKeySet, PassiveReadKey, ReadWriteSet, SortedVec, TxClass,
    VersionedReadSet,
};
use nodedb_types::TenantId;

/// Find two collection names whose vShards differ.
fn two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("col_{i}");
        let vshard =
            nodedb_types::CollectionKey::from_bare(nodedb::types::DatabaseId::DEFAULT, &name)
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

fn base_tx_class() -> TxClass {
    let (col_a, col_b) = two_distinct_collections();
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
    TxClass::new(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid base TxClass")
}

#[test]
fn submit_rejects_dependent_read_too_large() {
    // 4-byte limit: each surrogate is 4 bytes, so 2 surrogates = 8 bytes > 4.
    let config = SequencerConfig {
        max_dependent_read_bytes_per_txn: 4,
        tenant_inbox_quota: 100,
        ..SequencerConfig::default()
    };
    let (inbox, _rx) = new_inbox(10, &config);

    let mut tx = base_tx_class();
    tx.dependent_reads = Some(DependentReadSpec {
        passive_reads: {
            let mut m = BTreeMap::new();
            m.insert(
                999u32,
                vec![PassiveReadKey {
                    engine_key: EngineKeySet::Document {
                        collection: "passive_col".to_owned(),
                        surrogates: SortedVec::new(vec![1u32, 2u32]), // 8 bytes
                    },
                }],
            );
            m
        },
        expected: BTreeMap::new(),
    });

    let err = inbox.submit(tx).expect_err("should reject: too large");
    assert!(
        matches!(
            err,
            SequencerError::DependentReadTooLarge { bytes: 8, limit: 4 }
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn submit_accepts_dependent_read_within_byte_limit() {
    // 8-byte limit: 2 surrogates × 4 bytes = 8 bytes = exactly at limit.
    let config = SequencerConfig {
        max_dependent_read_bytes_per_txn: 8,
        tenant_inbox_quota: 100,
        ..SequencerConfig::default()
    };
    let (inbox, _rx) = new_inbox(10, &config);

    let mut tx = base_tx_class();
    tx.dependent_reads = Some(DependentReadSpec {
        passive_reads: {
            let mut m = BTreeMap::new();
            m.insert(
                999u32,
                vec![PassiveReadKey {
                    engine_key: EngineKeySet::Document {
                        collection: "passive_col".to_owned(),
                        surrogates: SortedVec::new(vec![1u32, 2u32]), // 8 bytes
                    },
                }],
            );
            m
        },
        expected: BTreeMap::new(),
    });

    inbox.submit(tx).expect("should accept: exactly at limit");
}

#[test]
fn submit_rejects_dependent_read_fanout_too_wide() {
    // Only 1 passive vshard allowed.
    let config = SequencerConfig {
        max_dependent_read_passives_per_txn: 1,
        tenant_inbox_quota: 100,
        ..SequencerConfig::default()
    };
    let (inbox, _rx) = new_inbox(10, &config);

    let mut tx = base_tx_class();
    tx.dependent_reads = Some(DependentReadSpec {
        passive_reads: {
            let mut m = BTreeMap::new();
            for vshard in [101u32, 102u32] {
                m.insert(
                    vshard,
                    vec![PassiveReadKey {
                        engine_key: EngineKeySet::Document {
                            collection: format!("col_{vshard}"),
                            surrogates: SortedVec::new(vec![vshard]),
                        },
                    }],
                );
            }
            m
        },
        expected: BTreeMap::new(),
    });

    let err = inbox.submit(tx).expect_err("should reject: too wide");
    assert!(
        matches!(
            err,
            SequencerError::DependentReadFanoutTooWide {
                passives: 2,
                limit: 1
            }
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn submit_accepts_dependent_read_exactly_at_fanout_limit() {
    // Exactly 2 passives allowed, txn has 2 passives.
    let config = SequencerConfig {
        max_dependent_read_passives_per_txn: 2,
        tenant_inbox_quota: 100,
        ..SequencerConfig::default()
    };
    let (inbox, _rx) = new_inbox(10, &config);

    let mut tx = base_tx_class();
    tx.dependent_reads = Some(DependentReadSpec {
        passive_reads: {
            let mut m = BTreeMap::new();
            for vshard in [101u32, 102u32] {
                m.insert(
                    vshard,
                    vec![PassiveReadKey {
                        engine_key: EngineKeySet::Document {
                            collection: format!("col_{vshard}"),
                            surrogates: SortedVec::new(vec![vshard]),
                        },
                    }],
                );
            }
            m
        },
        expected: BTreeMap::new(),
    });

    inbox
        .submit(tx)
        .expect("should accept: exactly at fanout limit");
}
