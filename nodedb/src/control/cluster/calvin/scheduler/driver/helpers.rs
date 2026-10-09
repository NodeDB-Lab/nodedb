// SPDX-License-Identifier: BUSL-1.1

//! Expansion and decoding helpers for the Calvin scheduler driver.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_cluster::calvin::types::{EngineKeySet, LockKeyWire, SequencedTxn, TxnIdWire};

use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, LockMode, TxnId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::wire as plan_wire;

impl From<TxnIdWire> for TxnId {
    fn from(wire: TxnIdWire) -> Self {
        TxnId::new(wire.epoch, wire.position)
    }
}

/// Decode a [`LockKeyWire`] transport twin into the real scheduler-side
/// [`LockKey`]. Mirrors the field shapes of [`expand_rw_set`]'s per-engine key
/// construction (interned `Arc<str>` / `Arc<[u8]>` for cheap clones).
pub(super) fn decode_lock_key(wire: &LockKeyWire) -> LockKey {
    match wire {
        LockKeyWire::Surrogate {
            collection,
            surrogate,
        } => LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: *surrogate,
        },
        LockKeyWire::Kv { collection, key } => LockKey::Kv {
            collection: Arc::from(collection.as_str()),
            key: Arc::from(key.as_slice()),
        },
        LockKeyWire::Edge {
            collection,
            src,
            dst,
        } => LockKey::Edge {
            collection: Arc::from(collection.as_str()),
            src: *src,
            dst: *dst,
        },
        LockKeyWire::Collection { collection } => LockKey::Collection {
            collection: Arc::from(collection.as_str()),
        },
        LockKeyWire::Unique {
            collection,
            index,
            value,
        } => LockKey::Unique {
            collection: Arc::from(collection.as_str()),
            index: Arc::from(index.as_str()),
            value: Arc::from(value.as_slice()),
        },
    }
}

/// Expand the read set and write set of a sequenced transaction into the
/// moded lock request the scheduler acquires.
///
/// - A write row key (surrogate, KV key, edge pair, UNIQUE value) is
///   `Exclusive`, and its collection key is `Intent`.
/// - A write [`EngineKeySet::Collection`] and a write array are `Exclusive`
///   on the collection key.
/// - A read row key is `Shared`, with no collection key.
/// - A read [`EngineKeySet::Collection`] is `Shared` on the collection key.
///
/// A key named twice takes the merged mode (see [`LockMode::merge`]).
pub(crate) fn expand_rw_set(txn: &SequencedTxn) -> BTreeMap<LockKey, LockMode> {
    let mut keys = BTreeMap::new();
    for ks in &txn.tx_class.read_set.0 {
        add_key_set(&mut keys, ks, LockMode::Shared);
    }
    for ks in &txn.tx_class.write_set.0 {
        add_key_set(&mut keys, ks, LockMode::Exclusive);
    }
    keys
}

/// Expand write key sets alone into the moded lock request a transaction of
/// those writes takes, by the rules of [`expand_rw_set`]. The
/// write-admission gate locks an autocommit write with it, so the write and
/// a Calvin transaction of the same plan take the same keys.
pub(crate) fn expand_write_key_sets(sets: &[EngineKeySet]) -> BTreeMap<LockKey, LockMode> {
    let mut keys = BTreeMap::new();
    for ks in sets {
        add_key_set(&mut keys, ks, LockMode::Exclusive);
    }
    keys
}

/// Add the keys of `ks` to `keys`. `row_mode` is `Shared` for a read set and
/// `Exclusive` for a write set.
fn add_key_set(keys: &mut BTreeMap<LockKey, LockMode>, ks: &EngineKeySet, row_mode: LockMode) {
    let coll: Arc<str> = Arc::from(ks.collection());
    let whole = LockKey::Collection {
        collection: Arc::clone(&coll),
    };
    match ks {
        EngineKeySet::Document { surrogates, .. } | EngineKeySet::Vector { surrogates, .. } => {
            for &surrogate in surrogates.iter() {
                let key = LockKey::Surrogate {
                    collection: Arc::clone(&coll),
                    surrogate,
                };
                request(keys, key, row_mode);
            }
        }
        EngineKeySet::Kv { keys: kv_keys, .. } => {
            for k in kv_keys.iter() {
                let key = LockKey::Kv {
                    collection: Arc::clone(&coll),
                    key: Arc::from(k.as_slice()),
                };
                request(keys, key, row_mode);
            }
        }
        EngineKeySet::Edge { edges, .. } => {
            for &(src, dst) in edges.iter() {
                let key = LockKey::Edge {
                    collection: Arc::clone(&coll),
                    src,
                    dst,
                };
                request(keys, key, row_mode);
            }
        }
        EngineKeySet::Unique { index, values, .. } => {
            let index: Arc<str> = Arc::from(index.as_str());
            for value in values.iter() {
                let key = LockKey::Unique {
                    collection: Arc::clone(&coll),
                    index: Arc::clone(&index),
                    value: Arc::from(value.as_slice()),
                };
                request(keys, key, row_mode);
            }
        }
        // Each participating vShard's lock table holds the whole array on
        // that vShard. A whole-collection key set covers the collection.
        EngineKeySet::Array { .. } | EngineKeySet::Collection { .. } => {
            request(keys, whole, row_mode);
            return;
        }
    }
    // A row writer announces itself on its collection, so a collection-wide
    // writer or a predicate reader of that collection orders against it.
    if row_mode == LockMode::Exclusive {
        request(keys, whole, LockMode::Intent);
    }
}

/// Request `key` in `mode`, merged with any mode already requested for it.
fn request(keys: &mut BTreeMap<LockKey, LockMode>, key: LockKey, mode: LockMode) {
    keys.entry(key)
        .and_modify(|held| *held = held.merge(mode))
        .or_insert(mode);
}

/// Decode a `Vec<PhysicalPlan>` from the opaque plan bytes stored in a
/// `TxClass`.
pub(super) fn decode_plans(plan_bytes: &[u8]) -> crate::Result<Vec<PhysicalPlan>> {
    plan_wire::decode_batch(plan_bytes).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("plan decode: {e}"),
    })
}
