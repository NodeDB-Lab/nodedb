// SPDX-License-Identifier: BUSL-1.1

//! Expansion and decoding helpers for the Calvin scheduler driver.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_cluster::calvin::types::{
    EngineKeySet, HomedPair, LockKeyWire, SequencedTxn, TxnIdWire,
};

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

/// The lock table a key expansion is for.
///
/// Only graph lock pairs read the site. Every other key set locks on every
/// participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockSite {
    /// The lock table of one vShard: it locks the pairs homed on it.
    On(u32),
    /// A lock table that sees the whole request: it locks every pair.
    Everywhere,
}

/// Expand the read set and write set of a sequenced transaction into the
/// moded lock request the scheduler acquires at `site`.
///
/// - A write row key (surrogate, KV key, edge pair, UNIQUE value) is
///   `Exclusive`, and its collection key is `Intent`.
/// - A write [`EngineKeySet::Collection`] and a write array are `Exclusive`
///   on the collection key.
/// - A read row key is `Shared`, and its collection key is `IntentShared`.
/// - A read [`EngineKeySet::Collection`] and a read array are `Shared` on the
///   collection key.
///
/// A graph pair is requested only at the site its home names. A key
/// named twice takes the merged mode (see [`LockMode::merge`]).
pub(crate) fn expand_rw_set(txn: &SequencedTxn, site: LockSite) -> BTreeMap<LockKey, LockMode> {
    let mut keys = BTreeMap::new();
    for ks in &txn.tx_class.read_set.0 {
        add_key_set(&mut keys, ks, Access::Read, site);
    }
    for ks in &txn.tx_class.write_set.0 {
        add_key_set(&mut keys, ks, Access::Write, site);
    }
    keys
}

/// Expand write key sets alone into the moded lock request a transaction of
/// those writes takes, by the rules of [`expand_rw_set`]. The
/// write-admission gate locks an autocommit write with it, so the write and
/// a Calvin transaction of the same plan take the same keys. The gate's
/// write has one home, so its one table locks every pair.
pub(crate) fn expand_write_key_sets(sets: &[EngineKeySet]) -> BTreeMap<LockKey, LockMode> {
    let mut keys = BTreeMap::new();
    for ks in sets {
        add_key_set(&mut keys, ks, Access::Write, LockSite::Everywhere);
    }
    keys
}

/// The graph lock pairs `site` locks from the sorted `edges`.
///
/// `On(v)` yields every `home: None` pair, then the run homed on `v`.
/// `Everywhere` yields every pair. The slice is sorted by `(home, pair)`,
/// so the `None` entries lead and each home is one run.
fn pairs_on(edges: &[HomedPair], site: LockSite) -> impl Iterator<Item = (u32, u32)> + '_ {
    let (everywhere, own): (&[HomedPair], &[HomedPair]) = match site {
        LockSite::Everywhere => (edges, &[]),
        LockSite::On(vshard) => {
            let home = Some(vshard);
            let unhomed_end = edges.partition_point(|p| p.home.is_none());
            let run_start = edges.partition_point(|p| p.home < home);
            let run_end = edges.partition_point(|p| p.home <= home);
            (&edges[..unhomed_end], &edges[run_start..run_end])
        }
    };
    everywhere.iter().chain(own).map(|p| p.pair)
}

/// Whether a key set comes from a read set or a write set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    Read,
    Write,
}

impl Access {
    /// The mode of a row key, and of a whole-collection key set.
    fn row_mode(self) -> LockMode {
        match self {
            Access::Read => LockMode::Shared,
            Access::Write => LockMode::Exclusive,
        }
    }

    /// The mode a row key announces on its collection key.
    fn announce_mode(self) -> LockMode {
        match self {
            Access::Read => LockMode::IntentShared,
            Access::Write => LockMode::Intent,
        }
    }
}

/// Add the keys of `ks` to `keys` for `access` at `site`.
fn add_key_set(
    keys: &mut BTreeMap<LockKey, LockMode>,
    ks: &EngineKeySet,
    access: Access,
    site: LockSite,
) {
    let row_mode = access.row_mode();
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
        // A graph pair locks on its home only. A pair with no home locks on
        // every participant, as do the other key sets.
        EngineKeySet::Edge { edges, .. } => {
            for (src, dst) in pairs_on(edges.as_slice(), site) {
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
    // A row key announces itself on its collection, so a collection-wide
    // writer orders against every row reader and writer of the collection,
    // and a predicate reader orders against every row writer.
    request(keys, whole, access.announce_mode());
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use nodedb_cluster::calvin::types::{HomedPair, SequencedTxn, TxClass};
    use nodedb_physical::physical_plan::{BatchEdge, GraphOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::{LockSite, expand_rw_set, pairs_on};
    use crate::control::cluster::calvin::scheduler::lock_manager::{
        AcquireOutcome, LockKey, LockManager, LockMode, TxnId,
    };
    use crate::control::planner::calvin::tx_class::shared::node_lock_pair;
    use crate::control::planner::calvin::tx_class::static_builder::build_single_vshard_tx_class;
    use crate::types::{DatabaseId, RecordHomes, TenantId, VShardId};

    const ROOT: &str = "root";
    const ROOT_SURROGATE: u32 = 1;

    fn collection() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "g")
    }

    fn leaf(i: u32) -> String {
        format!("leaf_{i}")
    }

    fn leaf_surrogate(i: u32) -> u32 {
        i + 2
    }

    fn task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn sequenced(plan: PhysicalPlan) -> SequencedTxn {
        let tx: TxClass = build_single_vshard_tx_class(&[task(plan)], TenantId::new(1), &[])
            .expect("a valid transaction class");
        SequencedTxn {
            epoch: 1,
            position: 0,
            tx_class: Arc::new(tx),
            epoch_system_ms: 1_700_000_000_000,
            epoch_vshard_txn_count: 1,
            lock_owner: None,
            local_plans: None,
        }
    }

    /// The star `root -> leaf_0 .. leaf_{n-1}` as one batch.
    fn star_batch(n: u32) -> PhysicalPlan {
        let edges = (0..n)
            .map(|i| BatchEdge {
                collection: collection(),
                src_id: ROOT.to_owned(),
                label: "L".to_owned(),
                dst_id: leaf(i),
                src_surrogate: Surrogate::new(ROOT_SURROGATE),
                dst_surrogate: Surrogate::new(leaf_surrogate(i)),
                valid_from_ms: None,
                valid_until_ms: None,
            })
            .collect();
        PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges })
    }

    /// The single edge `root -> leaf_i`.
    fn star_edge(i: u32) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: collection(),
            src_id: ROOT.to_owned(),
            label: "L".to_owned(),
            dst_id: leaf(i),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(ROOT_SURROGATE),
            dst_surrogate: Surrogate::new(leaf_surrogate(i)),
            valid_from_ms: None,
            valid_until_ms: None,
        })
    }

    fn participants(txn: &SequencedTxn) -> Vec<u32> {
        txn.tx_class
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect()
    }

    /// A 2000-edge star locks each pair only on its homes. Each vShard
    /// locks its own leaves' pairs, the root's node pair and the collection
    /// key. The total stays linear in the edge count.
    #[test]
    fn star_batch_locks_each_pair_on_its_homes_only() {
        const N: u32 = 2000;
        let txn = sequenced(star_batch(N));

        // The distinct pairs each home locks, from the real node lock pairs,
        // so a node-name hash collision cannot skew the count.
        let mut pairs_by_home: BTreeMap<u32, BTreeSet<(u32, u32)>> = BTreeMap::new();
        for i in 0..N {
            let name = leaf(i);
            for home in RecordHomes::edge(ROOT, &name).iter() {
                pairs_by_home.entry(home.as_u32()).or_default().extend([
                    (ROOT_SURROGATE, leaf_surrogate(i)),
                    node_lock_pair(ROOT),
                    node_lock_pair(&name),
                ]);
            }
        }

        let vshards = participants(&txn);
        assert_eq!(
            vshards.iter().copied().collect::<BTreeSet<u32>>(),
            pairs_by_home.keys().copied().collect::<BTreeSet<u32>>(),
            "the participants are exactly the endpoint homes"
        );

        let root_home = VShardId::from_key(ROOT.as_bytes()).as_u32();
        let mut total = 0usize;
        for &v in &vshards {
            let mut table = LockManager::new();
            let keys = expand_rw_set(&txn, LockSite::On(v));
            assert_eq!(
                table.acquire(TxnId::new(1, 0), keys).outcome,
                AcquireOutcome::Ready
            );
            // The pairs homed on `v`, plus the collection's `Intent` key.
            let expected = pairs_by_home.get(&v).map_or(0, BTreeSet::len) + 1;
            assert_eq!(table.lock_count(), expected, "vShard {v}");
            if v == root_home {
                assert_eq!(table.lock_count(), 2 * N as usize + 2);
            }
            total += table.lock_count();
        }
        assert!(
            total <= 4 * N as usize + 2 * vshards.len(),
            "total locks {total} over {} participants",
            vshards.len()
        );
    }

    /// At each participant, a batch locks exactly the union of what its
    /// single-edge writes lock there.
    #[test]
    fn star_batch_locks_the_union_of_its_single_edges() {
        const N: u32 = 64;
        let batch = sequenced(star_batch(N));
        let singles: Vec<SequencedTxn> = (0..N).map(|i| sequenced(star_edge(i))).collect();

        for v in participants(&batch) {
            let mut union: BTreeMap<LockKey, LockMode> = BTreeMap::new();
            for single in singles.iter().filter(|s| participants(s).contains(&v)) {
                for (key, mode) in expand_rw_set(single, LockSite::On(v)) {
                    union
                        .entry(key)
                        .and_modify(|held| *held = held.merge(mode))
                        .or_insert(mode);
                }
            }
            assert_eq!(expand_rw_set(&batch, LockSite::On(v)), union, "vShard {v}");
        }
    }

    /// A vShard takes the unhomed pairs and its own run. `Everywhere` takes
    /// every pair.
    #[test]
    fn pairs_on_selects_unhomed_pairs_and_the_own_run() {
        let edges = nodedb_cluster::calvin::types::SortedVec::new(vec![
            HomedPair::on(7, (1, 2)),
            HomedPair::everywhere((9, 9)),
            HomedPair::on(3, (4, 5)),
            HomedPair::on(7, (6, 0)),
            HomedPair::on(3, (1, 2)),
        ]);
        let on = |site| pairs_on(edges.as_slice(), site).collect::<Vec<_>>();
        assert_eq!(on(LockSite::On(7)), vec![(9, 9), (1, 2), (6, 0)]);
        assert_eq!(on(LockSite::On(3)), vec![(9, 9), (1, 2), (4, 5)]);
        assert_eq!(on(LockSite::On(5)), vec![(9, 9)]);
        assert_eq!(on(LockSite::Everywhere).len(), 5);
    }
}
