// SPDX-License-Identifier: BUSL-1.1

//! Read-set projections and node lock pairs shared by the static and
//! dependent `TxClass` builders. The write keys live in
//! [`super::write_keys`].

use std::collections::{BTreeMap, BTreeSet};

use crate::control::server::shared::session::read_set::{ReadKey, ReadPoint, ReadSetEntry};
use nodedb_cluster::calvin::types::{
    EngineKeySet, EngineTag, ReadKeyIdent, ReadWriteSet, SortedVec, VersionedReadEntry,
    VersionedReadSet,
};

/// Map the neutral session read-set into the replicated, versioned
/// [`VersionedReadSet`] carried on the `TxClass`.
///
/// Each [`ReadSetEntry`] becomes one [`VersionedReadEntry`], preserving
/// engine, collection, read version, home, and the point/predicate
/// distinction. Own-overlay exclusion already happened at capture time, so
/// this is a faithful 1:1 projection.
pub(super) fn versioned_reads_from(reads: &[ReadSetEntry]) -> VersionedReadSet {
    VersionedReadSet::new(
        reads
            .iter()
            .map(|entry| VersionedReadEntry {
                engine: entry.engine,
                collection: entry.collection.clone(),
                key: match &entry.key {
                    ReadKey::Point { repr } => ReadKeyIdent::Point(repr.to_key_repr()),
                    ReadKey::Predicate => ReadKeyIdent::Predicate,
                    ReadKey::IndexEq { field, value } => ReadKeyIdent::IndexEq {
                        field: field.clone(),
                        value: value.clone(),
                    },
                    ReadKey::IndexRange { field, lo, hi } => ReadKeyIdent::IndexRange {
                        field: field.clone(),
                        lo: lo.clone(),
                        hi: hi.clone(),
                    },
                },
                read_version: entry.read_version,
                home_vshard: entry.home.map(|home| home.as_u32()),
            })
            .collect(),
    )
}

/// Build the routing and lock `read_set` for a Calvin `TxClass` from the
/// neutral session read-set. This is the key-IDENTITY set for participants
/// and locks, not the versioned OCC set ([`versioned_reads_from`]).
///
/// Each read maps to the keys the scheduler locks `Shared`:
/// - A point read locks its row: a surrogate or a KV key.
/// - A predicate, index-equality, or index-range read locks its whole
///   collection.
///
/// An unhomed read participates on its collection's vShard. A homed read
/// (one vShard of a cross-shard graph read) participates on its home only.
/// A homed read with no collection name enlists its home and locks nothing.
pub(super) fn read_set_from(reads: &[ReadSetEntry]) -> ReadWriteSet {
    let mut locks = ReadLocks::default();
    for entry in reads {
        match entry.home {
            Some(home) => locks.homed(entry, home.as_u32()),
            None if entry.collection.is_empty() => {}
            None => locks.unhomed(entry),
        }
    }
    ReadWriteSet::new(locks.into_key_sets())
}

/// The read lock keys of a session read-set, by engine and collection.
/// Every map is ordered, so the key sets are identical on every node.
#[derive(Default)]
struct ReadLocks {
    documents: BTreeMap<String, Vec<u32>>,
    vectors: BTreeMap<String, Vec<u32>>,
    kv: BTreeMap<String, Vec<Vec<u8>>>,
    /// Collection to the vShards its homed reads enlist without a lock key.
    homes: BTreeMap<String, Vec<u32>>,
    /// Collection to the vShards it is read whole on. Empty: its own vShard.
    whole: BTreeMap<String, BTreeSet<u32>>,
}

impl ReadLocks {
    /// A read homed on `home`: it participates there only.
    fn homed(&mut self, entry: &ReadSetEntry, home: u32) {
        let collection = entry.collection.clone();
        if collection.is_empty() {
            self.homes.entry(collection).or_default().push(home);
            return;
        }
        match &entry.key {
            // A row key routes by its collection, not by this home, so a
            // homed row read only enlists its home.
            ReadKey::Point { .. } => self.homes.entry(collection).or_default().push(home),
            ReadKey::Predicate | ReadKey::IndexEq { .. } | ReadKey::IndexRange { .. } => {
                self.whole.entry(collection).or_default().insert(home);
            }
        }
    }

    /// A read of a named collection on the collection's own vShard.
    fn unhomed(&mut self, entry: &ReadSetEntry) {
        let collection = entry.collection.clone();
        match &entry.key {
            ReadKey::Point {
                repr: ReadPoint::Surrogate(surrogate),
            } => {
                let rows = match entry.engine {
                    EngineTag::Vector => &mut self.vectors,
                    EngineTag::Document
                    | EngineTag::Graph
                    | EngineTag::Kv
                    | EngineTag::Text
                    | EngineTag::Columnar
                    | EngineTag::Timeseries
                    | EngineTag::Spatial
                    | EngineTag::Crdt
                    | EngineTag::Query
                    | EngineTag::Meta
                    | EngineTag::Array
                    | EngineTag::ClusterArray => &mut self.documents,
                };
                rows.entry(collection).or_default().push(*surrogate);
            }
            ReadKey::Point {
                repr: ReadPoint::KvKey(key),
            } => self.kv.entry(collection).or_default().push(key.to_vec()),
            ReadKey::Predicate | ReadKey::IndexEq { .. } | ReadKey::IndexRange { .. } => {
                self.whole.entry(collection).or_default();
            }
        }
    }

    /// The key sets, one per engine and collection, ordered by collection.
    fn into_key_sets(self) -> Vec<EngineKeySet> {
        let mut sets: Vec<EngineKeySet> = Vec::new();
        sets.extend(
            self.documents
                .into_iter()
                .map(|(collection, rows)| EngineKeySet::Document {
                    collection,
                    surrogates: SortedVec::new(rows),
                }),
        );
        sets.extend(
            self.vectors
                .into_iter()
                .map(|(collection, rows)| EngineKeySet::Vector {
                    collection,
                    surrogates: SortedVec::new(rows),
                }),
        );
        sets.extend(
            self.kv
                .into_iter()
                .map(|(collection, keys)| EngineKeySet::Kv {
                    collection,
                    keys: SortedVec::new(keys),
                }),
        );
        sets.extend(
            self.homes
                .into_iter()
                .map(|(collection, homes)| EngineKeySet::Edge {
                    collection,
                    edges: SortedVec::new(Vec::new()),
                    home_vshards: SortedVec::new(homes),
                }),
        );
        sets.extend(
            self.whole
                .into_iter()
                .map(|(collection, vshards)| EngineKeySet::Collection {
                    collection,
                    vshards: SortedVec::new(vshards.into_iter().collect()),
                }),
        );
        sets.sort_by(|a, b| a.collection().cmp(b.collection()));
        sets
    }
}

/// The lock pair of node `node` within one edge collection.
///
/// Every edge write takes it for both endpoints, and a node delete's guard
/// takes it for its node, so a guard and every write of an edge on its node
/// run in sequence order on the node's key home. A real edge never locks a
/// pair with a zero destination surrogate, so the pair never aliases an
/// edge. Two node names that hash alike only serialize together.
pub(crate) fn node_lock_pair(node: &str) -> (u32, u32) {
    let hash = crate::util::fnv1a_hash(node.as_bytes());
    ((hash ^ (hash >> 32)) as u32, 0)
}

/// Lockstep proof that the write-admission gate and the Calvin scheduler
/// derive IDENTICAL lock keys for the same op — if they diverged, a
/// gate-fenced write and a sequenced txn will lock different keys.
#[cfg(test)]
mod lockstep_tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::helpers::{LockSite, expand_rw_set};
    use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, LockMode};
    use crate::control::planner::calvin::tx_class::static_builder::build_single_vshard_tx_class;
    use crate::control::server::shared::write_admission::lock_keys::plan_lock_keys;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_cluster::calvin::types::{SequencedTxn, TxClass};
    use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::Surrogate;
    use std::collections::BTreeSet;
    use std::sync::Arc;

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

    /// The row keys the scheduler locks `Exclusive` for `tx`, from its real
    /// `expand_rw_set`. The collection's `Intent` key is left out: the gate
    /// fences row keys only.
    fn scheduler_lock_keys(tx: TxClass) -> BTreeSet<LockKey> {
        expand_rw_set(
            &SequencedTxn {
                epoch: 1,
                position: 0,
                tx_class: std::sync::Arc::new(tx),
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
                local_plans: None,
            },
            LockSite::Everywhere,
        )
        .into_iter()
        .filter(|(_, mode)| *mode == LockMode::Exclusive)
        .map(|(key, _)| key)
        .collect()
    }

    fn assert_gate_matches_scheduler(plan: PhysicalPlan) {
        let t = task(plan);
        let (_, gate_keys) =
            plan_lock_keys(&t.plan).expect("op must be fast-path eligible for this test");
        let tx = build_single_vshard_tx_class(&[t], TenantId::new(1), &[])
            .expect("valid single-vshard TxClass");
        let scheduler_keys = scheduler_lock_keys(tx);
        assert_eq!(
            gate_keys, scheduler_keys,
            "gate and scheduler must lock the identical key set"
        );
    }

    #[test]
    fn kv_incr_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Kv(KvOp::Incr {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "counters",
            ),
            key: b"ctr".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: Surrogate::new(3),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
        }));
    }

    #[test]
    fn kv_cas_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Kv(KvOp::Cas {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "counters",
            ),
            key: b"ctr".to_vec(),
            expected: vec![],
            new_value: vec![],
            surrogate: Surrogate::new(3),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        }));
    }

    #[test]
    fn document_upsert_gate_key_matches_scheduler_key() {
        assert_gate_matches_scheduler(PhysicalPlan::Document(DocumentOp::Upsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".to_owned(),
            value: vec![],
            on_conflict_updates: vec![],
            surrogate: Surrogate::new(9),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }));
    }

    /// A bound point delete on the fast path locks its surrogate and its row
    /// id, exactly as the scheduler does, so an unbound delete sequenced
    /// through Calvin orders against it by the row id. The fence and the
    /// keyed order lock still name the row by its surrogate key alone.
    #[test]
    fn document_point_delete_gate_keys_match_scheduler_keys_with_row_id() {
        use crate::control::server::shared::write_admission::lock_keys::plan_row_key;
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".to_owned(),
            surrogate: Some(Surrogate::new(9)),
            pk_bytes: b"d1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        let (_, gate_keys) = plan_lock_keys(&plan).expect("a bound point delete is fast-path");
        assert!(gate_keys.contains(&LockKey::Kv {
            collection: Arc::from("docs"),
            key: Arc::from(b"d1".as_slice()),
        }));
        assert_eq!(
            plan_row_key(&plan),
            Some(LockKey::Surrogate {
                collection: Arc::from("docs"),
                surrogate: 9,
            })
        );
        assert_gate_matches_scheduler(plan);
    }

    /// A single-home edge write on the fast path locks its edge and both
    /// endpoints' node lock pairs, exactly as the scheduler does, so a node
    /// delete's guard orders against it either way.
    #[test]
    fn edge_put_gate_keys_match_scheduler_keys_with_node_locks() {
        let src = "n0";
        let dst = (1u32..)
            .map(|i| format!("n{i}"))
            .find(|name| {
                crate::types::VShardId::from_key(name.as_bytes())
                    == crate::types::VShardId::from_key(src.as_bytes())
            })
            .expect("some node shares the source's key home");
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "g",
            ),
            src_id: src.to_owned(),
            label: "L".to_owned(),
            dst_id: dst.clone(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        });
        let (_, gate_keys) = plan_lock_keys(&plan).expect("a single-home edge is fast-path");
        for node in [src, dst.as_str()] {
            let (lock_src, lock_dst) = node_lock_pair(node);
            assert!(gate_keys.contains(&LockKey::Edge {
                collection: Arc::from("g"),
                src: lock_src,
                dst: lock_dst,
            }));
        }
        assert_gate_matches_scheduler(plan);
    }
}

/// The participant set and the routing oracle must agree about where a plan
/// lives: the write keys' collections feed the participant list, and the
/// scheduler's `plan_vshard` oracle decides who gets the plan. A
/// disagreement enlists a shard, hands it nothing, and aborts far from the
/// cause. These tests pin the agreement directly.
#[cfg(test)]
mod routing_agreement_tests {
    use crate::control::planner::calvin::tx_class::static_builder::build_static_tx_class;
    use crate::control::planner::calvin::tx_class::write_keys::{WriteKeys, add_plan_write_keys};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::Surrogate;

    const TENANT: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    /// The binding's source and its balance target. Asserted to hash apart
    /// by [`the_fixture_spans_two_vshards`] — a co-resident pair will never
    /// produce the two-task plan this file is about.
    const SOURCE: &str = "route_entries";
    const TARGET: &str = "route_accounts";

    /// Build a task homed the way PRODUCTION homes it, not by asking the
    /// write keys, which makes the agreement true by construction.
    fn task(plan: PhysicalPlan, vshard_id: VShardId) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TENANT,
            vshard_id,
            database_id: DB,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// The pair a cross-shard materialized-sum statement produces: the source
    /// write, and the balance task homed by the same function
    /// `append_cross_shard_balance_tasks` homes it with.
    fn statement_tasks() -> Vec<PhysicalTask> {
        vec![
            task(
                source_write(),
                nodedb_types::CollectionKey::from_bare(DB, SOURCE).vshard(),
            ),
            task(balance_write(), crate::query::sum_target_vshard(DB, TARGET)),
        ]
    }

    fn source_write() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: nodedb_types::QualifiedCollection::new(DB, SOURCE),
            document_id: "e1".to_owned(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(11),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn balance_write() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: nodedb_types::QualifiedCollection::new(DB, TARGET),
            document_id: "0000010f".to_owned(),
            surrogate: Surrogate::new(271),
            column: "balance".to_owned(),
            delta: "25".to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc-1".to_owned(),
            declared_primary_key: None,
        })
    }

    #[test]
    fn the_fixture_spans_two_vshards() {
        assert_ne!(
            nodedb_types::CollectionKey::from_bare(DB, SOURCE).vshard(),
            nodedb_types::CollectionKey::from_bare(DB, TARGET).vshard(),
            "the balance-pairing case only exists when source and target hash apart"
        );
    }

    /// A balance write locks the TARGET row of the TARGET collection it
    /// names: not an empty collection on vShard 0, and not the collection
    /// key every balance write will share.
    #[test]
    fn a_balance_write_locks_the_target_row() {
        let mut keys = WriteKeys::default();
        add_plan_write_keys(&mut keys, &balance_write()).expect("a balance write has keys");
        assert_eq!(
            keys.into_key_sets(),
            vec![EngineKeySet::Document {
                collection: TARGET.to_owned(),
                surrogates: SortedVec::new(vec![271]),
            }]
        );
    }

    /// The pair enlists exactly the two shards that hold work, no third.
    #[test]
    fn the_pair_enlists_only_the_shards_that_hold_work() {
        let tasks = statement_tasks();
        let tx = build_static_tx_class(&tasks, TENANT, &[]).expect("build the transaction class");

        let mut expected = vec![
            nodedb_types::CollectionKey::from_bare(DB, SOURCE).vshard(),
            nodedb_types::CollectionKey::from_bare(DB, TARGET).vshard(),
        ];
        expected.sort_by_key(|v| v.as_u32());
        assert_eq!(
            tx.participating_vshards(),
            expected.as_slice(),
            "every enlisted shard must be one the routing oracle sends a plan to"
        );
    }

    /// Every task's own home agrees with the participant the class enlists for
    /// it. Stated over the task list rather than over one op, so a future write
    /// shape appended alongside a source write is covered by the same rule.
    #[test]
    fn every_task_homes_on_a_shard_the_class_enlists() {
        let tasks = statement_tasks();
        let tx = build_static_tx_class(&tasks, TENANT, &[]).expect("build the transaction class");
        for task in &tasks {
            assert!(
                tx.participating_vshards().contains(&task.vshard_id),
                "task homed on {:?} is not enlisted; it would be dispatched to a shard \
                 that never voted",
                task.vshard_id
            );
        }
    }
}

/// A graph read and an edge write meet on every vShard the read observed.
#[cfg(test)]
mod graph_read_lock_tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::helpers::{LockSite, expand_rw_set};
    use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;
    use crate::control::planner::calvin::tx_class::write_keys::WriteKeys;
    use crate::control::server::shared::session::read_set::ReadOrigin;
    use crate::types::{DatabaseId, RecordHomes, TenantId, VShardId};
    use nodedb_cluster::calvin::types::{SequencedTxn, TxClass};
    use std::sync::Arc;

    fn graph_read(home: Option<VShardId>) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Graph,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: "g".to_owned(),
            key: ReadKey::Predicate,
            read_version: nodedb_types::WriteVersion::logged(0, 5),
            origin: ReadOrigin::Session,
            home,
        }
    }

    fn sequenced(read_set: ReadWriteSet, write_keys: WriteKeys) -> SequencedTxn {
        let tx_class = TxClass::new_single_vshard_in_database(
            read_set,
            ReadWriteSet::new(write_keys.into_key_sets()),
            Vec::new(),
            TenantId::new(1),
            DatabaseId::DEFAULT,
            None,
            VersionedReadSet::default(),
        )
        .expect("valid single-vshard TxClass");
        SequencedTxn {
            epoch: 1,
            position: 0,
            tx_class: Arc::new(tx_class),
            epoch_system_ms: 1_700_000_000_000,
            epoch_vshard_txn_count: 1,
            lock_owner: None,
            local_plans: None,
        }
    }

    #[test]
    fn a_graph_read_conflicts_with_an_edge_write_on_the_observed_vshard() {
        let owner = RecordHomes::edge("a", "b").owner();

        let mut reader_writes = WriteKeys::default();
        reader_writes.rows("other", [1]);
        let reader = sequenced(
            read_set_from(&[graph_read(None), graph_read(Some(owner))]),
            reader_writes,
        );

        let mut edge_writes = WriteKeys::default();
        edge_writes.edge("g", "a", "b", (1, 2));
        let writer = sequenced(ReadWriteSet::new(Vec::new()), edge_writes);

        let site = LockSite::On(owner.as_u32());
        let whole = LockKey::Collection {
            collection: Arc::from("g"),
        };
        let read_mode = *expand_rw_set(&reader, site)
            .get(&whole)
            .expect("the graph read locks the collection");
        let write_mode = *expand_rw_set(&writer, site)
            .get(&whole)
            .expect("the edge write announces on the collection");
        assert!(
            !read_mode.compatible(write_mode),
            "a graph read ({read_mode:?}) must conflict with an edge write ({write_mode:?})"
        );
    }
}
