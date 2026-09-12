// SPDX-License-Identifier: BUSL-1.1

//! Write-set-identity extractors shared by the static and dependent `TxClass`
//! builders.

use crate::control::server::shared::session::read_set::{ReadKey, ReadSetEntry};
use nodedb_cluster::calvin::types::{
    EngineKeySet, EngineTag, ReadKeyIdent, ReadWriteSet, SortedVec, VersionedReadEntry,
    VersionedReadSet,
};
use nodedb_physical::physical_plan::{
    DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};

/// Map the neutral session read-set into the replicated, LSN-versioned
/// [`VersionedReadSet`] carried on the `TxClass`.
///
/// Each [`ReadSetEntry`] becomes one [`VersionedReadEntry`], preserving
/// engine, collection, `read_version_lsn` (the collection's write floor —
/// the sound OCC comparand, not the core-global `read_lsn`), and the
/// point/predicate distinction. Own-overlay exclusion already happened at
/// capture time, so this is a faithful 1:1 projection.
pub(super) fn versioned_reads_from(reads: &[ReadSetEntry]) -> VersionedReadSet {
    VersionedReadSet::new(
        reads
            .iter()
            .map(|entry| VersionedReadEntry {
                engine: entry.engine,
                collection: entry.collection.clone(),
                key: match &entry.key {
                    ReadKey::Point { repr } => ReadKeyIdent::Point(repr.clone()),
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
                read_lsn: entry.read_version_lsn,
            })
            .collect(),
    )
}

/// Build the routing/identity `read_set` for a Calvin `TxClass` from the
/// neutral session read-set — the key-IDENTITY set for participant/routing,
/// not the LSN-versioned OCC set ([`versioned_reads_from`]).
///
/// A [`ReadSetEntry`] carries only `(engine, collection)`, no key identity,
/// so it maps to a COLLECTION-homed [`EngineKeySet`] with an empty key
/// vector — over-approximating participants, which is required and safe for
/// a graph/edge read (no endpoint homes to build an `EngineKeySet::Edge`
/// from): more validation, never a dropped participant.
pub(super) fn read_set_from(reads: &[ReadSetEntry]) -> ReadWriteSet {
    use std::collections::BTreeSet;

    // Dedup by (engine-variant, collection): many reads on one collection
    // collapse to a single keyset, keeping the Raft-log read_set compact.
    // Vector/KV keep their engine variant; every other engine routes by
    // collection name via a Document keyset.
    let mut vector_colls: BTreeSet<String> = BTreeSet::new();
    let mut kv_colls: BTreeSet<String> = BTreeSet::new();
    let mut doc_colls: BTreeSet<String> = BTreeSet::new();
    for entry in reads {
        if entry.collection.is_empty() {
            continue;
        }
        match entry.engine {
            EngineTag::Vector => {
                vector_colls.insert(entry.collection.clone());
            }
            EngineTag::Kv => {
                kv_colls.insert(entry.collection.clone());
            }
            EngineTag::Document
            | EngineTag::Graph
            | EngineTag::Text
            | EngineTag::Columnar
            | EngineTag::Timeseries
            | EngineTag::Spatial
            | EngineTag::Crdt
            | EngineTag::Query
            | EngineTag::Meta
            | EngineTag::Array
            | EngineTag::ClusterArray => {
                doc_colls.insert(entry.collection.clone());
            }
        }
    }
    let mut sets: Vec<EngineKeySet> = Vec::new();
    for collection in vector_colls {
        sets.push(EngineKeySet::Vector {
            collection,
            surrogates: SortedVec::new(vec![]),
        });
    }
    for collection in kv_colls {
        sets.push(EngineKeySet::Kv {
            collection,
            keys: SortedVec::new(vec![]),
        });
    }
    for collection in doc_colls {
        sets.push(EngineKeySet::Document {
            collection,
            surrogates: SortedVec::new(vec![]),
        });
    }
    ReadWriteSet::new(sets)
}

/// Extract `(collection, raw byte keys)` from a KV write plan, or `None` for a
/// KV op with no statically-known point keys (e.g. `BatchPut`). Single-key
/// read-modify-write ops (`Incr`/`IncrFloat`/`Cas`/`GetSet`/`FieldSet`) key on
/// the same `(collection, key)` pair as `Put`/`Insert`, so they sequence
/// identically to the write-admission gate's `kv_point_key`.
pub(super) fn kv_write_keys(op: &KvOp) -> Option<(String, Vec<Vec<u8>>)> {
    match op {
        KvOp::Put {
            collection, key, ..
        }
        | KvOp::Insert {
            collection, key, ..
        }
        | KvOp::InsertIfAbsent {
            collection, key, ..
        }
        | KvOp::InsertOnConflictUpdate {
            collection, key, ..
        }
        | KvOp::Incr {
            collection, key, ..
        }
        | KvOp::IncrFloat {
            collection, key, ..
        }
        | KvOp::Cas {
            collection, key, ..
        }
        | KvOp::GetSet {
            collection, key, ..
        }
        | KvOp::FieldSet {
            collection, key, ..
        } => Some((collection.to_string(), vec![key.clone()])),
        KvOp::Delete {
            collection, keys, ..
        } => Some((collection.to_string(), keys.clone())),
        _ => None,
    }
}

/// Extract `(collection, surrogates)` from a Vector write plan, or `None` for a
/// Vector op with no statically-known surrogate identity (e.g. node-id delete).
pub(super) fn vector_write_surrogates(op: &VectorOp) -> Option<(String, Vec<u32>)> {
    match op {
        VectorOp::Insert {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            ..
        } => Some((collection.to_string(), vec![surrogate.as_u32()])),
        VectorOp::BatchInsert {
            collection,
            surrogates,
            ..
        } => Some((
            collection.to_string(),
            surrogates.iter().map(|s| s.as_u32()).collect(),
        )),
        _ => None,
    }
}

/// Extract the collection name from a write plan.
///
/// This name feeds the participant set (hashed by
/// `participating_vshards_in_database`), so an empty name doesn't fail
/// loudly — it hashes to vShard 0, gets enlisted, and aborts far from the
/// real cause ("homes no local write plans or reads"). The document arm is
/// exhaustive for exactly that reason: a new `DocumentOp` is a compile
/// error here, not a silent empty name.
pub(crate) fn collection_name_from_plan(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Document(op) => document_write_collection(op),
        PhysicalPlan::Kv(
            KvOp::Put { collection, .. }
            | KvOp::Insert { collection, .. }
            | KvOp::InsertIfAbsent { collection, .. }
            | KvOp::InsertOnConflictUpdate { collection, .. }
            | KvOp::Delete { collection, .. }
            | KvOp::BatchPut { collection, .. }
            | KvOp::Incr { collection, .. }
            | KvOp::IncrFloat { collection, .. }
            | KvOp::Cas { collection, .. }
            | KvOp::GetSet { collection, .. }
            | KvOp::FieldSet { collection, .. }
            // Predicate DML homes on the collection it names — matching
            // `plan_vshard`'s KV routing, which the participant set must
            // agree with.
            | KvOp::PredicateUpdate { collection, .. }
            | KvOp::PredicateDelete { collection, .. },
        ) => collection.to_string(),
        PhysicalPlan::Vector(
            VectorOp::Insert { collection, .. }
            | VectorOp::BatchInsert { collection, .. }
            | VectorOp::Delete { collection, .. }
            | VectorOp::DeleteBySurrogate { collection, .. },
        ) => collection.to_string(),
        PhysicalPlan::Graph(
            GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
        ) => collection.to_string(),
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
            collection.to_string()
        }
        _ => String::new(),
    }
}

/// The collection a DOCUMENT write plan homes on, exhaustively.
fn document_write_collection(op: &DocumentOp) -> String {
    match op {
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. }
        | DocumentOp::Truncate { collection, .. }
        // Homes on the TARGET collection — same one the routing oracle uses.
        | DocumentOp::ApplyBalanceDelta { collection, .. } => collection.to_string(),
        DocumentOp::InsertSelect {
            target_collection, ..
        } => target_collection.to_string(),
        // Cross-collection writes: Control-Plane orchestrators resolve them
        // into concrete point writes before dispatch, so no raw plan reaches
        // this builder; the routing oracle names them `Unroutable`.
        DocumentOp::Merge { .. } | DocumentOp::UpdateFromJoin { .. } => String::new(),
        // Proposed directly through Raft, so no plan reaches this builder.
        DocumentOp::ResolvedWrite { .. } => String::new(),
        // Reads and index DDL: caller skips non-`is_write_plan` before here.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => String::new(),
    }
}

/// Extract a surrogate from a write plan (returns 0 when unavailable).
pub(super) fn surrogate_from_plan(plan: &PhysicalPlan) -> u32 {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { surrogate, .. }
            | DocumentOp::PointInsert { surrogate, .. }
            | DocumentOp::PointDelete { surrogate, .. }
            | DocumentOp::PointUpdate { surrogate, .. }
            | DocumentOp::Upsert { surrogate, .. }
            // The TARGET row's identity — two balance writes onto one row
            // serialize; falling through to `0` would lock all of them together.
            | DocumentOp::ApplyBalanceDelta { surrogate, .. },
        ) => surrogate.as_u32(),
        _ => 0,
    }
}

/// Lockstep proof that the write-admission gate and the Calvin scheduler
/// derive IDENTICAL lock keys for the same op — if they diverged, a
/// gate-fenced write and a sequenced txn would lock different keys.
#[cfg(test)]
mod lockstep_tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;
    use crate::control::planner::calvin::tx_class::static_builder::build_single_vshard_tx_class;
    use crate::control::server::shared::write_admission::lock_keys::plan_lock_keys;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_cluster::calvin::types::EngineKeySet;
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

    /// Mirrors the scheduler's `expand_rw_set` `EngineKeySet` → `LockKey`
    /// mapping (fixed translation). What's under test is whether the
    /// extractor threads the same `(collection, key/surrogate)` as the
    /// gate's `plan_lock_keys`, not this mapping.
    fn scheduler_lock_keys(sets: &[EngineKeySet]) -> BTreeSet<LockKey> {
        let mut keys = BTreeSet::new();
        for ks in sets {
            match ks {
                EngineKeySet::Document {
                    collection,
                    surrogates,
                }
                | EngineKeySet::Vector {
                    collection,
                    surrogates,
                } => {
                    let coll: Arc<str> = Arc::from(collection.as_str());
                    for &surrogate in surrogates.iter() {
                        keys.insert(LockKey::Surrogate {
                            collection: Arc::clone(&coll),
                            surrogate,
                        });
                    }
                }
                EngineKeySet::Kv {
                    collection,
                    keys: kv_keys,
                } => {
                    let coll: Arc<str> = Arc::from(collection.as_str());
                    for k in kv_keys.iter() {
                        keys.insert(LockKey::Kv {
                            collection: Arc::clone(&coll),
                            key: Arc::from(k.as_slice()),
                        });
                    }
                }
                EngineKeySet::Edge {
                    collection, edges, ..
                } => {
                    let coll: Arc<str> = Arc::from(collection.as_str());
                    for &(src, dst) in edges.iter() {
                        keys.insert(LockKey::Edge {
                            collection: Arc::clone(&coll),
                            src,
                            dst,
                        });
                    }
                }
            }
        }
        keys
    }

    fn assert_gate_matches_scheduler(plan: PhysicalPlan) {
        let t = task(plan);
        let (_, gate_keys) =
            plan_lock_keys(&t.plan).expect("op must be fast-path eligible for this test");
        let tx = build_single_vshard_tx_class(&[t], TenantId::new(1), &[])
            .expect("valid single-vshard TxClass");
        let scheduler_keys = scheduler_lock_keys(&tx.write_set.0);
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
}

/// The participant set and the routing oracle must agree about where a plan
/// lives: [`collection_name_from_plan`] feeds the participant list, the
/// scheduler's `plan_vshard` oracle decides who actually gets the plan. A
/// disagreement enlists a shard, hands it nothing, and aborts far from the
/// cause. An op missing from the extractor silently hashes to vShard 0.
/// These tests pin the agreement directly.
#[cfg(test)]
mod routing_agreement_tests {
    use super::*;
    use crate::control::planner::calvin::tx_class::static_builder::build_static_tx_class;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::Surrogate;

    const TENANT: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    /// The binding's source and its balance target. Asserted to hash apart
    /// by [`the_fixture_spans_two_vshards`] — a co-resident pair would never
    /// produce the two-task plan this file is about.
    const SOURCE: &str = "route_entries";
    const TARGET: &str = "route_accounts";

    /// Build a task homed the way PRODUCTION homes it, not by asking
    /// [`collection_name_from_plan`] — that would make the agreement true
    /// by construction and prove nothing.
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
                VShardId::from_collection_in_database(DB, SOURCE),
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
            VShardId::from_collection_in_database(DB, SOURCE),
            VShardId::from_collection_in_database(DB, TARGET),
            "the balance-pairing case only exists when source and target hash apart"
        );
    }

    /// A balance write reports the TARGET collection it names — an empty
    /// name here enlisted vShard 0 while the plan went to the target's shard.
    #[test]
    fn a_balance_write_reports_the_collection_it_mutates() {
        assert_eq!(collection_name_from_plan(&balance_write()), TARGET);
    }

    /// And the TARGET ROW's surrogate; falling through to `0` made every
    /// balance write share one lock key.
    #[test]
    fn a_balance_write_reports_the_target_rows_surrogate() {
        assert_eq!(surrogate_from_plan(&balance_write()), 271);
    }

    /// The pair enlists exactly the two shards that hold work — no third.
    /// Before the extractor named `ApplyBalanceDelta`, this came back as
    /// source-shard + vShard 0, and vShard 0 held no plan.
    #[test]
    fn the_pair_enlists_only_the_shards_that_hold_work() {
        let tasks = statement_tasks();
        let tx = build_static_tx_class(&tasks, TENANT, &[]).expect("build the transaction class");

        let mut expected = vec![
            VShardId::from_collection_in_database(DB, SOURCE),
            VShardId::from_collection_in_database(DB, TARGET),
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
