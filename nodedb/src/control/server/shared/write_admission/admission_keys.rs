// SPDX-License-Identifier: BUSL-1.1

//! The lock keys the write-admission gate takes for one write.
//!
//! The keys come from the Calvin write-key extraction and its lock-request
//! expansion. An autocommit write therefore takes the keys a Calvin
//! transaction of the same plan takes:
//!
//! - A row write locks each row `Exclusive` and its collection `Intent`.
//! - A collection-wide write locks its collection `Exclusive`: a predicate
//!   or bulk write, INSERT…SELECT, a truncate, a columnar update or delete,
//!   an array write, a KV predicate write, and a CRDT snapshot import.
//! - A document write that claims a UNIQUE value locks that value
//!   `Exclusive`.
//!
//! An append differs from its Calvin keys: a timeseries ingest or a columnar
//! insert locks the rows it names `Exclusive` and its collection `Intent`.
//!
//! - Two appends never contend at the gate. The data-group log orders them,
//!   alike on every replica.
//! - Every Calvin writer of a columnar or timeseries collection locks it
//!   `Exclusive`. So an append still orders against each Calvin writer.
//! - A contended append waits and never routes. A routed write applies
//!   outside the data-group log. Each replica then installs it against its
//!   own schema at its own point.
//!
//! A write no Calvin transaction sequences still takes keys:
//!
//! - A resolved write locks the rows it names, as a row write does.
//! - A text or spatial index write locks its collection `Intent`.
//! - A transaction batch takes the keys of every plan it carries.
//! - Any other write locks every collection it writes `Exclusive`.

use std::collections::BTreeMap;

use nodedb_cluster::calvin::types::EngineKeySet;
use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, KvOp, MetaOp, PhysicalPlan, TimeseriesOp, VectorOp,
};

use crate::control::cluster::calvin::scheduler::driver::helpers::expand_write_key_sets;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, LockMode};
use crate::control::planner::calvin::submit::unique_claims::catalog_unique_claim_sets;
use crate::control::planner::calvin::tx_class::write_keys::{WriteKeys, add_plan_write_keys};
use crate::control::planner::calvin::write_class::is_write_plan;
use crate::control::state::SharedState;
use crate::control::wal_replication::transaction_redo::collections::written_collections;
use crate::types::{DatabaseId, TenantId};

/// The moded lock request of one write.
#[derive(Debug, Default)]
pub(crate) struct AdmissionKeys {
    /// Every key the write holds, each in its mode.
    pub keys: BTreeMap<LockKey, LockMode>,
    /// Whether a contended write routes to the scheduler. A write a Calvin
    /// transaction cannot sequence, and an append, waits instead.
    pub sequenced: bool,
}

/// The lock request of `plan`, a write-class plan of `tenant_id` in
/// `database_id` that no Calvin scheduler applies.
pub(crate) fn plan_admission_keys(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<AdmissionKeys> {
    let (mut sets, sequenced) = plan_key_sets(plan)?;
    if sequenced {
        // A claim the plan does not carry locks the collection whole. The
        // expansion merges each key's modes, so the strongest mode wins.
        sets.extend(catalog_unique_claim_sets(
            shared,
            database_id,
            tenant_id.as_u64(),
            std::slice::from_ref(plan),
        )?);
    }
    Ok(AdmissionKeys {
        keys: expand_write_key_sets(&sets),
        sequenced,
    })
}

/// The lock request of a write that names only whole `collections`, each
/// locked `Exclusive`. A committed session transaction's redo and a synced
/// array op take it.
pub(crate) fn collection_admission_keys(collections: &[String]) -> AdmissionKeys {
    let mut keys = WriteKeys::default();
    for collection in collections {
        keys.whole_collection(collection);
    }
    AdmissionKeys {
        keys: expand_write_key_sets(&keys.into_key_sets()),
        sequenced: false,
    }
}

/// A Calvin-scheduled apply: the scheduler acquired its locks.
pub(super) fn is_calvin_apply(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Meta(
            MetaOp::CalvinExecuteStatic { .. }
                | MetaOp::CalvinExecutePassive { .. }
                | MetaOp::CalvinExecuteActive { .. }
                | MetaOp::RecordCalvinWriteVersions { .. }
                | MetaOp::CalvinFlush { .. }
                | MetaOp::CalvinDrop { .. }
                | MetaOp::CalvinResolve { .. }
        )
    )
}

/// The write key sets of `plan`, and whether it routes to the scheduler
/// when contended.
fn plan_key_sets(plan: &PhysicalPlan) -> crate::Result<(Vec<EngineKeySet>, bool)> {
    let mut keys = WriteKeys::default();
    match plan {
        // An append: its rows, and its collection `Intent` (see the module
        // docs). A timeseries row has no surrogate: its series names it.
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
            keys.rows(collection.as_str(), []);
        }
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection,
            surrogates,
            ..
        }) => keys.rows(
            collection.as_str(),
            surrogates.iter().map(|surrogate| surrogate.as_u32()),
        ),
        PhysicalPlan::Meta(MetaOp::TransactionBatch { plans, .. }) => {
            let mut sets = Vec::new();
            for inner in plans.iter().filter(|inner| is_write_plan(inner)) {
                sets.extend(plan_key_sets(inner)?.0);
            }
            return Ok((sets, false));
        }
        // An index write of rows its collection stores. No Calvin
        // transaction writes the index alone, so the collection key orders
        // it against a collection-wide writer.
        PhysicalPlan::Text(_) | PhysicalPlan::Spatial(_) => {
            for collection in written_collections(std::slice::from_ref(plan)) {
                keys.rows(&collection, []);
            }
        }
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => {
            if is_write_plan(plan) && add_plan_write_keys(&mut keys, plan).is_ok() {
                return Ok((keys.into_key_sets(), true));
            }
            // The extraction refuses a write no Calvin transaction can
            // sequence. The keys it added before refusing stay: they
            // over-lock and never under-lock.
            unsequenced_keys(&mut keys, plan);
        }
    }
    Ok((keys.into_key_sets(), false))
}

/// Add the keys of a write no Calvin transaction sequences: the rows a
/// resolved write names, or every collection the write names, whole.
fn unsequenced_keys(keys: &mut WriteKeys, plan: &PhysicalPlan) {
    match plan {
        PhysicalPlan::Document(DocumentOp::ResolvedWrite { mutations, .. }) => {
            for mutation in mutations {
                let collection = mutation.collection().as_str();
                keys.rows(collection, [mutation.surrogate().as_u32()]);
                keys.row_id(collection, mutation.document_id());
            }
        }
        PhysicalPlan::Kv(KvOp::ResolvedWrite { mutations, .. }) => {
            for mutation in mutations {
                keys.kv_keys(mutation.collection().as_str(), [mutation.key().to_vec()]);
            }
        }
        PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite {
            collection,
            mutations,
            ..
        }) => keys.vector_rows(
            collection.as_str(),
            mutations
                .iter()
                .map(|mutation| mutation.surrogate().as_u32()),
        ),
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => {
            for collection in written_collections(std::slice::from_ref(plan)) {
                keys.whole_collection(&collection);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{ColumnarInsertIntent, CrdtOp};
    use nodedb_physical::physical_plan::{DocumentResolvedMutation, KvResolvedMutation};

    use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::*;

    fn qualified(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    fn coll(name: &str) -> LockKey {
        LockKey::Collection {
            collection: Arc::from(qualified(name).as_str()),
        }
    }

    fn surrogate_key(name: &str, surrogate: u32) -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from(qualified(name).as_str()),
            surrogate,
        }
    }

    fn kv_key(name: &str, key: &[u8]) -> LockKey {
        LockKey::Kv {
            collection: Arc::from(qualified(name).as_str()),
            key: Arc::from(key),
        }
    }

    /// The keys of `plan`, with no UNIQUE claim lookup.
    fn keys_of(plan: &PhysicalPlan) -> (BTreeMap<LockKey, LockMode>, bool) {
        let (sets, sequenced) = plan_key_sets(plan).expect("key sets");
        (expand_write_key_sets(&sets), sequenced)
    }

    fn doc_insert(id: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: qualified("docs"),
            document_id: id.to_owned(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    /// A point write locks its row `Exclusive`, by surrogate and by row id,
    /// and its collection `Intent`.
    #[test]
    fn a_point_write_locks_its_row_and_announces_its_collection() {
        let (keys, sequenced) = keys_of(&doc_insert("x", 17));
        assert!(sequenced);
        assert_eq!(keys.get(&coll("docs")), Some(&LockMode::Intent));
        assert_eq!(
            keys.get(&surrogate_key("docs", 17)),
            Some(&LockMode::Exclusive)
        );
        assert_eq!(keys.get(&kv_key("docs", b"x")), Some(&LockMode::Exclusive));
        assert_eq!(keys.len(), 3);
    }

    /// A batch insert locks every row it inserts and its collection
    /// `Intent`.
    #[test]
    fn a_batch_insert_locks_each_row() {
        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: qualified("docs"),
            documents: vec![("a".to_owned(), Vec::new()), ("b".to_owned(), Vec::new())],
            surrogates: vec![Surrogate::new(1), Surrogate::new(2)],
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        let (keys, sequenced) = keys_of(&plan);
        assert!(sequenced);
        assert_eq!(keys.get(&coll("docs")), Some(&LockMode::Intent));
        for (id, surrogate) in [("a", 1), ("b", 2)] {
            assert_eq!(
                keys.get(&surrogate_key("docs", surrogate)),
                Some(&LockMode::Exclusive)
            );
            assert_eq!(
                keys.get(&kv_key("docs", id.as_bytes())),
                Some(&LockMode::Exclusive)
            );
        }
    }

    /// Every collection-wide shape locks its collection `Exclusive` and
    /// routes to the scheduler when contended.
    #[test]
    fn a_collection_wide_write_locks_its_collection_exclusive() {
        let plans = [
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection: qualified("docs"),
                restart_identity: false,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: qualified("docs"),
                filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: qualified("docs"),
                restart_identity: false,
            }),
            PhysicalPlan::Kv(KvOp::Truncate {
                collection: qualified("docs"),
                restart_identity: false,
            }),
            PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
                tenant_id: 1,
                collection: qualified("docs"),
                bytes: Vec::new(),
            }),
        ];
        for plan in plans {
            let (keys, sequenced) = keys_of(&plan);
            assert!(sequenced, "{plan:?}");
            assert_eq!(
                keys.get(&coll("docs")),
                Some(&LockMode::Exclusive),
                "{plan:?}"
            );
        }
    }

    /// A resolved write no Calvin transaction sequences still locks the rows
    /// it names, and waits rather than routes when contended.
    #[test]
    fn a_resolved_write_locks_the_rows_it_names() {
        let doc = PhysicalPlan::Document(DocumentOp::ResolvedWrite {
            mutations: vec![DocumentResolvedMutation::Delete {
                collection: qualified("docs"),
                document_id: "x".to_owned(),
                surrogate: Surrogate::new(17),
                pk_bytes: b"x".to_vec(),
                precondition: None,
                resolved_sum_targets: Vec::new(),
            }],
            response_payload: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        let (keys, sequenced) = keys_of(&doc);
        assert!(!sequenced);
        assert_eq!(
            keys.get(&surrogate_key("docs", 17)),
            Some(&LockMode::Exclusive)
        );
        assert_eq!(keys.get(&coll("docs")), Some(&LockMode::Intent));

        let kv = PhysicalPlan::Kv(KvOp::ResolvedWrite {
            mutations: vec![KvResolvedMutation::Put {
                collection: qualified("kv"),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                expire_at_ms: 0,
                surrogate: Surrogate::new(5),
                precondition: None,
            }],
            response_payload: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        let (keys, sequenced) = keys_of(&kv);
        assert!(!sequenced);
        assert_eq!(keys.get(&kv_key("kv", b"k")), Some(&LockMode::Exclusive));
        assert_eq!(keys.get(&coll("kv")), Some(&LockMode::Intent));
    }

    /// A transaction batch takes the keys of every write it carries.
    #[test]
    fn a_transaction_batch_takes_the_keys_of_its_writes() {
        let batch = PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![doc_insert("x", 17), doc_insert("y", 18)],
            txn_id: None,
        });
        let (keys, sequenced) = keys_of(&batch);
        assert!(!sequenced);
        assert_eq!(
            keys.get(&surrogate_key("docs", 17)),
            Some(&LockMode::Exclusive)
        );
        assert_eq!(
            keys.get(&surrogate_key("docs", 18)),
            Some(&LockMode::Exclusive)
        );
        assert_eq!(keys.get(&coll("docs")), Some(&LockMode::Intent));
    }

    fn ts_ingest() -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: qualified("metrics"),
            payload: b"metrics value=1.0 1000000000".to_vec(),
            format: "ilp".to_owned(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn columnar_insert(intent: ColumnarInsertIntent) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: qualified("events"),
            payload: Vec::new(),
            format: "msgpack".to_owned(),
            intent,
            on_conflict_updates: Vec::new(),
            surrogates: vec![Surrogate::new(4), Surrogate::new(5)],
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// A timeseries ingest locks its collection `Intent` and nothing else.
    /// It waits rather than routes when contended.
    #[test]
    fn a_timeseries_ingest_announces_its_collection_and_never_routes() {
        let (keys, sequenced) = keys_of(&ts_ingest());
        assert!(!sequenced);
        assert_eq!(keys.get(&coll("metrics")), Some(&LockMode::Intent));
        assert_eq!(keys.len(), 1);
    }

    /// A columnar insert of every intent locks its rows `Exclusive` and its
    /// collection `Intent`. It waits rather than routes when contended.
    #[test]
    fn a_columnar_insert_locks_its_rows_and_never_routes() {
        for intent in [
            ColumnarInsertIntent::Insert,
            ColumnarInsertIntent::InsertIfAbsent,
            ColumnarInsertIntent::Put,
            ColumnarInsertIntent::InsertUnique,
        ] {
            let (keys, sequenced) = keys_of(&columnar_insert(intent));
            assert!(!sequenced, "{intent:?}");
            assert_eq!(
                keys.get(&coll("events")),
                Some(&LockMode::Intent),
                "{intent:?}"
            );
            for surrogate in [4, 5] {
                assert_eq!(
                    keys.get(&surrogate_key("events", surrogate)),
                    Some(&LockMode::Exclusive),
                    "{intent:?}"
                );
            }
            assert_eq!(keys.len(), 3, "{intent:?}");
        }
    }

    /// Two concurrent ingests of one collection hold their keys together. A
    /// truncate of the collection cannot take it while either holds.
    #[test]
    fn concurrent_ingests_admit_together_and_fence_a_truncate() {
        let mut table = LockManager::new();
        let (first, _) = keys_of(&ts_ingest());
        let (second, _) = keys_of(&ts_ingest());
        assert!(table.try_acquire(TxnId::new(TxnId::AUTOCOMMIT_EPOCH, 0), first));
        assert!(table.try_acquire(TxnId::new(TxnId::AUTOCOMMIT_EPOCH, 1), second));
        let truncate = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: qualified("metrics"),
            restart_identity: false,
        });
        let (truncate_keys, _) = keys_of(&truncate);
        assert_eq!(
            truncate_keys.get(&coll("metrics")),
            Some(&LockMode::Exclusive)
        );
        assert!(!table.try_acquire(TxnId::new(TxnId::AUTOCOMMIT_EPOCH, 2), truncate_keys));
    }

    /// A committed redo locks every collection it writes `Exclusive`.
    #[test]
    fn a_whole_collection_request_locks_each_collection_exclusive() {
        let request = collection_admission_keys(&[
            qualified("a").as_str().to_owned(),
            qualified("b").as_str().to_owned(),
        ]);
        assert!(!request.sequenced);
        assert_eq!(request.keys.get(&coll("a")), Some(&LockMode::Exclusive));
        assert_eq!(request.keys.get(&coll("b")), Some(&LockMode::Exclusive));
        assert_eq!(request.keys.len(), 2);
    }
}
