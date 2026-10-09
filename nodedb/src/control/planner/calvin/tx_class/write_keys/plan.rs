// SPDX-License-Identifier: BUSL-1.1

//! The write keys of one physical plan, by engine family.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::set::WriteKeys;
use super::{columnar, crdt, document, graph, kv, restore, vector};
use crate::control::planner::calvin::write_class::is_write_plan;

/// The write keys of every write task among `tasks`. A task that writes
/// nothing adds none.
pub fn task_write_keys(tasks: &[PhysicalTask]) -> crate::Result<WriteKeys> {
    let mut keys = WriteKeys::default();
    for task in tasks.iter().filter(|task| is_write_plan(&task.plan)) {
        add_plan_write_keys(&mut keys, &task.plan)?;
    }
    Ok(keys)
}

/// Add the Calvin write keys of `plan` to `keys`.
///
/// Every write op of every engine is matched by name. A write with row
/// identity locks its rows, and the scheduler adds an `Intent` lock on their
/// collection. A write without one locks its whole collection. A graph write
/// locks its edges and nodes on their key homes.
///
/// A write no single transaction can sequence, and a plan that writes
/// nothing, is refused with a typed error. The caller never builds a
/// transaction that the scheduler will refuse at dispatch.
pub fn add_plan_write_keys(keys: &mut WriteKeys, plan: &PhysicalPlan) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Document(op) => document::add_keys(keys, op),
        PhysicalPlan::Kv(op) => kv::add_keys(keys, op),
        PhysicalPlan::Vector(op) => vector::add_keys(keys, op),
        PhysicalPlan::Graph(op) => graph::add_keys(keys, op),
        PhysicalPlan::Crdt(op) => crdt::add_keys(keys, op),
        PhysicalPlan::Columnar(op) => columnar::add_columnar_keys(keys, op),
        PhysicalPlan::Timeseries(op) => columnar::add_timeseries_keys(keys, op),
        PhysicalPlan::Array(op) => columnar::add_array_keys(keys, op),
        PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) => restore::add_keys(keys, batch),
        PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Err(not_a_write(
            "a text, spatial, query, meta or cluster-fanned plan",
        )),
    }
}

/// A plan in a Calvin write set that writes nothing. `what` names it.
pub(super) fn not_a_write(what: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "internal invariant break: {what} writes nothing, yet it reached the Calvin \
             write-key extraction; the builders skip every plan `is_write_plan` refuses"
        ),
    }
}

/// A write that no Calvin transaction sequences, with the reason.
pub(super) fn unsequenced(reason: &str) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!("this write cannot run as a Calvin transaction: {reason}"),
    }
}

/// Two transactions, the lower one sequenced first, each taking the lock
/// keys the scheduler expands from the `TxClass` the builders produce. A
/// guard is sound only when the higher transaction waits for the lower one.
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nodedb_cluster::calvin::types::SequencedTxn;
    use nodedb_physical::physical_plan::{
        BatchEdge, ColumnarOp, CrdtOp, CrdtWriteVerb, DocumentOp, GraphOp, KvOp, VectorOp,
    };
    use nodedb_physical::physical_task::PostSetOp;
    use nodedb_types::{CollectionKey, QualifiedCollection, Surrogate};

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set;
    use crate::control::cluster::calvin::scheduler::{
        AcquireOutcome, LockKey, LockManager, LockMode, TxnId,
    };
    use crate::control::planner::calvin::tx_class::build_single_vshard_tx_class;
    use crate::types::{DatabaseId, RecordHomes, TenantId, VShardId};

    const EPOCH: u64 = 9;
    const CRDT: &str = "crdt_nodes";
    const GRAPH: &str = "g";

    fn qualified(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
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

    /// The lock keys the scheduler takes for a transaction of `plans`.
    fn locks(plans: Vec<PhysicalPlan>, position: u32) -> BTreeMap<LockKey, LockMode> {
        let tasks: Vec<PhysicalTask> = plans.into_iter().map(task).collect();
        let tx_class = build_single_vshard_tx_class(&tasks, TenantId::new(1), &[])
            .expect("a valid transaction class");
        expand_rw_set(&SequencedTxn {
            epoch: EPOCH,
            position,
            tx_class,
            epoch_system_ms: 1_700_000_000_000,
            epoch_vshard_txn_count: 2,
            lock_owner: None,
        })
    }

    /// Acquire the lower transaction's keys, then the higher one's, in
    /// sequence order. `true` when the higher one waits; the lower one's
    /// release then hands it every key.
    fn higher_waits(lower: Vec<PhysicalPlan>, higher: Vec<PhysicalPlan>) -> bool {
        let (lower_id, higher_id) = (TxnId::new(EPOCH, 0), TxnId::new(EPOCH, 1));
        let mut table = LockManager::new();
        assert!(matches!(
            table.acquire(lower_id, locks(lower, 0)),
            AcquireOutcome::Ready
        ));
        let waits = matches!(
            table.acquire(higher_id, locks(higher, 1)),
            AcquireOutcome::Blocked
        );
        if waits {
            assert_eq!(table.release(lower_id), vec![higher_id]);
        }
        waits
    }

    fn crdt_upsert(id: &str) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: qualified(CRDT),
            document_id: id.to_owned(),
            fields_json: "{}".to_owned(),
            surrogate: Surrogate::new(41),
            partial: false,
            verb: CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn crdt_delete(id: &str, surrogate: Option<Surrogate>) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: qualified(CRDT),
            document_id: id.to_owned(),
            surrogate,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn presence_guard(present: &[&str], absent: &[&str]) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::NodePresenceGuard {
            collection: qualified(CRDT),
            vshard: CollectionKey::from_bare(DatabaseId::DEFAULT, CRDT)
                .vshard()
                .as_u32(),
            present: present.iter().map(|id| (*id).to_owned()).collect(),
            absent: absent.iter().map(|id| (*id).to_owned()).collect(),
        })
    }

    fn batch_edge(collection: &str, src: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: qualified(collection),
            src_id: src.to_owned(),
            label: "L".to_owned(),
            dst_id: dst.to_owned(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        }
    }

    fn node_guard(node: &str) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::NodeEdgeGuard {
            collection: qualified(GRAPH),
            node_id: node.to_owned(),
            expected: Vec::new(),
        })
    }

    /// An upsert binds and stores `x` at a lower position. A delete planned
    /// while `x` was unbound names it absent and carries an unbound delete.
    /// Its guard must run after the upsert installed, see `x` stored, and
    /// retry; before, it ran first and the delete was lost.
    #[test]
    fn a_crdt_delete_of_an_unbound_document_waits_for_its_upsert() {
        assert!(higher_waits(
            vec![crdt_upsert("x")],
            vec![presence_guard(&[], &["x"]), crdt_delete("x", None)],
        ));
        // The guard alone orders against the upsert, whatever the deletes
        // beside it lock.
        assert!(higher_waits(
            vec![crdt_upsert("x")],
            vec![presence_guard(&[], &["x"])]
        ));
        assert!(higher_waits(
            vec![crdt_upsert("x")],
            vec![presence_guard(&["x"], &[])]
        ));
    }

    /// Every writer of one CRDT document locks one key, bound or not, and
    /// writers of distinct documents do not wait on each other.
    #[test]
    fn crdt_writers_of_one_document_serialize_bound_or_not() {
        assert!(higher_waits(
            vec![crdt_delete("x", None)],
            vec![crdt_upsert("x")]
        ));
        assert!(higher_waits(
            vec![crdt_upsert("x")],
            vec![crdt_delete("x", Some(Surrogate::new(41)))],
        ));
        assert!(!higher_waits(
            vec![crdt_upsert("x")],
            vec![crdt_upsert("y")]
        ));
    }

    /// A collection-wide write can store or remove any document, so a
    /// presence guard waits for it.
    #[test]
    fn a_presence_guard_waits_for_a_collection_wide_write() {
        let truncate = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: qualified(CRDT),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(higher_waits(
            vec![truncate],
            vec![presence_guard(&[], &["x"])]
        ));
    }

    const DOCS: &str = "doc_rows";

    fn doc_insert(id: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: qualified(DOCS),
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

    fn doc_delete(id: &str, surrogate: Option<u32>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: qualified(DOCS),
            document_id: id.to_owned(),
            surrogate: surrogate.map(Surrogate::new),
            pk_bytes: id.as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        })
    }

    fn doc_update(id: &str, surrogate: Option<u32>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: qualified(DOCS),
            document_id: id.to_owned(),
            surrogate: surrogate.map(Surrogate::new),
            pk_bytes: id.as_bytes().to_vec(),
            updates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// An insert binds and stores `x` at a lower position. A delete or
    /// update planned while `x` was unbound must wait for it: its rebind at
    /// dispatch then finds the binding on every replica. Its row id lock
    /// orders it, so it never dispatches first on one replica and second on
    /// another.
    #[test]
    fn an_unbound_document_delete_or_update_waits_for_an_insert_of_its_id() {
        assert!(higher_waits(
            vec![doc_insert("x", 17)],
            vec![doc_delete("x", None)]
        ));
        assert!(higher_waits(
            vec![doc_insert("x", 17)],
            vec![doc_update("x", None)]
        ));
        let batch = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: qualified(DOCS),
            documents: vec![("w".to_owned(), Vec::new()), ("x".to_owned(), Vec::new())],
            surrogates: vec![Surrogate::new(16), Surrogate::new(17)],
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert!(higher_waits(vec![batch], vec![doc_delete("x", None)]));
    }

    fn doc_truncate() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection: qualified(DOCS),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// A truncate locks its collection `Exclusive`, and a point write takes
    /// `Intent` on it. So they serialize in sequencer order, whichever comes
    /// first.
    #[test]
    fn a_truncate_and_a_point_write_serialize_in_sequence_order() {
        assert!(higher_waits(
            vec![doc_truncate()],
            vec![doc_insert("x", 17)]
        ));
        assert!(higher_waits(
            vec![doc_insert("x", 17)],
            vec![doc_truncate()]
        ));
        assert!(higher_waits(
            vec![doc_delete("x", None)],
            vec![doc_truncate()]
        ));
        let collection_key = LockKey::Collection {
            collection: std::sync::Arc::from(DOCS),
        };
        assert_eq!(
            locks(vec![doc_truncate()], 0).get(&collection_key),
            Some(&LockMode::Exclusive)
        );
        assert_eq!(
            locks(vec![doc_insert("x", 17)], 0).get(&collection_key),
            Some(&LockMode::Intent)
        );
    }

    /// Every point writer of one document id serializes, bound or not, and
    /// writers of distinct ids do not wait on each other.
    #[test]
    fn document_writers_of_one_id_serialize_bound_or_not() {
        assert!(higher_waits(
            vec![doc_delete("x", None)],
            vec![doc_insert("x", 17)]
        ));
        assert!(higher_waits(
            vec![doc_update("x", Some(17))],
            vec![doc_delete("x", Some(17))]
        ));
        assert!(!higher_waits(
            vec![doc_insert("x", 17)],
            vec![doc_insert("y", 18)]
        ));
        assert!(!higher_waits(
            vec![doc_delete("x", Some(17))],
            vec![doc_delete("y", Some(18))]
        ));
    }

    /// A batch edge write locks both endpoints' node pairs, so it orders
    /// against a node delete's guard either way round. Before, it locked
    /// only a document key of an empty collection name.
    #[test]
    fn an_edge_batch_orders_against_a_node_guard() {
        for batch in [
            GraphOp::EdgePutBatch {
                edges: vec![batch_edge(GRAPH, "a", "b")],
            },
            GraphOp::EdgeDeleteBatch {
                edges: vec![batch_edge(GRAPH, "b", "a")],
            },
        ] {
            assert!(higher_waits(
                vec![PhysicalPlan::Graph(batch.clone())],
                vec![node_guard("a")]
            ));
            assert!(higher_waits(
                vec![node_guard("a")],
                vec![PhysicalPlan::Graph(batch)]
            ));
        }
        assert!(!higher_waits(
            vec![PhysicalPlan::Graph(GraphOp::EdgePutBatch {
                edges: vec![batch_edge(GRAPH, "c", "d")],
            })],
            vec![node_guard("a")]
        ));
    }

    /// A batch locks each edge as the single edge write does, in each edge's
    /// own collection, and enlists every endpoint home the routing oracle
    /// sends it to.
    #[test]
    fn an_edge_batch_keys_each_edge_on_its_homes() {
        let edges = vec![batch_edge(GRAPH, "a", "b"), batch_edge("h", "c", "d")];
        let batch = locks(
            vec![PhysicalPlan::Graph(GraphOp::EdgePutBatch {
                edges: edges.clone(),
            })],
            0,
        );
        let singles = locks(
            edges
                .iter()
                .map(|edge| {
                    PhysicalPlan::Graph(GraphOp::EdgePut {
                        collection: edge.collection.clone(),
                        src_id: edge.src_id.clone(),
                        label: edge.label.clone(),
                        dst_id: edge.dst_id.clone(),
                        properties: Vec::new(),
                        src_surrogate: edge.src_surrogate,
                        dst_surrogate: edge.dst_surrogate,
                    })
                })
                .collect(),
            0,
        );
        assert_eq!(batch, singles);

        let tasks = vec![task(PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: edges.clone(),
        }))];
        let tx_class =
            build_single_vshard_tx_class(&tasks, TenantId::new(1), &[]).expect("tx class");
        let mut homes: Vec<VShardId> = edges
            .iter()
            .flat_map(|edge| RecordHomes::edge(&edge.src_id, &edge.dst_id).iter())
            .collect();
        homes.sort_by_key(|vshard| vshard.as_u32());
        homes.dedup();
        assert_eq!(tx_class.participating_vshards(), homes.as_slice());
    }

    /// No write op keys an empty collection name: each one names the
    /// collection it writes, or a node's label namespace.
    #[test]
    fn no_write_op_locks_an_unnamed_collection() {
        let plans = [
            PhysicalPlan::Kv(KvOp::Expire {
                collection: qualified("kv"),
                key: b"k".to_vec(),
                ttl_ms: 5,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            PhysicalPlan::Kv(KvOp::Persist {
                collection: qualified("kv"),
                key: b"k".to_vec(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: qualified("col"),
                filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: qualified("vec"),
                field_name: "f".to_owned(),
                doc_id: "d".to_owned(),
            }),
            PhysicalPlan::Graph(GraphOp::SetNodeLabels {
                node_id: "n".to_owned(),
                labels: vec!["L".to_owned()],
            }),
        ];
        for plan in plans {
            let mut keys = WriteKeys::default();
            add_plan_write_keys(&mut keys, &plan).expect("a write op has keys");
            let sets = keys.into_key_sets();
            assert!(!sets.is_empty(), "{plan:?} locks nothing");
            assert!(
                sets.iter().all(|set| !set.collection().is_empty()),
                "{plan:?} keys an unnamed collection: {sets:?}"
            );
        }
    }

    /// A write no Calvin transaction can sequence is refused by name, never
    /// keyed onto some collection's vShard.
    #[test]
    fn an_unroutable_write_is_refused() {
        let plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: qualified("a"),
            dest_collection: qualified("b"),
            item_key: b"k".to_vec(),
            dest_key: b"k".to_vec(),
            surrogate: Surrogate::new(5),
            source_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            dest_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        let mut keys = WriteKeys::default();
        assert!(matches!(
            add_plan_write_keys(&mut keys, &plan),
            Err(crate::Error::BadRequest { .. })
        ));
    }
}
