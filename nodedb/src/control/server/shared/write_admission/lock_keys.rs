// SPDX-License-Identifier: BUSL-1.1

//! Point-write key extraction for the per-vShard write order.
//!
//! [`plan_lock_keys`] maps a single-vShard, single-identity point write to
//! the keys that name its row. The write-order fence reads them to decide
//! whether admission's guard already orders the row. [`plan_row_key`] names
//! the key the keyed order lock serializes on when no Calvin scheduler runs.
//! The gate's own lock request comes from `admission_keys`.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::driver::core::routing::{PlanRouting, plan_vshard};
use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp, VectorOp, VectorWriteTargets};

/// The vShard and the row keys of a POINT write. `None` for any plan that
/// isn't a single-home, single-identity point write.
pub(crate) fn plan_lock_keys(plan: &PhysicalPlan) -> Option<(VShardId, BTreeSet<LockKey>)> {
    // `plan_vshard` returns two vShards for a cross-home graph edge, which has no
    // single `(vShard, keys)` representation and is ineligible for the fast path.
    let vshard = match plan_vshard(plan) {
        PlanRouting::Vshards(v) => match v.as_slice() {
            [v] => *v,
            _ => return None,
        },
        PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
            return None;
        }
    };
    let key = point_lock_key(plan)?;
    let mut keys = BTreeSet::new();
    keys.insert(key);
    keys.extend(graph_node_lock_keys(plan));
    keys.extend(document_row_id_key(plan));
    Some((vshard, keys))
}

/// The one key that names the row a single-row point write stores: the key
/// two writes of that row both hold. `None` for a plan that is not a point
/// write. The write-order fence and the keyed order lock serialize on it.
pub(crate) fn plan_row_key(plan: &PhysicalPlan) -> Option<LockKey> {
    point_lock_key(plan)
}

/// The row id key a document point write holds besides its surrogate key,
/// the same key the Calvin tx-class builder locks. A delete or update planned
/// while the row's key was unbound locks only this key and the collection
/// key, so it orders against a fast-path insert of the same id.
fn document_row_id_key(plan: &PhysicalPlan) -> Option<LockKey> {
    use crate::control::planner::calvin::tx_class::write_keys::row_id_key;
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut {
                collection,
                document_id,
                ..
            }
            | DocumentOp::PointInsert {
                collection,
                document_id,
                ..
            }
            | DocumentOp::Upsert {
                collection,
                document_id,
                ..
            }
            | DocumentOp::PointDelete {
                collection,
                document_id,
                ..
            }
            | DocumentOp::PointUpdate {
                collection,
                document_id,
                ..
            },
        ) => Some(LockKey::Kv {
            collection: Arc::from(collection.as_str()),
            key: Arc::from(row_id_key(document_id).as_slice()),
        }),
        _ => None,
    }
}

/// The node lock keys an edge write holds besides its own edge key: one per
/// endpoint, the same pairs the Calvin tx-class builder locks, so a node
/// delete's guard and every write of an edge on its node run in order.
fn graph_node_lock_keys(plan: &PhysicalPlan) -> Vec<LockKey> {
    use crate::control::planner::calvin::tx_class::shared::node_lock_pair;
    match plan {
        PhysicalPlan::Graph(
            GraphOp::EdgePut {
                collection,
                src_id,
                dst_id,
                ..
            }
            | GraphOp::EdgeDelete {
                collection,
                src_id,
                dst_id,
                ..
            },
        ) => [src_id, dst_id]
            .into_iter()
            .map(|node| {
                let (src, dst) = node_lock_pair(node);
                LockKey::Edge {
                    collection: Arc::from(collection.as_str()),
                    src,
                    dst,
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The single deterministic lock key identifying a point write, or `None`.
///
/// Exhaustive over `PhysicalPlan` so a new engine variant forces a decision here.
fn point_lock_key(plan: &PhysicalPlan) -> Option<LockKey> {
    match plan {
        PhysicalPlan::Document(op) => document_point_key(op),
        PhysicalPlan::Kv(op) => kv_point_key(op),
        PhysicalPlan::Vector(op) => vector_point_key(op),
        PhysicalPlan::Graph(op) => graph_point_key(op),
        // Never carry a single-identity point write; route to the scheduler.
        PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

/// Document-engine point-write key: the row surrogate. `Upsert` carries a
/// pre-assigned single-row surrogate like the `Point*` ops, so it locks
/// identically. Predicate/multi-row writes route to the scheduler.
fn document_point_key(op: &DocumentOp) -> Option<LockKey> {
    match op {
        DocumentOp::PointPut {
            collection,
            surrogate,
            ..
        }
        | DocumentOp::PointInsert {
            collection,
            surrogate,
            ..
        }
        | DocumentOp::Upsert {
            collection,
            surrogate,
            ..
        } => Some(LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: surrogate.as_u32(),
        }),
        // A key unbound in its database names no row to lock.
        DocumentOp::PointDelete {
            collection,
            surrogate,
            ..
        }
        | DocumentOp::PointUpdate {
            collection,
            surrogate,
            ..
        } => surrogate.map(|s| LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: s.as_u32(),
        }),
        // Multi-row and cross-collection writes have no single point identity.
        DocumentOp::BatchInsert { .. }
        // N rows, each with its own identity; never admitted through this gate — the
        // write-resolve orchestrator proposes it directly with a content precondition.
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::Merge { .. }
        // Names one row but never admitted here: it's the sibling of an already
        // multi-shard write, so the scheduler locks it on the same key instead.
        | DocumentOp::ApplyBalanceDelta { .. }
        // Reads and index DDL take no write lock at all.
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => None,
    }
}

/// KV-engine point-write key: the single raw byte key. Covers plain writes,
/// single-key `Delete`, and single-key read-modify-write ops — all mutate
/// exactly one `(collection, key)` row. Multi-key/batch ops have no single identity.
fn kv_point_key(op: &KvOp) -> Option<LockKey> {
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
        } => Some(LockKey::Kv {
            collection: Arc::from(collection.as_str()),
            key: Arc::from(key.as_slice()),
        }),
        KvOp::Delete {
            collection, keys, ..
        } => match keys.as_slice() {
            [k] => Some(LockKey::Kv {
                collection: Arc::from(collection.as_str()),
                key: Arc::from(k.as_slice()),
            }),
            _ => None,
        },
        KvOp::BatchPut { .. } => None,
        _ => None,
    }
}

/// Vector-engine point-write key: the row surrogate. Batch, node-id delete,
/// sparse and multi-vector writes lack a single stable surrogate identity.
fn vector_point_key(op: &VectorOp) -> Option<LockKey> {
    match op {
        // A delete of a key its home never bound names no row to key.
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            ..
        } => surrogate.map(|s| LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: s.as_u32(),
        }),
        VectorOp::Insert {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DirectInsert {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DirectInsertIfAbsent {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DirectUpsert {
            collection,
            surrogate,
            ..
        } => Some(LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: surrogate.as_u32(),
        }),
        // A single point target keys like a point write; a wider target set
        // has no one stable identity.
        VectorOp::DirectDelete {
            collection,
            targets: VectorWriteTargets::Surrogates(surrogates),
            ..
        }
        | VectorOp::DirectUpdate {
            collection,
            targets: VectorWriteTargets::Surrogates(surrogates),
            ..
        } if surrogates.len() == 1 => Some(LockKey::Surrogate {
            collection: Arc::from(collection.as_str()),
            surrogate: surrogates[0].as_u32(),
        }),
        VectorOp::DirectDelete { .. } | VectorOp::DirectUpdate { .. } => None,
        // Every row of the collection: no single stable identity.
        VectorOp::DirectTruncate { .. } => None,
        VectorOp::BatchInsert { .. }
        | VectorOp::Delete { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. } => None,
        _ => None,
    }
}

/// Graph-engine point-write key: the directed edge identity. Single-home is
/// already guaranteed by [`plan_lock_keys`] (two-vShard edges never reach here).
fn graph_point_key(op: &GraphOp) -> Option<LockKey> {
    match op {
        GraphOp::EdgePut {
            collection,
            src_surrogate,
            dst_surrogate,
            ..
        }
        | GraphOp::EdgeDelete {
            collection,
            src_surrogate,
            dst_surrogate,
            ..
        } => Some(LockKey::Edge {
            collection: Arc::from(collection.as_str()),
            src: src_surrogate.as_u32(),
            dst: dst_surrogate.as_u32(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    fn kv_key(op: KvOp) -> LockKey {
        kv_point_key(&op).expect("expected a lock key for this KV op")
    }

    #[test]
    fn kv_incr_yields_kv_lock_key() {
        assert_eq!(
            kv_key(KvOp::Incr {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                key: b"k1".to_vec(),
                delta: 1,
                ttl_ms: 0,
                surrogate: Surrogate::new(1),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
            }),
            LockKey::Kv {
                collection: Arc::from("counters"),
                key: Arc::from(b"k1".as_slice()),
            }
        );
    }

    #[test]
    fn kv_incr_float_yields_kv_lock_key() {
        assert_eq!(
            kv_key(KvOp::IncrFloat {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                key: b"k1".to_vec(),
                delta: "1.5".into(),
                surrogate: Surrogate::new(1),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
            }),
            LockKey::Kv {
                collection: Arc::from("counters"),
                key: Arc::from(b"k1".as_slice()),
            }
        );
    }

    #[test]
    fn kv_cas_yields_kv_lock_key() {
        assert_eq!(
            kv_key(KvOp::Cas {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                key: b"k1".to_vec(),
                expected: vec![],
                new_value: vec![],
                surrogate: Surrogate::new(1),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            LockKey::Kv {
                collection: Arc::from("counters"),
                key: Arc::from(b"k1".as_slice()),
            }
        );
    }

    #[test]
    fn kv_get_set_yields_kv_lock_key() {
        assert_eq!(
            kv_key(KvOp::GetSet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                key: b"k1".to_vec(),
                new_value: vec![],
                surrogate: Surrogate::new(1),
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            LockKey::Kv {
                collection: Arc::from("counters"),
                key: Arc::from(b"k1".as_slice()),
            }
        );
    }

    #[test]
    fn kv_field_set_yields_kv_lock_key() {
        assert_eq!(
            kv_key(KvOp::FieldSet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                key: b"k1".to_vec(),
                updates: vec![],
                surrogate: Surrogate::new(1),
                if_present: false,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            LockKey::Kv {
                collection: Arc::from("counters"),
                key: Arc::from(b"k1".as_slice()),
            }
        );
    }

    #[test]
    fn kv_batch_put_stays_unfenced() {
        assert_eq!(
            kv_point_key(&KvOp::BatchPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                entries: vec![(b"k1".to_vec(), vec![]), (b"k2".to_vec(), vec![])],
                ttl_ms: 0,
                surrogates: vec![],
                returning: None,
                rls_filters: Vec::new(),
            }),
            None
        );
    }

    #[test]
    fn kv_multi_key_delete_stays_unfenced() {
        assert_eq!(
            kv_point_key(&KvOp::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                keys: vec![b"k1".to_vec(), b"k2".to_vec()],
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            }),
            None
        );
    }

    #[test]
    fn document_upsert_yields_surrogate_lock_key() {
        assert_eq!(
            document_point_key(&DocumentOp::Upsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                document_id: "d1".to_owned(),
                value: vec![],
                on_conflict_updates: vec![],
                surrogate: Surrogate::new(7),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            Some(LockKey::Surrogate {
                collection: Arc::from("docs"),
                surrogate: 7,
            })
        );
    }
}
