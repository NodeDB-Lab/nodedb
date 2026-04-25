//! Convert committed ReplicatedWrite entries back to PhysicalPlan for Data Plane execution.

use super::types::{ReplicatedEntry, ReplicatedWrite};
use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, VectorOp};
use crate::control::surrogate::SurrogateAssigner;
use crate::types::{TenantId, VShardId};

///
/// Returns `None` if the data is not a valid ReplicatedEntry (e.g., ConfChange or no-op).
///
/// `assigner`, when `Some`, drives follower-local surrogate binding for
/// the variants that carry one: each insert/upsert/put re-derives its
/// stable identity by calling `assigner.assign(collection, pk_bytes)`,
/// which writes the binding to the local catalog and emits a
/// `SurrogateBind` WAL record. When `None`, all surrogate fields fall
/// back to `Surrogate::ZERO` (used by tests that exercise the decoder
/// in isolation without spinning up `SharedState`).
pub fn from_replicated_entry(
    data: &[u8],
    assigner: Option<&SurrogateAssigner>,
) -> crate::Result<Option<(TenantId, VShardId, PhysicalPlan)>> {
    let entry = match ReplicatedEntry::from_bytes(data) {
        Some(e) => e,
        None => return Ok(None),
    };
    let plan = to_physical_plan(&entry.write, assigner)?;
    Ok(Some((
        TenantId::new(entry.tenant_id),
        VShardId::new(entry.vshard_id),
        plan,
    )))
}

fn assign_or_zero(
    assigner: Option<&SurrogateAssigner>,
    collection: &str,
    pk_bytes: &[u8],
) -> crate::Result<nodedb_types::Surrogate> {
    match assigner {
        Some(a) => a.assign(collection, pk_bytes),
        None => Ok(nodedb_types::Surrogate::ZERO),
    }
}

/// Convert a ReplicatedWrite back into a PhysicalPlan for Data Plane execution.
fn to_physical_plan(
    write: &ReplicatedWrite,
    assigner: Option<&SurrogateAssigner>,
) -> crate::Result<PhysicalPlan> {
    Ok(match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
        } => {
            let pk_bytes = document_id.as_bytes().to_vec();
            let surrogate = assign_or_zero(assigner, collection, &pk_bytes)?;
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                surrogate,
                pk_bytes,
            })
        }
        ReplicatedWrite::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
        } => {
            let surrogate = assign_or_zero(assigner, collection, document_id.as_bytes())?;
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                if_absent: *if_absent,
                surrogate,
            })
        }
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
        } => {
            let pk_bytes = document_id.as_bytes().to_vec();
            // Followers re-derive the surrogate via the local catalog
            // rev table; a missing binding means the row is unknown to
            // this replica and the delete is a no-op once dispatched.
            let surrogate = match assigner {
                Some(a) => a
                    .lookup(collection, &pk_bytes)?
                    .unwrap_or(nodedb_types::Surrogate::ZERO),
                None => nodedb_types::Surrogate::ZERO,
            };
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: collection.clone(),
                document_id: document_id.clone(),
                surrogate,
                pk_bytes,
            })
        }
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
        } => {
            let pk_bytes = document_id.as_bytes().to_vec();
            let surrogate = match assigner {
                Some(a) => a
                    .lookup(collection, &pk_bytes)?
                    .unwrap_or(nodedb_types::Surrogate::ZERO),
                None => nodedb_types::Surrogate::ZERO,
            };
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection: collection.clone(),
                document_id: document_id.clone(),
                surrogate,
                pk_bytes,
                updates: updates.clone(),
                returning: false,
            })
        }
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
            field_name,
            pk_bytes,
        } => {
            // Followers re-derive surrogate identity locally. With a
            // PK we share the leader's binding; without one (headless
            // batch element), allocate a follower-local anonymous
            // surrogate so the row is still globally addressable.
            let surrogate = match (assigner, pk_bytes) {
                (Some(a), Some(pk)) => a.assign(collection, pk)?,
                (Some(a), None) => a.assign_anonymous(collection)?,
                (None, _) => nodedb_types::Surrogate::ZERO,
            };
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: collection.clone(),
                vector: vector.clone(),
                dim: *dim,
                field_name: field_name.clone(),
                surrogate,
            })
        }
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
        } => {
            let surrogates = match assigner {
                Some(a) => {
                    let mut out = Vec::with_capacity(vectors.len());
                    for _ in vectors {
                        out.push(a.assign_anonymous(collection)?);
                    }
                    out
                }
                None => vec![nodedb_types::Surrogate::ZERO; vectors.len()],
            };
            PhysicalPlan::Vector(VectorOp::BatchInsert {
                collection: collection.clone(),
                vectors: vectors.clone(),
                dim: *dim,
                surrogates,
            })
        }
        ReplicatedWrite::VectorDelete {
            collection,
            vector_id,
        } => PhysicalPlan::Vector(VectorOp::Delete {
            collection: collection.clone(),
            vector_id: *vector_id,
        }),
        ReplicatedWrite::SetVectorParams {
            collection,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => PhysicalPlan::Vector(VectorOp::SetParams {
            collection: collection.clone(),
            m: *m,
            ef_construction: *ef_construction,
            metric: metric.clone(),
            index_type: index_type.clone(),
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        }),
        ReplicatedWrite::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
        } => {
            let surrogate = assign_or_zero(assigner, collection, document_id.as_bytes())?;
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: collection.clone(),
                document_id: document_id.clone(),
                delta: delta.clone(),
                peer_id: *peer_id,
                mutation_id: 0,
                surrogate,
            })
        }
        ReplicatedWrite::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
        } => {
            let src_surrogate = assign_or_zero(assigner, collection, src_id.as_bytes())?;
            let dst_surrogate = assign_or_zero(assigner, collection, dst_id.as_bytes())?;
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: collection.clone(),
                src_id: src_id.clone(),
                label: label.clone(),
                dst_id: dst_id.clone(),
                properties: properties.clone(),
                src_surrogate,
                dst_surrogate,
            })
        }
        ReplicatedWrite::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
        } => PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        }),
        ReplicatedWrite::KvPut {
            collection,
            key,
            value,
            ttl_ms,
        } => {
            let surrogate = assign_or_zero(assigner, collection, key)?;
            PhysicalPlan::Kv(KvOp::Put {
                collection: collection.clone(),
                key: key.clone(),
                value: value.clone(),
                ttl_ms: *ttl_ms,
                surrogate,
            })
        }
        ReplicatedWrite::KvDelete { collection, keys } => PhysicalPlan::Kv(KvOp::Delete {
            collection: collection.clone(),
            keys: keys.clone(),
        }),
        ReplicatedWrite::KvBatchPut {
            collection,
            entries,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::BatchPut {
            collection: collection.clone(),
            entries: entries.clone(),
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvExpire {
            collection,
            key,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Expire {
            collection: collection.clone(),
            key: key.clone(),
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvPersist { collection, key } => PhysicalPlan::Kv(KvOp::Persist {
            collection: collection.clone(),
            key: key.clone(),
        }),
        ReplicatedWrite::KvIncr {
            collection,
            key,
            delta,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Incr {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvIncrFloat {
            collection,
            key,
            delta,
        } => PhysicalPlan::Kv(KvOp::IncrFloat {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
        }),
        ReplicatedWrite::KvCas {
            collection,
            key,
            expected,
            new_value,
        } => PhysicalPlan::Kv(KvOp::Cas {
            collection: collection.clone(),
            key: key.clone(),
            expected: expected.clone(),
            new_value: new_value.clone(),
        }),
        ReplicatedWrite::KvGetSet {
            collection,
            key,
            new_value,
        } => PhysicalPlan::Kv(KvOp::GetSet {
            collection: collection.clone(),
            key: key.clone(),
            new_value: new_value.clone(),
        }),
        ReplicatedWrite::KvRegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection: collection.clone(),
            index_name: index_name.clone(),
            sort_columns: sort_columns.clone(),
            key_column: key_column.clone(),
            window_type: window_type.clone(),
            window_timestamp_column: window_timestamp_column.clone(),
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        }),
        ReplicatedWrite::KvDropSortedIndex { index_name } => {
            PhysicalPlan::Kv(KvOp::DropSortedIndex {
                index_name: index_name.clone(),
            })
        }
    })
}
