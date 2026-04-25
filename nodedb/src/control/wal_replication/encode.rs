//! Convert write-side PhysicalPlan variants to ReplicatedWrite for Raft proposal.

use super::types::{ReplicatedEntry, ReplicatedWrite};
use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, VectorOp};
use crate::types::{TenantId, VShardId};

pub fn to_replicated_entry(
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> Option<ReplicatedEntry> {
    let write = match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate: _,
            pk_bytes: _,
        }) => ReplicatedWrite::PointPut {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
        },
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate: _,
        }) => ReplicatedWrite::PointInsert {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
            if_absent: *if_absent,
        },
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate: _,
            pk_bytes: _,
        }) => ReplicatedWrite::PointDelete {
            collection: collection.clone(),
            document_id: document_id.clone(),
        },
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            updates,
            ..
        }) => ReplicatedWrite::PointUpdate {
            collection: collection.clone(),
            document_id: document_id.clone(),
            updates: updates.clone(),
        },
        PhysicalPlan::Vector(VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate: _,
        }) => ReplicatedWrite::VectorInsert {
            collection: collection.clone(),
            vector: vector.clone(),
            dim: *dim,
            field_name: field_name.clone(),
            // Surrogates are not part of the cross-node wire shape;
            // followers re-derive identity via the assigner. Without
            // the originating user PK at this hop, headless followers
            // call `assign_anonymous`.
            pk_bytes: None,
        },
        PhysicalPlan::Vector(VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates: _,
        }) => ReplicatedWrite::VectorBatchInsert {
            collection: collection.clone(),
            vectors: vectors.clone(),
            dim: *dim,
        },
        PhysicalPlan::Vector(VectorOp::Delete {
            collection,
            vector_id,
        }) => ReplicatedWrite::VectorDelete {
            collection: collection.clone(),
            vector_id: *vector_id,
        },
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        }) => ReplicatedWrite::SetVectorParams {
            collection: collection.clone(),
            m: *m,
            ef_construction: *ef_construction,
            metric: metric.clone(),
            index_type: index_type.clone(),
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        },
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id: _,
            surrogate: _,
        }) => ReplicatedWrite::CrdtApply {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
        },
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate: _,
            dst_surrogate: _,
        }) => ReplicatedWrite::EdgePut {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            properties: properties.clone(),
        },
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
        }) => ReplicatedWrite::EdgeDelete {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        },
        PhysicalPlan::Kv(KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
        }) => ReplicatedWrite::KvPut {
            collection: collection.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
        },
        PhysicalPlan::Kv(KvOp::Delete { collection, keys }) => ReplicatedWrite::KvDelete {
            collection: collection.clone(),
            keys: keys.clone(),
        },
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
        }) => ReplicatedWrite::KvBatchPut {
            collection: collection.clone(),
            entries: entries.clone(),
            ttl_ms: *ttl_ms,
        },
        PhysicalPlan::Kv(KvOp::Expire {
            collection,
            key,
            ttl_ms,
        }) => ReplicatedWrite::KvExpire {
            collection: collection.clone(),
            key: key.clone(),
            ttl_ms: *ttl_ms,
        },
        PhysicalPlan::Kv(KvOp::Persist { collection, key }) => ReplicatedWrite::KvPersist {
            collection: collection.clone(),
            key: key.clone(),
        },
        PhysicalPlan::Kv(KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
        }) => ReplicatedWrite::KvIncr {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
            ttl_ms: *ttl_ms,
        },
        PhysicalPlan::Kv(KvOp::IncrFloat {
            collection,
            key,
            delta,
        }) => ReplicatedWrite::KvIncrFloat {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
        },
        PhysicalPlan::Kv(KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
        }) => ReplicatedWrite::KvCas {
            collection: collection.clone(),
            key: key.clone(),
            expected: expected.clone(),
            new_value: new_value.clone(),
        },
        PhysicalPlan::Kv(KvOp::GetSet {
            collection,
            key,
            new_value,
        }) => ReplicatedWrite::KvGetSet {
            collection: collection.clone(),
            key: key.clone(),
            new_value: new_value.clone(),
        },
        PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        }) => ReplicatedWrite::KvRegisterSortedIndex {
            collection: collection.clone(),
            index_name: index_name.clone(),
            sort_columns: sort_columns.clone(),
            key_column: key_column.clone(),
            window_type: window_type.clone(),
            window_timestamp_column: window_timestamp_column.clone(),
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        },
        PhysicalPlan::Kv(KvOp::DropSortedIndex { index_name }) => {
            ReplicatedWrite::KvDropSortedIndex {
                index_name: index_name.clone(),
            }
        }
        // Not a write — reads, system ops, etc.
        _ => return None,
    };

    Some(ReplicatedEntry {
        tenant_id: tenant_id.as_u32(),
        vshard_id: vshard_id.as_u16(),
        write,
    })
}
