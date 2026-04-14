//! Distributed WAL write path — propose writes through Raft, apply after commit.
//!
//! Write flow:
//! 1. Handler serializes write as [`ReplicatedWrite`]
//! 2. Handler proposes to Raft via [`RaftLoop::propose`]
//! 3. Handler registers a waiter in [`ProposeTracker`] keyed by (group_id, log_index)
//! 4. Raft replicates to quorum and commits
//! 5. [`DistributedApplier`] receives committed entries, queues for async execution
//! 6. Background task dispatches each write to the local Data Plane
//! 7. If a waiter exists (leader path), sends the response; otherwise just applies (follower)

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, VectorOp};
use crate::types::{TenantId, VShardId};

/// Type alias for the Raft propose callback.
///
/// Takes `(vshard_id, serialized_entry)` and returns `(group_id, log_index)`.
pub type RaftProposer =
    dyn Fn(u16, Vec<u8>) -> std::result::Result<(u64, u64), crate::Error> + Send + Sync;

fn default_pq_m() -> usize {
    crate::engine::vector::index_config::DEFAULT_PQ_M
}
fn default_ivf_cells() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_CELLS
}
fn default_ivf_nprobe() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_NPROBE
}

// ── Replicated write envelope ───────────────────────────────────────

/// A write operation serialized for Raft replication.
///
/// Mirrors the write variants of [`PhysicalPlan`] but uses only types that
/// are trivially serializable (no `Arc`, no `Instant`).
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReplicatedWrite {
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
    },
    PointDelete {
        collection: String,
        document_id: String,
    },
    PointUpdate {
        collection: String,
        document_id: String,
        updates: Vec<(String, crate::bridge::physical_plan::UpdateValue)>,
    },
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        #[serde(default)]
        field_name: String,
        #[serde(default)]
        doc_id: Option<String>,
    },
    VectorBatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
    },
    VectorDelete {
        collection: String,
        vector_id: u32,
    },
    SetVectorParams {
        collection: String,
        m: usize,
        ef_construction: usize,
        metric: String,
        #[serde(default)]
        index_type: String,
        #[serde(default = "default_pq_m")]
        pq_m: usize,
        #[serde(default = "default_ivf_cells")]
        ivf_cells: usize,
        #[serde(default = "default_ivf_nprobe")]
        ivf_nprobe: usize,
    },
    CrdtApply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
    },
    EdgePut {
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
    },
    EdgeDelete {
        src_id: String,
        label: String,
        dst_id: String,
    },
    KvPut {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
    },
    KvDelete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },
    KvBatchPut {
        collection: String,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        ttl_ms: u64,
    },
    KvExpire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
    },
    KvPersist {
        collection: String,
        key: Vec<u8>,
    },
    KvIncr {
        collection: String,
        key: Vec<u8>,
        delta: i64,
        ttl_ms: u64,
    },
    KvIncrFloat {
        collection: String,
        key: Vec<u8>,
        delta: f64,
    },
    KvCas {
        collection: String,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
    },
    KvGetSet {
        collection: String,
        key: Vec<u8>,
        new_value: Vec<u8>,
    },
    KvRegisterSortedIndex {
        collection: String,
        index_name: String,
        sort_columns: Vec<(String, String)>,
        key_column: String,
        window_type: String,
        window_timestamp_column: String,
        window_start_ms: u64,
        window_end_ms: u64,
    },
    KvDropSortedIndex {
        index_name: String,
    },
}

/// Metadata carried alongside the write for routing on the receiving node.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicatedEntry {
    pub tenant_id: u32,
    pub vshard_id: u16,
    pub write: ReplicatedWrite,
}

impl ReplicatedEntry {
    /// Serialize to bytes for Raft log entry data.
    pub fn to_bytes(&self) -> Vec<u8> {
        zerompk::to_msgpack_vec(self).expect("ReplicatedEntry serialization cannot fail")
    }

    /// Deserialize from Raft log entry data bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        zerompk::from_msgpack(data).ok()
    }
}

/// Try to convert a PhysicalPlan write variant into a ReplicatedEntry.
///
/// Returns `None` for read operations or system commands.
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
        }) => ReplicatedWrite::PointPut {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
        },
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
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
            doc_id,
        }) => ReplicatedWrite::VectorInsert {
            collection: collection.clone(),
            vector: vector.clone(),
            dim: *dim,
            field_name: field_name.clone(),
            doc_id: doc_id.clone(),
        },
        PhysicalPlan::Vector(VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
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
        }) => ReplicatedWrite::CrdtApply {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
        },
        PhysicalPlan::Graph(GraphOp::EdgePut {
            src_id,
            label,
            dst_id,
            properties,
        }) => ReplicatedWrite::EdgePut {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            properties: properties.clone(),
        },
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            src_id,
            label,
            dst_id,
        }) => ReplicatedWrite::EdgeDelete {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        },
        PhysicalPlan::Kv(KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
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

/// Deserialize a Raft log entry's data bytes back to (TenantId, VShardId, PhysicalPlan).
///
/// Returns `None` if the data is not a valid ReplicatedEntry (e.g., ConfChange or no-op).
pub fn from_replicated_entry(data: &[u8]) -> Option<(TenantId, VShardId, PhysicalPlan)> {
    let entry = ReplicatedEntry::from_bytes(data)?;
    let plan = to_physical_plan(&entry.write);
    Some((
        TenantId::new(entry.tenant_id),
        VShardId::new(entry.vshard_id),
        plan,
    ))
}

/// Convert a ReplicatedWrite back into a PhysicalPlan for Data Plane execution.
fn to_physical_plan(write: &ReplicatedWrite) -> PhysicalPlan {
    match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
        } => PhysicalPlan::Document(DocumentOp::PointPut {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
        }),
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
        } => PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: collection.clone(),
            document_id: document_id.clone(),
        }),
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
        } => PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: collection.clone(),
            document_id: document_id.clone(),
            updates: updates.clone(),
            returning: false,
        }),
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
            field_name,
            doc_id,
        } => PhysicalPlan::Vector(VectorOp::Insert {
            collection: collection.clone(),
            vector: vector.clone(),
            dim: *dim,
            field_name: field_name.clone(),
            doc_id: doc_id.clone(),
        }),
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
        } => PhysicalPlan::Vector(VectorOp::BatchInsert {
            collection: collection.clone(),
            vectors: vectors.clone(),
            dim: *dim,
        }),
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
        } => PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
            mutation_id: 0,
        }),
        ReplicatedWrite::EdgePut {
            src_id,
            label,
            dst_id,
            properties,
        } => PhysicalPlan::Graph(GraphOp::EdgePut {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            properties: properties.clone(),
        }),
        ReplicatedWrite::EdgeDelete {
            src_id,
            label,
            dst_id,
        } => PhysicalPlan::Graph(GraphOp::EdgeDelete {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        }),
        ReplicatedWrite::KvPut {
            collection,
            key,
            value,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Put {
            collection: collection.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
        }),
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
    }
}

// ── Re-exports from distributed_applier ─────────────────────────────

pub use crate::control::distributed_applier::{
    ApplyBatch, DistributedApplier, ProposeResult, ProposeTracker, create_distributed_applier,
    run_apply_loop,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicated_entry_roundtrip() {
        let entry = ReplicatedEntry {
            tenant_id: 1,
            vshard_id: 42,
            write: ReplicatedWrite::PointPut {
                collection: "users".into(),
                document_id: "u1".into(),
                value: b"alice".to_vec(),
            },
        };

        let bytes = entry.to_bytes();
        let decoded = ReplicatedEntry::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.tenant_id, 1);
        assert_eq!(decoded.vshard_id, 42);
        match decoded.write {
            ReplicatedWrite::PointPut {
                collection,
                document_id,
                value,
            } => {
                assert_eq!(collection, "users");
                assert_eq!(document_id, "u1");
                assert_eq!(value, b"alice");
            }
            other => panic!("expected PointPut, got {other:?}"),
        }
    }

    #[test]
    fn all_write_variants_serialize() {
        let writes = vec![
            ReplicatedWrite::PointPut {
                collection: "c".into(),
                document_id: "d".into(),
                value: vec![1, 2, 3],
            },
            ReplicatedWrite::PointDelete {
                collection: "c".into(),
                document_id: "d".into(),
            },
            ReplicatedWrite::VectorInsert {
                collection: "v".into(),
                vector: vec![1.0, 2.0, 3.0],
                dim: 3,
                field_name: "embedding".into(),
                doc_id: Some("doc-1".into()),
            },
            ReplicatedWrite::CrdtApply {
                collection: "c".into(),
                document_id: "d".into(),
                delta: vec![0xAB],
                peer_id: 7,
            },
            ReplicatedWrite::EdgePut {
                src_id: "a".into(),
                label: "knows".into(),
                dst_id: "b".into(),
                properties: vec![],
            },
            ReplicatedWrite::EdgeDelete {
                src_id: "a".into(),
                label: "knows".into(),
                dst_id: "b".into(),
            },
        ];

        for write in writes {
            let entry = ReplicatedEntry {
                tenant_id: 1,
                vshard_id: 0,
                write,
            };
            let bytes = entry.to_bytes();
            let decoded = ReplicatedEntry::from_bytes(&bytes);
            assert!(decoded.is_some(), "failed to roundtrip: {entry:?}");
        }
    }

    #[test]
    fn propose_tracker_register_and_complete() {
        let tracker = ProposeTracker::new();
        let mut rx = tracker.register(1, 5);

        // Complete the waiter.
        assert!(tracker.complete(1, 5, Ok(b"result".to_vec())));

        // Should receive the result.
        let result = rx.try_recv().unwrap();
        assert_eq!(result.unwrap(), b"result");
    }

    #[test]
    fn propose_tracker_no_waiter_returns_false() {
        let tracker = ProposeTracker::new();
        assert!(!tracker.complete(1, 99, Ok(vec![])));
    }

    #[test]
    fn to_replicated_entry_writes_only() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        // Write — should produce Some.
        let plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "c".into(),
            document_id: "d".into(),
            value: vec![],
        });
        assert!(to_replicated_entry(tenant, vshard, &plan).is_some());

        // Read — should produce None.
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "c".into(),
            document_id: "d".into(),
            rls_filters: Vec::new(),
        });
        assert!(to_replicated_entry(tenant, vshard, &plan).is_none());
    }
}
