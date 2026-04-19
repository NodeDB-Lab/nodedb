//! Data Plane core snapshot — serializes all engine state for transfer.
//!
//! Used by InstallSnapshot (lagging follower recovery) and vShard migration
//! (Phase 1 base copy). Captures the full state of a Data Plane core:
//! - SparseEngine documents and indexes (from redb)
//! - EdgeStore edges and reverse edges (from redb)
//! - VectorCollection checkpoint bytes (in-memory, multi-segment)
//! - CRDT engine state per tenant (loro export)
//! - Watermark LSN

/// Serializable snapshot of a single vector collection.
///
/// The collection state is stored as opaque checkpoint bytes produced by
/// `VectorCollection::checkpoint_to_bytes()`. This handles the multi-segment
/// lifecycle (growing + sealed + building) transparently.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct HnswSnapshot {
    /// Tenant owner.
    #[serde(default)]
    pub tenant_id: u32,
    /// Collection name (without tenant prefix).
    pub collection: String,
    /// Checkpoint bytes from `VectorCollection::checkpoint_to_bytes()`.
    pub checkpoint_bytes: Vec<u8>,
}

/// Serializable snapshot of a CRDT tenant's state.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CrdtSnapshot {
    pub tenant_id: u32,
    pub peer_id: u64,
    /// Loro binary snapshot (from LoroDoc::export_snapshot).
    pub snapshot_bytes: Vec<u8>,
}

/// Serializable key-value pair from a redb table.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvPair {
    pub key: String,
    pub value: Vec<u8>,
}

/// Tenant-tagged key-value pair from a structurally-partitioned redb
/// table (e.g. the graph edge store). The tenant id is carried
/// explicitly — no lexical encoding.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TenantKvPair {
    pub tenant_id: u32,
    pub key: String,
    pub value: Vec<u8>,
}

/// Complete snapshot of a Data Plane core's state.
///
/// Designed for serialization via MessagePack and transfer over the network
/// as InstallSnapshot data or VShardEnvelope::SegmentChunk payloads.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CoreSnapshot {
    /// Core/vShard watermark LSN.
    pub watermark: u64,

    /// All documents from SparseEngine.
    pub sparse_documents: Vec<KvPair>,
    /// All secondary indexes from SparseEngine.
    pub sparse_indexes: Vec<KvPair>,

    /// All edges from EdgeStore, tenant-tagged. The reverse-edge
    /// index is rebuilt on restore from the forward records — not
    /// shipped separately.
    pub edges: Vec<TenantKvPair>,

    /// All HNSW vector indexes.
    pub hnsw_indexes: Vec<HnswSnapshot>,

    /// All CRDT tenant states.
    pub crdt_snapshots: Vec<CrdtSnapshot>,
}

impl CoreSnapshot {
    pub fn empty() -> Self {
        Self {
            watermark: 0,
            sparse_documents: Vec::new(),
            sparse_indexes: Vec::new(),
            edges: Vec::new(),
            hnsw_indexes: Vec::new(),
            crdt_snapshots: Vec::new(),
        }
    }

    /// Serialize to bytes for network transfer.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("CoreSnapshot: {e}"),
        })
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        zerompk::from_msgpack(data).ok()
    }

    /// Approximate size in bytes (for progress tracking).
    pub fn approx_size(&self) -> usize {
        let sparse = self
            .sparse_documents
            .iter()
            .map(|kv| kv.key.len() + kv.value.len())
            .sum::<usize>()
            + self
                .sparse_indexes
                .iter()
                .map(|kv| kv.key.len() + kv.value.len())
                .sum::<usize>();
        let edges = self
            .edges
            .iter()
            .map(|kv| kv.key.len() + kv.value.len() + 4)
            .sum::<usize>();
        let vectors: usize = self
            .hnsw_indexes
            .iter()
            .map(|h| h.checkpoint_bytes.len())
            .sum();
        let crdt: usize = self
            .crdt_snapshots
            .iter()
            .map(|c| c.snapshot_bytes.len())
            .sum();
        sparse + edges + vectors + crdt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_roundtrip() {
        let snap = CoreSnapshot::empty();
        let bytes = snap.to_bytes().unwrap();
        let decoded = CoreSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.watermark, 0);
        assert!(decoded.sparse_documents.is_empty());
        assert!(decoded.hnsw_indexes.is_empty());
    }

    #[test]
    fn snapshot_with_data_roundtrip() {
        let snap = CoreSnapshot {
            watermark: 42,
            sparse_documents: vec![
                KvPair {
                    key: "users:u1".into(),
                    value: b"alice".to_vec(),
                },
                KvPair {
                    key: "users:u2".into(),
                    value: b"bob".to_vec(),
                },
            ],
            sparse_indexes: vec![KvPair {
                key: "users:name:alice:u1".into(),
                value: vec![],
            }],
            edges: vec![TenantKvPair {
                tenant_id: 1,
                key: "u1\0knows\0u2".into(),
                value: b"{}".to_vec(),
            }],
            hnsw_indexes: vec![HnswSnapshot {
                tenant_id: 1,
                collection: "embeddings".into(),
                checkpoint_bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
            }],
            crdt_snapshots: vec![CrdtSnapshot {
                tenant_id: 1,
                peer_id: 100,
                snapshot_bytes: vec![0xAB, 0xCD],
            }],
        };

        let bytes = snap.to_bytes().unwrap();
        let decoded = CoreSnapshot::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.watermark, 42);
        assert_eq!(decoded.sparse_documents.len(), 2);
        assert_eq!(decoded.sparse_documents[0].key, "users:u1");
        assert_eq!(decoded.edges.len(), 1);
        assert_eq!(decoded.hnsw_indexes.len(), 1);
        assert_eq!(decoded.hnsw_indexes[0].collection, "embeddings");
        assert_eq!(decoded.hnsw_indexes[0].tenant_id, 1);
        assert_eq!(decoded.crdt_snapshots.len(), 1);
        assert!(decoded.approx_size() > 0);
    }

    #[test]
    fn hnsw_snapshot_checkpoint_bytes_roundtrip() {
        // Verify that checkpoint_bytes survive serialization/deserialization.
        let ckpt = vec![0x01u8, 0x02, 0x03, 0x04, 0x05];
        let snap = CoreSnapshot {
            hnsw_indexes: vec![HnswSnapshot {
                tenant_id: 1,
                collection: "test".into(),
                checkpoint_bytes: ckpt.clone(),
            }],
            ..CoreSnapshot::empty()
        };
        let bytes = snap.to_bytes().unwrap();
        let decoded = CoreSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.hnsw_indexes[0].collection, "test");
        assert_eq!(decoded.hnsw_indexes[0].checkpoint_bytes, ckpt);
    }
}
