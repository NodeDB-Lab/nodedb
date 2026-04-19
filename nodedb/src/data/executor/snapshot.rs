//! Snapshot export for CoreLoop state.

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Export the current state of all engines into a serializable snapshot.
    ///
    /// This captures the complete Data Plane state for this core:
    /// redb tables (sparse + edge), in-memory HNSW indexes, and CRDT state.
    pub fn export_snapshot(&self) -> crate::Result<crate::data::snapshot::CoreSnapshot> {
        use crate::data::snapshot::*;

        let sparse_documents: Vec<KvPair> = self
            .sparse
            .export_documents()?
            .into_iter()
            .map(|(k, v)| KvPair { key: k, value: v })
            .collect();

        let sparse_indexes: Vec<KvPair> = self
            .sparse
            .export_indexes()?
            .into_iter()
            .map(|(k, v)| KvPair { key: k, value: v })
            .collect();

        let edges: Vec<TenantKvPair> = self
            .edge_store
            .export_edges()?
            .into_iter()
            .map(|(tid, k, v)| TenantKvPair {
                tenant_id: tid.as_u32(),
                key: k,
                value: v,
            })
            .collect();

        let hnsw_indexes: Vec<HnswSnapshot> = self
            .vector_collections
            .iter()
            .filter_map(|(name, coll)| {
                let checkpoint_bytes = coll.checkpoint_to_bytes();
                if checkpoint_bytes.is_empty() {
                    return None;
                }
                let (tenant_id, collection) = name
                    .split_once(':')
                    .map(|(t, c)| (t.parse::<u32>().unwrap_or(0), c.to_string()))
                    .unwrap_or((0, name.clone()));
                Some(HnswSnapshot {
                    tenant_id,
                    collection,
                    checkpoint_bytes,
                })
            })
            .collect();

        let crdt_snapshots: Vec<CrdtSnapshot> = self
            .crdt_engines
            .iter()
            .map(|(tid, engine)| {
                Ok(CrdtSnapshot {
                    tenant_id: tid.as_u32(),
                    peer_id: engine.peer_id(),
                    snapshot_bytes: engine.export_snapshot_bytes()?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(CoreSnapshot {
            watermark: self.watermark.as_u64(),
            sparse_documents,
            sparse_indexes,
            edges,
            hnsw_indexes,
            crdt_snapshots,
        })
    }
}
