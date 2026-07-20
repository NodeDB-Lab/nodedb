// SPDX-License-Identifier: BUSL-1.1

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
            .map(|(db, tid, k, v)| TenantKvPair {
                database_id: db.as_u64(),
                tenant_id: tid.as_u64(),
                key: k,
                value: v,
            })
            .collect();

        let mut hnsw_indexes: Vec<HnswSnapshot> = Vec::new();
        for (key, coll) in self.vector_collections.iter() {
            if coll.is_empty() {
                continue; // empty collection: no state to snapshot
            }
            let checkpoint_bytes = coll
                .checkpoint_to_bytes(self.segment_keks.vector_checkpoint_kek.as_ref())
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!("vector snapshot encode failed: {e}"),
                })?;
            hnsw_indexes.push(HnswSnapshot {
                database_id: key.0.as_u64(),
                tenant_id: key.1.as_u64(),
                collection: key.2.clone(),
                checkpoint_bytes,
            });
        }

        let mut crdt_snapshots: Vec<CrdtSnapshot> = Vec::new();
        for ((database_id, tid), engine) in &self.crdt_engines {
            for (collection, snapshot_bytes) in engine.export_all_snapshots()? {
                crdt_snapshots.push(CrdtSnapshot {
                    database_id: database_id.as_u64(),
                    tenant_id: tid.as_u64(),
                    peer_id: engine.peer_id(),
                    collection,
                    snapshot_bytes,
                });
            }
        }

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
