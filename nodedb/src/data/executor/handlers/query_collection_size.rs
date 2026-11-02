// SPDX-License-Identifier: BUSL-1.1

//! Collection-size-estimate handler.
//!
//! Sums on-core bytes for `(tenant_id, collection)` across every
//! engine's in-memory state. Runs on every core — the caller sums
//! per-core responses for a cluster-wide estimate.
//!
//! Each engine contributes whatever it cheaply knows; missing state
//! is treated as 0 (the collection may have been purged from that
//! engine but still exist in another). The sum is advisory — used
//! by the `_system.dropped_collections.size_bytes_estimate` column
//! to surface "how much storage will a hard-delete reclaim?" without
//! waiting for a full purge cycle.

use crate::bridge::envelope::{Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_query_collection_size(
        &self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let key = (task.request.database_id, tid, collection.to_string());
        let mut total_bytes: u64 = 0;

        // KV engine: sum slot-value byte lengths in the live hash
        // table for this (tenant, collection). Approximate — includes
        // the per-entry overhead.
        let database_id = task.request.database_id.as_u64();
        total_bytes = total_bytes.saturating_add(self.kv_engine.collection_mem_usage(
            database_id,
            tenant_id,
            collection,
        ));

        // Columnar engine: flushed segments have their encoded
        // in-memory byte buffers; in-memory memtable is treated as 0
        // (refresh after flush for a stable number).
        let columnar_key = (task.request.database_id, tid, collection.to_string());
        if let Some(segs) = self.columnar_flushed_segments.get(&columnar_key) {
            for seg in segs {
                total_bytes = total_bytes.saturating_add(seg.len() as u64);
            }
        }
        // Row-version stamps and chains: the bytes history holds beside the
        // segments.
        if let Some(engine) = self.columnar_engines.get(&columnar_key) {
            total_bytes = total_bytes.saturating_add(engine.version_memory_bytes() as u64);
        }

        // Vector indexes, multi-vector members included: every node, live
        // and ended, and the node spans.
        let field_prefix = format!("{collection}:");
        for ((d, t, k), index) in &self.vector_collections {
            if *d == key.0 && *t == tid && (k == collection || k.starts_with(&field_prefix)) {
                total_bytes = total_bytes.saturating_add(index.resident_bytes() as u64);
            }
        }
        // Sparse-vector indexes and R-trees, live postings and entries with
        // their history.
        for ((d, t, c, _), index) in &self.sparse_vector_indexes {
            if *d == key.0 && *t == tid && c == collection {
                total_bytes = total_bytes.saturating_add(index.resident_bytes() as u64);
            }
        }
        for ((d, t, c, _), rtree) in &self.spatial_indexes {
            if *d == key.0 && *t == tid && c == collection {
                total_bytes = total_bytes.saturating_add(rtree.resident_bytes() as u64);
            }
        }
        // Timeseries partitions on disk, the ones a TRUNCATE ended included
        // until the cleanup horizon purges them.
        if let Some(registry) = self.ts_registries.get(&key) {
            for (_, entry) in registry.iter() {
                if entry.meta.state != nodedb_types::timeseries::PartitionState::Deleted {
                    total_bytes = total_bytes.saturating_add(entry.meta.size_bytes);
                }
            }
        }

        // Edge versions and ended full-text versions. A store that does not
        // read fails the query, as the document scan below does.
        let history = self
            .edge_store
            .collection_version_bytes(database_id, tid, collection)
            .and_then(|edges| {
                self.inverted
                    .text_history_bytes(database_id, tid, collection)
                    .map(|text| edges.saturating_add(text))
            });
        match history {
            Ok(bytes) => total_bytes = total_bytes.saturating_add(bytes),
            Err(e) => {
                return self.response_error(task, crate::bridge::envelope::ErrorCode::from(e));
            }
        }

        // Sparse documents: count the bytes of every stored version, history
        // and tombstones included, via a redb range scan of the
        // collection's prefix. O(N) in stored versions. A scan that fails
        // fails the query: a short count reads as a small collection.
        match self
            .sparse
            .approx_bytes_for_collection(database_id, tenant_id, collection)
        {
            Ok(bytes) => total_bytes = total_bytes.saturating_add(bytes),
            Err(e) => {
                return self.response_error(task, crate::bridge::envelope::ErrorCode::from(e));
            }
        }

        // Live inverted-index postings share the sparse redb, and the sparse
        // scan's range sum covers them.

        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&total_bytes.to_le_bytes());
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from(payload),
            watermark_lsn: self.watermark,
            error_code: None,
            stage_vote: None,
            read_vshards: crate::types::ReadShards::new(),
            write_set: Vec::new(),
        }
    }
}
