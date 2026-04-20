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
        tenant_id: u32,
        collection: &str,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let key = (tid, collection.to_string());
        let mut total_bytes: u64 = 0;

        // KV engine: sum slot-value byte lengths in the live hash
        // table for this (tenant, collection). Approximate — includes
        // the per-entry overhead.
        total_bytes =
            total_bytes.saturating_add(self.kv_engine.collection_mem_usage(tenant_id, collection));

        // Columnar engine: flushed segments have their encoded
        // in-memory byte buffers; in-memory memtable is treated as 0
        // (refresh after flush for a stable number).
        if let Some(segs) = self.columnar_flushed_segments.get(&key) {
            for seg in segs {
                total_bytes = total_bytes.saturating_add(seg.len() as u64);
            }
        }

        // Vector collections: byte-count is engine-internal and not
        // currently exposed as a cheap accessor. Contribution is 0
        // until that accessor lands; operators still see the
        // sparse + KV + columnar sum below.
        let _ = self.vector_collections.get(&key);

        // Sparse documents: count bytes via redb range scan of the
        // `{tenant}:{collection}:` prefix. Cheap O(N) over the
        // collection's rows — for millions of rows this is a handful
        // of redb btree seeks + iteration.
        total_bytes = total_bytes.saturating_add(
            self.sparse
                .approx_bytes_for_collection(tenant_id, collection),
        );

        // Inverted index postings share the sparse redb and don't
        // offer a cheap per-collection byte count today; covered by
        // the sparse scan's range-sum approximation.

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
        }
    }
}
