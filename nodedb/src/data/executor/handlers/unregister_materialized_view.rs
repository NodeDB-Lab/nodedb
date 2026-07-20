// SPDX-License-Identifier: BUSL-1.1

//! Materialized-view reclaim handler.
//!
//! Mirrors `execute_unregister_collection` one level up: a
//! materialized view has its own columnar segment files (populated
//! by the CDC refresh loop) that outlive the MV's catalog row when
//! the MV is dropped — unless reclaim runs on every follower. This
//! handler is dispatched from the Control-Plane post-apply flow for
//! `CatalogEntry::DeleteMaterializedView` on every node, so each
//! node cleans up its local copy symmetrically.
//!
//! Idempotent: missing in-memory state is a no-op; missing files
//! are a no-op. Safe to re-run after partial completion.

use tracing::info;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_unregister_materialized_view(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        name: &str,
    ) -> Response {
        info!(
            core = self.core_id,
            tenant_id, name, "starting materialized view reclaim"
        );
        let tid = TenantId::new(tenant_id);
        let db = task.request.database_id;
        let nm = name.to_string();

        // MV columnar state lives in the same per-core maps as a
        // regular collection — the MV refresh loop writes rows into
        // `columnar_engines[(tid, mv_name)]` / `columnar_memtables` /
        // `columnar_flushed_segments` keyed by the MV name. Reclaim
        // is the same `retain` sweep as `execute_unregister_collection`.
        let memtable_removed = self
            .columnar_memtables
            .remove(&(db, tid, nm.clone()))
            .is_some();
        // Drop the memtable's engine-memory reservation alongside it.
        self.columnar_memtable_mem.remove(&(db, tid, nm.clone()));

        let before_engines = self.columnar_engines.len();
        self.columnar_engines
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &nm));
        let engines_removed = before_engines - self.columnar_engines.len();

        let before_segments = self.columnar_flushed_segments.len();
        self.columnar_flushed_segments
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &nm));
        let segments_removed = before_segments - self.columnar_flushed_segments.len();
        // Lockstep: drop the surrogate sidecar for the same MV key.
        self.columnar_flushed_surrogates
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &nm));

        // Doc cache: an MV's rows can be cached the same way a
        // collection's are (the Control Plane surfaces `SELECT * FROM
        // <mv>` through the same path).
        self.doc_cache
            .evict_collection(task.request.database_id.as_u64(), tenant_id, name);

        // Aggregate + chain-hash + doc-config caches are all keyed
        // `(tenant, collection_or_mv_name)` — same retain sweep.
        self.aggregate_cache
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &nm));
        self.chain_hashes
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &nm));
        self.doc_configs
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &nm));

        info!(
            core = self.core_id,
            tenant_id,
            name,
            memtable_removed,
            engines_removed,
            segments_removed,
            "materialized view reclaim complete"
        );

        let summary = serde_json::json!({
            "tenant_id": tenant_id,
            "materialized_view": name,
            "memtable_removed": memtable_removed,
            "engines_removed": engines_removed,
            "segments_removed": segments_removed,
        });
        match crate::data::executor::response_codec::encode_json(&summary) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(_) => self.response_ok(task),
        }
    }
}
