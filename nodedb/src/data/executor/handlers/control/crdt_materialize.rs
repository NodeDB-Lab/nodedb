// SPDX-License-Identifier: BUSL-1.1

//! Materialization of applied CRDT deltas into the sparse document store.
//!
//! When a CRDT (Loro) delta is applied — whether from a sync peer or a native
//! client — the merged document must also be written into the sparse DOCUMENTS
//! store so `DocumentScan` / `ShapeSnapshot` observe it, exactly as a native
//! schemaless put does. These helpers are split out of `crdt.rs` to keep that
//! file within the file-size limit; they extend `CoreLoop` with the encode +
//! write steps invoked from `execute_crdt_apply`.
//!
//! Materialization reuses the same `apply_point_put` transaction helper the
//! native put path uses, so a synced document gets identical side effects:
//! column statistics, aggregate-cache invalidation, document cache, secondary
//! indexes, spatial R-tree, and vector index maintenance — plus a Data → Event
//! Plane `WriteEvent` (tagged with the task's `CrdtSync` source, so CDC/change
//! streams observe it while AFTER triggers do not cascade). The one deliberate
//! exclusion is inverted BM25 text indexing: the sync stream delivers that via
//! a separate `FtsIndexDoc` frame, so `index_text` is `false` here to avoid
//! double-indexing the same surrogate.
//!
//! Write-path enforcement is deliberately NOT run here either. Materialization
//! calls `apply_point_put` directly, one level BELOW the enforcement funnel, so
//! no materialized-sum delta is folded for a synced row. A delta is a RELATIVE
//! change to a target row's total, and these deltas already passed admission on
//! the ORIGIN replica — where the write was issued, its constraints decided, and
//! its target row credited. Folding again as the merged row lands on each
//! receiving replica would add the same amount once per replica.

use tracing::warn;

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::document::crdt_store::loro_value_to_json;
use crate::engine::document::store::StorageKey;

impl CoreLoop {
    /// Read the merged Loro row back and encode it into the schemaless
    /// MessagePack bytes the native put path accepts.
    ///
    /// Called while the CRDT engine `&mut` borrow is still live (the borrow
    /// checker forbids touching `self.sparse` here), so it is an associated
    /// function over the borrowed engine rather than a method. Returns `None`
    /// when the row is absent or cannot be converted — the caller then skips
    /// the sparse write. A materialization miss must never fail the delta
    /// apply: the Loro merge has already succeeded and the sync stream must
    /// not wedge.
    ///
    /// The returned bytes are the pre-canonicalization MessagePack, matching
    /// the raw `value` a native put receives; `apply_point_put` canonicalizes
    /// (or Binary-Tuple-encodes) internally, so encoding here would diverge
    /// from the native pipeline.
    pub(crate) fn encode_crdt_row(
        engine: &TenantCrdtEngine,
        collection: &str,
        document_id: &str,
    ) -> Option<Vec<u8>> {
        let loro_val = engine.read_row(collection, document_id)?;
        let json = loro_value_to_json(&loro_val);
        nodedb_types::json_to_msgpack(&json).ok()
    }

    /// Write the merged CRDT document into the sparse document store — with the
    /// same side effects a native schemaless put produces — so the synced write
    /// is fully visible to scans, statistics, secondary/spatial/vector indexes,
    /// and CDC.
    ///
    /// Routes through `apply_point_put` inside a single write transaction, then
    /// commits and emits the `WriteEvent`, mirroring `execute_point_put`. The
    /// storage key is the hex-encoded surrogate (identical to the native path),
    /// NOT the CRDT `document_id` (the user-facing Loro row id); bitemporal
    /// collections append a version per applied delta (handled inside
    /// `apply_point_put`), non-bitemporal collections overwrite by key
    /// (idempotent under replay). Inverted BM25 text indexing is skipped
    /// (`index_text: false`) — the sync path delivers a separate `FtsIndex`
    /// frame. Any failure is logged and swallowed (the transaction is dropped,
    /// leaving no partial write) so a materialization miss never wedges the
    /// sync stream.
    pub(crate) fn materialize_synced_document(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        value: &[u8],
    ) {
        self.materialize_document_write(task, tid, collection, surrogate, value, false);
    }

    /// Shared body of the sparse-store materialization. `index_text` gates
    /// inverted BM25 text indexing: `false` on the CRDT sync path (a separate
    /// `FtsIndex` frame delivers text), `true` for user SQL DML on a
    /// `crdt='true'` collection (no separate frame — the merged row is the
    /// only source).
    pub(super) fn materialize_document_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        value: &[u8],
        index_text: bool,
    ) {
        let database_id = task.request.database_id.as_u64();
        let storage_key = StorageKey::for_surrogate(surrogate);
        let row_key = storage_key.to_string();

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                warn!(core = self.core_id, %collection, error = %e, "crdt sync materialize: begin_write failed");
                return;
            }
        };

        let prior = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                document_id: row_key.as_str(),
                surrogate,
                value,
                index_text,
                user_roles: &task.request.user_roles,
                enforce: false,
                wal_lsn: task.wal_lsn(),
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    document_id = %storage_key,
                    error = %e,
                    "crdt sync materialize into sparse document store failed"
                );
                return;
            }
        };

        if let Err(e) = txn.commit() {
            warn!(core = self.core_id, %collection, error = %e, "crdt sync materialize: commit failed");
            return;
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Data → Event Plane. The task carries `EventSource::CrdtSync`, so CDC /
        // change streams observe the synced write while AFTER triggers skip it
        // (non-User events do not cascade).
        self.emit_put_event(
            task,
            tid,
            collection,
            storage_key.to_identity(),
            value,
            prior.prior_value.as_deref(),
        );
    }
}
