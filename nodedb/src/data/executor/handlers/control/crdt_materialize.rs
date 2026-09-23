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

use crate::data::executor::handlers::transaction::undo::document_outcome::{
    DocumentRow, push_put_undo,
};

use nodedb_types::{RowIdentity, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::document::store::StorageKey;

/// One CRDT row write to materialize into the sparse store.
pub(super) struct CrdtMaterializeWrite<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// The Loro row id the client addresses the row by: its client identity.
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    /// Gates inverted BM25 text indexing: `false` on the CRDT sync path (a
    /// separate `FtsIndex` frame delivers text), `true` for user SQL DML on a
    /// `crdt='true'` collection.
    pub index_text: bool,
}

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
        super::convert::crdt_row_body(&loro_val)
    }

    /// Write the merged CRDT document into the sparse document store — with the
    /// same side effects a native schemaless put produces — so the synced write
    /// is fully visible to scans, statistics, secondary/spatial/vector indexes,
    /// and CDC.
    ///
    /// Routes through `apply_point_put` inside a single write transaction, then
    /// commits and emits the `WriteEvent`, mirroring `execute_point_put`. The
    /// storage key is the hex-encoded surrogate (identical to the native path),
    /// NOT the CRDT `document_id` (the user-facing Loro row id, which the
    /// `WriteEvent` names the row by); bitemporal
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
        document_id: &str,
        surrogate: Surrogate,
        value: &[u8],
    ) {
        // A materialization miss is logged and never wedges the sync stream.
        if let Err(error) = self.materialize_document_write(
            task,
            CrdtMaterializeWrite {
                tid,
                collection,
                document_id,
                surrogate,
                value,
                index_text: false,
            },
        ) {
            warn!(
                core = self.core_id,
                %collection,
                surrogate = surrogate.as_u32(),
                %error,
                "crdt sync materialize into sparse document store failed"
            );
        }
    }

    /// Shared body of the sparse-store materialization. `index_text` gates
    /// inverted BM25 text indexing: `false` on the CRDT sync path (a separate
    /// `FtsIndex` frame delivers text), `true` for user SQL DML on a
    /// `crdt='true'` collection (no separate frame — the merged row is the
    /// only source). `document_id` is the Loro row id the client addresses
    /// the row by: the row's client identity.
    pub(super) fn materialize_document_write(
        &mut self,
        task: &ExecutionTask,
        write: CrdtMaterializeWrite<'_>,
    ) -> crate::Result<()> {
        let CrdtMaterializeWrite {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            index_text,
        } = write;
        let database_id = task.request.database_id.as_u64();
        let storage_key = StorageKey::for_surrogate(surrogate);

        let txn = self.sparse.begin_write()?;
        let outcome = self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                storage_key,
                surrogate,
                value,
                index_text,
                user_roles: &task.request.user_roles,
                enforce: false,
                wal_lsn: task.wal_lsn(),
                resolved_targets: &[],
            },
        )?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("crdt materialize commit: {e}"),
        })?;

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Data → Event Plane. The task carries `EventSource::CrdtSync`, so CDC /
        // change streams observe the synced write while AFTER triggers skip it
        // (non-User events do not cascade).
        self.emit_put_event(
            task,
            tid,
            collection,
            RowIdentity::from_user_key(document_id),
            value,
            outcome.prior_value.as_deref(),
        );
        if self.recording_redo_undo() {
            let mut undo = Vec::new();
            push_put_undo(
                &mut undo,
                DocumentRow {
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    identity: RowIdentity::from_user_key(document_id),
                },
                outcome,
                None,
            );
            self.record_redo_undo(undo);
        }
        Ok(())
    }
}
