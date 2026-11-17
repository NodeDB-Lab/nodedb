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
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::enforcement::chain_guard::{AbandonedWrite, abandon_write};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::document::store::StorageKey;
use crate::types::TenantId;
use loro::LoroValue;
use nodedb_crdt::state::RowWrite;

/// The row a one-row CRDT write projects into the sparse store.
pub(super) struct ProjectedRow<'a> {
    pub collection: &'a str,
    /// The Loro row id the client addresses the row by.
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// See [`CrdtMaterializeWrite::index_text`].
    pub index_text: bool,
}

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
    /// Read the merged Loro row back, its block lists resolved, and encode it
    /// into the native object body the native put path accepts.
    ///
    /// Called while the CRDT engine `&mut` borrow is still live (the borrow
    /// checker forbids touching `self.sparse` here), so it is an associated
    /// function over the borrowed engine rather than a method. `Ok(None)`
    /// when the row is absent. A row that does not decode (a Loro binary that
    /// is not a tagged value) is an error: the caller fails its read or
    /// write, or faults the sync apply, rather than drop the projection.
    ///
    /// The returned bytes are the pre-canonicalization body, matching the raw
    /// `value` a native put receives; `apply_point_put` canonicalizes (or
    /// Binary-Tuple-encodes) internally, so encoding here would diverge from
    /// the native pipeline.
    pub(crate) fn encode_crdt_row(
        engine: &TenantCrdtEngine,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        engine
            .read_row_deep(collection, document_id)
            .map(|loro_val| super::convert::crdt_row_body(&loro_val))
            .transpose()
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
    /// `WriteEvent` names the row by); every collection appends a version
    /// per materialized delta (handled inside `apply_point_put`). The
    /// live apply paths skip a re-delivered delta: it imports no Loro
    /// operation. Inverted BM25 text indexing is skipped
    /// (`index_text: false`) — the sync path delivers a separate `FtsIndex`
    /// frame.
    ///
    /// A failure drops the transaction, leaving no partial write, and is the
    /// caller's error. A live sync apply undoes its staged install and
    /// refuses the delta. WAL replay records the fault and stops, since its
    /// delta is already durable.
    pub(crate) fn materialize_synced_document(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: Surrogate,
        value: &[u8],
    ) -> crate::Result<()> {
        self.materialize_document_write(
            task,
            CrdtMaterializeWrite {
                tid,
                collection,
                document_id,
                surrogate,
                value,
                index_text: false,
            },
        )
    }

    /// Predict the deep row `write` leaves and encode it as the native body
    /// the sparse store takes. Changes nothing: a refused write answers here.
    pub(super) fn predict_row_body(
        &mut self,
        task: &ExecutionTask,
        row: &ProjectedRow<'_>,
        write: &RowWrite<'_>,
    ) -> crate::Result<(LoroValue, Vec<u8>)> {
        let predicted = self
            .get_crdt_engine(task.request.database_id, task.request.tenant_id)?
            .predict_row(row.collection, row.document_id, write)?;
        let body = super::convert::crdt_row_body(&predicted)?;
        Ok((predicted, body))
    }

    /// Land a predicted one-row write: project `body` into the sparse store,
    /// then run `write` on the CRDT document.
    ///
    /// A projection that fails returns its error and the document never
    /// changed. Once the projection has landed, the write's failure modes
    /// were all checked by the prediction, so a write that still fails, or
    /// leaves another row than `predicted`, fail-stops the core: the
    /// projection is ahead of the document, and a restart rebuilds both from
    /// the WAL.
    pub(super) fn land_row_write(
        &mut self,
        task: &ExecutionTask,
        row: &ProjectedRow<'_>,
        write: &RowWrite<'_>,
        predicted: &LoroValue,
        body: &[u8],
    ) -> crate::Result<()> {
        self.materialize_document_write(
            task,
            CrdtMaterializeWrite {
                tid: task.request.tenant_id.as_u64(),
                collection: row.collection,
                document_id: row.document_id,
                surrogate: row.surrogate,
                value: body,
                index_text: row.index_text,
            },
        )?;
        let landed = self
            .get_crdt_engine(task.request.database_id, task.request.tenant_id)
            .and_then(|engine| {
                engine.apply_row_write(row.collection, row.document_id, write)?;
                Ok(engine.read_row_deep(row.collection, row.document_id))
            });
        let fault = match landed {
            Ok(Some(written)) if &written == predicted => return Ok(()),
            Ok(written) => crate::Error::Internal {
                detail: format!(
                    "crdt write on '{}'/'{}' left {written:?}, not its predicted row \
                     {predicted:?}",
                    row.collection, row.document_id
                ),
            },
            Err(error) => error,
        };
        let detail = format!(
            "crdt write on '{}'/'{}' failed after its row projected: {fault}",
            row.collection, row.document_id
        );
        self.fail_stop_core(FailStopCause::CrdtWriteAfterProjection, &detail);
        Err(fault)
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

        // No chain guard: DDL refuses HASH_CHAIN on a CRDT collection, so this
        // write never reaches a chained row.
        // An abort drops the txn uncommitted, which reverses the durable
        // writes. `abandon_write` reverses the cache and in-memory index
        // entries.
        let txn = self.sparse.begin_write()?;
        let outcome = match self.apply_point_put(
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
                unique: crate::data::executor::enforcement::unique::UniqueJudge::Row,
                wal_lsn: task.wal_lsn(),
                resolved_targets: &[],
                // A merged CRDT row is never rewritten. Its guards were judged
                // before the merge or on the merged row.
                guards: crate::data::executor::enforcement::typeguard::GuardScope::InjectOnly,
            },
        ) {
            Ok(outcome) => outcome,
            Err(e) => {
                return Err(abandon_write(
                    self,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key),
                    e,
                ));
            }
        };
        if let Err(e) = txn.commit() {
            return Err(abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(outcome.memory_undo),
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("crdt materialize commit: {e}"),
                },
            ));
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);
        // The materialized version is stored history: the tenant's charge
        // takes it, as a document write's does.
        if let Err(e) = self.recharge_document_history(task.request.database_id, TenantId::new(tid))
        {
            crate::diag::history_charge_stale(&e, "crdt materialize");
            warn!(
                core = self.core_id,
                %collection,
                error = %e,
                "document history recharge after a crdt materialize failed; the prior charge stays"
            );
        }

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
                    collection,
                    storage_key,
                },
                outcome,
                None,
            );
            self.record_redo_undo(undo);
        }
        Ok(())
    }
}
