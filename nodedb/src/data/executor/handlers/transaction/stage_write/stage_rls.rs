// SPDX-License-Identifier: BUSL-1.1

//! Statement-time row-level-security enforcement for the columnar family's
//! staged writes.
//!
//! Staging is where an in-transaction statement's row image is produced and
//! made visible, so it is where the write policy has to decide it — the same
//! rule the document and key-value staging paths follow. COMMIT replay gates
//! the durable apply as well, and that stays: this is defence in depth, not a
//! replacement. What it adds is that a statement whose image the policy refuses
//! fails AT THE STATEMENT rather than reporting `{"affected": N}` and exposing
//! the refused row to the transaction's own reads until COMMIT fails.
//!
//! Every helper here decides the WHOLE set before the caller mutates any
//! overlay, so a refusal leaves the overlay exactly as it was.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::{ndb_field_to_value, row_values_to_object};
use crate::data::executor::handlers::rls_write_gate::admit_columnar_row;
use crate::data::executor::handlers::transaction::overlay::{Staged, decode_staged_row};
use crate::data::executor::handlers::upsert::apply_on_conflict_updates;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

impl CoreLoop {
    /// Decide a set of schema-ordered columnar rows against the compiled write
    /// policy.
    pub(super) fn stage_admit_columnar_rows<'a>(
        &self,
        task: &ExecutionTask,
        rls_write_check: &nodedb_types::RlsWriteCheck,
        rows: impl IntoIterator<Item = &'a [Value]>,
        schema: &ColumnarSchema,
        tid: u64,
        collection: &str,
    ) -> Result<(), Response> {
        if matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            return Ok(());
        }
        for row in rows {
            if let Err(error) = admit_columnar_row(
                rls_write_check,
                row,
                schema,
                tid,
                collection,
                &self.crs_context(),
            ) {
                return Err(self.response_error(task, error));
            }
        }
        Ok(())
    }

    /// The row image a staged `ColumnarOp::Insert` will ultimately persist.
    ///
    /// For a plain insert that is the coerced incoming row. For the ON CONFLICT
    /// DO UPDATE shape it is the prior row with the assignments applied —
    /// resolved here exactly as `insert_columnar_rows` resolves it on the
    /// durable path. This is both the image the write policy decides and the
    /// body that is staged, because they are the same row: deciding the
    /// submitted body instead would refuse `ON CONFLICT DO UPDATE SET
    /// owner = <me>` on someone else's row, which the merge makes legal, and
    /// staging the submitted body would show the transaction a row that COMMIT
    /// will not produce.
    ///
    /// The prior row is resolved through this transaction's own overlay first,
    /// then the engine. A columnar row's surrogate is content-addressed from
    /// its primary key, so an upsert targeting an existing row carries that
    /// row's surrogate and its staged supersede lands at the same overlay slot
    /// — which is exactly the row COMMIT replay will merge against, since by
    /// then the earlier statement has been applied. A tombstone means this
    /// transaction already removed the row, so the upsert inserts rather than
    /// merges.
    ///
    /// No prior row anywhere yields `values` unchanged, matching the durable
    /// path's insert branch.
    pub(super) fn staged_columnar_write_image(
        &self,
        txn_id: TxnId,
        engine_key: &(nodedb_types::DatabaseId, TenantId, String),
        schema: &ColumnarSchema,
        surrogate: Surrogate,
        values: Vec<Value>,
        on_conflict_updates: &[(String, UpdateValue)],
    ) -> Result<Vec<Value>, ErrorCode> {
        if on_conflict_updates.is_empty() {
            return Ok(values);
        }
        let staged = self
            .txn_overlays
            .get(&txn_id)
            .and_then(|overlay| overlay.get(engine_key, surrogate.0));
        let prior = match staged {
            Some(Staged::Tombstone) => None,
            Some(Staged::Put(body)) => decode_staged_row(body),
            None => {
                let Some(engine) = self.columnar_engines.get(engine_key) else {
                    return Ok(values);
                };
                let pk_bytes =
                    engine
                        .encode_pk_from_row(&values)
                        .map_err(|e| ErrorCode::Internal {
                            detail: format!("columnar insert: pk encode failed: {e}"),
                        })?;
                // A prior row that does not read refuses the statement: the
                // policy would otherwise decide against no prior row.
                self.read_columnar_row_by_pk(engine_key, &pk_bytes)
                    .map_err(ErrorCode::from)?
            }
        };
        let Some(prior) = prior else {
            return Ok(values);
        };

        let existing = row_values_to_object(schema, &prior);
        let excluded = row_values_to_object(schema, &values);
        let merged = apply_on_conflict_updates(
            existing,
            &excluded,
            on_conflict_updates,
            &self.crs_context(),
        )
        .map_err(ErrorCode::from)?;
        let Value::Object(merged) = merged else {
            return Err(ErrorCode::Internal {
                detail: "merged ON CONFLICT value was not an object".into(),
            });
        };
        let mut merged = schema
            .columns
            .iter()
            .map(|col| ndb_field_to_value(merged.get(&col.name), col))
            .collect::<Result<Vec<Value>, crate::Error>>()
            .map_err(ErrorCode::from)?;
        // The merged row is a new version: it carries this insert's stamp,
        // never the prior row's.
        crate::data::executor::handlers::columnar_write::keep_fresh_system_stamp(
            schema,
            &values,
            &mut merged,
        );
        Ok(merged)
    }
}
