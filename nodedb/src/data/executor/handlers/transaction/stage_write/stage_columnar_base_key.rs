// SPDX-License-Identifier: BUSL-1.1

//! Records which base row a staged columnar UPDATE or DELETE displaces.
//!
//! The overlay is keyed by surrogate and holds only the row's final image or a
//! tombstone. COMMIT resolve turns that into a row-image redo record, and the
//! record must name the base row the transaction replaced by its primary key.
//! A key-changing UPDATE and a DELETE remove a row the final image does not
//! name. The key is recorded here, at the statement, from the pre-image the
//! statement matched. It is recorded only when that pre-image is a base row:
//! the transaction has staged nothing for the surrogate yet.

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId, TxnId};

impl CoreLoop {
    /// Record the primary key of `row` as the base key of `surrogate` when
    /// the transaction has staged nothing for `surrogate` yet. Call it before
    /// staging the statement's put or tombstone for that surrogate.
    pub(super) fn stage_note_columnar_base_row(
        &mut self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        schema: &ColumnarSchema,
        surrogate: u32,
        row: &[Value],
    ) -> crate::Result<()> {
        let pk = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .and_then(|idx| row.get(idx))
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "columnar staging on '{}': matched row carries no primary-key value",
                    coll_key.2
                ),
            })?;
        self.stage_note_columnar_base_pk(txn_id, coll_key, surrogate, pk)
    }

    /// Record `pk` as the base key of `surrogate` when the transaction has
    /// staged nothing for `surrogate` yet.
    pub(super) fn stage_note_columnar_base_pk(
        &mut self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
        pk: &Value,
    ) -> crate::Result<()> {
        let staged_already = self
            .txn_overlays
            .get(&txn_id)
            .is_some_and(|overlay| overlay.get(coll_key, surrogate).is_some());
        if staged_already {
            return Ok(());
        }
        let pk_msgpack =
            nodedb_types::value_to_msgpack(pk).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("columnar base key of '{}': {e}", coll_key.2),
            })?;
        self.txn_overlay_mut(txn_id)
            .note_base_pk(coll_key, surrogate, pk_msgpack);
        Ok(())
    }
}
