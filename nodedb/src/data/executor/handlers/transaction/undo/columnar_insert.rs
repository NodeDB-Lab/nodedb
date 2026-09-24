// SPDX-License-Identifier: BUSL-1.1

//! Pre-image capture for a columnar insert that runs under an undo log.

use nodedb_columnar::pk_index::RowLocation;
use nodedb_physical::physical_plan::ColumnarInsertIntent;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TenantId;

/// The PK bytes an insert binds, and the memtable rows it displaces as
/// `(pk_bytes, prior_location)`.
pub(in crate::data::executor) type ColumnarUndoState = (Vec<Vec<u8>>, Vec<(Vec<u8>, RowLocation)>);

impl CoreLoop {
    /// The PK bytes a columnar insert of `rows` (schema-ordered values) will
    /// bind, and the memtable rows it will displace, captured before the
    /// insert runs.
    pub(in crate::data::executor) fn columnar_insert_undo_state(
        &self,
        collection_key: &(nodedb_types::DatabaseId, TenantId, String),
        rows: &[Vec<nodedb_types::Value>],
        intent: ColumnarInsertIntent,
    ) -> ColumnarUndoState {
        let mut inserted_pks: Vec<Vec<u8>> = Vec::new();
        let mut displaced: Vec<(Vec<u8>, RowLocation)> = Vec::new();
        let Some(engine) = self.columnar_engines.get(collection_key) else {
            return (inserted_pks, displaced);
        };
        for values in rows {
            let Ok(pk_bytes) = engine.encode_pk_from_row(values) else {
                continue;
            };

            match intent {
                // `InsertUnique` never displaces a prior row. A real conflict
                // fails the statement before this undo entry is used. On the
                // success path it matches `InsertIfAbsent`: record the new PK
                // only when nothing occupies it.
                ColumnarInsertIntent::InsertIfAbsent | ColumnarInsertIntent::InsertUnique => {
                    if !engine.pk_index().contains(&pk_bytes) {
                        inserted_pks.push(pk_bytes);
                    }
                }
                ColumnarInsertIntent::Insert | ColumnarInsertIntent::Put => {
                    if let Some(prior_loc) = engine.pk_index().get(&pk_bytes).copied()
                        && prior_loc.segment_id == engine.memtable_segment_id()
                    {
                        displaced.push((pk_bytes.clone(), prior_loc));
                    }
                    inserted_pks.push(pk_bytes);
                }
            }
        }

        (inserted_pks, displaced)
    }
}
