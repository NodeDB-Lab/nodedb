// SPDX-License-Identifier: BUSL-1.1

//! Settle a document or CRDT post-image reply that a later plan of the same
//! transaction rewrote.

use nodedb_types::Surrogate;

use super::images::{ReturningImageRow, RowLocation, staged_row_image};
use super::reply::CalvinReply;
use super::target::CollKey;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

impl CoreLoop {
    /// Decide a document or CRDT post-image reply when a later plan,
    /// staged from journal position `marker`, rewrote one of its rows. A base
    /// read after the install would report the later plan's row, so each row
    /// takes the image the `RETURNING` plan staged instead.
    pub(super) fn settle_rows_before_later_write(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tid: u64,
        reply: &mut CalvinReply,
        marker: usize,
    ) -> Result<(), ErrorCode> {
        let CalvinReply::PostImages(images) = &*reply else {
            return Ok(());
        };
        let database_id = task.request.database_id;
        let coll_key: CollKey = (database_id, TenantId::new(tid), images.collection.clone());
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };
        let later = overlay.slots_touched_since(marker, &coll_key);
        let rewritten = |surrogate: Surrogate| {
            later
                .iter()
                .find(|slot| slot.surrogate == surrogate.as_u32())
        };
        if !images.rows.iter().any(|(_, s)| rewritten(*s).is_some()) {
            return Ok(());
        }
        let at = RowLocation {
            engine: images.engine,
            database_id: database_id.as_u64(),
            tid,
            collection: &images.collection,
            // Post-images are document and CRDT rows only.
            vector_field: None,
        };
        let mut rows = Vec::with_capacity(images.rows.len());
        for (identity, surrogate) in &images.rows {
            // The row as the `RETURNING` plan left it: the later plan's prior
            // value when that plan touched it, else the overlay's current value.
            let staged = match rewritten(*surrogate) {
                Some(slot) => slot.before,
                None => overlay.get(&coll_key, surrogate.as_u32()),
            };
            if let Some(Staged::Put(body)) = staged {
                let (bytes, vector) = staged_row_image(images.engine, body)?;
                rows.push(ReturningImageRow {
                    identity: identity.clone(),
                    surrogate: *surrogate,
                    bytes,
                    vector,
                });
            }
        }
        let payload = self.calvin_render_rows(&at, &images.spec, &images.rls_filters, &rows)?;
        *reply = CalvinReply::Rows(payload);
        Ok(())
    }
}
