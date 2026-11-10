// SPDX-License-Identifier: BUSL-1.1

//! Settle a document or CRDT post-image reply that a later plan of the same
//! transaction rewrote.

use super::reply::CalvinReply;
use super::target::CollKey;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

impl CoreLoop {
    /// Decide a document or CRDT post-image reply when a later plan,
    /// staged from journal position `marker`, rewrote one of its rows. A base
    /// read after the install would report the later plan's row, so the
    /// reply answers the rows the `RETURNING` plan's install stores, derived
    /// when it staged.
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
        let coll_key: CollKey = (
            task.request.database_id,
            TenantId::new(tid),
            images.collection.clone(),
        );
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };
        let later = overlay.slots_touched_since(marker, &coll_key);
        let rewritten = images.rows.iter().any(|(_, surrogate)| {
            later
                .iter()
                .any(|slot| slot.surrogate == surrogate.as_u32())
        });
        if !rewritten {
            return Ok(());
        }
        let docs = images
            .staged_docs
            .as_deref()
            .ok_or_else(|| ErrorCode::Internal {
                detail: format!(
                    "calvin RETURNING: the post-images of '{}' carry no staged rows",
                    images.collection
                ),
            })?;
        let payload =
            self.calvin_render_docs(images.engine, &images.spec, &images.rls_filters, docs)?;
        *reply = CalvinReply::Rows(payload);
        Ok(())
    }
}
