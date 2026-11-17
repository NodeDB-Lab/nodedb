// SPDX-License-Identifier: BUSL-1.1

//! The current body of a row as one transaction sees it: BASE ∪ OVERLAY.
//!
//! Every staged read-modify-write (document update and upsert, document
//! delete under a write policy, CRDT row upsert and delete) resolves the
//! row through this one function so they agree on what "current" means.

use super::context::StageCtx;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::engine::document::store::StorageKey;

impl CoreLoop {
    /// The staged mutation of `surrogate` in `coll_key` inside `txn_id`, in
    /// any overlay tier. `None` when the transaction staged nothing there.
    pub(in crate::data::executor) fn staged_row(
        &self,
        txn_id: crate::types::TxnId,
        coll_key: &super::context::CollKey,
        surrogate: u32,
    ) -> crate::Result<Option<Staged>> {
        match self.txn_overlays.get(&txn_id) {
            Some(overlay) => Ok(overlay
                .get(coll_key, surrogate)?
                .map(std::borrow::Cow::into_owned)),
            None => Ok(None),
        }
    }

    /// The row's current stored body inside `ctx.txn_id`: a staged put wins
    /// over base, a staged tombstone means absent, otherwise base storage
    /// (the current version on a bitemporal collection). `Ok(None)` when the
    /// row is absent, tombstoned, or hidden by a staged TRUNCATE.
    pub(super) fn stage_current_body(&self, ctx: &StageCtx<'_>) -> crate::Result<Option<Vec<u8>>> {
        match self.staged_row(ctx.txn_id, &ctx.coll_key, ctx.surrogate.0)? {
            Some(Staged::Put(body)) => Ok(Some(body)),
            Some(Staged::Tombstone) => Ok(None),
            None if !self.stage_base_visible(ctx.txn_id, &ctx.coll_key) => Ok(None),
            None => {
                let storage_key = StorageKey::for_surrogate(ctx.surrogate);
                self.current_row_body(ctx.database_id, ctx.tid, ctx.collection, &storage_key)
            }
        }
    }
}
