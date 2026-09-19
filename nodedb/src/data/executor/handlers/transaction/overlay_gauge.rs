// SPDX-License-Identifier: BUSL-1.1

//! Choke-point accessors for the per-transaction staging overlays.
//!
//! Every site that first materializes a transaction's overlay must go through
//! `txn_overlay_mut` / `graph_txn_overlay_mut` / `array_txn_overlay_mut` so the shared
//! `active_txn_overlays` gauge stays exact. The gauge is decremented in
//! lockstep when the overlays are dropped (see `MetaOp::DropTxnOverlay`).

use std::sync::atomic::Ordering;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{
    ArrayTxnOverlay, GraphTxnOverlay, TxnOverlay,
};
use crate::types::TxnId;

impl CoreLoop {
    /// Get-or-create this transaction's staging overlay, bumping the
    /// `active_txn_overlays` gauge on FIRST creation. The single creation choke
    /// point for `txn_overlays` — every staging site must go through here so the
    /// gauge stays exact.
    pub(in crate::data::executor) fn txn_overlay_mut(&mut self, txn_id: TxnId) -> &mut TxnOverlay {
        if !self.txn_overlays.contains_key(&txn_id)
            && let Some(m) = &self.metrics
        {
            m.active_txn_overlays.fetch_add(1, Ordering::Relaxed);
        }
        // Refresh the lease stamp on every staged write so the overlay reaper
        // never reclaims a transaction that is still writing.
        let ord = self.hlc.next_ordinal();
        let overlay = self.txn_overlays.entry(txn_id).or_default();
        overlay.touch(ord);
        overlay
    }

    /// Get-or-create this transaction's GRAPH staging overlay, bumping the
    /// `active_txn_overlays` gauge on FIRST creation. The single creation choke
    /// point for `graph_txn_overlays` — every staging site must go through here
    /// so the gauge stays exact.
    pub(in crate::data::executor) fn graph_txn_overlay_mut(
        &mut self,
        txn_id: TxnId,
    ) -> &mut GraphTxnOverlay {
        if !self.graph_txn_overlays.contains_key(&txn_id)
            && let Some(m) = &self.metrics
        {
            m.active_txn_overlays.fetch_add(1, Ordering::Relaxed);
        }
        // Refresh the lease stamp on every staged graph write (see
        // `txn_overlay_mut`).
        let ord = self.hlc.next_ordinal();
        let overlay = self.graph_txn_overlays.entry(txn_id).or_default();
        overlay.touch(ord);
        overlay
    }

    /// Get-or-create this transaction's ARRAY staging overlay, bumping the
    /// `active_txn_overlays` gauge on FIRST creation. The single creation choke
    /// point for `array_txn_overlays` — every staging site must go through here
    /// so the gauge stays exact.
    pub(in crate::data::executor) fn array_txn_overlay_mut(
        &mut self,
        txn_id: TxnId,
    ) -> &mut ArrayTxnOverlay {
        if !self.array_txn_overlays.contains_key(&txn_id)
            && let Some(m) = &self.metrics
        {
            m.active_txn_overlays.fetch_add(1, Ordering::Relaxed);
        }
        // Refresh the lease stamp on every staged array write (see
        // `txn_overlay_mut`).
        let ord = self.hlc.next_ordinal();
        let overlay = self.array_txn_overlays.entry(txn_id).or_default();
        overlay.touch(ord);
        overlay
    }
}
