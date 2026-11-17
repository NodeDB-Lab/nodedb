// SPDX-License-Identifier: BUSL-1.1

//! Choke-point accessors for the per-transaction staging overlays.
//!
//! Every site that first materializes a transaction's overlay must go through
//! `txn_overlay_mut` / `graph_txn_overlay_mut` / `array_txn_overlay_mut` so the shared
//! `active_txn_overlays` gauge stays exact. The gauge is decremented in
//! lockstep when the overlays are dropped (see `MetaOp::DropTxnOverlay`).
//!
//! Each overlay spills into the transaction's directory
//! `data_dir/txn-spill/core-N/<txn_id>/`, past `[tuning.query]
//! max_txn_overlay_bytes` of hot entries. The directory is created on the
//! first spill and removed by `drop_overlay_entry`.

use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::spill::cleanup::SpillKind;
use crate::data::executor::handlers::spill::tiered::SpillSpace;
use crate::data::executor::handlers::transaction::overlay::{
    ArrayTxnOverlay, GraphTxnOverlay, TxnOverlay,
};
use crate::types::TxnId;

/// The spill directory of `txn_id` on the core of `data_dir` and `core_id`.
pub(in crate::data::executor) fn txn_spill_dir(
    data_dir: &std::path::Path,
    core_id: usize,
    txn_id: TxnId,
) -> PathBuf {
    SpillKind::Txn
        .core_dir(data_dir, core_id)
        .join(txn_id.as_u64().to_string())
}

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
        let overlay = match self.txn_overlays.entry(txn_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(TxnOverlay::with_spill(SpillSpace::new(
                txn_spill_dir(&self.data_dir, self.core_id, txn_id),
                "doc",
                self.query_tuning.max_txn_overlay_bytes,
            ))),
        };
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
        let overlay = match self.graph_txn_overlays.entry(txn_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(GraphTxnOverlay::with_spill(SpillSpace::new(
                txn_spill_dir(&self.data_dir, self.core_id, txn_id),
                "graph",
                self.query_tuning.max_txn_overlay_bytes,
            ))),
        };
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
        let overlay = match self.array_txn_overlays.entry(txn_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(ArrayTxnOverlay::with_spill(SpillSpace::new(
                txn_spill_dir(&self.data_dir, self.core_id, txn_id),
                "array",
                self.query_tuning.max_txn_overlay_bytes,
            ))),
        };
        overlay.touch(ord);
        overlay
    }
}
