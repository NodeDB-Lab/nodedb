// SPDX-License-Identifier: BUSL-1.1

//! Deterministic lease GC for per-transaction staging overlays.
//!
//! Each in-flight transaction stages its not-yet-durable writes into four
//! per-`TxnId` maps on `CoreLoop` (`txn_overlays`, `graph_txn_overlays`,
//! `array_txn_overlays`, `txn_created_columnar_engines`). They are normally released by
//! `MetaOp::DropTxnOverlay`, emitted by COMMIT/ROLLBACK. The teardown hooks
//! (pgwire `on_connection_end` → reclaim, native `run()` reclaim) drop the
//! overlay best-effort — they log and continue if the dispatch fails. This
//! module is the backstop for the residue: overlays orphaned when a client
//! vanishes, a teardown dispatch fails, or a vShard leader moves mid-txn.
//!
//! Every staged write and every in-transaction read-your-own-write refreshes
//! the overlay's `last_touch` stamp (see `TxnOverlay::touch`), so a still-live
//! transaction — even a long read-only one — always carries a fresh stamp.
//! [`CoreLoop::reap_expired_overlays`] reclaims only overlays whose stamp has
//! aged past [`OVERLAY_LEASE_NS`]. It runs synchronously from the maintenance
//! tick, between reactor tasks — never interleaved with a COMMIT's
//! `MetaOp::ResolveTxn` (which reads the overlay to build the RedoRecord),
//! because the core is single-threaded per shard.

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TxnId;

/// Conservative logical-nanosecond lease after which an untouched per-txn
/// staging overlay is treated as abandoned and reclaimed.
///
/// INVARIANT: this MUST exceed the maximum legitimate live-idle window of an
/// open transaction. If it did not, a still-active txn's overlay could be
/// reaped while single-shard COMMIT (`MetaOp::ResolveTxn`) still needs to read
/// it to build the RedoRecord for vector/FTS index durability — corrupting a
/// live transaction. The governing bound is the session idle timeout (auth
/// config `idle_timeout_secs`, default 3600s = 1h; `session_absolute_timeout_secs`
/// caps total session life when set): a vanished client's connection is torn
/// down at that timeout, firing the best-effort overlay drop, and this reaper
/// only backstops the case where that teardown dispatch itself failed. Six
/// hours sits comfortably above the default 1h idle timeout with generous
/// margin. Biasing LONG is deliberate — a too-long lease only delays
/// reclaiming dead overlays, a too-short lease corrupts live ones.
///
/// Deriving this from the auth config (`>= 2 * max(idle_timeout, absolute_timeout)`)
/// is the follow-up once that config is threaded onto the Data-Plane core; it
/// is a Control-Plane `auth` config today and is not reachable from here.
pub(in crate::data::executor) const OVERLAY_LEASE_NS: i64 = 6 * 3_600 * 1_000_000_000;

/// Per-maintenance-tick cap on overlays reclaimed per call, mirroring the KV
/// expiry wheel's per-tick reap budget: a huge leak is drained across ticks so
/// a single maintenance pass never spikes reactor latency. The remainder is
/// reaped on subsequent ticks.
pub(in crate::data::executor) const OVERLAY_REAP_BUDGET: usize = 1_024;

impl CoreLoop {
    /// Refresh the lease stamp for `txn_id` on whichever of the three staging
    /// overlays currently hold it. Called from every in-transaction
    /// read-your-own-write path so a long read-only transaction never ages out
    /// (the load-bearing safety property of the reaper). A no-op for a txn with
    /// no overlay.
    pub(in crate::data::executor) fn touch_overlay(&self, txn_id: TxnId) {
        let ord = self.hlc.next_ordinal();
        if let Some(overlay) = self.txn_overlays.get(&txn_id) {
            overlay.touch(ord);
        }
        if let Some(overlay) = self.graph_txn_overlays.get(&txn_id) {
            overlay.touch(ord);
        }
        if let Some(overlay) = self.array_txn_overlays.get(&txn_id) {
            overlay.touch(ord);
        }
    }

    /// Release all staging state for `txn_id`: the value/TTL overlay, the
    /// parallel GRAPH and ARRAY overlays, and any still-empty columnar engines this
    /// transaction auto-created during staging. Decrements the
    /// `active_txn_overlays` gauge by the number of overlays removed and
    /// returns that count.
    ///
    /// The single canonical teardown, shared by `MetaOp::DropTxnOverlay`
    /// (commit/rollback) and the lease reaper so the two can never drift. The
    /// still-empty columnar-engine check matches the DropTxnOverlay contract:
    /// a rolled-back (or reaped) txn's auto-created engine never had rows leave
    /// the overlay, so its memtable is empty and the phantom engine is dropped;
    /// a committed txn populated the memtable before this runs, so it survives.
    pub(in crate::data::executor) fn drop_overlay_entry(&mut self, txn_id: TxnId) -> u64 {
        let removed = u64::from(self.txn_overlays.remove(&txn_id).is_some())
            + u64::from(self.graph_txn_overlays.remove(&txn_id).is_some())
            + u64::from(self.array_txn_overlays.remove(&txn_id).is_some());
        if removed > 0
            && let Some(m) = &self.metrics
        {
            m.active_txn_overlays.fetch_sub(removed, Ordering::Relaxed);
        }
        if let Some(created) = self.txn_created_columnar_engines.remove(&txn_id) {
            for engine_key in created {
                let still_empty = self
                    .columnar_engines
                    .get(&engine_key)
                    .is_some_and(|engine| engine.memtable().is_empty());
                if still_empty {
                    self.columnar_engines.remove(&engine_key);
                }
            }
        }
        removed
    }

    /// Reclaim per-txn staging overlays whose lease has expired.
    ///
    /// A txn survives when the MAX of its value-, graph-, and array-overlay
    /// stamps is at or above `peek() - OVERLAY_LEASE_NS` — a refresh on any
    /// overlay keeps the whole transaction alive. Capped at
    /// [`OVERLAY_REAP_BUDGET`] per call so a mass leak drains across ticks
    /// without stalling the reactor. Runs between tasks on the single-threaded
    /// core, so it never interleaves with a COMMIT resolve.
    pub(in crate::data::executor) fn reap_expired_overlays(&mut self) {
        let threshold = self.hlc.peek().saturating_sub(OVERLAY_LEASE_NS);

        // Union of txn ids across every overlay — a txn may hold only one of
        // them (value-only, graph-only, or array-only), so no map alone is
        // complete.
        let candidates: HashSet<TxnId> = self
            .txn_overlays
            .keys()
            .chain(self.graph_txn_overlays.keys())
            .chain(self.array_txn_overlays.keys())
            .copied()
            .collect();

        let mut expired: Vec<TxnId> = Vec::new();
        for txn_id in candidates {
            let value_ord = self.txn_overlays.get(&txn_id).map(|o| o.last_touch());
            let graph_ord = self.graph_txn_overlays.get(&txn_id).map(|o| o.last_touch());
            let array_ord = self.array_txn_overlays.get(&txn_id).map(|o| o.last_touch());
            let Some(max_ord) = value_ord
                .into_iter()
                .chain(graph_ord)
                .chain(array_ord)
                .max()
            else {
                continue;
            };
            if max_ord < threshold {
                expired.push(txn_id);
                if expired.len() >= OVERLAY_REAP_BUDGET {
                    break;
                }
            }
        }

        if expired.is_empty() {
            return;
        }
        let reaped = expired.len();
        for txn_id in expired {
            self.drop_overlay_entry(txn_id);
        }
        tracing::warn!(
            core = self.core_id,
            reaped,
            "overlay lease GC: reclaimed abandoned per-txn staging overlays past \
             lease (client vanished / teardown dispatch failed / leader moved \
             mid-txn)"
        );
    }
}
