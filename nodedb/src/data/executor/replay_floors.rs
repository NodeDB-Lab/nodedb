// SPDX-License-Identifier: BUSL-1.1

//! Per-engine replay stamps recovered from on-disk checkpoints at boot,
//! consulted by WAL replay so a restored checkpoint is not re-derived from
//! records it already contains.
//!
//! ## Why replaying the records a stamp does not hold is safe
//!
//! A checkpoint's [`ReplayStamp`] names exactly the records its state holds:
//! every record at or below its prefix, and the records above the prefix this
//! core applied before the checkpoint. Every other record replays, through the
//! same paths, in LSN order, on top of the restored state.
//!
//! A replayed record can have a lower LSN than a record the state already
//! holds: it was still on its way when the higher one applied. The live core
//! applied the two in that same order, so replay reproduces the live result.
//! Every KV WAL record replays either as an absolute overwrite (`kv_put`,
//! `kv_batch_put`, `kv_delete`, `kv_truncate`) or as a delta re-executed
//! against the engine's current state (`kv_incr`, `kv_cas`, `kv_field_set`,
//! `kv_transfer`, ...), and both land where the live apply landed.
//!
//! Records the stamp holds MUST be skipped. For the absolute-overwrite
//! records re-applying is merely redundant, but for the delta records it is
//! corruption: an increment already folded into the checkpoint would be
//! counted twice.
//!
//! ## Why a floor is engine-wide rather than per-collection
//!
//! A KV record can span two collections (`kv_transfer_item` moves a row between
//! them). With per-collection floors those two collections could disagree —
//! source covered, destination not — and the record is then unrepresentable:
//! skipping it drops the destination's insert, applying it double-debits the
//! source. Publishing every collection's file under one generation, named by a
//! single manifest, makes disagreement unreachable by construction: all live
//! collections advance to one LSN together or none do.
//!
//! ## Adding an engine
//!
//! Add a field to [`ReplayFloors`], populate it from that engine's
//! `load_*_checkpoints` boot path, and consult it from that engine's replay
//! arms. Nothing here needs reshaping — engines do not share a floor.
//!
//! ## Which engines need one
//!
//! An engine whose WAL records are deltas or appends against current state
//! needs one. The sparse-vector engine carries one as well: its checkpoint
//! names the records it holds, and deciding every record by that stamp keeps
//! replay in the order the live core applied them.
//!
//! Checkpointed engines with no field here:
//!
//! * The sync idempotency gate — `SyncSeqAdvance` advances both its maps by
//!   max-wins, so re-folding a record already contained in the restored state
//!   cannot change it. What that restore needs instead is for replay to MERGE
//!   into it rather than replace it; see `install_sync_hwm_maps`.
//! * Graph node labels — `GraphNodeLabelSet` ORs a bit on and
//!   `GraphNodeLabelRemove` ANDs it off, both keyed by `(node, label)` NAME, so
//!   a record re-applied over the restored bitset lands on the same bit. The
//!   names are why: the restore keys by name rather than by local node id
//!   precisely because ids are not stable across restarts, and replay uses the
//!   same `add_node_label` / `remove_node_label` entry points as the live
//!   handler.
//! * The array and timeseries engines. Each carries its own stamp per
//!   artifact rather than one here: an array manifest carries the stamp of
//!   the flush that last published it, and a timeseries partition carries the
//!   stamp of the flush that wrote it. Arrays and timeseries collections flush
//!   independently of one another, so an engine-wide field is wrong for both.
//!   Both still decide a record through `ReplayStamp::skips`.
//! * Full-text search — `FtsIndex` rewrites the surrogate's posting, length and
//!   stats entries wholesale, deriving the corpus-counter deltas from the prior
//!   doc-length row read in the same write transaction, so a re-applied record
//!   lands on the state it already produced; `FtsDelete` decrements only when a
//!   prior length existed. See `wal_replay_fts.rs`, which proves it.
//! * Spatial — `SpatialPut` overwrites the surrogate's document body and
//!   replaces (rather than appends) its R-tree entry, guarded by the docmap the
//!   entry is always written with; `SpatialDelete` removes an absent entry as a
//!   no-op. See `wal_replay_spatial.rs`, which proves it.
//!
//! ## Why there is no general applied-LSN replay cursor
//!
//! A single durable "replay reached LSN X" cursor would look like it subsumes
//! all of the above. It does not, and it cannot be made correct here.
//!
//! The reason is that replay is not one write. A record's effect lands in
//! whichever engines it touches — a redb transaction, an in-memory HNSW graph,
//! an mmap'd tile segment — and no cursor write can be made atomic with all of
//! them at once. A cursor persisted before an engine absorbed the record skips
//! it forever on the next boot; persisted after, it is exactly the per-engine
//! floor already recorded here, only coarser: one engine's slow flush would
//! hold the shared cursor back for every other engine.
//!
//! What makes a cursor unnecessary is a property replay already holds: **WAL
//! replay never advances a DURABLE gate ahead of the state it describes.** Every
//! floor and watermark in this file is set from a checkpoint at boot or by a
//! completed flush — never by the act of replaying — and every durable side
//! effect replay itself performs is an overwrite keyed by identity (a surrogate
//! or a coordinate), not an accumulation. So a crash in the middle of replay
//! leaves nothing to reconcile: the in-memory work is simply gone, the durable
//! work is exactly what a repeat of it would produce, and the next boot restarts
//! from the same floors and converges on the same state. That is what makes
//! replay resumable, and it is a stronger guarantee than a cursor could give,
//! because it does not depend on a write ordering that spans engines.
//!
//! The obligation this places on new code is the one the per-engine notes
//! above discharge: an engine whose replay is a DELTA against current state, or
//! whose replay writes durable state non-idempotently, must gate itself — with
//! a floor here, or with a per-collection watermark it carries itself. It must
//! not assume a cursor will cover it.

use crate::data::executor::applied_prefix::ReplayStamp;

/// Checkpoint-restored replay floors for every engine on one core.
///
/// Lives on `CoreLoop` rather than being threaded through `replay_all_wal` as a
/// parameter because it is restored core state, exactly like the watermark: the
/// `load_*_checkpoints` boot methods produce it and every replay path reads it
/// through `&self`. Default (all-unset) means "no checkpoint restored", which
/// gates nothing and replays the full WAL — the safe direction.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct ReplayFloors {
    /// KV engine floor, populated by `CoreLoop::load_kv_checkpoints`.
    pub(in crate::data::executor) kv: ReplayFloor,

    /// Columnar engine floor, populated by
    /// `CoreLoop::load_columnar_checkpoints`.
    ///
    /// Columnar needs a floor for a blunter reason than KV's. KV's delta
    /// records are a minority of its record classes; for columnar,
    /// re-applying is corrupting on the ordinary path:
    ///
    /// * `ColumnarOp::Update` is implemented as delete-old-PK + insert-new-row
    ///   (`wal_replay_columnar_dml.rs` states this as its idempotence
    ///   constraint), so a record folded into the restored engine and
    ///   replayed again appends a duplicate row.
    /// * `ColumnarOp::Insert` upserts by tombstoning the prior row for its
    ///   PK, which masks the duplicate on a plain collection but NOT on a
    ///   `bitemporal=true` one: `MutationEngine::insert` deliberately skips
    ///   the upsert-tombstone there so every version is retained, and a
    ///   replayed insert becomes a second version visible to `AS OF` queries.
    ///
    /// `ColumnarOp::Delete` is idempotent (tombstone bit + PK-index removal),
    /// but it shares the floor with the two above: the floor is engine-wide,
    /// and a record class that tolerates gating does not need an exemption
    /// from it.
    pub(in crate::data::executor) columnar: ReplayFloor,

    /// Vector engine floor (HNSW, multi-vector and direct-row indexes),
    /// populated by `CoreLoop::load_vector_checkpoints`.
    ///
    /// An HNSW insert appends a node and never dedups, so a record the
    /// restored generation holds must not replay. The generation is published
    /// whole under one manifest, so one engine-wide stamp describes every
    /// index in it, including an index emptied or dropped since.
    pub(in crate::data::executor) vector: ReplayFloor,

    /// Sparse-vector engine floor, populated by
    /// `CoreLoop::load_sparse_vector_checkpoints`.
    ///
    /// A sparse put upserts by `doc_id`, so re-applying a record the restored
    /// generation holds lands on the same postings. The stamp still decides
    /// every record, so replay applies exactly the records the live core
    /// applied after the generation was written, in LSN order.
    pub(in crate::data::executor) sparse_vector: ReplayFloor,
}

/// What an engine's restored checkpoint holds.
///
/// `None` means no checkpoint was restored, so nothing is gated and the full WAL
/// replays. Shared by every engine in [`ReplayFloors`]: the gating rule is
/// [`ReplayStamp::skips`] for every engine. What differs between them is WHY
/// they need one at all, which is documented on each field above rather than
/// on this type.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct ReplayFloor {
    stamp: Option<ReplayStamp>,
}

impl ReplayFloor {
    /// Record what the restored checkpoint holds.
    ///
    /// Set once per boot, from the manifest that named the restored generation.
    pub(in crate::data::executor) fn set(&mut self, stamp: ReplayStamp) {
        self.stamp = Some(stamp);
    }

    /// Whether a record at `record_lsn` is already folded into the restored
    /// checkpoint, or has a final outcome that is not an apply, and must
    /// therefore NOT be replayed. Every KV, columnar, vector and sparse-vector
    /// skip site asks here.
    pub(in crate::data::executor) fn covers(&self, record_lsn: u64) -> bool {
        self.stamp
            .as_ref()
            .is_some_and(|stamp| stamp.skips(record_lsn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::applied_prefix::stamp::LsnRange;

    #[test]
    fn unset_floor_covers_nothing() {
        let floor = ReplayFloor::default();
        assert!(!floor.covers(1));
        assert!(
            !floor.covers(u64::MAX),
            "no checkpoint restored must never gate a record"
        );
    }

    #[test]
    fn covers_is_inclusive_of_the_stamped_prefix() {
        let mut floor = ReplayFloor::default();
        floor.set(ReplayStamp::through(100));
        assert!(floor.covers(99), "below the prefix is already durable");
        assert!(floor.covers(100), "the prefix itself is already durable");
        assert!(!floor.covers(101), "above the prefix must replay");
    }

    #[test]
    fn covers_the_applied_set_and_replays_the_gaps_in_it() {
        let mut floor = ReplayFloor::default();
        floor.set(ReplayStamp {
            prefix: 100,
            applied_above: vec![LsnRange {
                start: 103,
                end: 104,
            }],
        });
        assert!(floor.covers(103));
        assert!(floor.covers(104));
        assert!(
            !floor.covers(101),
            "a record in flight when the checkpoint was written must replay"
        );
        assert!(!floor.covers(105));
    }
}
