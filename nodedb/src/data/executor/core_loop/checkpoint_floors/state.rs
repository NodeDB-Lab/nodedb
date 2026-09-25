// SPDX-License-Identifier: BUSL-1.1

//! What this core is durable through OUTSIDE the WAL, per engine.
//!
//! Grouped into one type because every field here answers the same question and
//! obeys the same rule: WAL segments below the LSN a core reports are DELETED, so
//! nothing may be claimed that was not actually put on stable storage. Failing to
//! advance one of these costs WAL growth; overstating one costs data.

use crate::data::executor::applied_prefix::AppliedPrefix;
use crate::data::executor::replay_floors::ReplayFloors;
use crate::types::Lsn;

/// Per-engine non-WAL durability points for one core.
///
/// Every `*_durable_lsn` is advanced ONLY by a fully successful flush of its
/// engine, and read by `execute_checkpoint`, which folds them into the single
/// minimum it reports to the checkpoint manager — the LSN that authorises
/// `WalManager::truncate_before` to unlink every sealed segment below it.
pub(in crate::data::executor) struct CheckpointFloors {
    /// Highest LSN the KV engine is known to be durable through OUTSIDE the WAL
    /// (i.e. in `{data_dir}/kv-ckpt/`), advanced only by a fully successful
    /// `checkpoint_kv_engines`.
    ///
    /// `KvEngine` is pure in-memory state with no redb store behind it, so the
    /// checkpoint file is its only non-WAL durability. When a flush fails, this
    /// — not the watermark — is what the core may report to the checkpoint
    /// manager, because WAL segments below the reported LSN are DELETED. Failing
    /// to advance it costs WAL growth; overstating it costs data.
    pub(in crate::data::executor) kv_durable_lsn: Lsn,

    /// Highest LSN the sparse-vector engine is known to be durable through
    /// OUTSIDE the WAL (i.e. in `{data_dir}/sparse-vector-ckpt/`), advanced only
    /// by a fully successful `checkpoint_sparse_vector_indexes` and restored at
    /// boot from the live generation's manifest.
    ///
    /// `sparse_vector_indexes` is in-memory state with no redb store behind it,
    /// so its checkpoint files are its only non-WAL durability. Same rule as
    /// `kv_durable_lsn`: when a flush fails, this — not the watermark — is what
    /// the core may report to the checkpoint manager.
    pub(in crate::data::executor) sparse_vector_durable_lsn: Lsn,

    /// Highest LSN the sync idempotency gate (`sync_hwm` +
    /// `producer_epoch_floor`) is known to be durable through OUTSIDE the WAL
    /// (i.e. in `{data_dir}/sync-hwm-ckpt/`), advanced only by a fully
    /// successful `checkpoint_sync_hwm` and restored at boot from the state file.
    ///
    /// The gate's only other durable copy is the `SyncSeqAdvance` WAL records.
    /// Overstating this deletes them and resets the gate, whose failure mode is
    /// not a missing row but a duplicated one: already-applied sync frames are
    /// re-admitted against a zeroed high-watermark.
    pub(in crate::data::executor) sync_hwm_durable_lsn: Lsn,

    /// Highest LSN the columnar engine is known to be durable through OUTSIDE
    /// the WAL (i.e. in `{data_dir}/columnar-ckpt/`), advanced only by a fully
    /// successful `checkpoint_columnar_engines` and restored at boot from the
    /// live generation's manifest.
    ///
    /// Both halves of the columnar engine are in-memory with no redb store
    /// behind them — `columnar_engines` holds the memtable, PK index and delete
    /// bitmaps, and `columnar_flushed_segments` holds encoded segment bytes that
    /// are never written to disk — so the checkpoint files are their only
    /// non-WAL durability. Same rule as `kv_durable_lsn`: when a flush fails,
    /// this — not the watermark — is what the core may report to the checkpoint
    /// manager.
    pub(in crate::data::executor) columnar_durable_lsn: Lsn,

    /// Highest LSN the CSR graph node labels are known to be durable through
    /// OUTSIDE the WAL (i.e. in `{data_dir}/graph-label-ckpt/`), advanced only
    /// by a fully successful `checkpoint_graph_labels` and restored at boot from
    /// the state file.
    ///
    /// Scoped to the LABELS, not the graph: edges are committed to the redb
    /// `EdgeStore` at apply time and rebuilt into the CSR in `CoreLoop::open`,
    /// so no LSN gates them. `CsrIndex::node_label_bits` has no store behind it,
    /// and its only other durable copy is the `GraphNodeLabelSet` /
    /// `GraphNodeLabelRemove` records. Same rule as `kv_durable_lsn`: when a
    /// flush fails, this — not the watermark — is what the core may report.
    pub(in crate::data::executor) graph_label_durable_lsn: Lsn,

    /// Highest LSN the array engine is known to be durable through OUTSIDE the
    /// WAL (i.e. in its on-disk tile segments), advanced only by a fully
    /// successful `checkpoint_array_engines`.
    ///
    /// The array engine writes no checkpoint file of its own — its durable form
    /// is the segments `ArrayEngine::flush` produces and `ArrayStore::open`
    /// mmaps back — so this is not restored at boot: it starts at zero and is
    /// first advanced by this process's own successful flush. Until then a
    /// failed flush clamps truncation to zero, which costs WAL growth and never
    /// data. `ArrayStore::memtable` is memory-only, so overstating this deletes
    /// the `ArrayPut` records that are the sole copy of every un-flushed cell.
    pub(in crate::data::executor) array_durable_lsn: Lsn,

    /// Highest LSN the timeseries engine is known to be durable through OUTSIDE
    /// the WAL (i.e. in its on-disk L1 partitions), advanced only by a fully
    /// successful `checkpoint_timeseries_memtables`.
    ///
    /// Like the array engine, timeseries writes no checkpoint file of its own —
    /// its durable form is the partitions `flush_ts_collection` produces and
    /// `load_ts_registries` reads back — so this is not restored at boot: it
    /// starts at zero and is first advanced by this process's own successful
    /// flush. Until then a failed flush clamps truncation to zero, which costs
    /// WAL growth and never data. `columnar_memtables` is memory-only, so
    /// overstating this deletes the `TimeseriesBatch` records that are the sole
    /// copy of every un-flushed row.
    pub(in crate::data::executor) ts_durable_lsn: Lsn,

    /// Highest LSN the vector indexes are known to be durable through OUTSIDE
    /// the WAL (i.e. in `{data_dir}/vector-ckpt/`), advanced only by a fully
    /// successful `checkpoint_vector_indexes`.
    ///
    /// Restored at boot from the live generation's manifest
    /// (`load_vector_checkpoints`), and otherwise advanced by this process's
    /// own successful flush.
    ///
    /// Scoped to what the rebuild cannot reach.
    /// `rebuild_vector_indexes_from_store` re-indexes every document of a
    /// vector-indexed collection from the durable redb `sparse` store, so
    /// document-borne vectors survive without this. A `VectorOp::Insert` writes
    /// a bare vector with no document, and nothing on that path reaches
    /// `sparse` — for those the checkpoint file and the WAL record are the only
    /// two copies. Same rule as `kv_durable_lsn`: when a flush fails, this — not
    /// the watermark — is what the core may report.
    pub(in crate::data::executor) vector_durable_lsn: Lsn,

    /// Replay-stamp prefix of the newest KV checkpoint generation on disk,
    /// restored at boot from the manifest. Restart replay skips every KV
    /// record at or below it, so a committed record applied at or below it
    /// must be published again (`redo_apply::cover`). Never a truncation
    /// floor.
    pub(in crate::data::executor) kv_published_lsn: Lsn,

    /// Replay-stamp prefix of the newest columnar checkpoint generation on
    /// disk, whichever flush published it, restored at boot from the
    /// manifest. Same rule as `kv_published_lsn`.
    pub(in crate::data::executor) columnar_published_lsn: Lsn,

    /// Replay-stamp prefix of the newest vector checkpoint generation on
    /// disk, whichever flush published it, restored at boot from the
    /// manifest. Same rule as `kv_published_lsn`. Never a truncation floor:
    /// that is `vector_durable_lsn`.
    pub(in crate::data::executor) vector_published_lsn: Lsn,

    /// Replay-stamp prefix of the newest sparse-vector checkpoint generation
    /// on disk, restored at boot from the manifest. Same rule as
    /// `kv_published_lsn`.
    pub(in crate::data::executor) sparse_vector_published_lsn: Lsn,

    /// Highest LSN the CRDT engines are known to be durable through OUTSIDE the
    /// WAL (i.e. in `{data_dir}/crdt-ckpt/`), advanced only by a fully
    /// successful `checkpoint_crdt_engines`.
    ///
    /// Not restored at boot: `load_crdt_checkpoints` imports the Loro snapshots
    /// back into the engines, but a Loro snapshot carries CRDT versions, not the
    /// WAL LSN this core had reached when it was written. So this starts at zero
    /// and is first advanced by this process's own successful flush.
    ///
    /// `TenantCrdtEngine` is in-memory `LoroDoc`s with no store behind them, so
    /// these files and the WAL delta records are their only two copies. Same
    /// rule as `kv_durable_lsn`: when a flush fails, this — not the watermark —
    /// is what the core may report.
    pub(in crate::data::executor) crdt_durable_lsn: Lsn,

    /// Highest LSN the spatial R-trees are known to be durable through OUTSIDE
    /// the WAL (i.e. in `{data_dir}/spatial-ckpt/`), advanced only by a fully
    /// successful `checkpoint_spatial_indexes`. Not restored at boot — the
    /// checkpoint files carry no core LSN — so it starts at zero and is first
    /// advanced by this process's own successful flush.
    ///
    /// Scoped to what no WAL-independent rebuild reaches. Geometry on a
    /// columnar-family (`engine='spatial'`) collection is re-derived from restored
    /// columnar rows by `restore_columnar_geometry_indexes` and survives without
    /// this. Geometry on a DOCUMENT collection is rebuilt at boot by WAL redo of
    /// its document `Put`s (the same `apply_point_put_spatial` side-effect as the
    /// live write), but nothing re-derives it from the redb `sparse` store — so
    /// once the WAL is truncated below a row's `Put`, this checkpoint file and the
    /// remaining `Put` records are the only two copies of that row's geometry
    /// entry. Same rule as `kv_durable_lsn`: when a flush fails, this — not the
    /// watermark — is what the core may report.
    pub(in crate::data::executor) spatial_durable_lsn: Lsn,

    /// Per-engine replay stamps recovered from on-disk checkpoints during
    /// boot, before WAL replay. Consulted by the replay paths
    /// so records already folded into a restored checkpoint are not applied a
    /// second time. Empty outside boot, and empty means "replay everything".
    pub(in crate::data::executor) replay_floors: ReplayFloors,

    /// The node's outcome floor as this core last read it from a request, and
    /// the records this core applied above it: the replay stamp every KV,
    /// columnar, vector and sparse-vector checkpoint, array manifest and
    /// timeseries partition carries.
    pub(in crate::data::executor) applied_prefix: AppliedPrefix,
}
