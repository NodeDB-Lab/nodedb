// SPDX-License-Identifier: BUSL-1.1

//! Boot stage 1 of 3: restore every engine's on-disk checkpoint.
//!
//! Runs before the catalog seed and before WAL replay. See
//! `load_boot_checkpoints` for why that order is the only sound one.

use crate::data::executor::core_loop::CoreLoop;

/// Load vector + spatial + sparse vector + CRDT + KV + sync-gate + graph-label
/// checkpoints (fast recovery).
///
/// # Ordering (load-bearing)
///
/// Every loader here MUST run BEFORE `replay_all_wal` and BEFORE
/// `seed_catalog_state`: a checkpoint restores state as of the LSN it was
/// stamped with, and replay then resumes strictly ABOVE that LSN. Loading after
/// replay would overwrite newer replayed state with the older checkpoint's rows;
/// loading after the seed would leave the seed's empty columnar engine in place
/// and silently discard the restored rows.
///
/// The relative order of the loaders WITHIN this function is load-bearing too —
/// each call below carries the comment stating what its position rests on.
pub(super) fn load_boot_checkpoints(core: &mut CoreLoop) -> crate::Result<()> {
    // The array engine needs no loader: its checkpoint IS its on-disk tile
    // segments, which `ArrayStore::open` mmaps whenever the array is opened —
    // by replay, or lazily by the first read.
    //
    // A corrupt or unreadable vector, spatial, sparse-vector, CRDT, KV,
    // columnar, sync-HWM, graph-label, or timeseries-registry checkpoint is
    // fail-stop: its `Err` propagates out of boot so the core refuses to
    // come up, rather than silently skipping the checkpoint and serving
    // truncated state (the WAL below the checkpoint LSN is already gone).
    // Every loader below is `?`-propagated.
    core.load_vector_checkpoints()?;
    core.load_spatial_checkpoints()?;
    core.load_sparse_vector_checkpoints()?;
    core.load_crdt_checkpoints()?;
    // KV has no redb store behind it, so its checkpoint is the only
    // non-WAL copy of its rows: if the WAL was truncated past the
    // checkpoint LSN, this load is the ONLY thing that brings them back.
    // It also installs the per-collection replay floors that
    // `replay_kv_wal` uses to skip records already folded in.
    core.load_kv_checkpoints()?;
    // Columnar is memory-only on both halves — the live memtables in
    // `columnar_engines` and the encoded segment bytes in
    // `columnar_flushed_segments`, which no code path writes to disk —
    // so like KV this load is the only thing that brings its rows back
    // once truncation has passed the checkpoint LSN. It installs the
    // replay floor that the columnar arms use to skip records already
    // folded in, which columnar needs more sharply than KV does:
    // `ColumnarOp::Update` is delete-old-PK + insert-new-row, so an
    // un-gated record duplicates the row rather than merely rewriting
    // it.
    //
    // Runs after `load_spatial_checkpoints` so its rebuild of the
    // geometry R-tree entries OVERLAYS the restored R-tree rather than
    // racing it (the rebuild is remove-then-insert per document, so the
    // overlay is idempotent), and before `seed_columnar_schemas`, which
    // skips collections that already have an engine — restoring
    // first is what stops the seed from replacing a restored engine with
    // an empty one.
    core.load_columnar_checkpoints()?;
    // The sync idempotency gate is rebuilt at boot ONLY from the
    // `SyncSeqAdvance` WAL records, so once truncation passes them this
    // load is the only thing that brings the high-watermarks back.
    // `replay_all_wal` merges the records above this state into it
    // max-wins rather than replacing it, so this must run first. A corrupt
    // generation is fail-stop for the same reason KV's is: there is no other
    // durable copy once the WAL below it is gone.
    core.load_sync_hwm_checkpoint()?;
    // Graph EDGES need no load here — they are committed to the redb
    // `EdgeStore` at apply time and `CoreLoop::open` has already rebuilt
    // the whole CSR from it. Node LABELS have no store at all: their
    // only other durable copy is the `GraphNodeLabelSet` /
    // `GraphNodeLabelRemove` records, so once truncation passes them
    // this load is the only thing that brings the labels back — and its
    // absence is quiet, since the labeled node and its edges still
    // return, only a label-scoped `MATCH` stops matching them.
    // `replay_graph_node_label_wal` then applies the records above this
    // state; both are absolute bit operations, so no floor gates them. A
    // corrupt generation is fail-stop for the same reason: there is no other
    // durable copy of the labels once the WAL below it is gone. A per-label
    // install fault found WHILE restoring an otherwise-valid generation is
    // NOT this case — it never claims the durable LSN, so it stays a
    // non-fatal, logged skip inside the loader itself.
    core.load_graph_label_checkpoint()?;
    // Timeseries needs no checkpoint loader either — its checkpoint IS
    // the on-disk L1 partitions its flush writes. What it does need is
    // its partition registries and replay stamps rebuilt from them, because
    // replay's per-collection skip gate reads the stamps: a
    // `TimeseriesBatch` the stamp names is already in a partition, or a
    // truncate removed it, and must not replay. A timeseries ingest is an
    // append, so nothing masks a duplicate. A committed `partition.meta` or
    // stamp that will not decode is fail-stop: it is corruption of state
    // this core is about to claim is durable. An UNCOMMITTED partition
    // directory (no `partition.meta` at all — the remains of an interrupted
    // flush) is a legitimate, silent skip.
    core.load_ts_registries()?;
    Ok(())
}
