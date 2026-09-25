// SPDX-License-Identifier: BUSL-1.1

//! Columnar collection checkpoint write + load operations for `CoreLoop`.
//!
//! ## Why columnar needs this at all
//!
//! Both halves of the columnar engine are memory-only:
//!
//! * `CoreLoop::columnar_engines` — a `nodedb_columnar::MutationEngine` per
//!   collection, holding the live memtable rows, the PK index, the delete
//!   bitmaps, and the segment-id counters.
//! * `CoreLoop::columnar_flushed_segments` — the encoded bytes of every segment
//!   the memtable has been flushed into, held in a `HashMap` and never written
//!   to disk, plus `columnar_flushed_surrogates`, the per-row cross-engine
//!   identity sidecar held in lockstep with it.
//!
//! Neither has a redb store behind it, so before this module the WAL held the
//! ONLY durable copy of every columnar row. Columnar writes advance the core
//! watermark (`columnar_write/insert.rs`), the periodic checkpoint reported that
//! watermark as "everything below here is durable", and the manager truncated
//! the WAL segments below it — deleting the sole copy of state that had never
//! been flushed anywhere.
//!
//! ## Why a checkpoint blob and not real on-disk segment files
//!
//! `columnar_flushed_segments` holding encoded segments in RAM is a genuine
//! defect, but it is a *memory* defect, and writing those segments out as real
//! `.ndbs` files does not fix the *durability* one:
//!
//! * A segment file can only express rows that have been FLUSHED. The memtable
//!   rows below the flush threshold, the PK index, and the delete bitmaps are
//!   not segments and have no segment-file representation. They are the majority
//!   of a collection's state at any moment, so their LSN could not advance and
//!   the truncation hazard would stay wide open — which is the whole bug.
//! * Segments are not immutable once written. `MutationEngine::insert`
//!   tombstones a prior row for the same PK *inside an already-flushed segment*
//!   by setting a bit in that segment's delete bitmap. A segment's visible
//!   content is therefore a function of bitmap state that must become durable
//!   together with it — so a segment-file scheme still needs an atomically
//!   published sidecar, i.e. exactly the generation + manifest below.
//!
//! So real segment files are strictly additional work, not an alternative. What
//! this module writes is what is already resident in RAM, so it adds no memory
//! cost; when an on-disk segment reader eventually replaces the in-memory map,
//! the blobs it must read are the same `SegmentWriter` output this file already
//! carries.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/columnar-ckpt/core-{core_id}/
//!     MANIFEST                                              # names the live generation
//!     gen-{n}/db-{did}-tenant-{tid}-coll-{hex(coll)}.ckpt   # one file per collection
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and each core only owns the collections routed to its vShards. Filenames
//! mirror the CRDT / KV checkpoint scheme: the collection is hex-encoded so the
//! `-coll-` separator can never collide with a name containing `-`, `/` or `:`.
//! Unlike KV, the database id is part of the name — `columnar_engines` is keyed
//! by `(DatabaseId, TenantId, String)` and the id is not recoverable from the
//! collection name.
//!
//! ## Why a generation + manifest, and not a stamp per file
//!
//! Identical to `kv_checkpoint`: the replay stamp a checkpoint carries gates
//! WAL replay, so it must be on disk, and recording it per file is unsound. A
//! flush is per-collection and can partially fail, and a crash between two
//! per-file writes leaves collection `a` stamped at LSN 900 while `b` stays at
//! 400 — permanently. Writing every collection into a fresh `gen-{n}/` and
//! publishing the whole set with ONE atomic manifest write removes the split by
//! construction: every live collection advances to a single stamp together, or
//! none do and the previous generation stays live. The manifest is the only
//! thing that makes a generation visible, so a torn or abandoned write is inert
//! garbage rather than a half-published state.
//!
//! ## The surrogate lockstep invariant
//!
//! `columnar_flushed_segments[key][i]` and `columnar_flushed_surrogates[key][i]`
//! describe the same segment: the outer index is the segment index, and
//! `segment_id == index + 1`. `scan_flushed.rs` resolves a flushed row's
//! cross-engine identity purely by that positional agreement, so a checkpoint
//! that restored one without the other — or restored them at different lengths —
//! would not fail loudly; it would silently answer cross-engine prefilters with
//! the WRONG rows.
//!
//! The invariant is upheld by making the alternative unrepresentable rather than
//! by checking for it: `MutationEngine::export_snapshot` takes the blobs and the
//! surrogate table in one call and stores them in one `ColumnarEngineSnapshot`,
//! and `from_snapshot` hands both back from one decode. There is no code path
//! that writes or reads one of the two alone.
//!
//! ## What is persisted, and what is reconstructed
//!
//! Persisted: everything `ColumnarEngineSnapshot` carries — memtable columns and
//! their per-row surrogates, PK index, flushed + memtable delete bitmaps, the
//! flushed segment blobs and their surrogate sidecar, and the segment-id
//! counters.
//!
//! Reconstructed, deliberately:
//!
//! * The **schema** is also inside the snapshot, because the engine's live
//!   schema is not always the catalog's — `seed_columnar_schemas` prepends the
//!   bitemporal columns for a `bitemporal=true` collection, and restoring the
//!   catalog shape would misalign every column index. `seed_columnar_schemas`
//!   skips collections this load has already restored, so the two never fight.
//! * The **geometry R-tree** is rebuilt from the restored rows by
//!   `geometry_restore.rs` rather than being carried here. See that file for why
//!   it cannot simply be left to the spatial checkpoint.
//!
//! ## The contract this module upholds
//!
//! The LSN `CoreLoop::checkpoint_columnar_engines` returns is a promise that
//! every columnar row at or below it is reconstructible WITHOUT the WAL. WAL
//! segments below the reported LSN get deleted, so the promise must never be
//! optimistic: the writer returns the watermark only once the manifest naming a
//! complete generation has landed, and surfaces a typed error otherwise so the
//! caller clamps instead of truncating.

mod format;
mod geometry_restore;
mod load;
mod manifest;
mod paths;
mod write;
