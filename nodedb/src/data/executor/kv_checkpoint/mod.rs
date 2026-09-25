// SPDX-License-Identifier: BUSL-1.1

//! KV collection checkpoint write + load operations for `CoreLoop`.
//!
//! ## Why KV needs this at all
//!
//! `KvEngine` is a pure in-memory `HashMap<u64, KvHashTable>`. Unlike the
//! document / graph engines it has no redb store behind it, so before this
//! module the WAL held the ONLY durable copy of every KV row. The periodic
//! checkpoint reported the core watermark as "everything below here is durable"
//! and the manager truncated the WAL segments below it — deleting the sole copy
//! of KV state that had never been flushed anywhere.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/kv-ckpt/core-{core_id}/
//!     MANIFEST                                    # names the live generation
//!     gen-{n}/tenant-{tid}-coll-{hex(coll)}.ckpt  # one file per collection
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and each core only owns the collections routed to its vShards. Filenames
//! mirror the CRDT checkpoint scheme (`crdt_checkpoint/`): the collection is
//! hex-encoded so the `-coll-` separator can never collide with a name
//! containing `-`, `/` or `:`.
//!
//! ## Why a generation + manifest, and not a stamp per file
//!
//! The replay stamp a checkpoint carries is what lets replay skip records it
//! already contains, so it must be recorded on disk. Recording it *per file*
//! looks simpler but is unsound: a flush is per-collection and can partially
//! fail, leaving collection `a` stamped at LSN 900 while `b` stays at 400. Two
//! things then break.
//!
//! * `kv_transfer_item` moves a row BETWEEN two collections. If the source is
//!   covered by its floor and the destination is not, the record is
//!   unrepresentable: skipping it drops the destination's insert, applying it
//!   double-debits the source.
//! * A crash between two per-file writes leaves the same split permanently.
//!
//! Writing every collection into a fresh `gen-{n}/` and publishing the whole set
//! with ONE atomic manifest write removes the split by construction — every live
//! collection advances to a single stamp together, or none do and the previous
//! generation stays live. The manifest is the only thing that makes a generation
//! visible, so a torn or abandoned write is inert garbage rather than a
//! half-published state.
//!
//! ## Why the index registrations ride along
//!
//! A row's index CONTENT could in principle be rebuilt by replaying the row, but
//! the REGISTRATION cannot be rebuilt from anything: its only durable record is
//! the `kv_register_index` / `kv_register_sorted_index` WAL record, which this
//! checkpoint's own replay floor gates out of a WAL that truncation then
//! deletes. So each collection file carries its registrations next to its rows,
//! published by the same manifest write — a generation with rows but no
//! registrations is not a state this format can express.
//!
//! The content rides along too, rather than being re-derived from the restored
//! rows, because content is a function of the write history and not of the rows:
//! a `backfill=false` registration deliberately omits every row that predates
//! it. See `index_restore.rs`.
//!
//! ## The contract this module upholds
//!
//! The LSN `CoreLoop::checkpoint_kv_engines` returns is a promise that every KV
//! row AND every index registration at or below it is durable OUTSIDE the WAL.
//! WAL segments below the
//! reported LSN get deleted, so the promise must never be optimistic: the writer
//! returns the watermark only once the manifest naming a complete generation has
//! landed, and surfaces a typed error otherwise so the caller clamps instead of
//! truncating.

mod decoded;
mod format;
mod index_decode;
mod index_export;
mod index_format;
mod index_restore;
mod load;
mod manifest;
mod paths;
mod write;
