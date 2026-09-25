// SPDX-License-Identifier: BUSL-1.1

//! Vector (HNSW) index checkpoint write + load operations for `CoreLoop`.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/vector-ckpt/core-{core_id}/
//!     MANIFEST                        # names the live generation
//!     gen-{n}/{db}:{tid}:{coll}.ckpt  # one file per live index
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and each core only owns the collections routed to its vShards; it also means
//! the loader needs no core-ownership filter on the filename.
//!
//! ## Why a generation + manifest
//!
//! An index emptied by deletes has nothing to write, and a dropped one is not in
//! the map at all. Under a FLAT directory both cases left the previous cycle's
//! populated file as the newest thing on disk while the flush still reported the
//! core watermark — authorising truncation of the very delete records that would
//! have re-emptied it. The result was resurrection: acknowledged deletes undone
//! by a restart, with nothing left in the WAL to redo them.
//!
//! Writing every live index into a fresh `gen-{n}/` and publishing the set with
//! ONE atomic manifest write removes that by construction. The manifest is the
//! only thing that makes a generation reachable, so "no file for this index"
//! restores as "no vectors" instead of as last cycle's contents, and a torn or
//! abandoned write is inert garbage rather than a half-published state.
//!
//! ## Replay floor
//!
//! The manifest carries the core's replay stamp: every record the generation
//! holds. A restart installs it as the vector replay floor. An HNSW insert
//! appends a node and never dedups, so a record the stamp names must not
//! replay. Every other record replays in LSN order, including a lower-LSN
//! record still in flight when the generation was written. The manifest also
//! carries `durable_through_lsn`, the point a failed flush clamps WAL
//! truncation to after a restart.

mod build_completions;
mod format;
mod load;
mod manifest;
mod paths;
mod publish;
#[cfg(test)]
mod test_support;
mod write;

pub(crate) use manifest::read_vector_manifest_at;
pub(crate) use paths::{vector_ckpt_dir, vector_ckpt_gen_dir, vector_ckpt_stem};
pub(crate) use publish::{next_generation, publish_vector_generation};

#[cfg(test)]
pub(crate) use format::test_manifest_bytes;
