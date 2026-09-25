// SPDX-License-Identifier: BUSL-1.1

//! Sparse-vector inverted-index checkpoint write + load operations for
//! `CoreLoop`.
//!
//! ## Why the sparse-vector engine needs this
//!
//! `sparse_vector_indexes` is a plain in-memory `HashMap<_, SparseInvertedIndex>`
//! with no redb store behind it, so its only durable copy is the
//! `SparseVectorPut` / `SparseVectorDelete` WAL records that rebuild it at boot.
//! A flush existed before this module, but it hung off an INDEPENDENT periodic
//! timer in the data-plane runtime, so it had no ordering relationship with the
//! checkpoint that authorises WAL truncation: the checkpoint could report the
//! core watermark and delete the segments holding the sparse-vector records
//! while that timer had not yet fired. The flush is now driven from
//! `execute_checkpoint` and reports the LSN it made durable, so truncation can
//! never outrun it.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/sparse-vector-ckpt/core-{core_id}/
//!     MANIFEST                                       # names the live generation
//!     gen-{n}/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and each core only owns the collections routed to its vShards; it also means
//! the loader needs no core-ownership filter on the filename.
//!
//! ## Why a generation + manifest
//!
//! The flush is per-index and can partially fail. The LSN this checkpoint
//! reports is a deletion authority — WAL segments below it are unlinked — so it
//! has to describe the WHOLE engine, not the indexes that happened to succeed.
//! Writing every index into a fresh `gen-{n}/` and publishing the set with ONE
//! atomic manifest write makes that statement true by construction: either every
//! live index advanced to a single LSN, or the previous generation stays live at
//! its older LSN and the caller clamps to it. A half-published state is not
//! expressible, so a torn or abandoned write is inert garbage rather than a
//! generation whose LSN overstates what is on disk.
//!
//! ## Replay floor
//!
//! The manifest carries the core's replay stamp, and a restart installs it as
//! the sparse-vector replay floor. A record the stamp names is skipped. Every
//! other record replays in LSN order on top of the restored generation,
//! including a lower-LSN record still in flight when the generation was
//! written, so the replayed indexes equal the live ones.

mod format;
mod load;
mod manifest;
mod paths;
mod write;

#[cfg(test)]
pub(crate) use format::test_manifest_bytes;
pub(crate) use manifest::read_sparse_vector_manifest_at;
pub(crate) use paths::{sparse_vector_checkpoint_prefix, sparse_vector_ckpt_gen_dir};
// Only reclaim's tests build a checkpoint dir from the outside; the write and
// load paths reach `paths` directly.
#[cfg(test)]
pub(crate) use paths::sparse_vector_ckpt_dir;
