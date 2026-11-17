// SPDX-License-Identifier: BUSL-1.1

//! CRDT tenant checkpoint load operations for `CoreLoop`, and the naming every
//! path through this checkpoint shares.
//!
//! The matching write path lives in `handlers/control/checkpoint_crdt.rs`, next
//! to the checkpoint orchestration that calls it.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/crdt-ckpt/core-{core_id}/
//!     MANIFEST                  # names the live generation
//!     gen-{n}/{hash(key)}.ckpt  # one file per collection, key stored inside
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and each core only owns the CRDT fragments routed to its vShards.
//!
//! ## Why a generation + manifest
//!
//! One file is written PER COLLECTION. A flat directory lets a collection
//! dropped between two cycles leave its file as the newest thing on disk while
//! the flush still reports the core watermark — so the deletes get truncated
//! out of the WAL and the collection reloads at every subsequent boot,
//! forever. Publishing every live collection into a fresh
//! `gen-{n}/` and swinging one manifest makes the live set self-describing:
//! what the generation does not name does not exist.

mod format;
mod load;
mod manifest;
mod paths;
mod publish;
#[cfg(test)]
mod test_support;

pub(crate) use manifest::{read_crdt_manifest_at, storage_err};
pub(crate) use paths::{CRDT_CKPT_MANIFEST, crdt_ckpt_dir, crdt_ckpt_gen_dir, crdt_ckpt_key};
pub(crate) use publish::publish_crdt_generation;

#[cfg(test)]
pub(crate) use format::test_manifest_bytes;
#[cfg(test)]
pub(crate) use test_support::open_core_at;
