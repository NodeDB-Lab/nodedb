// SPDX-License-Identifier: BUSL-1.1

//! Spatial R-tree checkpoint write + load operations for `CoreLoop`.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/spatial-ckpt/core-{core_id}/
//!     MANIFEST                    # names the live generation
//!     gen-{n}/{hash(key)}.ckpt    # the R-tree, key stored inside
//!     gen-{n}/{hash(key)}.docmap  # its identity half, same key
//! ```
//!
//! ## Why a generation + manifest
//!
//! Two defects share one cause here, and one mechanism closes both.
//!
//! * An R-tree and its doc_map were published as two INDEPENDENT commits. The
//!   doc_map is not optional company: without it an entry resolves to no
//!   document. A crash between the two renames left generation N+1 geometry
//!   paired with generation N identity, and nothing on the read path could tell.
//! * The directory was flat, so an index that disappeared between cycles left
//!   its previous file as the newest thing on disk while the flush still
//!   reported the core watermark — resurrecting geometry for rows that no longer
//!   exist, with the WAL records that removed them already truncated.
//!
//! Writing every live index's pair into a fresh `gen-{n}/` and publishing the
//! whole set with ONE atomic manifest write fixes both by construction: the
//! manifest is the only thing that makes a generation reachable, so a pair is
//! either wholly visible or wholly invisible, and "no file" restores as "no
//! entries" rather than as last cycle's contents.
//!
//! ## Why no replay floor
//!
//! Geometry indexing is a side effect re-run by WAL redo (`apply_point_put_spatial`)
//! and by the columnar restore (`restore_columnar_geometry_indexes`), both keyed
//! by document id, so replaying above and below the generation's stamp lands on
//! the same entries. The stamp is carried anyway, because it is what a failed
//! flush clamps WAL truncation to after a restart.

mod format;
mod load;
mod manifest;
mod paths;
#[cfg(test)]
mod test_support;
mod write;

pub(crate) use manifest::read_spatial_manifest_at;
#[cfg(test)]
pub(crate) use paths::spatial_ckpt_key;
pub(crate) use paths::{
    SPATIAL_CKPT_MANIFEST, spatial_ckpt_dir, spatial_ckpt_gen_dir, spatial_map_key,
};

#[cfg(test)]
pub(crate) use format::test_manifest_bytes;
