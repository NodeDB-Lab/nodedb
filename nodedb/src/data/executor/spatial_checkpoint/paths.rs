// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for spatial checkpoints: directories, generation
//! directories, and the per-index checkpoint key.
//!
//! Each index is checkpointed to `{hash(key)}.ckpt` (the R-tree) and
//! `{hash(key)}.docmap` (its doc_map companion), where the key is
//! `(db, tid, collection, field)`. Both files store the key. [`spatial_ckpt_key`]
//! is the single key builder shared by the write path, the load path, and
//! reclaim, so the three can never drift.

use nodedb_types::DatabaseId;

use crate::data::executor::checkpoint_key::{CheckpointEngine, CheckpointKey};
use crate::types::TenantId;

/// Filename of the manifest that names the live generation.
pub(crate) const SPATIAL_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's spatial checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; it also means the loader needs no core-ownership filter on the
/// filename.
pub(crate) fn spatial_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir
        .join("spatial-ckpt")
        .join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-index files (R-trees and doc_maps).
pub(crate) fn spatial_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// Extension of the doc_map companion that shares its R-tree's stem.
pub(super) const SPATIAL_DOCMAP_EXTENSION: &str = "docmap";

/// The checkpoint key for one spatial index.
pub(crate) fn spatial_ckpt_key(db: u64, tid: u64, coll: &str, field: &str) -> CheckpointKey {
    CheckpointKey::new(
        CheckpointEngine::Spatial,
        db,
        tid,
        vec![coll.to_string(), field.to_string()],
    )
}

/// The `(db, tid, collection, field)` map key a stored key restores under, or
/// `None` when the key is not a spatial key.
pub(crate) fn spatial_map_key(
    key: &CheckpointKey,
) -> Option<(DatabaseId, TenantId, String, String)> {
    match (key.engine, key.parts.as_slice()) {
        (CheckpointEngine::Spatial, [coll, field]) => Some((
            DatabaseId::new(key.database_id),
            TenantId::new(key.tenant_id),
            coll.clone(),
            field.clone(),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrips_through_map_key() {
        let key = spatial_ckpt_key(7, 42, "my_places", "geo_field");
        let parsed = spatial_map_key(&key).expect("spatial key");
        assert_eq!(parsed.0, DatabaseId::new(7));
        assert_eq!(parsed.1, TenantId::new(42));
        assert_eq!(parsed.2, "my_places");
        assert_eq!(parsed.3, "geo_field");
    }

    /// Collection and field are separate key parts, so moving bytes between
    /// them names a different file.
    #[test]
    fn part_boundary_is_part_of_the_name() {
        assert_ne!(
            spatial_ckpt_key(0, 1, "a_b", "c").file_name(),
            spatial_ckpt_key(0, 1, "a", "b_c").file_name()
        );
    }

    #[test]
    fn max_length_names_fit() {
        let name = spatial_ckpt_key(0, 1, &"c".repeat(255), &"f".repeat(200)).file_name();
        assert!(name.len() < 64, "{name}");
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/spatial-ckpt/core-0");
        assert_ne!(spatial_ckpt_gen_dir(base, 0), spatial_ckpt_gen_dir(base, 1));
    }
}
