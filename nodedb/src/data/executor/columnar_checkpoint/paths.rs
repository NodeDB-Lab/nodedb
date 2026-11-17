// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for columnar checkpoints: directories and the
//! per-collection checkpoint key.
//!
//! Both the write path (`write.rs`) and the load path (`load.rs`) build every
//! path through these helpers so the two can never drift. A path divergence
//! between writer and reader is silent, and its symptom is indistinguishable
//! from data loss.

use crate::data::executor::checkpoint_key::{CheckpointEngine, CheckpointKey};

/// Filename of the manifest that names the live generation.
pub(crate) const COLUMNAR_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's columnar checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; without it, cores would race-overwrite each other's files and persist
/// only a partial fragment. It also means the loader needs no core-ownership
/// filter — a core only ever sees its own collections.
pub(crate) fn columnar_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir
        .join("columnar-ckpt")
        .join(format!("core-{core_id}"))
}

/// Directory holding one generation's collection files.
pub(crate) fn columnar_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// The checkpoint key for one collection.
///
/// The database id is part of the key because `columnar_engines` is keyed by
/// `(DatabaseId, TenantId, String)` and the id cannot be recovered from the
/// collection name — restoring under the wrong database id would hide every
/// restored row from the queries that look for it. The file name is a
/// fixed-length hash of the key, and the file stores the key.
pub(crate) fn columnar_ckpt_key(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> CheckpointKey {
    CheckpointKey::new(
        CheckpointEngine::Columnar,
        database_id,
        tenant_id,
        vec![collection.to_string()],
    )
}

/// The `(database_id, tenant_id, collection)` a stored key restores under, or
/// `None` when the key is not a columnar key.
pub(super) fn columnar_map_key(key: &CheckpointKey) -> Option<(u64, u64, String)> {
    match (key.engine, key.parts.as_slice()) {
        (CheckpointEngine::Columnar, [collection]) => {
            Some((key.database_id, key.tenant_id, collection.clone()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `columnar_ckpt_dir` must isolate cores sharing one `data_dir` — without
    /// the per-core subdir they race-overwrite each other's checkpoints.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = columnar_ckpt_dir(base, 0);
        let d1 = columnar_ckpt_dir(base, 1);
        assert_ne!(d0, d1);
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/columnar-ckpt/core-0");
        assert_ne!(
            columnar_ckpt_gen_dir(base, 0),
            columnar_ckpt_gen_dir(base, 1)
        );
    }

    #[test]
    fn key_roundtrips_through_map_key() {
        for coll in ["events", "2/orders", "a-coll-b", "weird:name", "-coll-"] {
            let (did, tid, parsed) =
                columnar_map_key(&columnar_ckpt_key(7, 42, coll)).expect("columnar key");
            assert_eq!((did, tid), (7, 42));
            assert_eq!(parsed, coll);
        }
    }

    /// The database id is part of the key, so two databases' same-name
    /// collections get distinct files.
    #[test]
    fn database_id_distinguishes_names() {
        assert_ne!(
            columnar_ckpt_key(1, 42, "events").file_name(),
            columnar_ckpt_key(2, 42, "events").file_name()
        );
    }

    #[test]
    fn max_length_collection_name_fits() {
        let name = columnar_ckpt_key(0, 1, &"c".repeat(255)).file_name();
        assert!(name.len() < 64, "{name}");
    }
}
