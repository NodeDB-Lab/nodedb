// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for sparse-vector checkpoints: directories, generation
//! directories, and the per-index checkpoint key.
//!
//! The write path, the load path, and reclaim all build every key through
//! these helpers so the three can never drift. Each index is checkpointed to
//! `{hash(key)}.ckpt`, where the key is `(db, tid, collection, field)` and the
//! file stores it.

use nodedb_types::DatabaseId;

use crate::data::executor::checkpoint_key::{CheckpointEngine, CheckpointKey};
use crate::types::TenantId;

/// Filename of the manifest that names the live generation.
pub(crate) const SPARSE_VECTOR_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's sparse-vector checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; without it, cores would race-overwrite each other's files and persist
/// only a partial fragment. It also means the loader needs no core-ownership
/// filter — a core only ever sees its own indexes.
pub(crate) fn sparse_vector_ckpt_dir(
    data_dir: &std::path::Path,
    core_id: usize,
) -> std::path::PathBuf {
    data_dir
        .join("sparse-vector-ckpt")
        .join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-index files.
pub(crate) fn sparse_vector_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// The checkpoint key for one sparse-vector index.
pub(crate) fn sparse_vector_ckpt_key(db: u64, tid: u64, coll: &str, field: &str) -> CheckpointKey {
    CheckpointKey::new(
        CheckpointEngine::SparseVector,
        db,
        tid,
        vec![coll.to_string(), field.to_string()],
    )
}

/// The `(db, tid, collection, field)` map key a stored key restores under, or
/// `None` when the key is not a sparse-vector key.
pub(crate) fn sparse_vector_map_key(
    key: &CheckpointKey,
) -> Option<(DatabaseId, TenantId, String, String)> {
    match (key.engine, key.parts.as_slice()) {
        (CheckpointEngine::SparseVector, [coll, field]) => Some((
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

    /// `sparse_vector_ckpt_dir` must isolate cores sharing one `data_dir` —
    /// without the per-core subdir they race-overwrite each other's checkpoints.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = sparse_vector_ckpt_dir(base, 0);
        let d1 = sparse_vector_ckpt_dir(base, 1);
        assert_ne!(d0, d1);
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/sparse-vector-ckpt/core-0");
        assert_ne!(
            sparse_vector_ckpt_gen_dir(base, 0),
            sparse_vector_ckpt_gen_dir(base, 1)
        );
    }

    #[test]
    fn key_roundtrips_through_map_key() {
        let key = sparse_vector_ckpt_key(7, 42, "my_docs", "title_field");
        let parsed = sparse_vector_map_key(&key).expect("sparse key");
        assert_eq!(parsed.0, DatabaseId::new(7));
        assert_eq!(parsed.1, TenantId::new(42));
        assert_eq!(parsed.2, "my_docs");
        assert_eq!(parsed.3, "title_field");
    }

    #[test]
    fn max_length_names_fit() {
        let name = sparse_vector_ckpt_key(0, 1, &"c".repeat(255), &"f".repeat(200)).file_name();
        assert!(name.len() < 64, "{name}");
    }

    #[test]
    fn non_sparse_key_is_none() {
        let key = CheckpointKey::new(
            CheckpointEngine::Spatial,
            0,
            1,
            vec!["a".into(), "b".into()],
        );
        assert!(sparse_vector_map_key(&key).is_none());
    }
}
