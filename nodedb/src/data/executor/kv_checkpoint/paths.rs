// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for KV checkpoints: directories and the per-collection
//! checkpoint key.
//!
//! Both the write path (`write.rs`) and the load path (`load.rs`) build every
//! path through these helpers so the two can never drift. A path divergence
//! between writer and reader is silent, and its symptom is indistinguishable
//! from data loss.

use crate::data::executor::checkpoint_key::{CheckpointEngine, CheckpointKey};

/// Filename of the manifest that names the live generation.
pub(crate) const KV_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's KV checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; without it, cores would race-overwrite each other's files and persist
/// only a partial fragment. It also means the loader needs no core-ownership
/// filter — a core only ever sees its own collections.
pub(crate) fn kv_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("kv-ckpt").join(format!("core-{core_id}"))
}

/// Directory holding one generation's collection files.
pub(crate) fn kv_ckpt_gen_dir(ckpt_dir: &std::path::Path, generation: u64) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// The checkpoint key for one collection. The KV map is keyed by
/// `(tenant, collection)` with no database id, so the key's database id is
/// always 0. The file name is a fixed-length hash of the key, and the file
/// stores the key.
pub(crate) fn kv_ckpt_key(tenant_id: u64, collection: &str) -> CheckpointKey {
    CheckpointKey::new(
        CheckpointEngine::Kv,
        0,
        tenant_id,
        vec![collection.to_string()],
    )
}

/// The `(tenant_id, collection)` a stored key restores under, or `None` when
/// the key is not a KV key.
pub(crate) fn kv_map_key(key: &CheckpointKey) -> Option<(u64, String)> {
    match (key.engine, key.database_id, key.parts.as_slice()) {
        (CheckpointEngine::Kv, 0, [collection]) => Some((key.tenant_id, collection.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `kv_ckpt_dir` must isolate cores sharing one `data_dir` — without the
    /// per-core subdir they race-overwrite each other's checkpoints.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = kv_ckpt_dir(base, 0);
        let d1 = kv_ckpt_dir(base, 1);
        assert_ne!(d0, d1);
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/kv-ckpt/core-0");
        assert_ne!(kv_ckpt_gen_dir(base, 0), kv_ckpt_gen_dir(base, 1));
    }

    #[test]
    fn key_roundtrips_through_map_key() {
        for coll in ["users", "2/orders", "a-coll-b", "weird:name", "-coll-"] {
            let (tid, parsed) = kv_map_key(&kv_ckpt_key(42, coll)).expect("kv key");
            assert_eq!(tid, 42);
            assert_eq!(parsed, coll);
        }
    }

    #[test]
    fn max_length_collection_name_fits() {
        let name = kv_ckpt_key(1, &"c".repeat(255)).file_name();
        assert!(name.len() < 64, "{name}");
    }

    #[test]
    fn non_kv_key_is_none() {
        let key = CheckpointKey::new(CheckpointEngine::Crdt, 0, 1, vec!["users".into()]);
        assert!(kv_map_key(&key).is_none());
    }
}
