// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for CRDT checkpoints: directories, generation
//! directories, and the per-collection checkpoint key.
//!
//! The write path, the load path, and reclaim all build every path through
//! these helpers so the three can never drift.

use crate::data::executor::checkpoint_key::{CheckpointEngine, CheckpointKey};

/// Filename of the manifest that names the live generation.
pub(crate) const CRDT_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's CRDT checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across cores
/// and a tenant's CRDT state is fragmented across cores by collection —
/// without it, cores would race-overwrite the same file and persist only a
/// partial fragment.
pub(crate) fn crdt_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("crdt-ckpt").join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-collection files.
pub(crate) fn crdt_ckpt_gen_dir(ckpt_dir: &std::path::Path, generation: u64) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// The checkpoint key for one collection. The file name is a fixed-length
/// hash of it, and the file stores it, so no collection name length can
/// overflow the name and reclaim matches by the stored key.
pub(crate) fn crdt_ckpt_key(database_id: u64, tenant_id: u64, collection: &str) -> CheckpointKey {
    CheckpointKey::new(
        CheckpointEngine::Crdt,
        database_id,
        tenant_id,
        vec![collection.to_string()],
    )
}

/// The `(database_id, tenant_id, collection)` a stored key restores under, or
/// `None` when the key is not a CRDT key.
pub(super) fn crdt_map_key(key: &CheckpointKey) -> Option<(u64, u64, String)> {
    match (key.engine, key.parts.as_slice()) {
        (CheckpointEngine::Crdt, [collection]) => {
            Some((key.database_id, key.tenant_id, collection.clone()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrips_through_map_key() {
        for coll in ["orders", "2/orders", "a-coll-b", "weird:name", "-coll-"] {
            let (database_id, tid, parsed) =
                crdt_map_key(&crdt_ckpt_key(3, 7, coll)).expect("crdt key");
            assert_eq!(database_id, 3);
            assert_eq!(tid, 7);
            assert_eq!(parsed, coll);
        }
    }

    /// A collection whose name merely starts with the target's bytes gets a
    /// distinct file name.
    #[test]
    fn longer_collection_name_gets_a_distinct_name() {
        assert_ne!(
            crdt_ckpt_key(0, 1, "orders").file_name(),
            crdt_ckpt_key(0, 1, "orders_archive").file_name()
        );
    }

    #[test]
    fn max_length_collection_name_fits() {
        let name = crdt_ckpt_key(0, 1, &"c".repeat(255)).file_name();
        assert!(name.len() < 64, "{name}");
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/crdt-ckpt/core-0");
        assert_ne!(crdt_ckpt_gen_dir(base, 0), crdt_ckpt_gen_dir(base, 1));
    }
}
