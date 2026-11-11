// SPDX-License-Identifier: BUSL-1.1

//! Vector engine reclaim — unlink a dropped collection's HNSW checkpoint files.
//!
//! Checkpoint layout:
//! `{data_dir}/vector-ckpt/core-{n}/gen-{m}/{hash(key)}.ckpt`, where the key
//! comes from `vector_ckpt_key` and names the bare collection or one of its
//! named-field indexes, with each core's `MANIFEST` naming its live generation.
//! Each file stores its key, and reclaim matches files by that stored key.
//! Reclaim of a whole collection walks EVERY core's directory — the caller does
//! not know which core(s) ever held the collection — and resolves each core's
//! live generation through the same manifest reader the load path uses, so the
//! two can never disagree about which files are reachable.
//!
//! Unlinking a file out of a published generation does not break the manifest's
//! promise. That promise is about the collections that are LIVE, and this runs
//! only once a collection is dropped: the load path restoring nothing for it is
//! the correct outcome, and leaving the file would resurrect a dropped
//! collection's index on the next boot.

use std::path::Path;

use super::keyed::{reclaim_keyed_exact, reclaim_keyed_matching};
use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::checkpoint_key::CheckpointKey;
use crate::data::executor::vector_checkpoint::{
    read_vector_manifest_at, vector_ckpt_dir, vector_ckpt_gen_dir, vector_ckpt_key, vector_map_key,
};

/// Unlink every vector checkpoint file for `(database_id, tenant_id, collection)`
/// across every core's live generation. Returns stats; idempotent (missing
/// files count as 0).
pub fn reclaim_vector_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let root = data_dir.join("vector-ckpt");
    let cores = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read vector checkpoint root",
                path: root,
                source,
            });
        }
    };

    // A key belongs to the collection when it names the bare collection or one
    // of its field indexes (`{collection}:{field}`). Comparing the decoded key,
    // not a name prefix, keeps "docs" from matching "docs_archive".
    let field_prefix = format!("{collection}:");
    let matches = |key: &CheckpointKey| {
        vector_map_key(key).is_some_and(|(db, tid, coll_key)| {
            db.as_u64() == database_id
                && tid.as_u64() == tenant_id
                && (coll_key == collection || coll_key.starts_with(&field_prefix))
        })
    };

    let mut stats = ReclaimStats::default();
    for core_entry in cores {
        let core_entry = core_entry.map_err(|source| ReclaimError::Io {
            operation: "read vector checkpoint core entry",
            path: root.clone(),
            source,
        })?;
        let core_dir = core_entry.path();
        if !core_dir.is_dir() {
            continue;
        }
        let Some(gen_dir) = live_generation_dir(&core_dir)? else {
            continue;
        };
        reclaim_keyed_matching("vector", &gen_dir, &[], matches, &mut stats)?;
    }
    Ok(stats)
}

/// Resolve one core's live generation directory, or `Ok(None)` when the core
/// has published nothing.
///
/// A corrupt manifest is fail-closed: skipping it could release a same-name
/// CREATE while the predecessor's index files remain reachable.
fn live_generation_dir(core_dir: &Path) -> Result<Option<std::path::PathBuf>> {
    match read_vector_manifest_at(core_dir) {
        Ok(Some(manifest)) => Ok(Some(vector_ckpt_gen_dir(core_dir, manifest.generation))),
        Ok(None) => Ok(None),
        Err(error) => Err(ReclaimError::Manifest {
            engine: "vector",
            path: core_dir.join("MANIFEST"),
            detail: error.to_string(),
        }),
    }
}

/// Unlink the checkpoint of exactly one vector index on one core — the `.ckpt`
/// whose key is `{coll}` for the default field, or `{coll}:{field}` for a named
/// one — leaving every other index of the same collection in place.
/// Idempotent.
///
/// Unlike [`reclaim_vector_checkpoints`], this does not fan out across every
/// core's subdirectory: `VectorOp::DropIndex` runs independently on each core
/// that owns a copy of the collection's in-memory state, and each such core
/// unlinks only the file it itself could have written — its own core id's live
/// generation. That keeps concurrent cores dropping the same index from
/// touching each other's subdirectories.
pub fn reclaim_vector_index_checkpoint(
    data_dir: &Path,
    core_id: usize,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
) -> Result<ReclaimStats> {
    let coll_key = if field_name.is_empty() {
        collection.to_string()
    } else {
        format!("{collection}:{field_name}")
    };
    let key = vector_ckpt_key(database_id, tenant_id, &coll_key);
    let core_dir = vector_ckpt_dir(data_dir, core_id);
    let Some(gen_dir) = live_generation_dir(&core_dir)? else {
        return Ok(ReclaimStats::default());
    };
    let mut stats = ReclaimStats::default();
    reclaim_keyed_exact("vector", &gen_dir, &key, &[], &mut stats)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::checkpoint_key::write_keyed_checkpoint;
    use crate::data::executor::vector_checkpoint::test_manifest_bytes;
    use tempfile::TempDir;

    /// Publish a live generation under `core_id` holding `files`, through the
    /// same manifest the load path reads. Returns the path of each file.
    fn publish(
        data_dir: &Path,
        core_id: usize,
        generation: u64,
        files: &[(CheckpointKey, &[u8])],
    ) -> Vec<std::path::PathBuf> {
        let ckpt_dir = vector_ckpt_dir(data_dir, core_id);
        let gen_dir = vector_ckpt_gen_dir(&ckpt_dir, generation);
        std::fs::create_dir_all(&gen_dir).expect("mkdir");
        let paths = files
            .iter()
            .map(|(key, bytes)| write_keyed_checkpoint(&gen_dir, key, bytes).expect("write"))
            .collect();
        let manifest = test_manifest_bytes(generation);
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "MANIFEST", &manifest)
            .expect("manifest");
        paths
    }

    fn key(db: u64, tid: u64, coll_key: &str) -> CheckpointKey {
        vector_ckpt_key(db, tid, coll_key)
    }

    fn gen_dir_of(data_dir: &Path, core_id: usize, generation: u64) -> std::path::PathBuf {
        vector_ckpt_gen_dir(&vector_ckpt_dir(data_dir, core_id), generation)
    }

    fn size(path: &Path) -> u64 {
        std::fs::metadata(path).expect("metadata").len()
    }

    #[test]
    fn unlinks_bare_and_named_field_ckpts() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        let paths = publish(
            base,
            0,
            0,
            &[
                (key(0, 1, "users"), b"x"),
                (key(0, 1, "users:email"), b"xy"),
                (key(0, 1, "users:name"), b"xyz"),
                // Other collection: must not touch.
                (key(0, 1, "orders"), b"keep"),
                // Different tenant: must not touch.
                (key(0, 2, "users"), b"keep2"),
                // Different database: must not touch.
                (key(1, 1, "users"), b"keepdb"),
            ],
        );
        // Staging debris of a write that never renamed.
        let staged = paths[2].with_extension("ckpt.tmp");
        std::fs::rename(&paths[2], &staged).expect("rename");
        let expected = size(&paths[0]) + size(&paths[1]) + size(&staged);

        let stats = reclaim_vector_checkpoints(base, 0, 1, "users").expect("reclaim");
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, expected);
        for kept in &paths[3..] {
            assert!(kept.exists(), "{}", kept.display());
        }
        assert!(!paths[0].exists() && !paths[1].exists() && !staged.exists());
    }

    /// The dropped collection's vectors could have been checkpointed by ANY
    /// core sharing `data_dir` — reclaim must reach every `core-N`
    /// subdirectory, or a file survives the DROP indefinitely.
    #[test]
    fn unlinks_across_every_core_subdirectory() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        let core0 = publish(base, 0, 2, &[(key(0, 1, "docs"), b"a")]);
        let core1 = publish(
            base,
            1,
            0,
            &[
                (key(0, 1, "docs:emb"), b"bb"),
                // Different core, different collection: must survive.
                (key(0, 1, "posts"), b"keep"),
            ],
        );

        let stats = reclaim_vector_checkpoints(base, 0, 1, "docs").expect("reclaim");
        assert_eq!(stats.files_unlinked, 2, "both cores' files must be reached");
        assert!(!core0[0].exists());
        assert!(!core1[0].exists());
        assert!(core1[1].exists());
    }

    /// Files under a SUPERSEDED generation are already unreachable: the manifest
    /// alone decides what a boot restores, so reclaim leaves them to the write
    /// path's own cleanup rather than walking directories nothing can read.
    #[test]
    fn ignores_superseded_generations() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 1, &[(key(0, 1, "docs"), b"live")]);
        let stale = gen_dir_of(tmp.path(), 0, 0);
        std::fs::create_dir_all(&stale).expect("mkdir");
        let old = write_keyed_checkpoint(&stale, &key(0, 1, "docs"), b"old").expect("write");

        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "docs").expect("reclaim");
        assert_eq!(stats.files_unlinked, 1, "only the live generation's file");
        assert!(old.exists());
    }

    #[test]
    fn corrupt_manifest_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let core_dir = vector_ckpt_dir(tmp.path(), 0);
        std::fs::create_dir_all(&core_dir).expect("mkdir");
        std::fs::write(core_dir.join("MANIFEST"), b"not-a-manifest").expect("manifest");

        let error = reclaim_vector_checkpoints(tmp.path(), 0, 1, "docs").expect_err("must fail");
        assert!(error.to_string().contains("manifest"));
    }

    /// A published file whose key cannot be read has an unknown owner, so the
    /// pass fails rather than guessing.
    #[test]
    fn unreadable_key_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 0, &[]);
        let garbage = gen_dir_of(tmp.path(), 0, 0).join(key(0, 1, "users").file_name());
        std::fs::write(&garbage, b"garbage").expect("write");

        let error = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").expect_err("must fail");
        assert!(matches!(error, ReclaimError::Key { .. }), "{error}");
        assert!(garbage.exists());
    }

    #[test]
    fn unlink_failure_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(tmp.path(), 0, 0, &[(key(0, 1, "users"), b"x")]);
        // A directory where the staging file is expected: `remove_file` fails.
        let invalid_target = paths[0].with_extension("ckpt.tmp");
        std::fs::create_dir_all(&invalid_target).expect("mkdir");

        let error = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").expect_err("must fail");
        assert!(error.to_string().contains("unlink checkpoint"), "{error}");
        assert!(invalid_target.exists());
    }

    #[test]
    fn empty_dir_is_noop() {
        let tmp = TempDir::new().expect("tempdir");
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "x").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }

    #[test]
    fn index_scoped_reclaim_spares_sibling_indexes() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(
            tmp.path(),
            0,
            0,
            &[
                (key(0, 1, "docs"), b"default"),
                (key(0, 1, "docs:text_emb"), b"text"),
                (key(0, 1, "docs:image_emb"), b"image"),
            ],
        );

        let stats = reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "text_emb")
            .expect("reclaim");
        assert_eq!(stats.files_unlinked, 1);
        assert!(!paths[1].exists());
        assert!(paths[2].exists());
        assert!(paths[0].exists());

        // The default (unnamed) field targets the bare key only.
        reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "").expect("reclaim");
        assert!(!paths[0].exists());
        assert!(paths[2].exists());
    }

    #[test]
    fn index_scoped_reclaim_is_idempotent() {
        let tmp = TempDir::new().expect("tempdir");
        let stats =
            reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "emb").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }

    /// `reclaim_vector_index_checkpoint` only touches the core it is told
    /// about — a file checkpointed by a different core must survive, proving
    /// the single-index drop path does not race a sibling core's own state.
    #[test]
    fn index_scoped_reclaim_does_not_touch_other_cores() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(tmp.path(), 1, 0, &[(key(0, 1, "docs"), b"x")]);

        let stats =
            reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(paths[0].exists());
    }

    #[test]
    fn no_false_prefix_match() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(tmp.path(), 0, 0, &[(key(0, 1, "users_archive"), b"keep")]);
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(paths[0].exists());
    }

    /// A 255-byte collection with a 200-byte field index checkpoints under a
    /// short name, and reclaim of the collection finds it by its stored key.
    #[test]
    fn long_named_collection_and_field_are_reclaimed() {
        let tmp = TempDir::new().expect("tempdir");
        let coll = "c".repeat(255);
        let field_key = format!("{coll}:{}", "f".repeat(200));
        let paths = publish(
            tmp.path(),
            0,
            0,
            &[
                (key(0, 1, &coll), b"bare"),
                (key(0, 1, &field_key), b"field"),
            ],
        );
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, &coll).expect("reclaim");
        assert_eq!(stats.files_unlinked, 2);
        assert!(!paths[0].exists() && !paths[1].exists());
    }
}
