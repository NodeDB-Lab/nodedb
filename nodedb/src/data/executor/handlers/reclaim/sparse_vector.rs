// SPDX-License-Identifier: BUSL-1.1

//! Sparse-vector engine reclaim — unlink a dropped collection's checkpoint
//! files.
//!
//! Checkpoint layout:
//! `{data_dir}/sparse-vector-ckpt/core-{n}/gen-{m}/{hash(key)}.ckpt`, with each core's `MANIFEST` naming its live generation. Reclaim walks EVERY
//! core's directory, because `data_dir` is shared and the dropped collection may
//! have been routed to any of them, and resolves each core's live generation
//! through the same manifest reader the load path uses so the two can never
//! disagree about which files are reachable.
//!
//! Each file stores its key, and reclaim matches the stored key through the
//! SAME decoder the load path uses ([`sparse_vector_map_key`]).
//!
//! Unlinking a file out of a published generation does not break the manifest's
//! promise. That promise is about the collections that are LIVE, and this runs
//! only once a collection is dropped: the load path restoring nothing for it is
//! the correct outcome, and leaving the file would resurrect a dropped
//! collection's index on the next boot.

use std::path::Path;

use super::keyed::reclaim_keyed_matching;
use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::checkpoint_key::CheckpointKey;
use crate::data::executor::sparse_vector_checkpoint::{
    read_sparse_vector_manifest_at, sparse_vector_ckpt_gen_dir, sparse_vector_map_key,
};

/// Unlink every sparse-vector checkpoint file for
/// `(database_id, tenant_id, collection)`, across every core. Returns stats;
/// idempotent.
pub fn reclaim_sparse_vector_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let root = data_dir.join("sparse-vector-ckpt");
    let cores = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read sparse-vector checkpoint root",
                path: root,
                source,
            });
        }
    };

    // Every field index of the collection matches; the decoded key is
    // compared whole, so "docs" never matches "docs_archive".
    let matches = |key: &CheckpointKey| {
        sparse_vector_map_key(key).is_some_and(|(db, tid, coll, _)| {
            db.as_u64() == database_id && tid.as_u64() == tenant_id && coll == collection
        })
    };

    let mut stats = ReclaimStats::default();
    for core_entry in cores {
        let core_entry = core_entry.map_err(|source| ReclaimError::Io {
            operation: "read sparse-vector core entry",
            path: root.clone(),
            source,
        })?;
        let core_dir = core_entry.path();
        if !core_dir.is_dir() {
            continue;
        }
        let Some(core_id) = core_dir
            .file_name()
            .and_then(|s| s.to_str())
            .and_then(|n| n.strip_prefix("core-"))
            .and_then(|n| n.parse::<usize>().ok())
        else {
            continue;
        };
        // No manifest means no reachable generation for this core, so there is
        // nothing a boot could restore and nothing to reclaim. A corrupt
        // manifest is fail-closed: skipping it could release same-name CREATE
        // while predecessor index files remain reachable.
        let manifest = match read_sparse_vector_manifest_at(&core_dir, core_id) {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(error) => {
                return Err(ReclaimError::Manifest {
                    engine: "sparse-vector",
                    path: core_dir.join("MANIFEST"),
                    detail: error.to_string(),
                });
            }
        };
        let gen_dir = sparse_vector_ckpt_gen_dir(&core_dir, manifest.generation);
        reclaim_keyed_matching("sparse-vector", &gen_dir, &[], matches, &mut stats)?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::checkpoint_key::write_keyed_checkpoint;
    use crate::data::executor::sparse_vector_checkpoint::{
        sparse_vector_ckpt_dir, sparse_vector_ckpt_key,
    };
    use tempfile::TempDir;

    /// Publish a live generation under `core_id` holding one file per key,
    /// through the same manifest the load path reads. Returns each file's path.
    fn publish(
        data_dir: &Path,
        core_id: usize,
        generation: u64,
        keys: &[CheckpointKey],
    ) -> Vec<std::path::PathBuf> {
        let ckpt_dir = sparse_vector_ckpt_dir(data_dir, core_id);
        let gen_dir = sparse_vector_ckpt_gen_dir(&ckpt_dir, generation);
        std::fs::create_dir_all(&gen_dir).expect("mkdir");
        let paths = keys
            .iter()
            .map(|key| write_keyed_checkpoint(&gen_dir, key, b"x").expect("write"))
            .collect();
        let manifest =
            crate::data::executor::sparse_vector_checkpoint::test_manifest_bytes(generation);
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "MANIFEST", &manifest)
            .expect("manifest");
        paths
    }

    fn key(db: u64, tid: u64, coll: &str, field: &str) -> CheckpointKey {
        sparse_vector_ckpt_key(db, tid, coll, field)
    }

    /// Reclaim must reach files under EVERY core's live generation — the dropped
    /// collection could have been routed to any core, and a file left behind
    /// restores a dropped collection's index on the next boot.
    #[test]
    fn unlinks_the_collections_files_across_cores() {
        let tmp = TempDir::new().expect("tempdir");
        let core0 = publish(
            tmp.path(),
            0,
            3,
            &[
                key(0, 1, "docs", "title"),
                key(0, 1, "docs", "body"),
                // Keep: different collection.
                key(0, 1, "posts", "title"),
            ],
        );
        let core1 = publish(
            tmp.path(),
            1,
            0,
            &[
                key(0, 1, "docs", "title"),
                // Keep: different tenant.
                key(0, 2, "docs", "title"),
                // Keep: different database.
                key(1, 1, "docs", "title"),
            ],
        );

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").expect("reclaim");
        assert_eq!(
            stats.files_unlinked, 3,
            "both of core 0's files and core 1's one file must be unlinked"
        );
        assert!(!core0[0].exists() && !core0[1].exists());
        assert!(core0[2].exists());
        assert!(!core1[0].exists());
        assert!(core1[1].exists() && core1[2].exists());
    }

    /// A collection whose name merely starts with the target's bytes must
    /// survive — the stored key is compared whole.
    #[test]
    fn does_not_unlink_a_longer_collection_name() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(tmp.path(), 0, 0, &[key(0, 1, "docs_archive", "title")]);

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(paths[0].exists());
    }

    /// A 255-byte collection with a 200-byte field reclaims by stored key.
    #[test]
    fn long_names_are_reclaimed() {
        let tmp = TempDir::new().expect("tempdir");
        let coll = "c".repeat(255);
        let paths = publish(tmp.path(), 0, 0, &[key(0, 1, &coll, &"f".repeat(200))]);
        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, &coll).expect("reclaim");
        assert_eq!(stats.files_unlinked, 1);
        assert!(!paths[0].exists());
    }

    #[test]
    fn corrupt_manifest_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let core_dir = sparse_vector_ckpt_dir(tmp.path(), 0);
        std::fs::create_dir_all(&core_dir).expect("mkdir");
        std::fs::write(core_dir.join("MANIFEST"), b"not-a-manifest").expect("manifest");

        let error =
            reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").expect_err("must fail");
        assert!(error.to_string().contains("manifest"));
    }

    /// Files under a SUPERSEDED generation are already unreachable: the manifest
    /// alone decides what a boot restores, so reclaim leaves them to the write
    /// path's own cleanup rather than walking directories nothing can read.
    #[test]
    fn ignores_superseded_generations() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 1, &[key(0, 1, "docs", "title")]);
        let stale = sparse_vector_ckpt_gen_dir(&sparse_vector_ckpt_dir(tmp.path(), 0), 0);
        std::fs::create_dir_all(&stale).expect("mkdir");
        let old =
            write_keyed_checkpoint(&stale, &key(0, 1, "docs", "title"), b"old").expect("write");

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").expect("reclaim");
        assert_eq!(stats.files_unlinked, 1, "only the live generation's file");
        assert!(old.exists());
    }

    /// No checkpoint root at all is the common case (no sparse-vector index ever
    /// flushed) and must be a silent no-op, not an error.
    #[test]
    fn absent_root_is_a_noop() {
        let tmp = TempDir::new().expect("tempdir");
        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }
}
