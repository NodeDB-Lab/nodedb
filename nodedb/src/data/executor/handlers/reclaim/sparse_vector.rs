// SPDX-License-Identifier: BUSL-1.1

//! Sparse-vector engine reclaim — unlink a dropped collection's checkpoint
//! files.
//!
//! Checkpoint layout:
//! `{data_dir}/sparse-vector-ckpt/core-{n}/gen-{m}/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`,
//! with each core's `MANIFEST` naming its live generation. Reclaim walks EVERY
//! core's directory, because `data_dir` is shared and the dropped collection may
//! have been routed to any of them, and resolves each core's live generation
//! through the same manifest reader the load path uses so the two can never
//! disagree about which files are reachable.
//!
//! The filename prefix is built by the SAME encoder the write path uses
//! ([`sparse_vector_checkpoint_prefix`]) so the `starts_with` match can never
//! drift from the on-disk names.
//!
//! Unlinking a file out of a published generation does not break the manifest's
//! promise. That promise is about the collections that are LIVE, and this runs
//! only once a collection is dropped: the load path restoring nothing for it is
//! the correct outcome, and leaving the file would resurrect a dropped
//! collection's index on the next boot.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::sparse_vector_checkpoint::{
    read_sparse_vector_manifest_at, sparse_vector_checkpoint_prefix, sparse_vector_ckpt_gen_dir,
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

    // Build the prefix via the shared encoder so it always matches the
    // filenames the write path produced.
    let prefix = sparse_vector_checkpoint_prefix(database_id, tenant_id, collection);

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
        reclaim_generation(&gen_dir, &prefix, &mut stats)?;
    }
    Ok(stats)
}

/// Unlink every file in `gen_dir` whose name carries `prefix`.
fn reclaim_generation(gen_dir: &Path, prefix: &str, stats: &mut ReclaimStats) -> Result<()> {
    let entries = std::fs::read_dir(gen_dir).map_err(|source| ReclaimError::Io {
        operation: "read sparse-vector live generation",
        path: gen_dir.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| ReclaimError::Io {
            operation: "read sparse-vector generation entry",
            path: gen_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with(prefix) {
            continue;
        }
        // `.ckpt.tmp` is debris from a write that never renamed; it belongs to
        // this collection just the same and nothing else will collect it.
        if !(name.ends_with(".ckpt") || name.ends_with(".ckpt.tmp")) {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                stats.files_unlinked = stats.files_unlinked.saturating_add(1);
                stats.bytes_freed = stats.bytes_freed.saturating_add(size);
                debug!(path = %path.display(), size, "sparse-vector reclaim: unlinked");
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ReclaimError::Io {
                    operation: "unlink sparse-vector checkpoint",
                    path,
                    source,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::sparse_vector_checkpoint::sparse_vector_ckpt_dir;
    use tempfile::TempDir;

    /// Publish a live generation under `core_id` holding `files`, through the
    /// same manifest the load path reads.
    fn publish(data_dir: &Path, core_id: usize, generation: u64, files: &[&str]) {
        let ckpt_dir = sparse_vector_ckpt_dir(data_dir, core_id);
        let gen_dir = sparse_vector_ckpt_gen_dir(&ckpt_dir, generation);
        std::fs::create_dir_all(&gen_dir).expect("mkdir");
        for f in files {
            std::fs::write(gen_dir.join(f), b"x").expect("write");
        }
        let manifest =
            crate::data::executor::sparse_vector_checkpoint::test_manifest_bytes(generation);
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "MANIFEST", &manifest)
            .expect("manifest");
    }

    /// Reclaim must reach files under EVERY core's live generation — the dropped
    /// collection could have been routed to any core, and a file left behind
    /// restores a dropped collection's index on the next boot.
    #[test]
    fn unlinks_the_collections_files_across_cores() {
        let tmp = TempDir::new().expect("tempdir");
        publish(
            tmp.path(),
            0,
            3,
            &[
                "0_1_docs_title.ckpt",
                "0_1_docs_body.ckpt",
                // Keep: different collection.
                "0_1_posts_title.ckpt",
            ],
        );
        publish(
            tmp.path(),
            1,
            0,
            &[
                "0_1_docs_title.ckpt",
                // Keep: different tenant.
                "0_2_docs_title.ckpt",
                // Keep: different database.
                "1_1_docs_title.ckpt",
            ],
        );

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").unwrap();
        assert_eq!(
            stats.files_unlinked, 3,
            "both of core 0's files and core 1's one file must be unlinked"
        );

        let gen0 = sparse_vector_ckpt_gen_dir(&sparse_vector_ckpt_dir(tmp.path(), 0), 3);
        assert!(!gen0.join("0_1_docs_title.ckpt").exists());
        assert!(!gen0.join("0_1_docs_body.ckpt").exists());
        assert!(gen0.join("0_1_posts_title.ckpt").exists());

        let gen1 = sparse_vector_ckpt_gen_dir(&sparse_vector_ckpt_dir(tmp.path(), 1), 0);
        assert!(!gen1.join("0_1_docs_title.ckpt").exists());
        assert!(gen1.join("0_2_docs_title.ckpt").exists());
        assert!(gen1.join("1_1_docs_title.ckpt").exists());
    }

    /// A collection whose encoded name merely starts with the target's bytes
    /// must survive — the `_` in the prefix is a structural separator and the
    /// encoder escapes literal underscores precisely so this cannot collide.
    #[test]
    fn does_not_unlink_a_longer_collection_name() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 0, &["0_1_docs%5Farchive_title.ckpt"]);

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").unwrap();
        assert_eq!(stats.files_unlinked, 0);
        let gen_dir = sparse_vector_ckpt_gen_dir(&sparse_vector_ckpt_dir(tmp.path(), 0), 0);
        assert!(gen_dir.join("0_1_docs%5Farchive_title.ckpt").exists());
    }

    #[test]
    fn corrupt_manifest_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let core_dir = sparse_vector_ckpt_dir(tmp.path(), 0);
        std::fs::create_dir_all(&core_dir).expect("mkdir");
        std::fs::write(core_dir.join("MANIFEST"), b"not-a-manifest").expect("manifest");

        let error = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").unwrap_err();
        assert!(error.to_string().contains("manifest"));
    }

    /// Files under a SUPERSEDED generation are already unreachable: the manifest
    /// alone decides what a boot restores, so reclaim leaves them to the write
    /// path's own cleanup rather than walking directories nothing can read.
    #[test]
    fn ignores_superseded_generations() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 1, &["0_1_docs_title.ckpt"]);
        let stale = sparse_vector_ckpt_gen_dir(&sparse_vector_ckpt_dir(tmp.path(), 0), 0);
        std::fs::create_dir_all(&stale).expect("mkdir");
        std::fs::write(stale.join("0_1_docs_title.ckpt"), b"old").expect("write");

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").unwrap();
        assert_eq!(stats.files_unlinked, 1, "only the live generation's file");
        assert!(stale.join("0_1_docs_title.ckpt").exists());
    }

    /// No checkpoint root at all is the common case (no sparse-vector index ever
    /// flushed) and must be a silent no-op, not an error.
    #[test]
    fn absent_root_is_a_noop() {
        let tmp = TempDir::new().expect("tempdir");
        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs").unwrap();
        assert_eq!(stats.files_unlinked, 0);
    }
}
