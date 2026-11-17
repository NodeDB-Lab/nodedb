// SPDX-License-Identifier: BUSL-1.1

//! CRDT engine reclaim — unlink a dropped collection's Loro checkpoint files.
//!
//! Checkpoint layout:
//! `{data_dir}/crdt-ckpt/core-{n}/gen-{m}/{hash(key)}.ckpt`, with each core's
//! `MANIFEST` naming its live generation.
//!
//! The CRDT checkpoint is written PER COLLECTION, not per tenant, so a dropped
//! collection leaves a file of its own behind. Without this pass that file stays
//! in the live generation, and `load_crdt_checkpoints` re-imports the dropped
//! collection's Loro state at every subsequent boot — indefinitely, since the
//! WAL records that dropped it are truncated away long before.
//!
//! Reclaim walks EVERY core's directory, because `data_dir` is shared and a
//! tenant's CRDT state is fragmented across cores by collection. The key comes
//! from the SAME builder the write path uses ([`crdt_ckpt_key`]), and the file
//! must store exactly that key before it is unlinked.

use std::path::Path;

use super::keyed::reclaim_keyed_exact;
use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::crdt_checkpoint::{
    crdt_ckpt_gen_dir, crdt_ckpt_key, read_crdt_manifest_at,
};

/// Unlink every CRDT checkpoint file for `(database_id, tenant_id, collection)`
/// across every core's live generation. Returns stats; idempotent.
pub fn reclaim_crdt_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let root = data_dir.join("crdt-ckpt");
    let cores = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read CRDT checkpoint root",
                path: root,
                source,
            });
        }
    };

    let key = crdt_ckpt_key(database_id, tenant_id, collection);

    let mut stats = ReclaimStats::default();
    for core_entry in cores {
        let core_entry = core_entry.map_err(|source| ReclaimError::Io {
            operation: "read CRDT checkpoint core entry",
            path: root.clone(),
            source,
        })?;
        let core_dir = core_entry.path();
        if !core_dir.is_dir() {
            continue;
        }
        // No manifest means no reachable generation for this core, so there is
        // nothing a boot could restore and nothing to reclaim. A corrupt
        // manifest is fail-closed: skipping it could release a same-name CREATE
        // while the predecessor's Loro state remains reachable, and a re-created
        // collection would inherit the dropped one's rows.
        let gen_dir = match read_crdt_manifest_at(&core_dir) {
            Ok(Some(manifest)) => crdt_ckpt_gen_dir(&core_dir, manifest.generation),
            Ok(None) => continue,
            Err(error) => {
                return Err(ReclaimError::Manifest {
                    engine: "crdt",
                    path: core_dir.join("MANIFEST"),
                    detail: error.to_string(),
                });
            }
        };
        // `.ckpt.tmp` is debris from a write that never renamed; it belongs to
        // this collection just the same and nothing else will collect it.
        reclaim_keyed_exact("crdt", &gen_dir, &key, &[], &mut stats)?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::checkpoint_key::{CheckpointKey, write_keyed_checkpoint};
    use crate::data::executor::crdt_checkpoint::{crdt_ckpt_dir, test_manifest_bytes};
    use tempfile::TempDir;

    /// Publish a live generation under `core_id` holding `files`, through the
    /// same manifest the load path reads. Returns each file's path.
    fn publish(
        data_dir: &Path,
        core_id: usize,
        generation: u64,
        files: &[(CheckpointKey, &[u8])],
    ) -> Vec<std::path::PathBuf> {
        let ckpt_dir = crdt_ckpt_dir(data_dir, core_id);
        let gen_dir = crdt_ckpt_gen_dir(&ckpt_dir, generation);
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

    fn gen_dir_of(data_dir: &Path, core_id: usize, generation: u64) -> std::path::PathBuf {
        crdt_ckpt_gen_dir(&crdt_ckpt_dir(data_dir, core_id), generation)
    }

    /// The dropped collection's CRDT state could have been checkpointed by ANY
    /// core — reclaim must reach every one, or the collection reloads at boot.
    #[test]
    fn unlinks_the_collections_file_across_cores() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        let core0 = publish(
            base,
            0,
            2,
            &[
                (crdt_ckpt_key(0, 1, "orders"), b"x"),
                // Different collection: must survive.
                (crdt_ckpt_key(0, 1, "users"), b"keep"),
            ],
        );
        let core1 = publish(
            base,
            1,
            0,
            &[
                (crdt_ckpt_key(0, 1, "orders"), b"yy"),
                // Different tenant: must survive.
                (crdt_ckpt_key(0, 2, "orders"), b"keep"),
                // Different database: must survive.
                (crdt_ckpt_key(1, 1, "orders"), b"keep"),
            ],
        );
        let expected = [&core0[0], &core1[0]]
            .iter()
            .map(|p| std::fs::metadata(p).expect("metadata").len())
            .sum::<u64>();

        let stats = reclaim_crdt_checkpoints(base, 0, 1, "orders").expect("reclaim");
        assert_eq!(stats.files_unlinked, 2, "both cores' files must be reached");
        assert_eq!(stats.bytes_freed, expected);
        assert!(!core0[0].exists());
        assert!(core0[1].exists());
        assert!(!core1[0].exists());
        assert!(core1[1].exists());
        assert!(core1[2].exists());
    }

    /// A collection whose name merely starts with the target's bytes must
    /// survive: the match is on the full stored key.
    #[test]
    fn does_not_unlink_a_longer_collection_name() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(
            tmp.path(),
            0,
            0,
            &[(crdt_ckpt_key(0, 1, "orders_archive"), b"keep")],
        );

        let stats = reclaim_crdt_checkpoints(tmp.path(), 0, 1, "orders").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(paths[0].exists());
    }

    /// A file at the collection's name that stores another key is not the
    /// collection's: reclaim fails closed and leaves it.
    #[test]
    fn file_holding_another_key_is_refused() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(tmp.path(), 0, 0, &[(crdt_ckpt_key(0, 1, "users"), b"keep")]);
        let target = gen_dir_of(tmp.path(), 0, 0).join(crdt_ckpt_key(0, 1, "orders").file_name());
        std::fs::rename(&paths[0], &target).expect("rename");

        let error = reclaim_crdt_checkpoints(tmp.path(), 0, 1, "orders").expect_err("must fail");
        assert!(matches!(error, ReclaimError::Key { .. }), "{error}");
        assert!(target.exists());
    }

    /// Files under a SUPERSEDED generation are already unreachable.
    #[test]
    fn ignores_superseded_generations() {
        let tmp = TempDir::new().expect("tempdir");
        publish(
            tmp.path(),
            0,
            1,
            &[(crdt_ckpt_key(0, 1, "orders"), b"live")],
        );
        let stale = gen_dir_of(tmp.path(), 0, 0);
        std::fs::create_dir_all(&stale).expect("mkdir");
        let old =
            write_keyed_checkpoint(&stale, &crdt_ckpt_key(0, 1, "orders"), b"old").expect("write");

        let stats = reclaim_crdt_checkpoints(tmp.path(), 0, 1, "orders").expect("reclaim");
        assert_eq!(stats.files_unlinked, 1, "only the live generation's file");
        assert!(old.exists());
    }

    #[test]
    fn long_collection_name_is_reclaimed() {
        let tmp = TempDir::new().expect("tempdir");
        let coll = "c".repeat(255);
        let paths = publish(tmp.path(), 0, 0, &[(crdt_ckpt_key(0, 1, &coll), b"x")]);
        let stats = reclaim_crdt_checkpoints(tmp.path(), 0, 1, &coll).expect("reclaim");
        assert_eq!(stats.files_unlinked, 1);
        assert!(!paths[0].exists());
    }

    #[test]
    fn corrupt_manifest_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let core_dir = crdt_ckpt_dir(tmp.path(), 0);
        std::fs::create_dir_all(&core_dir).expect("mkdir");
        std::fs::write(core_dir.join("MANIFEST"), b"not-a-manifest").expect("manifest");

        let error = reclaim_crdt_checkpoints(tmp.path(), 0, 1, "orders").expect_err("must fail");
        assert!(error.to_string().contains("manifest"));
    }

    #[test]
    fn absent_root_is_a_noop() {
        let tmp = TempDir::new().expect("tempdir");
        let stats = reclaim_crdt_checkpoints(tmp.path(), 0, 1, "orders").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }
}
