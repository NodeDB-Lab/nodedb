// SPDX-License-Identifier: BUSL-1.1

//! Spatial engine reclaim — unlink a dropped collection's R*-tree checkpoint
//! and docmap files.
//!
//! Checkpoint layout:
//! `{data_dir}/spatial-ckpt/core-{n}/gen-{m}/{hash(key)}.ckpt` plus its paired
//! `{hash(key)}.docmap`, with each core's `MANIFEST` naming its live
//! generation. Reclaim walks EVERY core's directory, because `data_dir` is
//! shared and the dropped collection may have been routed to any of them, and
//! resolves each core's live generation through the same manifest reader the
//! load path uses so the two can never disagree about which files are
//! reachable. Each `.ckpt` stores its key, and reclaim matches the stored key
//! through the SAME decoder the load path uses ([`spatial_map_key`]).
//!
//! Unlinking a file out of a published generation does not break the manifest's
//! promise. That promise is about the collections that are LIVE, and this runs
//! only once a collection is dropped: leaving the files would resurrect a
//! dropped collection's geometry on the next boot.

use std::path::Path;

use super::keyed::reclaim_keyed_matching;
use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::checkpoint_key::CheckpointKey;
use crate::data::executor::spatial_checkpoint::{
    read_spatial_manifest_at, spatial_ckpt_gen_dir, spatial_map_key,
};

/// Extension of the doc_map companion that shares each R-tree's stem.
const DOCMAP_EXTENSION: &str = "docmap";

/// Unlink every spatial checkpoint + docmap file for
/// `(database_id, tenant_id, collection)`, across every core's live
/// generation. Returns stats; idempotent.
pub fn reclaim_spatial_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let root = data_dir.join("spatial-ckpt");
    let cores = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read spatial checkpoint root",
                path: root,
                source,
            });
        }
    };

    // Every field index of the collection matches; the decoded key is
    // compared whole, so "places" never matches "places_archive".
    let matches = |key: &CheckpointKey| {
        spatial_map_key(key).is_some_and(|(db, tid, coll, _)| {
            db.as_u64() == database_id && tid.as_u64() == tenant_id && coll == collection
        })
    };

    let mut stats = ReclaimStats::default();
    for core_entry in cores {
        let core_entry = core_entry.map_err(|source| ReclaimError::Io {
            operation: "read spatial checkpoint core entry",
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
        // while predecessor files remain reachable.
        let gen_dir = match read_spatial_manifest_at(&core_dir) {
            Ok(Some(manifest)) => spatial_ckpt_gen_dir(&core_dir, manifest.generation),
            Ok(None) => continue,
            Err(error) => {
                return Err(ReclaimError::Manifest {
                    engine: "spatial",
                    path: core_dir.join("MANIFEST"),
                    detail: error.to_string(),
                });
            }
        };
        // Both the R-tree checkpoint AND its paired docmap go, or the docmap
        // is left orphaned with no checkpoint to resolve entries for.
        reclaim_keyed_matching(
            "spatial",
            &gen_dir,
            &[DOCMAP_EXTENSION],
            matches,
            &mut stats,
        )?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::checkpoint_key::{
        write_keyed_checkpoint, write_keyed_checkpoint_as,
    };
    use crate::data::executor::spatial_checkpoint::{
        spatial_ckpt_dir, spatial_ckpt_key, test_manifest_bytes,
    };
    use tempfile::TempDir;

    /// One index to publish: its key and whether its docmap is written too.
    struct Pair {
        coll: String,
        field: String,
        docmap: bool,
    }

    /// Publish a live generation under `core_id` holding `pairs` (tenant 1,
    /// database 0), through the same manifest the load path reads. Returns every
    /// file written.
    fn publish(
        data_dir: &Path,
        core_id: usize,
        generation: u64,
        pairs: &[Pair],
    ) -> Vec<std::path::PathBuf> {
        let ckpt_dir = spatial_ckpt_dir(data_dir, core_id);
        let gen_dir = spatial_ckpt_gen_dir(&ckpt_dir, generation);
        std::fs::create_dir_all(&gen_dir).expect("mkdir");
        let mut paths = Vec::new();
        for pair in pairs {
            let key = spatial_ckpt_key(0, 1, &pair.coll, &pair.field);
            paths.push(write_keyed_checkpoint(&gen_dir, &key, b"rtree").expect("write"));
            if pair.docmap {
                paths.push(
                    write_keyed_checkpoint_as(&gen_dir, &key, DOCMAP_EXTENSION, b"map")
                        .expect("write docmap"),
                );
            }
        }
        let manifest = test_manifest_bytes(generation);
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "MANIFEST", &manifest)
            .expect("manifest");
        paths
    }

    fn pair(coll: &str, field: &str, docmap: bool) -> Pair {
        Pair {
            coll: coll.to_string(),
            field: field.to_string(),
            docmap,
        }
    }

    fn gen_dir_of(data_dir: &Path, core_id: usize, generation: u64) -> std::path::PathBuf {
        spatial_ckpt_gen_dir(&spatial_ckpt_dir(data_dir, core_id), generation)
    }

    /// Both halves of a pair go, across every core's live generation — a
    /// surviving docmap is an orphan, and a surviving R-tree resurrects
    /// geometry for a collection that no longer exists.
    #[test]
    fn unlinks_both_halves_across_cores() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        let core0 = publish(base, 0, 3, &[pair("places", "geom", true)]);
        let core1 = publish(
            base,
            1,
            0,
            &[
                pair("places", "home", false),
                // Different collection: must survive.
                pair("stores", "geom", false),
            ],
        );

        let stats = reclaim_spatial_checkpoints(base, 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 3);
        assert!(!core0[0].exists());
        assert!(!core0[1].exists(), "the docmap goes with its R-tree");
        assert!(!core1[0].exists());
        assert!(core1[1].exists());
    }

    /// A collection whose name merely starts with the target's bytes must
    /// survive — the stored key is compared whole.
    #[test]
    fn does_not_unlink_a_longer_collection_name() {
        let tmp = TempDir::new().expect("tempdir");
        let paths = publish(tmp.path(), 0, 0, &[pair("places_archive", "geom", true)]);

        let stats = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(paths.iter().all(|p| p.exists()));
    }

    /// Files under a SUPERSEDED generation are already unreachable.
    #[test]
    fn ignores_superseded_generations() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 1, &[pair("places", "geom", false)]);
        let stale = gen_dir_of(tmp.path(), 0, 0);
        std::fs::create_dir_all(&stale).expect("mkdir");
        let old = write_keyed_checkpoint(&stale, &spatial_ckpt_key(0, 1, "places", "geom"), b"old")
            .expect("write");

        let stats = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 1, "only the live generation's file");
        assert!(old.exists());
    }

    /// A 255-byte collection with a 200-byte field reclaims by stored key.
    #[test]
    fn long_names_are_reclaimed() {
        let tmp = TempDir::new().expect("tempdir");
        let coll = "c".repeat(255);
        let paths = publish(tmp.path(), 0, 0, &[pair(&coll, &"f".repeat(200), true)]);
        let stats = reclaim_spatial_checkpoints(tmp.path(), 0, 1, &coll).expect("reclaim");
        assert_eq!(stats.files_unlinked, 2);
        assert!(paths.iter().all(|p| !p.exists()));
    }

    #[test]
    fn corrupt_manifest_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let core_dir = spatial_ckpt_dir(tmp.path(), 0);
        std::fs::create_dir_all(&core_dir).expect("mkdir");
        std::fs::write(core_dir.join("MANIFEST"), b"not-a-manifest").expect("manifest");

        let error = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect_err("must fail");
        assert!(error.to_string().contains("manifest"));
    }

    #[test]
    fn absent_root_is_a_noop() {
        let tmp = TempDir::new().expect("tempdir");
        let stats = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }
}
