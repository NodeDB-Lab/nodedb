// SPDX-License-Identifier: BUSL-1.1

//! Columnar engine reclaim — unlink a dropped collection's columnar
//! checkpoint file.
//!
//! Checkpoint layout:
//! `{data_dir}/columnar-ckpt/core-{n}/gen-{m}/db-{did}-tenant-{tid}-coll-{hex(coll)}.ckpt`,
//! with each core's `MANIFEST` naming its live generation. Reclaim walks EVERY
//! core's directory, because `data_dir` is shared and the dropped collection
//! can be routed to any of them. Reclaim resolves each core's live
//! generation through the same manifest reader the load path uses so the two
//! can never disagree about which files are reachable. The filename is built
//! by the SAME helper the write path uses ([`columnar_ckpt_filename`]).
//!
//! Every columnar-family collection has this file: `columnar`, `timeseries`
//! on columnar, and `spatial`, whose rows the columnar engine stores. The
//! columnar restore reinstalls the engine and rebuilds a spatial collection's
//! R-tree from the file's rows, so a file left behind resurrects a dropped
//! collection on the next boot, and the next checkpoint makes it permanent.
//!
//! Unlinking a file out of a published generation does not break the
//! manifest's promise. That promise is about the collections that are LIVE:
//! the dropped collection's records above the replay floor are tombstoned, and
//! the ones below it are no longer in the WAL.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::columnar_checkpoint::{
    columnar_ckpt_filename, columnar_ckpt_gen_dir, read_columnar_manifest_at,
};

/// Unlink the columnar checkpoint file for `(database_id, tenant_id,
/// collection)` from every core's live generation. Returns stats; idempotent.
pub fn reclaim_columnar_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let root = data_dir.join("columnar-ckpt");
    let cores = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read columnar checkpoint root",
                path: root,
                source,
            });
        }
    };

    let file_name = columnar_ckpt_filename(database_id, tenant_id, collection);
    let tmp_name = format!("{file_name}.tmp");

    let mut stats = ReclaimStats::default();
    for core_entry in cores {
        let core_entry = core_entry.map_err(|source| ReclaimError::Io {
            operation: "read columnar checkpoint core entry",
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
        let gen_dir = match read_columnar_manifest_at(&core_dir) {
            Ok(Some(manifest)) => columnar_ckpt_gen_dir(&core_dir, manifest.generation),
            Ok(None) => continue,
            Err(error) => {
                return Err(ReclaimError::Manifest {
                    engine: "columnar",
                    path: core_dir.join("MANIFEST"),
                    detail: error.to_string(),
                });
            }
        };
        for name in [&file_name, &tmp_name] {
            unlink(&gen_dir.join(name), &mut stats)?;
        }
    }
    Ok(stats)
}

/// Unlink `path`. A missing file is a no-op, not an error.
fn unlink(path: &Path, stats: &mut ReclaimStats) -> Result<()> {
    let size = match std::fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "stat columnar checkpoint",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    match std::fs::remove_file(path) {
        Ok(()) => {
            stats.files_unlinked = stats.files_unlinked.saturating_add(1);
            stats.bytes_freed = stats.bytes_freed.saturating_add(size);
            debug!(path = %path.display(), size, "columnar reclaim: unlinked");
            Ok(())
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ReclaimError::Io {
            operation: "unlink columnar checkpoint",
            path: path.to_path_buf(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::columnar_checkpoint::{columnar_ckpt_dir, test_manifest_bytes};
    use tempfile::TempDir;

    /// Publish a live generation under `core_id` holding `files`, through the
    /// same manifest the load path reads.
    fn publish(data_dir: &Path, core_id: usize, generation: u64, files: &[(&str, &[u8])]) {
        let ckpt_dir = columnar_ckpt_dir(data_dir, core_id);
        let gen_dir = columnar_ckpt_gen_dir(&ckpt_dir, generation);
        std::fs::create_dir_all(&gen_dir).expect("mkdir");
        for (name, bytes) in files {
            std::fs::write(gen_dir.join(name), bytes).expect("write");
        }
        nodedb_wal::segment::write_checkpoint_framed(
            &ckpt_dir,
            "MANIFEST",
            &test_manifest_bytes(generation),
        )
        .expect("manifest");
    }

    fn live_file(data_dir: &Path, core_id: usize, generation: u64, name: &str) -> bool {
        columnar_ckpt_gen_dir(&columnar_ckpt_dir(data_dir, core_id), generation)
            .join(name)
            .exists()
    }

    /// The dropped collection's file goes from every core's live generation;
    /// another collection's file stays.
    #[test]
    fn unlinks_the_collection_across_cores() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        let dropped = columnar_ckpt_filename(0, 1, "places");
        let kept = columnar_ckpt_filename(0, 1, "places_archive");
        publish(base, 0, 3, &[(&dropped, b"x"), (&kept, b"keep")]);
        publish(base, 1, 0, &[(&dropped, b"yy")]);

        let stats = reclaim_columnar_checkpoints(base, 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 2);
        assert_eq!(stats.bytes_freed, 3);
        assert!(!live_file(base, 0, 3, &dropped));
        assert!(!live_file(base, 1, 0, &dropped));
        assert!(
            live_file(base, 0, 3, &kept),
            "a collection whose name extends the dropped one survives"
        );
    }

    /// Another database's or tenant's same-name collection survives.
    #[test]
    fn leaves_other_scopes_alone() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        let other_db = columnar_ckpt_filename(1024, 1, "places");
        let other_tenant = columnar_ckpt_filename(0, 2, "places");
        publish(base, 0, 1, &[(&other_db, b"a"), (&other_tenant, b"b")]);

        let stats = reclaim_columnar_checkpoints(base, 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(live_file(base, 0, 1, &other_db));
        assert!(live_file(base, 0, 1, &other_tenant));
    }

    /// A missing root, or a core with no manifest, has nothing to reclaim.
    #[test]
    fn absent_root_or_manifest_is_a_no_op() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        assert_eq!(
            reclaim_columnar_checkpoints(base, 0, 1, "places")
                .expect("absent root")
                .files_unlinked,
            0
        );
        std::fs::create_dir_all(columnar_ckpt_dir(base, 0)).expect("mkdir");
        assert_eq!(
            reclaim_columnar_checkpoints(base, 0, 1, "places")
                .expect("absent manifest")
                .files_unlinked,
            0
        );
    }

    /// A corrupt manifest is an error, never a silent skip.
    #[test]
    fn corrupt_manifest_fails_closed() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        let ckpt_dir = columnar_ckpt_dir(base, 0);
        std::fs::create_dir_all(&ckpt_dir).expect("mkdir");
        std::fs::write(ckpt_dir.join("MANIFEST"), b"not a manifest").expect("write");
        let error = reclaim_columnar_checkpoints(base, 0, 1, "places")
            .expect_err("a corrupt manifest must fail the reclaim");
        assert!(matches!(
            error,
            ReclaimError::Manifest {
                engine: "columnar",
                ..
            }
        ));
    }
}
