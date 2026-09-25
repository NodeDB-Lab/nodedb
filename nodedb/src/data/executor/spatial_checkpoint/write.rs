// SPDX-License-Identifier: BUSL-1.1

//! The spatial checkpoint write path: export every live R-tree AND its doc_map
//! into a fresh generation, then publish the whole set with one atomic manifest
//! write.

use tracing::{info, warn};

use super::format::{SPATIAL_CKPT_FORMAT_VERSION, SpatialCheckpointManifest};
use super::manifest::{read_spatial_manifest_at, storage_err};
use super::paths::{
    SPATIAL_CKPT_MANIFEST, checkpoint_stem, spatial_ckpt_dir, spatial_ckpt_gen_dir,
};
use crate::data::executor::checkpoint_outcome::CheckpointOutcome;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush every in-memory R-tree to disk and report the LSN they are now
    /// durable through, plus the number of checkpoint files published.
    ///
    /// When `spatial_checkpoint_kek` is set, checkpoint files are written
    /// encrypted (AES-256-GCM SEGV framing) and plaintext loads are refused.
    ///
    /// ## Why this returns a `Result` and an LSN
    ///
    /// `spatial_indexes` holds entries from two different write paths, and once
    /// the WAL is truncated only one of them still has a rebuild independent of
    /// this file:
    ///
    /// - Geometry on a COLUMNAR-family collection (`engine='spatial'`) is
    ///   re-derived at boot by `restore_columnar_geometry_indexes` from the rows
    ///   the columnar checkpoint restored, so it survives the loss of this file.
    /// - Geometry on a DOCUMENT collection is indexed by `apply_point_put_spatial`,
    ///   the same side-effect on both the live write and the WAL redo path, so it
    ///   is rebuilt at boot from every document `Put` still in the WAL. But
    ///   nothing re-derives it from the redb `sparse` store where the document
    ///   itself lives on. So once the WAL is truncated below a row's `Put`, this
    ///   checkpoint is the R-tree's only surviving copy of that row's geometry
    ///   entry.
    ///
    /// Rather than rank those two halves against each other at truncation time,
    /// the flush reports honestly for both: anything that cannot be published
    /// returns `Err`, and the caller clamps the reported checkpoint LSN to the
    /// last LSN the R-trees were known durable through. Over-reporting would
    /// drop geometry entries while the rows they point at survive — a spatial
    /// predicate silently stops matching rows a full scan still returns.
    ///
    /// The reported LSN is the checkpoint floor (`checkpoint_floor`): the
    /// checkpoint runs on the core's own thread between tasks, so every record
    /// at or below that outcome floor that this core applied is in the export
    /// below.
    pub(crate) fn checkpoint_spatial_indexes(&self) -> crate::Result<CheckpointOutcome> {
        let durable_lsn = self.checkpoint_floor();

        let ckpt_dir = spatial_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        // Never reuse a generation number: a reader holding the old manifest
        // must keep seeing an intact old generation until the new one is
        // published, so the new files cannot be written over the live ones.
        let live = read_spatial_manifest_at(&ckpt_dir)?;
        let generation = live.as_ref().map_or(0, |m| m.generation.wrapping_add(1));
        let gen_dir = spatial_ckpt_gen_dir(&ckpt_dir, generation);
        // A directory already at this exact generation can only be debris from a
        // cycle that failed before publishing (its manifest was never written),
        // so clearing it discards nothing reachable.
        if gen_dir.exists() {
            std::fs::remove_dir_all(&gen_dir)
                .map_err(|e| storage_err(&gen_dir, "clear stale generation dir", &e))?;
        }
        std::fs::create_dir_all(&gen_dir)
            .map_err(|e| storage_err(&gen_dir, "create generation dir", &e))?;

        let files_written = self.write_spatial_generation(&gen_dir)?;
        self.publish_spatial_generation(&ckpt_dir, generation, durable_lsn)?;

        // The previous generation is now unreachable. Removing it reclaims disk
        // but is NOT required for correctness — the manifest alone decides what
        // is live — so a failure here is logged, never propagated.
        if let Some(old) = live {
            let old_dir = spatial_ckpt_gen_dir(&ckpt_dir, old.generation);
            if old_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&old_dir)
            {
                warn!(
                    core = self.core_id,
                    dir = %old_dir.display(),
                    error = %e,
                    "failed to remove superseded spatial checkpoint generation; it is \
                     unreachable and will be retried next cycle"
                );
            }
        }

        info!(
            core = self.core_id,
            generation,
            files_written,
            total = self.spatial_indexes.len(),
            durable_through_lsn = durable_lsn.as_u64(),
            "spatial checkpoint published"
        );
        Ok(CheckpointOutcome {
            durable_lsn,
            files_written,
        })
    }

    /// Write one R-tree file and one doc_map file per live index into `gen_dir`.
    /// Returns the count of files written.
    ///
    /// The doc_map is never optional company for the R-tree:
    /// `load_spatial_checkpoints` needs it to map an entry id back to a document
    /// id, so an R-tree published without one restores as entries that resolve
    /// to nothing. It is written even when it holds no entries, so the pair on
    /// disk is always complete and the load path can treat a missing companion
    /// as the corruption it is.
    fn write_spatial_generation(&self, gen_dir: &std::path::Path) -> crate::Result<usize> {
        let kek = self.segment_keks.spatial_checkpoint_kek.as_ref();
        let mut files_written = 0usize;
        for ((db, tid, coll, field), rtree) in &self.spatial_indexes {
            let stem = checkpoint_stem(*db, *tid, coll, field);
            let bytes = rtree
                .checkpoint_to_bytes(kek)
                .map_err(|e| storage_err(&gen_dir.join(&stem), "encode R-tree", &e))?;
            // An R-tree emptied by deletes is written, not skipped: the encoder
            // always produces a header, so the file records that the index is
            // durably EMPTY. An index that has disappeared from the map
            // altogether writes nothing, and that is sound only because
            // `gen_dir` is FRESH — the previous generation is retired whole by
            // the manifest swing below, so "no file" restores as "no entries"
            // rather than leaving last cycle's populated pair reachable under a
            // manifest claiming a later LSN.
            let ckpt_name = format!("{stem}.ckpt");
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &ckpt_name, &bytes)
                .map_err(|e| storage_err(&gen_dir.join(&ckpt_name), "publish checkpoint", &e))?;
            files_written += 1;

            let doc_entries: Vec<(u64, String)> = self
                .spatial_doc_map
                .iter()
                .filter(|((d, t, c, f, _), _)| d == db && t == tid && c == coll && f == field)
                .map(|((_, _, _, _, entry_id), doc_id)| (*entry_id, doc_id.clone()))
                .collect();
            let map_bytes =
                zerompk::to_msgpack_vec(&doc_entries).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!("spatial doc_map encode failed for {stem}: {e}"),
                })?;
            let map_name = format!("{stem}.docmap");
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &map_name, &map_bytes)
                .map_err(|e| storage_err(&gen_dir.join(&map_name), "publish doc_map", &e))?;
            files_written += 1;
        }
        Ok(files_written)
    }

    /// Publish a written generation by atomically replacing the manifest.
    ///
    /// This single write is the commit point of the whole checkpoint, and it is
    /// what makes an R-tree and its doc_map ONE publication: before it, neither
    /// half of any pair is reachable; after it, both halves of every pair are.
    /// A crash between the two file writes can no longer leave generation N+1
    /// geometry paired with generation N identity, because the manifest naming
    /// N+1 was never written.
    fn publish_spatial_generation(
        &self,
        ckpt_dir: &std::path::Path,
        generation: u64,
        durable_through: Lsn,
    ) -> crate::Result<()> {
        let manifest = SpatialCheckpointManifest {
            format_version: SPATIAL_CKPT_FORMAT_VERSION,
            generation,
            durable_through_lsn: durable_through.as_u64(),
        };
        let bytes =
            zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
                format: "msgpack".to_string(),
                detail: format!("spatial checkpoint manifest encode failed: {e}"),
            })?;
        let path = ckpt_dir.join(SPATIAL_CKPT_MANIFEST);
        nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, SPATIAL_CKPT_MANIFEST, &bytes)
            .map_err(|e| storage_err(&path, "publish manifest", &e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{BoundingBox, DatabaseId};

    use super::super::test_support::open_core_at;
    use super::*;
    use crate::engine::spatial::{RTree, RTreeEntry};
    use crate::types::TenantId;

    fn key() -> (DatabaseId, TenantId, String, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(7),
            "places".to_string(),
            "geom".to_string(),
        )
    }

    fn test_rtree_memory(db: DatabaseId, tid: TenantId) -> nodedb_mem::ScopedMemory {
        nodedb_mem::ScopedMemory::new(
            crate::data::executor::core_loop::test_governor(),
            db,
            tid,
            nodedb_mem::EngineId::Spatial,
        )
    }

    fn seed_one_entry(core: &mut CoreLoop) {
        let (db, tid, coll, field) = key();
        let mut rtree = RTree::new(test_rtree_memory(db, tid));
        rtree.insert(RTreeEntry {
            id: 1,
            bbox: BoundingBox::new(0.0, 0.0, 1.0, 1.0),
        });
        core.spatial_indexes.insert(key(), rtree);
        core.spatial_doc_map
            .insert((db, tid, coll, field, 1), "doc-1".to_string());
    }

    /// An R-tree and its doc_map are published as ONE unit. The proof that the
    /// commit point is the manifest and not either file: a generation directory
    /// holding both halves is invisible until the manifest names it, and once it
    /// does, both halves restore together.
    #[test]
    fn rtree_and_docmap_are_published_as_one_generation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        seed_one_entry(&mut core);

        let outcome = core
            .checkpoint_spatial_indexes()
            .expect("flush must publish");
        assert_eq!(
            outcome.files_written, 2,
            "the R-tree and its doc_map are both written, always as a pair"
        );

        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        let gen_dir = spatial_ckpt_gen_dir(&ckpt_dir, 0);
        let stem = checkpoint_stem(DatabaseId::DEFAULT, TenantId::new(7), "places", "geom");
        assert!(gen_dir.join(format!("{stem}.ckpt")).exists());
        assert!(
            gen_dir.join(format!("{stem}.docmap")).exists(),
            "the doc_map must live in the SAME generation as its R-tree"
        );
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect("a published generation must load");
        assert_eq!(
            restored.spatial_indexes.get(&key()).map(|r| r.len()),
            Some(1)
        );
        let (db, tid, coll, field) = key();
        assert_eq!(
            restored
                .spatial_doc_map
                .get(&(db, tid, coll, field, 1))
                .map(String::as_str),
            Some("doc-1"),
            "the identity half must restore with the geometry half"
        );
    }

    /// An index that has disappeared from the map — dropped, or evicted with
    /// its collection — must not restore the previous generation's geometry.
    /// The flush still reports its floor, so the WAL records that built
    /// those entries are already deletable; leaving the old file reachable
    /// would resurrect geometry for rows that no longer exist.
    #[test]
    fn dropped_index_does_not_resurrect_the_previous_generation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        seed_one_entry(&mut core);
        core.checkpoint_spatial_indexes()
            .expect("first flush must publish");

        core.spatial_indexes.remove(&key());
        let (db, tid, coll, field) = key();
        core.spatial_doc_map.remove(&(db, tid, coll, field, 1));
        core.checkpoint_spatial_indexes()
            .expect("second flush must publish");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect("load must succeed");
        assert!(
            restored.spatial_indexes.is_empty(),
            "an index absent from the published generation must not restore"
        );
    }

    /// A doc_map is written even when it holds no entries, so an R-tree can
    /// never be published without its companion — the state the load path
    /// treats as corruption must be unreachable from the write path.
    #[test]
    fn empty_docmap_is_still_written_beside_its_rtree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        // Geometry with no doc_map entries at all: the shape a columnar-family
        // index takes before any document row maps into it.
        let (db, tid, _, _) = key();
        let mut rtree = RTree::new(test_rtree_memory(db, tid));
        rtree.insert(RTreeEntry {
            id: 1,
            bbox: BoundingBox::new(0.0, 0.0, 1.0, 1.0),
        });
        core.spatial_indexes.insert(key(), rtree);

        core.checkpoint_spatial_indexes()
            .expect("flush must publish");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect("an R-tree with an empty doc_map must still load");
        assert_eq!(
            restored.spatial_indexes.get(&key()).map(|r| r.len()),
            Some(1)
        );
    }
}
