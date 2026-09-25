// SPDX-License-Identifier: BUSL-1.1

//! The columnar checkpoint write path: export every collection into a fresh
//! generation, then publish the whole set with one atomic manifest write.

use tracing::{info, warn};

use super::format::{
    COLUMNAR_CKPT_FORMAT_VERSION, ColumnarCheckpointFile, ColumnarCheckpointManifest,
};
use super::manifest::storage_err;
use super::paths::{
    COLUMNAR_CKPT_MANIFEST, columnar_ckpt_dir, columnar_ckpt_filename, columnar_ckpt_gen_dir,
};
use crate::data::executor::applied_prefix::ReplayStamp;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush every columnar collection on this core to disk and return the LSN
    /// the columnar engine reports as its truncation floor.
    ///
    /// Returns `Ok` only once a manifest naming a COMPLETE generation has
    /// landed. Any failure returns `Err` — the caller must then clamp the
    /// reported checkpoint LSN to the last LSN columnar was known durable
    /// through, so a failed flush costs WAL growth instead of data.
    ///
    /// ## What replay skips
    ///
    /// The manifest carries the core's [`ReplayStamp`], taken on the core's own
    /// thread between tasks, so no write interleaves with the export. The
    /// stamp names every record the exported engines hold: every record at or
    /// below the outcome floor, and every record above it this core applied.
    /// A record still on its way to the core is in neither, so restart replay
    /// applies it. The tracker has no exact stamp while its applied set is
    /// dropped; the checkpoint then fails.
    ///
    /// Columnar has no slack for an applied record the stamp does not name:
    /// `ColumnarOp::Update` is delete-old-PK + insert-new-row, so a record
    /// applied before the export and replayed again after it duplicates the
    /// row. Every columnar record that mutates an engine therefore notes its
    /// LSN after applying: `execute_columnar_insert`, `execute_columnar_update`
    /// and `execute_columnar_delete` call `note_collection_write_lsn`, which
    /// records the LSN as applied.
    ///
    /// A record whose live execution affected ZERO rows notes no LSN and so
    /// replays. That is safe and stays safe: it matched nothing against the
    /// state that the export captured, so re-executing the same predicate
    /// against that same restored state matches nothing again.
    pub(in crate::data::executor) fn checkpoint_columnar_engines(&mut self) -> crate::Result<Lsn> {
        let durable_through = self.checkpoint_floor();
        let replay = self.floors.applied_prefix.stamp()?;

        let ckpt_dir = columnar_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        // Never reuse a generation number: a reader holding the old manifest
        // must keep seeing an intact old generation until the new one is
        // published, so the new files cannot be written over the live ones.
        let live = self.read_columnar_manifest(&ckpt_dir)?;
        let generation = live.as_ref().map_or(0, |m| m.generation.wrapping_add(1));
        let gen_dir = columnar_ckpt_gen_dir(&ckpt_dir, generation);
        // A directory already at this exact generation can only be debris from a
        // cycle that failed before publishing (its manifest was never written),
        // so clearing it discards nothing reachable.
        if gen_dir.exists() {
            std::fs::remove_dir_all(&gen_dir)
                .map_err(|e| storage_err(&gen_dir, "clear stale generation dir", &e))?;
        }
        std::fs::create_dir_all(&gen_dir)
            .map_err(|e| storage_err(&gen_dir, "create generation dir", &e))?;

        let written = self.write_columnar_generation(&gen_dir)?;
        let prefix = Lsn::new(replay.prefix);
        let applied_ranges = replay.applied_above.len();
        self.publish_columnar_generation(&ckpt_dir, generation, replay)?;

        // The previous generation is now unreachable. Removing it reclaims disk
        // but is NOT required for correctness — the manifest alone decides what
        // is live — so a failure here is logged, never propagated: it must not
        // clamp an LSN whose data is already safely published.
        if let Some(old) = live {
            let old_dir = columnar_ckpt_gen_dir(&ckpt_dir, old.generation);
            if old_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&old_dir)
            {
                warn!(
                    core = self.core_id,
                    dir = %old_dir.display(),
                    error = %e,
                    "failed to remove superseded columnar checkpoint generation; it is \
                     unreachable and will be retried next cycle"
                );
            }
        }

        info!(
            core = self.core_id,
            generation,
            collections = written,
            durable_through_lsn = durable_through.as_u64(),
            replay_prefix = prefix.as_u64(),
            applied_ranges,
            "columnar checkpoint published"
        );
        Ok(durable_through)
    }

    /// Write one file per live collection into `gen_dir`. Returns the count.
    ///
    /// Every file is fsynced before this returns, so once the caller's manifest
    /// write lands the generation it names is already complete on stable
    /// storage.
    fn write_columnar_generation(&self, gen_dir: &std::path::Path) -> crate::Result<usize> {
        let mut written = 0usize;
        for (key, engine) in &self.columnar_engines {
            let (db_id, tenant_id, collection) = key;

            // The segment blobs and their surrogate sidecar are handed to the
            // exporter TOGETHER, from the two maps that hold them in lockstep,
            // and land in one snapshot. Reading them here as one pair — rather
            // than exporting the engine and appending identity later — is what
            // makes an index-misaligned checkpoint unrepresentable rather than
            // merely unlikely.
            //
            // An absent entry in either map is `&[]`, not a skip: a collection
            // that has never flushed has no segments and no surrogates, and both
            // halves agree at length zero.
            let segments: &[Vec<u8>] = self
                .columnar_flushed_segments
                .get(key)
                .map_or(&[], Vec::as_slice);
            let surrogates: &[Vec<Option<nodedb_types::Surrogate>>] = self
                .columnar_flushed_surrogates
                .get(key)
                .map_or(&[], Vec::as_slice);

            let snapshot = engine.export_snapshot(segments, surrogates).map_err(|e| {
                crate::Error::Storage {
                    engine: "columnar".to_string(),
                    detail: format!(
                        "columnar checkpoint export failed for database {} tenant {} \
                         collection {collection}: {e}",
                        db_id.as_u64(),
                        tenant_id.as_u64()
                    ),
                }
            })?;

            let file = ColumnarCheckpointFile {
                format_version: COLUMNAR_CKPT_FORMAT_VERSION,
                engine: snapshot,
            };
            let bytes =
                zerompk::to_msgpack_vec(&file).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!(
                        "columnar checkpoint encode failed for database {} tenant {} \
                         collection {collection}: {e}",
                        db_id.as_u64(),
                        tenant_id.as_u64()
                    ),
                })?;

            let fname = columnar_ckpt_filename(db_id.as_u64(), tenant_id.as_u64(), collection);
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &fname, &bytes).map_err(|e| {
                crate::Error::Storage {
                    engine: "columnar".to_string(),
                    detail: format!(
                        "columnar checkpoint write failed for database {} tenant {} \
                         collection {collection}: {e}",
                        db_id.as_u64(),
                        tenant_id.as_u64()
                    ),
                }
            })?;
            written += 1;
        }
        Ok(written)
    }

    /// Publish a written generation by atomically replacing the manifest.
    ///
    /// This single write is the commit point of the whole checkpoint: before it
    /// nothing changed; after it the entire generation is live under one stamp. It
    /// also fsyncs `ckpt_dir`, the same directory holding the `gen-{n}/` entry,
    /// so that entry cannot still be pending when the manifest naming it becomes
    /// visible.
    fn publish_columnar_generation(
        &self,
        ckpt_dir: &std::path::Path,
        generation: u64,
        replay: ReplayStamp,
    ) -> crate::Result<()> {
        let manifest = ColumnarCheckpointManifest {
            format_version: COLUMNAR_CKPT_FORMAT_VERSION,
            generation,
            replay,
        };
        let bytes =
            zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
                format: "msgpack".to_string(),
                detail: format!("columnar checkpoint manifest encode failed: {e}"),
            })?;
        let path = ckpt_dir.join(COLUMNAR_CKPT_MANIFEST);
        nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, COLUMNAR_CKPT_MANIFEST, &bytes)
            .map_err(|e| storage_err(&path, "publish manifest", &e))?;
        Ok(())
    }
}
