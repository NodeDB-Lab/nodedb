// SPDX-License-Identifier: BUSL-1.1

//! The sparse-vector checkpoint write path: export every live index into a
//! fresh generation, then publish the whole set with one atomic manifest write.

use tracing::{info, warn};

use super::format::{SPARSE_VECTOR_CKPT_FORMAT_VERSION, SparseVectorCheckpointManifest};
use super::manifest::{read_sparse_vector_manifest_at, storage_err};
use super::paths::{
    SPARSE_VECTOR_CKPT_MANIFEST, sparse_vector_checkpoint_stem, sparse_vector_ckpt_dir,
    sparse_vector_ckpt_gen_dir,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush every sparse-vector index on this core to disk and return the LSN
    /// the sparse-vector engine is now durable through.
    ///
    /// Returns `Ok(watermark)` only once a manifest naming a COMPLETE generation
    /// has landed. Any failure returns `Err` — the caller must then clamp the
    /// reported checkpoint LSN to the last LSN this engine was known durable
    /// through, so a failed flush costs WAL growth instead of data.
    ///
    /// Stamping the generation with the core watermark rests on this: it runs
    /// on the core's own thread between
    /// tasks, so every sparse-vector write the core has admitted is already
    /// folded into the in-memory indexes exported here. Where a sparse-vector
    /// write did not itself raise the watermark, the stamp merely UNDERSTATES
    /// this engine's durability, and understating is the safe direction — the
    /// record replays idempotently on top of the restored index.
    pub(in crate::data::executor) fn checkpoint_sparse_vector_indexes(&self) -> crate::Result<Lsn> {
        let durable_through = self.watermark;

        let ckpt_dir = sparse_vector_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        // Never reuse a generation number: a reader holding the old manifest
        // must keep seeing an intact old generation until the new one is
        // published, so the new files cannot be written over the live ones.
        let live = read_sparse_vector_manifest_at(&ckpt_dir, self.core_id)?;
        let generation = live.as_ref().map_or(0, |m| m.generation.wrapping_add(1));
        let gen_dir = sparse_vector_ckpt_gen_dir(&ckpt_dir, generation);
        // A directory already at this exact generation can only be debris from a
        // cycle that failed before publishing (its manifest was never written),
        // so clearing it discards nothing reachable.
        if gen_dir.exists() {
            std::fs::remove_dir_all(&gen_dir)
                .map_err(|e| storage_err(&gen_dir, "clear stale generation dir", &e))?;
        }
        std::fs::create_dir_all(&gen_dir)
            .map_err(|e| storage_err(&gen_dir, "create generation dir", &e))?;

        let written = self.write_sparse_vector_generation(&gen_dir)?;
        self.publish_sparse_vector_generation(&ckpt_dir, generation, durable_through)?;

        // The previous generation is now unreachable. Removing it reclaims disk
        // but is NOT required for correctness — the manifest alone decides what
        // is live — so a failure here is logged, never propagated: it must not
        // clamp an LSN whose data is already safely published.
        if let Some(old) = live {
            let old_dir = sparse_vector_ckpt_gen_dir(&ckpt_dir, old.generation);
            if old_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&old_dir)
            {
                warn!(
                    core = self.core_id,
                    dir = %old_dir.display(),
                    error = %e,
                    "failed to remove superseded sparse-vector checkpoint generation; it \
                     is unreachable and will be retried next cycle"
                );
            }
        }

        info!(
            core = self.core_id,
            generation,
            indexes = written,
            durable_through_lsn = durable_through.as_u64(),
            "sparse vector checkpoint published"
        );
        Ok(durable_through)
    }

    /// Write one file per live index into `gen_dir`. Returns the count.
    ///
    /// Every file is fsynced before this returns, so once the caller's manifest
    /// write lands the generation it names is already complete on stable
    /// storage.
    fn write_sparse_vector_generation(&self, gen_dir: &std::path::Path) -> crate::Result<usize> {
        let mut written = 0usize;
        for ((db, tid, coll, field), index) in &self.sparse_vector_indexes {
            // An index emptied by deletes is written, not skipped: the file
            // records that the index is durably EMPTY. Skipping it would leave
            // the previous generation's populated file as the newest thing on
            // disk under a manifest claiming a later LSN — resurrecting every
            // document the deletes removed.
            let bytes = index.checkpoint_to_bytes()?;
            let stem = sparse_vector_checkpoint_stem(db.as_u64(), tid.as_u64(), coll, field);
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &format!("{stem}.ckpt"), &bytes)
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse_vector".to_string(),
                    detail: format!(
                        "sparse-vector checkpoint write failed for database {} tenant {} \
                         collection {coll} field {field}: {e}",
                        db.as_u64(),
                        tid.as_u64()
                    ),
                })?;
            written += 1;
        }
        Ok(written)
    }

    /// Publish a written generation by atomically replacing the manifest.
    ///
    /// This single write is the commit point of the whole checkpoint: before it
    /// nothing changed; after it the entire generation is live at one LSN. It
    /// also fsyncs `ckpt_dir`, the same directory holding the `gen-{n}/` entry,
    /// so that entry cannot still be pending when the manifest naming it becomes
    /// visible.
    fn publish_sparse_vector_generation(
        &self,
        ckpt_dir: &std::path::Path,
        generation: u64,
        durable_through: Lsn,
    ) -> crate::Result<()> {
        let manifest = SparseVectorCheckpointManifest {
            format_version: SPARSE_VECTOR_CKPT_FORMAT_VERSION,
            generation,
            durable_through_lsn: durable_through.as_u64(),
        };
        let bytes =
            zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
                format: "msgpack".to_string(),
                detail: format!("sparse-vector checkpoint manifest encode failed: {e}"),
            })?;
        let path = ckpt_dir.join(SPARSE_VECTOR_CKPT_MANIFEST);
        nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, SPARSE_VECTOR_CKPT_MANIFEST, &bytes)
            .map_err(|e| storage_err(&path, "publish manifest", &e))?;
        Ok(())
    }
}
