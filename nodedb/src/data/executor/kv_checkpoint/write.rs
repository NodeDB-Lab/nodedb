// SPDX-License-Identifier: BUSL-1.1

//! The KV checkpoint write path: export every collection into a fresh
//! generation, then publish the whole set with one atomic manifest write.

use tracing::{info, warn};

use super::format::{
    KV_CKPT_FORMAT_VERSION, KvCheckpointEntry, KvCheckpointFile, KvCheckpointManifest,
};
use super::index_export::export_collection_indexes;
use super::manifest::storage_err;
use super::paths::{KV_CKPT_MANIFEST, kv_ckpt_dir, kv_ckpt_filename, kv_ckpt_gen_dir};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush every KV collection on this core to disk and return the LSN the KV
    /// engine is now durable through.
    ///
    /// Returns `Ok(watermark)` only once a manifest naming a COMPLETE generation
    /// has landed. Any failure returns `Err` — the caller must then clamp the
    /// reported checkpoint LSN to the last LSN KV was known durable through, so
    /// a failed flush costs WAL growth instead of data.
    ///
    /// Stamping the generation with the core watermark is exact, not
    /// approximate: this runs on the core's own thread between tasks, and a KV
    /// write only reaches `note_kv_write_lsn` (which raises the watermark) after
    /// it has been applied to the table. So every KV row with `lsn <= watermark`
    /// is already in the tables exported here. Records above the watermark
    /// belong to other engines and are gated by their own floors.
    ///
    /// Index DDL is the one KV record that does NOT raise the watermark
    /// (`execute_kv_register_index` and its siblings note no write LSN, having no
    /// row to attribute one to), so a registration made after the last row write
    /// is exported into a generation stamped below its own LSN, and replays again
    /// on top of the restored state. That is harmless in both directions and must
    /// stay so: replaying a register whose registration is already restored is a
    /// no-op (`add_index` reports the field as already indexed and skips the
    /// backfill), and replaying a drop whose registration the export therefore
    /// never saw is a no-op too.
    ///
    /// Every published generation raises `kv_published_lsn`, the LSN restart
    /// restores KV from (see `redo_apply::cover`).
    pub(in crate::data::executor) fn checkpoint_kv_engines(&mut self) -> crate::Result<Lsn> {
        let durable_through = self.watermark;

        let ckpt_dir = kv_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        // Never reuse a generation number: a reader holding the old manifest
        // must keep seeing an intact old generation until the new one is
        // published, so the new files cannot be written over the live ones.
        let live = self.read_kv_manifest(&ckpt_dir)?;
        let generation = live.as_ref().map_or(0, |m| m.generation.wrapping_add(1));
        let gen_dir = kv_ckpt_gen_dir(&ckpt_dir, generation);
        // A directory already at this exact generation can only be debris from a
        // cycle that failed before publishing (its manifest was never written),
        // so clearing it discards nothing reachable.
        if gen_dir.exists() {
            std::fs::remove_dir_all(&gen_dir)
                .map_err(|e| storage_err(&gen_dir, "clear stale generation dir", &e))?;
        }
        std::fs::create_dir_all(&gen_dir)
            .map_err(|e| storage_err(&gen_dir, "create generation dir", &e))?;

        let written = self.write_kv_generation(&gen_dir)?;
        self.publish_kv_generation(&ckpt_dir, generation, durable_through)?;
        self.floors.kv_published_lsn = self.floors.kv_published_lsn.max(durable_through);

        // The previous generation is now unreachable. Removing it reclaims disk
        // but is NOT required for correctness — the manifest alone decides what
        // is live — so a failure here is logged, never propagated: it must not
        // clamp an LSN whose data is already safely published.
        if let Some(old) = live {
            let old_dir = kv_ckpt_gen_dir(&ckpt_dir, old.generation);
            if old_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&old_dir)
            {
                warn!(
                    core = self.core_id,
                    dir = %old_dir.display(),
                    error = %e,
                    "failed to remove superseded KV checkpoint generation; it is \
                     unreachable and will be retried next cycle"
                );
            }
        }

        info!(
            core = self.core_id,
            generation,
            collections = written,
            durable_through_lsn = durable_through.as_u64(),
            "KV checkpoint published"
        );
        Ok(durable_through)
    }

    /// Write one file per live collection into `gen_dir`. Returns the count.
    ///
    /// Every file is fsynced before this returns, so once the caller's manifest
    /// write lands the generation it names is already complete on stable
    /// storage.
    fn write_kv_generation(&self, gen_dir: &std::path::Path) -> crate::Result<usize> {
        let mut written = 0usize;
        for coll in self.kv_engine.live_collections() {
            // A collection with no table yet is not a skip: `CREATE INDEX`
            // before the first `INSERT` leaves one that holds registrations and
            // no rows, and the registrations are exactly what the WAL stops
            // carrying once this generation publishes.
            let entries: Vec<KvCheckpointEntry> = coll
                .table
                .map(|table| {
                    table
                        .export_entries_with_surrogates()
                        .into_iter()
                        .map(|e| KvCheckpointEntry {
                            key: e.key,
                            value: e.value,
                            expire_at_ms: e.expire_at_ms,
                            surrogate: e.surrogate.0,
                        })
                        .collect()
                })
                .unwrap_or_default();

            let file = KvCheckpointFile {
                format_version: KV_CKPT_FORMAT_VERSION,
                entries,
                indexes: export_collection_indexes(&self.kv_engine, coll.table_key),
            };
            let bytes =
                zerompk::to_msgpack_vec(&file).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!(
                        "KV checkpoint encode failed for tenant {} collection {}: {e}",
                        coll.tenant_id, coll.collection
                    ),
                })?;

            let fname = kv_ckpt_filename(coll.tenant_id, coll.collection);
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &fname, &bytes).map_err(|e| {
                crate::Error::Storage {
                    engine: "kv".to_string(),
                    detail: format!(
                        "KV checkpoint write failed for tenant {} collection {}: {e}",
                        coll.tenant_id, coll.collection
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
    /// nothing changed; after it the entire generation is live at one LSN. It
    /// also fsyncs `ckpt_dir`, the same directory holding the `gen-{n}/` entry,
    /// so that entry cannot still be pending when the manifest naming it becomes
    /// visible.
    fn publish_kv_generation(
        &self,
        ckpt_dir: &std::path::Path,
        generation: u64,
        durable_through: Lsn,
    ) -> crate::Result<()> {
        let manifest = KvCheckpointManifest {
            format_version: KV_CKPT_FORMAT_VERSION,
            generation,
            durable_through_lsn: durable_through.as_u64(),
        };
        let bytes =
            zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
                format: "msgpack".to_string(),
                detail: format!("KV checkpoint manifest encode failed: {e}"),
            })?;
        let path = ckpt_dir.join(KV_CKPT_MANIFEST);
        nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, KV_CKPT_MANIFEST, &bytes)
            .map_err(|e| storage_err(&path, "publish manifest", &e))?;
        Ok(())
    }
}
