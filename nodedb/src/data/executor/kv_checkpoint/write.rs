// SPDX-License-Identifier: BUSL-1.1

//! The KV checkpoint write path: export every collection into a fresh
//! generation, then publish the whole set with one atomic manifest write.

use tracing::{info, warn};

use super::format::{
    KV_CKPT_FORMAT_VERSION, KvCheckpointEntry, KvCheckpointFile, KvCheckpointManifest,
};
use super::history_codec::export_history;
use super::index_export::export_collection_indexes;
use super::manifest::storage_err;
use super::paths::{KV_CKPT_MANIFEST, kv_ckpt_dir, kv_ckpt_gen_dir, kv_ckpt_key};
use crate::data::executor::applied_prefix::ReplayStamp;
use crate::data::executor::checkpoint_key::write_keyed_checkpoint;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush every KV collection on this core to disk and return the LSN the KV
    /// engine reports as its truncation floor.
    ///
    /// Returns `Ok` only once a manifest naming a COMPLETE generation has
    /// landed. Any failure returns `Err` — the caller must then clamp the
    /// reported checkpoint LSN to the last LSN KV was known durable through, so
    /// a failed flush costs WAL growth instead of data.
    ///
    /// ## What replay skips
    ///
    /// The manifest carries the core's [`ReplayStamp`],
    /// taken on the core's own thread between tasks, so no write interleaves
    /// with the export. The stamp names every record the exported tables hold:
    /// every record at or below the outcome floor, and every record above it
    /// this core applied. A record still on its way to the core is in neither,
    /// so restart replay applies it. The tracker has no exact stamp while its
    /// applied set is dropped; the checkpoint then fails.
    ///
    /// Index DDL is the one KV record that notes no applied LSN
    /// (`execute_kv_register_index` and its siblings note no write LSN, having no
    /// row to attribute one to), so a registration made above the outcome floor
    /// can be exported into a generation whose stamp does not name it, and it
    /// replays again on top of the restored state. That is harmless in both
    /// directions and must stay so: replaying a register whose registration is
    /// already restored is a no-op (`add_index` reports the field as already
    /// indexed and skips the backfill), and replaying a drop whose registration
    /// the export therefore never saw is a no-op too.
    pub(in crate::data::executor) fn checkpoint_kv_engines(&mut self) -> crate::Result<Lsn> {
        let durable_through = self.checkpoint_floor();
        let replay = self.floors.applied_prefix.stamp()?;

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
        let prefix = Lsn::new(replay.prefix);
        let applied_ranges = replay.applied_above.len();
        self.publish_kv_generation(&ckpt_dir, generation, replay)?;

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
            replay_prefix = prefix.as_u64(),
            applied_ranges,
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
                            sys_from: e.sys_from,
                        })
                        .collect()
                })
                .unwrap_or_default();

            let history = coll.table.map(export_history).unwrap_or_default();

            let file = KvCheckpointFile {
                format_version: KV_CKPT_FORMAT_VERSION,
                entries,
                history,
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

            let key = kv_ckpt_key(coll.tenant_id, coll.collection);
            write_keyed_checkpoint(gen_dir, &key, &bytes).map_err(|e| crate::Error::Storage {
                engine: "kv".to_string(),
                detail: format!(
                    "KV checkpoint write failed for tenant {} collection {}: {e}",
                    coll.tenant_id, coll.collection
                ),
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
    fn publish_kv_generation(
        &self,
        ckpt_dir: &std::path::Path,
        generation: u64,
        replay: ReplayStamp,
    ) -> crate::Result<()> {
        let manifest = KvCheckpointManifest {
            format_version: KV_CKPT_FORMAT_VERSION,
            generation,
            replay,
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

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::data::executor::crdt_checkpoint::open_core_at;

    /// A 255-byte collection name checkpoints under a short file name and
    /// restores its rows under the full name.
    #[test]
    fn long_collection_name_checkpoints_and_restores() {
        let dir = tempfile::tempdir().expect("tempdir");
        let collection = "k".repeat(255);

        let mut core = open_core_at(dir.path());
        core.kv_engine
            .put_with_absolute_expiry(
                crate::engine::kv::KvPutParams {
                    database_id: 0,
                    tenant_id: 7,
                    collection: &collection,
                    key: b"alice",
                    value: b"va",
                    ttl_ms: 0,
                    now_ms: 1_000,
                    surrogate: Surrogate(11),
                },
                0,
            )
            .expect("a bound row writes");
        core.checkpoint_kv_engines()
            .expect("a long-named collection must publish");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored.load_kv_checkpoints().expect("load must succeed");
        let restored_rows: Vec<(String, usize)> = restored
            .kv_engine
            .live_collections()
            .map(|c| (c.collection.to_string(), c.table.map_or(0, |t| t.len())))
            .collect();
        assert_eq!(restored_rows, vec![(collection, 1)]);
    }
}
