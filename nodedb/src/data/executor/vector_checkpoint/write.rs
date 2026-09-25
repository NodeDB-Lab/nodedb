// SPDX-License-Identifier: BUSL-1.1

//! The vector checkpoint write path: export every live index into a fresh
//! generation, then publish the whole set with one atomic manifest write.

use tracing::{info, warn};

use super::manifest::{read_vector_manifest_at, storage_err};
use super::paths::{vector_ckpt_dir, vector_ckpt_gen_dir};
use super::publish::publish_vector_generation;
use crate::data::executor::checkpoint_outcome::CheckpointOutcome;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush every vector index to disk and report the LSN they are now durable
    /// through, plus the number of checkpoint files published.
    ///
    /// After checkpointing, WAL replay only needs to process entries since the
    /// checkpoint — not the entire history.
    ///
    /// ## Why this returns a `Result` and an LSN
    ///
    /// The HNSW is not fully reconstructible without the WAL.
    /// `rebuild_vector_indexes_from_store` re-indexes the redb `sparse`
    /// documents of every collection carrying a `CREATE VECTOR INDEX`, but a
    /// vector does not have to arrive as a document: `VectorOp::Insert` writes a
    /// bare `(vector, surrogate, pk_bytes)` straight into `vector_collections`,
    /// and nothing on that path puts a row in `sparse` for the rebuild to find.
    /// Those vectors exist in exactly two places — this checkpoint and the
    /// `VectorOp::Insert` WAL records — so a flush that fails while still
    /// letting the core report its floor deletes the only surviving copy.
    ///
    /// The failure is therefore all-or-nothing by construction: any index that
    /// cannot be published returns `Err`, and the caller clamps the reported
    /// checkpoint LSN to the last LSN vectors were known durable through. A
    /// partial success cannot be expressed, because the LSN it would justify
    /// does not exist.
    ///
    /// The reported LSN is the checkpoint floor (`checkpoint_floor`): the
    /// checkpoint runs on the core's own thread between tasks, so every record
    /// at or below that outcome floor that this core applied is in the export
    /// below.
    ///
    /// The manifest carries the core's replay stamp: the records every index
    /// in the generation holds. Restart replay skips exactly those.
    ///
    /// `vector_durable_lsn` stays the caller's to raise.
    pub(crate) fn checkpoint_vector_indexes(&mut self) -> crate::Result<CheckpointOutcome> {
        let durable_lsn = self.checkpoint_floor();
        let replay = self.floors.applied_prefix.stamp()?;

        let ckpt_dir = vector_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        // Never reuse a generation number: a reader holding the old manifest
        // must keep seeing an intact old generation until the new one is
        // published, so the new files cannot be written over the live ones.
        let live = read_vector_manifest_at(&ckpt_dir)?;
        let generation = live.as_ref().map_or(0, |m| m.generation.wrapping_add(1));
        let gen_dir = vector_ckpt_gen_dir(&ckpt_dir, generation);
        // A directory already at this exact generation can only be debris from a
        // cycle that failed before publishing (its manifest was never written),
        // so clearing it discards nothing reachable.
        if gen_dir.exists() {
            std::fs::remove_dir_all(&gen_dir)
                .map_err(|e| storage_err(&gen_dir, "clear stale generation dir", &e))?;
        }
        std::fs::create_dir_all(&gen_dir)
            .map_err(|e| storage_err(&gen_dir, "create generation dir", &e))?;

        let files_written = self.write_vector_generation(&gen_dir)?;
        let prefix = Lsn::new(replay.prefix);
        let applied_ranges = replay.applied_above.len();
        publish_vector_generation(&ckpt_dir, generation, replay)?;

        // The previous generation is now unreachable. Removing it reclaims disk
        // but is NOT required for correctness — the manifest alone decides what
        // is live — so a failure here is logged, never propagated: it must not
        // clamp an LSN whose data is already safely published.
        if let Some(old) = live {
            let old_dir = vector_ckpt_gen_dir(&ckpt_dir, old.generation);
            if old_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&old_dir)
            {
                warn!(
                    core = self.core_id,
                    dir = %old_dir.display(),
                    error = %e,
                    "failed to remove superseded vector checkpoint generation; it is \
                     unreachable and will be retried next cycle"
                );
            }
        }

        info!(
            core = self.core_id,
            generation,
            files_written,
            total = self.vector_collections.len(),
            durable_through_lsn = durable_lsn.as_u64(),
            replay_prefix = prefix.as_u64(),
            applied_ranges,
            "vector checkpoint published"
        );
        Ok(CheckpointOutcome {
            durable_lsn,
            files_written,
        })
    }

    /// Write one file per live index into `gen_dir`. Returns the count.
    ///
    /// Every file is fsynced before this returns, so once the caller's manifest
    /// write lands the generation it names is already complete on stable
    /// storage.
    fn write_vector_generation(&self, gen_dir: &std::path::Path) -> crate::Result<usize> {
        let mut files_written = 0usize;
        for (key, collection) in &self.vector_collections {
            // A collection emptied by deletes writes no file, and that is only
            // sound because `gen_dir` is FRESH: the previous generation is
            // retired whole by the manifest swing below, so "no file" restores
            // as "no vectors" rather than leaving the older populated file as
            // the newest thing on disk under a manifest claiming a later LSN.
            if collection.is_empty() {
                continue;
            }
            let build_key = CoreLoop::vector_build_key(key);
            let bytes = collection
                .checkpoint_to_bytes(self.segment_keks.vector_checkpoint_kek.as_ref())
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!(
                        "vector checkpoint encode failed for {build_key} ({} vectors): {e}",
                        collection.len()
                    ),
                })?;
            let filename = format!(
                "{}.ckpt",
                super::paths::vector_ckpt_stem(key.0.as_u64(), key.1.as_u64(), &key.2)
            );
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &filename, &bytes)
                .map_err(|e| storage_err(&gen_dir.join(&filename), "publish checkpoint", &e))?;
            files_written += 1;
        }
        Ok(files_written)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, Surrogate};

    use super::super::test_support::open_core_at;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;
    use crate::types::{Lsn, TenantId};

    fn collection_key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(7),
            "docs:emb".to_string(),
        )
    }

    fn collection_with_one_vector() -> VectorCollection {
        let mut coll = VectorCollection::new(4, HnswParams::default());
        coll.insert_with_surrogate(vec![0.1, 0.2, 0.3, 0.4], Surrogate::new(1));
        coll
    }

    /// The resurrection guard. A collection checkpointed at generation N and
    /// then EMPTIED by deletes must not come back at boot: the deletes are
    /// acknowledged and the checkpoint still reports its floor, so the WAL
    /// records that would have re-deleted the vectors can be gone. Each cycle
    /// therefore writes a new generation that omits the empty collection.
    #[test]
    fn emptied_collection_does_not_resurrect_the_previous_generation() {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut core = open_core_at(dir.path());
        core.vector_collections
            .insert(collection_key(), collection_with_one_vector());
        core.checkpoint_vector_indexes()
            .expect("first flush must publish");

        // Every vector is deleted, leaving the collection present but empty —
        // exactly the state a DELETE of the last row produces.
        let coll = core
            .vector_collections
            .get_mut(&collection_key())
            .expect("collection is registered");
        coll.delete_by_surrogate(Surrogate::new(1));
        assert!(coll.is_empty(), "the only vector was deleted");

        core.checkpoint_vector_indexes()
            .expect("second flush must publish");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_vector_checkpoints()
            .expect("load must succeed");
        assert!(
            restored
                .vector_collections
                .get(&collection_key())
                .is_none_or(|c| c.is_empty()),
            "an emptied collection must not restore the previous generation's vectors"
        );
    }

    /// A collection dropped from the map entirely between two cycles must not
    /// restore either: the new generation names no file for it, and the old
    /// generation stops being reachable the moment the manifest swings.
    #[test]
    fn dropped_collection_does_not_survive_the_next_cycle() {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut core = open_core_at(dir.path());
        core.vector_collections
            .insert(collection_key(), collection_with_one_vector());
        core.checkpoint_vector_indexes()
            .expect("first flush must publish");

        core.vector_collections.remove(&collection_key());
        core.checkpoint_vector_indexes()
            .expect("second flush must publish");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_vector_checkpoints()
            .expect("load must succeed");
        assert!(
            restored.vector_collections.is_empty(),
            "a collection absent from the published generation must not restore"
        );
    }

    /// The ordinary round-trip: a published generation restores every index it
    /// names, and the LSN it was stamped with comes back as this engine's
    /// last-known durable point.
    #[test]
    fn published_generation_restores_its_indexes_and_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut core = open_core_at(dir.path());
        core.vector_collections
            .insert(collection_key(), collection_with_one_vector());
        core.advance_watermark(Lsn::new(4_242));
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(4_242));
        let outcome = core
            .checkpoint_vector_indexes()
            .expect("flush must publish");
        assert_eq!(outcome.files_written, 1);
        assert_eq!(outcome.durable_lsn, Lsn::new(4_242));
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_vector_checkpoints()
            .expect("load must succeed");
        assert_eq!(
            restored
                .vector_collections
                .get(&collection_key())
                .map(|c| c.len()),
            Some(1),
            "the published index must restore under its own key"
        );
        assert_eq!(
            restored.floors.vector_durable_lsn,
            Lsn::new(4_242),
            "the restored durable LSN is what a failed flush clamps to"
        );
    }
}
