// SPDX-License-Identifier: BUSL-1.1

//! The vector checkpoint load path: decode the published generation whole,
//! install its indexes, and restore the LSN they are durable through.

use std::collections::HashMap;

use nodedb_types::DatabaseId;
use tracing::info;

use super::manifest::{read_vector_manifest_at, storage_err};
use super::paths::{parse_vector_ckpt_stem, vector_ckpt_dir, vector_ckpt_gen_dir};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::vector::collection::VectorCollection;
use crate::types::{Lsn, TenantId};

/// A decoded generation: every index it holds, keyed exactly as
/// `CoreLoop::vector_collections` keys them.
type DecodedVectorGeneration = HashMap<(DatabaseId, TenantId, String), VectorCollection>;

impl CoreLoop {
    /// Load HNSW checkpoints from disk on startup, before WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/vector-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed — a core only ever sees its own indexes. Only
    /// the generation the manifest names is read: files under a superseded
    /// generation are unreachable by construction, which is what stops an index
    /// emptied or dropped since the last cycle from coming back.
    ///
    /// WAL replay then only needs to process entries after the checkpoint LSN.
    ///
    /// # Fail-stop on corruption
    ///
    /// A vector checkpoint is the only non-WAL home of `VectorOp::Insert`
    /// vectors once the WAL below its LSN has been truncated, so a checkpoint
    /// that exists but cannot be read or decoded is unrecoverable data loss.
    /// This returns `Err` in that case instead of skipping the file, and the
    /// boot sequence (`load_boot_checkpoints`) refuses to bring the core up. An
    /// absent checkpoint directory — or one with no manifest — is not an error:
    /// it just means nothing has been published yet, and WAL replay
    /// reconstructs everything.
    pub fn load_vector_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = vector_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = read_vector_manifest_at(&ckpt_dir)? else {
            return Ok(());
        };
        let gen_dir = vector_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Decode the WHOLE generation before installing any of it. The manifest
        // promises a complete set at one LSN; installing a subset and then
        // claiming that LSN as this engine's durable point would authorise
        // truncating the WAL records of the indexes that failed to decode.
        let decoded = self.decode_vector_generation(&gen_dir)?;

        let loaded = decoded.len();
        let mut vectors = 0usize;
        for (key, collection) in decoded {
            vectors += collection.len();
            self.vector_collections.insert(key, collection);
        }

        // Claimed only once every index is in: this LSN is what a failed flush
        // clamps truncation to, so claiming it over a half-restored generation
        // would authorise deleting the records that would have completed it.
        self.floors.vector_durable_lsn = Lsn::new(manifest.durable_through_lsn);
        self.floors.vector_published_lsn = Lsn::new(manifest.replay.prefix);
        let replay_prefix = manifest.replay.prefix;
        let applied_ranges = manifest.replay.applied_above.len();
        self.floors.replay_floors.vector.set(manifest.replay);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            loaded,
            vectors,
            durable_through_lsn = manifest.durable_through_lsn,
            replay_prefix,
            applied_ranges,
            "vector checkpoint restored"
        );
        Ok(())
    }

    /// Read and decode every index file in a generation.
    ///
    /// `Err` if any file is unreadable, unparseable, or undecodable — the caller
    /// then restores nothing.
    fn decode_vector_generation(
        &self,
        gen_dir: &std::path::Path,
    ) -> crate::Result<DecodedVectorGeneration> {
        let entries = std::fs::read_dir(gen_dir)
            .map_err(|e| storage_err(gen_dir, "read live generation dir", &e))?;

        let mut decoded = DecodedVectorGeneration::new();
        for entry in entries {
            let entry = entry.map_err(|source| CheckpointDecodeError::DirEntry { source })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // Checkpoint filenames come from `vector_ckpt_stem`. This directory
            // is engine-private, so a `.ckpt` whose stem does not parse is a
            // corrupted real checkpoint, not a foreign file to skip.
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let key = parse_vector_ckpt_stem(stem).ok_or_else(|| {
                CheckpointDecodeError::UnparseableFilename {
                    stem: stem.to_string(),
                }
            })?;

            // A framing/CRC fault or a decode fault below is fail-stop, not a
            // skip: the file is present, so its bytes are the only surviving
            // copy of these vectors once the WAL below the generation's LSN has
            // been truncated.
            let bytes = nodedb_wal::segment::read_checkpoint_framed(&path)?;
            let kek = self.segment_keks.vector_checkpoint_kek.as_ref();
            let memory = nodedb_mem::ScopedMemory::new(
                self.governor.clone(),
                key.0,
                key.1,
                nodedb_mem::EngineId::Vector,
            );
            let collection = VectorCollection::from_checkpoint(&bytes, kek, memory)?;
            decoded.insert(key, collection);
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::{VECTOR_CKPT_FORMAT_VERSION, VectorCheckpointManifest};
    use super::super::paths::VECTOR_CKPT_MANIFEST;
    use super::super::test_support::open_core_at;
    use super::*;

    /// An absent checkpoint directory is not corruption — a fresh data
    /// directory must load cleanly with nothing restored.
    #[test]
    fn absent_dir_is_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        core.load_vector_checkpoints()
            .expect("an absent checkpoint dir must not be treated as corruption");
        assert!(core.vector_collections.is_empty());
    }

    /// A manifest that exists but is corrupt must fail the load, not be treated
    /// as absent: the WAL below the generation it names may already be gone, so
    /// silently restoring nothing is permanent, unannounced data loss.
    #[test]
    fn corrupt_manifest_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = vector_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        std::fs::write(
            ckpt_dir.join(VECTOR_CKPT_MANIFEST),
            b"not a valid checkpoint frame",
        )
        .expect("write garbage manifest");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_vector_checkpoints()
            .expect_err("a corrupt manifest must fail the load, not silently skip it");
    }

    /// A manifest from a future format must be refused rather than misparsed.
    #[test]
    fn unknown_manifest_version_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = vector_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        let bytes = zerompk::to_msgpack_vec(&VectorCheckpointManifest {
            format_version: VECTOR_CKPT_FORMAT_VERSION + 1,
            generation: 0,
            durable_through_lsn: 5,
            replay: crate::types::replay_stamp::ReplayStamp::default(),
        })
        .expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, VECTOR_CKPT_MANIFEST, &bytes)
            .expect("write");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_vector_checkpoints()
            .expect_err("a manifest this build cannot read must fail the load");
    }

    /// A `.ckpt` file inside the live generation whose bytes are not a valid
    /// frame must fail the load, not be skipped.
    #[test]
    fn corrupt_index_file_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = vector_ckpt_dir(&core.data_dir, core.core_id);
        let gen_dir = vector_ckpt_gen_dir(&ckpt_dir, 0);
        std::fs::create_dir_all(&gen_dir).expect("create gen dir");
        let stem = super::super::paths::vector_ckpt_stem(0, 1, "docs");
        std::fs::write(gen_dir.join(format!("{stem}.ckpt")), b"garbage")
            .expect("write garbage index");
        let bytes = super::super::format::test_manifest_bytes(0);
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, VECTOR_CKPT_MANIFEST, &bytes)
            .expect("write manifest");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_vector_checkpoints()
            .expect_err("a corrupt index file must fail the load, not skip it");
    }

    /// A bare `VectorPut` record (collection, vector, dim) at `lsn`.
    fn vector_put(lsn: u64, vector: Vec<f32>) -> nodedb_wal::WalRecord {
        let dim = vector.len();
        nodedb_wal::WalRecord::new(nodedb_wal::record::WalRecordArgs {
            record_type: nodedb_wal::record::RecordType::VectorPut as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: zerompk::to_msgpack_vec(&("emb", vector, dim)).expect("encode put"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("build record")
    }

    /// Record B at LSN 30 applies and the checkpoint is written while record A
    /// at LSN 20 is still on its way. A applies after the checkpoint, and the
    /// core stops before another one. Replay applies A once and skips B, so
    /// the replayed index equals the live one.
    #[test]
    fn a_vector_write_in_flight_at_a_checkpoint_replays_once() {
        use crate::engine::vector::hnsw::HnswParams;

        let dir = tempfile::tempdir().expect("tempdir");
        let key = CoreLoop::vector_index_key(0, 1, "emb", "");
        let live = {
            let mut core = open_core_at(dir.path());
            core.floors
                .applied_prefix
                .observe_outcome_floor(Lsn::new(10));
            let mut collection = VectorCollection::new(3, HnswParams::default());
            collection.insert(vec![0.0, 0.0, 1.0]);
            core.vector_collections.insert(key.clone(), collection);
            core.floors.applied_prefix.note_applied(Lsn::new(30));
            core.checkpoint_vector_indexes().expect("checkpoint");
            if let Some(collection) = core.vector_collections.get_mut(&key) {
                collection.insert(vec![1.0, 0.0, 0.0]);
            }
            core.floors.applied_prefix.note_applied(Lsn::new(20));
            core.vector_collections.get(&key).map(|c| c.len())
        };
        assert_eq!(live, Some(2));

        let mut restored = open_core_at(dir.path());
        restored.load_vector_checkpoints().expect("load");
        assert_eq!(
            restored.vector_collections.get(&key).map(|c| c.len()),
            Some(1),
            "the checkpoint holds B and not A"
        );
        restored.replay_vector_wal(
            &[
                vector_put(20, vec![1.0, 0.0, 0.0]),
                vector_put(30, vec![0.0, 0.0, 1.0]),
            ],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert_eq!(
            restored.vector_collections.get(&key).map(|c| c.len()),
            live,
            "replay applies A once and never applies B again"
        );
    }
}
