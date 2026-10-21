// SPDX-License-Identifier: BUSL-1.1

//! The spatial checkpoint load path: decode the published generation whole —
//! every R-tree WITH its doc_map — and install it.

use std::collections::HashMap;

use nodedb_types::DatabaseId;
use tracing::info;

use super::manifest::{read_spatial_manifest_at, storage_err};
use super::paths::{parse_spatial_key, spatial_ckpt_dir, spatial_ckpt_gen_dir};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::spatial::RTree;
use crate::types::{Lsn, TenantId};

/// One index restored from a generation: its R-tree and the entry-id → document
/// -id pairs that give its entries an identity.
struct DecodedSpatialIndex {
    rtree: RTree,
    doc_entries: Vec<(u64, String)>,
}

/// A decoded generation, keyed exactly as `CoreLoop::spatial_indexes` keys it.
type DecodedSpatialGeneration =
    HashMap<(DatabaseId, TenantId, String, String), DecodedSpatialIndex>;

impl CoreLoop {
    /// Load R-tree checkpoints from disk on startup.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/spatial-ckpt/core-{core_id}/`), and within it only the
    /// generation the manifest names. Files under a superseded generation are
    /// unreachable by construction, which is what stops an index dropped since
    /// the last cycle from coming back.
    ///
    /// When `spatial_checkpoint_kek` is set, plaintext checkpoint files are
    /// rejected and encrypted files are decrypted before loading.
    ///
    /// A corrupt or unreadable generation (bad framing, bad CRC, an unparseable
    /// filename, a rejected R-tree decode, or a missing/corrupt docmap
    /// companion) is fail-stop: its `Err` propagates out of boot so the core
    /// refuses to come up, rather than silently loading a partial index once the
    /// WAL below the generation's LSN is already truncated. An absent checkpoint
    /// directory — or one with no manifest — is not corruption and stays
    /// `Ok(())`.
    pub fn load_spatial_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = spatial_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = read_spatial_manifest_at(&ckpt_dir)? else {
            return Ok(());
        };
        let gen_dir = spatial_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Decode the WHOLE generation before installing any of it: an R-tree
        // whose doc_map failed to decode must not be installed at all, because
        // its entries would resolve to no document.
        let decoded = self.decode_spatial_generation(&gen_dir)?;

        let loaded = decoded.len();
        let mut entries = 0usize;
        for ((db, tid, coll, field), index) in decoded {
            entries += index.rtree.len();
            for (entry_id, doc_id) in index.doc_entries {
                self.spatial_doc_map
                    .insert((db, tid, coll.clone(), field.clone(), entry_id), doc_id);
            }
            self.spatial_indexes
                .insert((db, tid, coll, field), index.rtree);
        }

        self.floors.spatial_durable_lsn = Lsn::new(manifest.durable_through_lsn);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            loaded,
            entries,
            durable_through_lsn = manifest.durable_through_lsn,
            "spatial checkpoint restored"
        );
        Ok(())
    }

    /// Read and decode every R-tree/doc_map pair in a generation.
    fn decode_spatial_generation(
        &self,
        gen_dir: &std::path::Path,
    ) -> crate::Result<DecodedSpatialGeneration> {
        let dir_entries = std::fs::read_dir(gen_dir)
            .map_err(|e| storage_err(gen_dir, "read live generation dir", &e))?;

        let mut decoded = DecodedSpatialGeneration::new();
        for entry in dir_entries {
            let entry = entry.map_err(|source| CheckpointDecodeError::DirEntry { source })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // This directory is engine-private (only this module's write path
            // creates `.ckpt` files here), so a `.ckpt` whose stem does not
            // parse is a corrupted real checkpoint, not a foreign file to skip.
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let key = parse_spatial_key(&stem)
                .ok_or_else(|| CheckpointDecodeError::UnparseableFilename { stem: stem.clone() })?;

            let bytes = nodedb_wal::segment::read_checkpoint_framed(&path)?;
            let kek = self.segment_keks.spatial_checkpoint_kek.as_ref();
            let memory = nodedb_mem::ScopedMemory::new(
                self.governor.clone(),
                key.0,
                key.1,
                nodedb_mem::EngineId::Spatial,
            );
            let rtree = RTree::from_checkpoint(&bytes, kek, memory)?;

            // The doc_map is not optional company for the R-tree: the write
            // path always publishes both into the same generation, so a missing
            // or undecodable one here means the R-tree's entries would resolve
            // to no document at all — an inconsistent generation, not a
            // legitimate absence.
            let map_path = gen_dir.join(format!("{stem}.docmap"));
            let map_bytes = nodedb_wal::segment::read_checkpoint_framed(&map_path)?;
            let doc_entries: Vec<(u64, String)> =
                zerompk::from_msgpack(&map_bytes).map_err(|source| {
                    CheckpointDecodeError::MsgpackDecode {
                        path: map_path,
                        source,
                    }
                })?;

            decoded.insert(key, DecodedSpatialIndex { rtree, doc_entries });
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::BoundingBox;

    use super::super::format::{
        SPATIAL_CKPT_FORMAT_VERSION, SpatialCheckpointManifest, test_manifest_bytes,
    };
    use super::super::paths::{SPATIAL_CKPT_MANIFEST, checkpoint_stem};
    use super::super::test_support::open_core_at;
    use super::*;
    use crate::engine::spatial::RTreeEntry;

    /// Publish `stem`'s files into generation 0 and swing the manifest, so the
    /// load path sees exactly what a real flush would have made reachable.
    fn publish(ckpt_dir: &std::path::Path, write_files: impl FnOnce(&std::path::Path)) {
        let gen_dir = spatial_ckpt_gen_dir(ckpt_dir, 0);
        std::fs::create_dir_all(&gen_dir).expect("create gen dir");
        write_files(&gen_dir);
        let bytes = test_manifest_bytes(0);
        nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, SPATIAL_CKPT_MANIFEST, &bytes)
            .expect("write manifest");
    }

    fn one_entry_rtree_bytes() -> Vec<u8> {
        let memory = nodedb_mem::ScopedMemory::new(
            crate::data::executor::core_loop::test_governor(),
            DatabaseId::new(0),
            TenantId::new(7),
            nodedb_mem::EngineId::Spatial,
        );
        let rtree = RTree::bulk_load(
            vec![RTreeEntry {
                id: 1,
                bbox: BoundingBox::new(0.0, 0.0, 1.0, 1.0),
            }],
            memory,
            crate::engine::spatial::TreeSrid::WGS84,
        );
        rtree.checkpoint_to_bytes(None).expect("encode R-tree")
    }

    /// An absent checkpoint directory is not corruption — a fresh data
    /// directory must load cleanly with nothing restored.
    #[test]
    fn absent_dir_is_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        core.load_spatial_checkpoints()
            .expect("an absent checkpoint dir must not be treated as corruption");
        assert!(core.spatial_indexes.is_empty());
    }

    /// A `.ckpt` file that exists but is not valid checkpoint framing must fail
    /// the load, not be treated as absent: for a document-collection index this
    /// file is the only surviving copy of that row's geometry entry once the WAL
    /// below its LSN is truncated.
    #[test]
    fn corrupt_ckpt_frame_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        let stem = checkpoint_stem(DatabaseId::new(0), TenantId::new(7), "pts", "geom");
        publish(&ckpt_dir, |gen_dir| {
            std::fs::write(
                gen_dir.join(format!("{stem}.ckpt")),
                b"not a valid checkpoint frame",
            )
            .expect("write garbage checkpoint");
        });
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("a corrupt R-tree checkpoint frame must fail the load, not skip it");
    }

    /// A `.ckpt` filename whose stem does not parse is a corrupted real
    /// checkpoint, not a foreign file to ignore — this directory is
    /// engine-private and only ever holds files this module wrote.
    #[test]
    fn unparseable_stem_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        publish(&ckpt_dir, |gen_dir| {
            std::fs::write(gen_dir.join("not_a_valid_stem.ckpt"), b"irrelevant")
                .expect("write file with bad stem");
        });
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("an unparseable checkpoint filename must fail the load, not skip it");
    }

    /// A valid R-tree whose companion `.docmap` is missing is an inconsistent
    /// generation: the write path always publishes both, so a missing one means
    /// the R-tree's entries would resolve to no document.
    #[test]
    fn missing_docmap_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        let stem = checkpoint_stem(DatabaseId::new(0), TenantId::new(7), "pts", "geom");
        publish(&ckpt_dir, |gen_dir| {
            let bytes = one_entry_rtree_bytes();
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &format!("{stem}.ckpt"), &bytes)
                .expect("publish checkpoint");
            // Deliberately do NOT write the `.docmap` companion.
        });
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("a missing docmap companion must fail the load, not skip it");
    }

    /// A `.docmap` that exists but does not decode must fail the load for the
    /// same reason a missing one does.
    #[test]
    fn corrupt_docmap_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        let stem = checkpoint_stem(DatabaseId::new(0), TenantId::new(7), "pts", "geom");
        publish(&ckpt_dir, |gen_dir| {
            let bytes = one_entry_rtree_bytes();
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &format!("{stem}.ckpt"), &bytes)
                .expect("publish checkpoint");
            // Frame is valid, but the payload is not MessagePack.
            nodedb_wal::segment::write_checkpoint_framed(
                gen_dir,
                &format!("{stem}.docmap"),
                b"not msgpack",
            )
            .expect("publish garbage docmap");
        });
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("an undecodable docmap must fail the load, not skip it");
    }

    /// A manifest that exists but is corrupt must fail the load, not be treated
    /// as absent.
    #[test]
    fn corrupt_manifest_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        std::fs::write(ckpt_dir.join(SPATIAL_CKPT_MANIFEST), b"not a frame")
            .expect("write garbage manifest");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("a corrupt manifest must fail the load, not silently skip it");
    }

    /// A manifest from a future format must be refused rather than misparsed.
    #[test]
    fn unknown_manifest_version_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        let bytes = zerompk::to_msgpack_vec(&SpatialCheckpointManifest {
            format_version: SPATIAL_CKPT_FORMAT_VERSION + 1,
            generation: 0,
            durable_through_lsn: 5,
        })
        .expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, SPATIAL_CKPT_MANIFEST, &bytes)
            .expect("write");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("a manifest this build cannot read must fail the load");
    }

    /// The happy path end to end: a published pair restores both the index and
    /// the entry-id-to-document-id mapping.
    #[test]
    fn valid_checkpoint_and_docmap_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        let db = DatabaseId::new(0);
        let tid = TenantId::new(7);
        let stem = checkpoint_stem(db, tid, "pts", "geom");
        publish(&ckpt_dir, |gen_dir| {
            let bytes = one_entry_rtree_bytes();
            nodedb_wal::segment::write_checkpoint_framed(gen_dir, &format!("{stem}.ckpt"), &bytes)
                .expect("publish checkpoint");

            let doc_entries: Vec<(u64, String)> = vec![(1, "doc-1".to_string())];
            let map_bytes = zerompk::to_msgpack_vec(&doc_entries).expect("encode docmap");
            nodedb_wal::segment::write_checkpoint_framed(
                gen_dir,
                &format!("{stem}.docmap"),
                &map_bytes,
            )
            .expect("publish docmap");
        });
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect("a valid checkpoint and docmap must load cleanly");

        let key = (db, tid, "pts".to_string(), "geom".to_string());
        assert_eq!(restored.spatial_indexes.get(&key).map(|r| r.len()), Some(1));
        assert_eq!(
            restored
                .spatial_doc_map
                .get(&(db, tid, "pts".to_string(), "geom".to_string(), 1))
                .map(String::as_str),
            Some("doc-1")
        );
    }
}
