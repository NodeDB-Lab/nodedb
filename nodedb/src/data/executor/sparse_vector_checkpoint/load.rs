// SPDX-License-Identifier: BUSL-1.1

//! The sparse-vector checkpoint load path: decode the published generation
//! whole, install its indexes, and restore the LSN they are durable through.

use std::collections::HashMap;

use nodedb_types::DatabaseId;
use tracing::info;

use super::manifest::read_sparse_vector_manifest_at;
use super::paths::{parse_sparse_vector_key, sparse_vector_ckpt_dir, sparse_vector_ckpt_gen_dir};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::vector::sparse::SparseInvertedIndex;
use crate::types::{Lsn, TenantId};

/// A decoded generation: every index it holds, keyed exactly as
/// `CoreLoop::sparse_vector_indexes` keys them.
type DecodedSparseVectorGeneration =
    HashMap<(DatabaseId, TenantId, String, String), SparseInvertedIndex>;

impl CoreLoop {
    /// Load the sparse-vector checkpoint from disk on startup, BEFORE WAL
    /// replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/sparse-vector-ckpt/core-{core_id}/`), so no core-ownership
    /// filter on the filename is needed — a core only ever sees its own indexes.
    ///
    /// The restored generation's LSN becomes `sparse_vector_durable_lsn`, so a
    /// flush that fails before the first successful checkpoint of this process
    /// clamps to what the PREVIOUS process actually made durable instead of
    /// pinning WAL truncation at zero. Its replay stamp becomes the
    /// sparse-vector replay floor: restart replay skips exactly the records
    /// the restored indexes hold.
    ///
    /// # Fail-stop on corruption
    ///
    /// A sparse-vector checkpoint is a non-WAL home of its indexes once the WAL
    /// below its LSN has been truncated, so a checkpoint that exists but cannot
    /// be read or decoded is unrecoverable data loss. This returns `Err` in
    /// that case instead of skipping it, and the boot sequence refuses to
    /// bring the core up. An absent checkpoint directory is not an error — WAL
    /// replay reconstructs everything.
    pub fn load_sparse_vector_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = sparse_vector_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = read_sparse_vector_manifest_at(&ckpt_dir, self.core_id)? else {
            return Ok(());
        };
        let gen_dir = sparse_vector_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Decode the WHOLE generation before installing any of it. The manifest
        // promises a complete set at one LSN; installing a subset and then
        // claiming that LSN as this engine's durable point would authorise
        // truncating the WAL records of the indexes that failed to decode.
        // Either way the failure must abort boot rather than restore nothing
        // silently — the WAL below this LSN may already be gone.
        let decoded = self.decode_sparse_vector_generation(&gen_dir)?;

        let indexes = decoded.len();
        let mut docs = 0usize;
        for (key, index) in decoded {
            docs += index.doc_count();
            self.sparse_vector_indexes.insert(key, index);
        }

        // Claimed only once every index is in: this LSN is what a failed flush
        // clamps truncation to, so claiming it over a half-restored generation
        // would authorise deleting the records that would have completed it.
        self.floors.sparse_vector_durable_lsn = Lsn::new(manifest.durable_through_lsn);
        let replay_prefix = manifest.replay.prefix;
        let applied_ranges = manifest.replay.applied_above.len();
        self.floors.sparse_vector_published_lsn = Lsn::new(replay_prefix);
        self.floors.replay_floors.sparse_vector.set(manifest.replay);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            indexes,
            docs,
            durable_through_lsn = manifest.durable_through_lsn,
            replay_prefix,
            applied_ranges,
            "sparse vector checkpoint restored"
        );
        Ok(())
    }

    /// Read and decode every index file in a generation.
    ///
    /// `Err` if any file in the directory is unreadable or undecodable — the
    /// caller then restores nothing.
    fn decode_sparse_vector_generation(
        &self,
        gen_dir: &std::path::Path,
    ) -> Result<DecodedSparseVectorGeneration, CheckpointDecodeError> {
        let entries =
            std::fs::read_dir(gen_dir).map_err(|source| CheckpointDecodeError::ScanDir {
                dir: gen_dir.to_path_buf(),
                source,
            })?;

        let mut decoded = DecodedSparseVectorGeneration::new();
        for entry in entries {
            let entry = entry.map_err(|source| CheckpointDecodeError::DirEntry { source })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let key = parse_sparse_vector_key(stem).ok_or_else(|| {
                CheckpointDecodeError::UnparseableFilename {
                    stem: stem.to_string(),
                }
            })?;

            let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
                CheckpointDecodeError::ReadFile {
                    path: path.clone(),
                    source,
                }
            })?;
            let index = SparseInvertedIndex::from_checkpoint(&bytes)
                .ok_or_else(|| CheckpointDecodeError::UndecodableIndex { path: path.clone() })?;
            decoded.insert(key, index);
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::{SPARSE_VECTOR_CKPT_FORMAT_VERSION, SparseVectorCheckpointManifest};
    use super::super::paths::{SPARSE_VECTOR_CKPT_MANIFEST, sparse_vector_checkpoint_stem};
    use super::*;
    use nodedb_types::SparseVector;

    fn index_with(docs: &[(&str, &[(u32, f32)])]) -> SparseInvertedIndex {
        let mut index = SparseInvertedIndex::new();
        for (doc_id, entries) in docs {
            let vector = SparseVector::from_entries(entries.to_vec()).expect("finite weights");
            index.insert(doc_id, &vector);
        }
        index
    }

    /// The full disk round-trip an index takes through a checkpoint: export,
    /// write, read back, decode. The restored index must answer for exactly the
    /// documents the original held — an index that decodes to fewer documents
    /// while the manifest reports the same LSN is silent data loss.
    #[test]
    fn index_roundtrips_through_the_checkpoint_file() {
        let index = index_with(&[
            ("doc-a", &[(1, 0.5), (7, 0.25)]),
            ("doc-b", &[(7, 1.0), (9, 0.125)]),
        ]);
        assert_eq!(index.doc_count(), 2);

        let tmp = tempfile::tempdir().expect("tempdir");
        let stem = sparse_vector_checkpoint_stem(0, 7, "docs", "emb");
        let path = tmp.path().join(format!("{stem}.ckpt"));
        let bytes = index.checkpoint_to_bytes().expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(tmp.path(), &format!("{stem}.ckpt"), &bytes)
            .expect("write");

        let read_back = nodedb_wal::segment::read_checkpoint_framed(&path).expect("read");
        let restored = SparseInvertedIndex::from_checkpoint(&read_back).expect("decode");

        assert_eq!(
            restored.doc_count(),
            index.doc_count(),
            "every document must survive the round-trip"
        );
        assert_eq!(
            restored.dim_count(),
            index.dim_count(),
            "every indexed dimension must survive the round-trip"
        );
        assert_eq!(
            restored.total_postings(),
            index.total_postings(),
            "no posting may be dropped by the round-trip"
        );
    }

    /// An index emptied by deletes must round-trip as EMPTY. If an empty export
    /// were unreadable the load path would abort the whole generation, and if it
    /// were skipped the older populated file would outlive the deletes.
    #[test]
    fn emptied_index_roundtrips_as_empty() {
        let mut index = index_with(&[("doc-a", &[(1, 0.5)])]);
        index.delete("doc-a");
        assert!(index.is_empty(), "the only document was deleted");

        let bytes = index.checkpoint_to_bytes().expect("encode");
        let restored =
            SparseInvertedIndex::from_checkpoint(&bytes).expect("an empty index must still decode");
        assert!(
            restored.is_empty(),
            "an emptied index must restore empty, never resurrect its documents"
        );
    }

    /// The manifest is the only record of the LSN a generation is durable
    /// through, and both the reported checkpoint LSN and the post-restart clamp
    /// rest on it: it must survive the round-trip exactly.
    #[test]
    fn manifest_roundtrips_generation_and_lsn() {
        let written = SparseVectorCheckpointManifest {
            format_version: SPARSE_VECTOR_CKPT_FORMAT_VERSION,
            generation: 4,
            durable_through_lsn: 8_128,
            replay: crate::types::replay_stamp::ReplayStamp::default(),
        };
        let tmp = tempfile::tempdir().expect("tempdir");
        let bytes = zerompk::to_msgpack_vec(&written).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(
            tmp.path(),
            SPARSE_VECTOR_CKPT_MANIFEST,
            &bytes,
        )
        .expect("write");

        let decoded = read_sparse_vector_manifest_at(tmp.path(), 0)
            .expect("manifest must read")
            .expect("manifest file exists, so this must be Some");
        assert_eq!(
            decoded.durable_through_lsn, 8_128,
            "the manifest must report exactly the LSN it was written with"
        );
        assert_eq!(decoded.generation, 4);
    }

    /// A manifest from a future format must be refused, not misparsed: a
    /// misparse would install indexes this build cannot read while claiming an
    /// LSN that authorises deleting the records behind them. Refusing it must
    /// be a hard `Err`, not a silent "treat as absent" — the WAL below the LSN
    /// it names may already be truncated, so silently restoring nothing would
    /// be permanent, unannounced data loss.
    #[test]
    fn unknown_manifest_version_is_rejected() {
        let written = SparseVectorCheckpointManifest {
            format_version: SPARSE_VECTOR_CKPT_FORMAT_VERSION + 1,
            generation: 1,
            durable_through_lsn: 5,
            replay: crate::types::replay_stamp::ReplayStamp::default(),
        };
        let tmp = tempfile::tempdir().expect("tempdir");
        let bytes = zerompk::to_msgpack_vec(&written).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(
            tmp.path(),
            SPARSE_VECTOR_CKPT_MANIFEST,
            &bytes,
        )
        .expect("write");

        read_sparse_vector_manifest_at(tmp.path(), 0)
            .expect_err("a manifest this build cannot read must fail the load, not gate nothing");
    }

    /// No manifest means no live generation: nothing restores and nothing is
    /// claimed durable, so replay falls back to the full WAL.
    #[test]
    fn absent_manifest_reads_as_none() {
        let tmp = tempfile::tempdir().expect("tempdir");
        assert!(
            read_sparse_vector_manifest_at(tmp.path(), 0)
                .expect("an absent manifest must not error")
                .is_none()
        );
    }

    /// A manifest that exists but is corrupt (truncated / bad frame) must fail
    /// the load, not be treated as absent: proves the `CheckpointDecodeError`
    /// from a bad `ReadFile` propagates all the way out of
    /// `load_sparse_vector_checkpoints` as `Err`.
    #[test]
    fn corrupt_manifest_fails_the_load() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(tmp.path());
        let ckpt_dir = sparse_vector_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        let manifest_path = ckpt_dir.join(SPARSE_VECTOR_CKPT_MANIFEST);
        std::fs::write(&manifest_path, b"not a valid checkpoint frame")
            .expect("write garbage manifest");
        drop(core);

        let mut restored = open_core_at(tmp.path());
        restored
            .load_sparse_vector_checkpoints()
            .expect_err("a corrupt manifest must fail the load, not silently skip it");
    }

    /// A core rooted at `dir`, so two cores in one test share a data dir the way
    /// a restart does: the second reads exactly what the first wrote.
    fn open_core_at(dir: &std::path::Path) -> CoreLoop {
        use std::sync::Arc;

        use nodedb_bridge::buffer::RingBuffer;
        use nodedb_types::OrdinalClock;

        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let hlc = Arc::new(OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // no requests are dispatched in this test
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            hlc,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    /// The end-to-end contract, through the real write and load paths on two
    /// cores over one data dir: the flush must report exactly the LSN it made
    /// durable, and a restart must get every index back from disk alone — which
    /// is all that stands between the indexes and a WAL truncated past them.
    #[test]
    fn flush_reports_its_lsn_and_a_restart_restores_every_index() {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut before = open_core_at(dir.path());
        before.sparse_vector_indexes.insert(
            (
                DatabaseId::new(0),
                TenantId::new(7),
                "docs".into(),
                "emb".into(),
            ),
            index_with(&[("doc-a", &[(1, 0.5)]), ("doc-b", &[(2, 0.25)])]),
        );
        // A second index, under a name whose literal `_` the filename encoding
        // has to escape — a stem that round-trips wrong restores under a key no
        // query ever computes, and the index reads back missing.
        before.sparse_vector_indexes.insert(
            (
                DatabaseId::new(0),
                TenantId::new(7),
                "my_docs".into(),
                "title_emb".into(),
            ),
            index_with(&[("doc-c", &[(3, 1.0)])]),
        );
        before.advance_watermark(Lsn::new(1_234));

        let reported = before
            .checkpoint_sparse_vector_indexes()
            .expect("flush to a writable dir must succeed");
        assert_eq!(
            reported,
            Lsn::new(1_234),
            "the flush must report exactly the LSN it made durable — the manager \
             deletes WAL segments below whatever this returns"
        );
        // Released before the next core opens: a core owns its data dir's redb
        // exclusively, so a restart is modelled by dropping this one first.
        drop(before);

        let mut after = open_core_at(dir.path());
        assert!(
            after.sparse_vector_indexes.is_empty(),
            "a fresh core holds no indexes, or this test proves nothing"
        );
        after
            .load_sparse_vector_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(
            after.sparse_vector_indexes.len(),
            2,
            "every index in the published generation must restore"
        );
        let restored = after
            .sparse_vector_indexes
            .get(&(
                DatabaseId::new(0),
                TenantId::new(7),
                "my_docs".into(),
                "title_emb".into(),
            ))
            .expect("an index must restore under the exact key it was written with");
        assert_eq!(restored.doc_count(), 1);
        assert_eq!(
            after.floors.sparse_vector_durable_lsn,
            Lsn::new(1_234),
            "the restored durable LSN is what a failed flush clamps to; losing it \
             would pin truncation at zero for the rest of the process"
        );
    }
}
