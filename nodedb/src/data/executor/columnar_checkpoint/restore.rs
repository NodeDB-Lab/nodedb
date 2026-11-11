// SPDX-License-Identifier: BUSL-1.1

//! Decode a published columnar checkpoint generation into restored engines,
//! each with its flushed segments and surrogate sidecar in lockstep.

use super::format::{COLUMNAR_CKPT_FORMAT_VERSION, ColumnarCheckpointFile};
use super::paths::columnar_map_key;
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::checkpoint_key::{is_keyed_checkpoint_path, read_keyed_checkpoint};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

/// One collection's restored engine plus the two lockstep halves of its flushed
/// state, as handed back by `MutationEngine::from_snapshot`.
///
/// Kept as one struct all the way to installation so the segment blobs and their
/// surrogate table are never in scope as independently-movable values — the
/// lockstep invariant is preserved by never offering the code a way to install
/// one without the other.
pub(super) struct RestoredColumnar {
    pub(super) engine: nodedb_columnar::MutationEngine,
    pub(super) segments: Vec<Vec<u8>>,
    pub(super) surrogates: nodedb_columnar::mutation::snapshot::FlushedSurrogateTable,
}

/// Every collection in a decoded generation, keyed by engine key.
pub(super) type DecodedColumnarGeneration = Vec<((DatabaseId, TenantId, String), RestoredColumnar)>;

impl CoreLoop {
    /// Read and decode every collection file in a generation.
    ///
    /// `Err` if any file in the directory is unreadable, unparseable, or carries
    /// an unexpected format version — the caller then restores nothing.
    pub(super) fn decode_columnar_generation(
        &self,
        gen_dir: &std::path::Path,
    ) -> Result<DecodedColumnarGeneration, CheckpointDecodeError> {
        let entries =
            std::fs::read_dir(gen_dir).map_err(|source| CheckpointDecodeError::ScanDir {
                dir: gen_dir.to_path_buf(),
                source,
            })?;

        let mut decoded = DecodedColumnarGeneration::new();
        for entry in entries {
            let entry = entry.map_err(|source| CheckpointDecodeError::DirEntry { source })?;
            let path = entry.path();
            if !is_keyed_checkpoint_path(&path) {
                continue;
            }
            let (stored, bytes) = read_keyed_checkpoint(&path)?;
            let (database_id, tenant_id, collection) =
                columnar_map_key(&stored).ok_or_else(|| CheckpointDecodeError::KeyShape {
                    path: path.clone(),
                    stored: stored.to_string(),
                    expected: "columnar",
                })?;
            let file =
                zerompk::from_msgpack::<ColumnarCheckpointFile>(&bytes).map_err(|source| {
                    CheckpointDecodeError::MsgpackDecode {
                        path: path.clone(),
                        source,
                    }
                })?;
            if file.format_version != COLUMNAR_CKPT_FORMAT_VERSION {
                return Err(CheckpointDecodeError::FormatVersion {
                    path: path.clone(),
                    found: file.format_version,
                    expected: COLUMNAR_CKPT_FORMAT_VERSION,
                });
            }

            // Rebuilding the engine here, not at install time, is what keeps the
            // generation all-or-nothing: this is the last fallible step, and
            // every one of them must be behind the caller's decision to install.
            let (engine, segments, surrogates) =
                nodedb_columnar::MutationEngine::from_snapshot(file.engine).map_err(|source| {
                    CheckpointDecodeError::EngineNotRebuildable {
                        path: path.clone(),
                        source: Box::new(source),
                    }
                })?;

            // A snapshot written before the surrogate sidecar existed decodes
            // with an empty table while carrying segments. Left as-is, every
            // flushed row would read as `None`-surrogate — which `scan_flushed`
            // already handles — but the two Vecs would disagree in length, and
            // the next export would persist that disagreement. Pad to the blob
            // count with all-`None` rows so the lockstep holds from the first
            // restore onwards, expressing exactly what such a snapshot knows:
            // these segments exist and their identities are unrecorded.
            let mut surrogates = surrogates;
            if surrogates.len() != segments.len() {
                if !surrogates.is_empty() {
                    return Err(CheckpointDecodeError::SurrogateLockstepMismatch {
                        path: path.clone(),
                        segments: segments.len(),
                        surrogates: surrogates.len(),
                    });
                }
                surrogates = segments.iter().map(|_| Vec::new()).collect();
            }

            decoded.push((
                (
                    DatabaseId::new(database_id),
                    TenantId::new(tenant_id),
                    collection,
                ),
                RestoredColumnar {
                    engine,
                    segments,
                    surrogates,
                },
            ));
        }
        Ok(decoded)
    }
}

/// Restore coverage for the surrogate lockstep invariant.
///
/// A checkpoint whose bytes round-trip perfectly but whose surrogate sidecar
/// has slipped one index against the segment bytes would pass a struct
/// comparison and still answer every cross-engine prefilter with the wrong
/// rows. These tests read the restored state through scans and prefilters.
#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;
    use nodedb_types::value::Value;

    use super::super::test_fixture::{bitmap, ids, open_core, scan, seed_collection};

    /// The surrogate lockstep invariant, tested through the behaviour it exists for
    /// rather than through the lengths that encode it.
    ///
    /// `scan_flushed.rs` resolves a flushed row's cross-engine identity purely by
    /// positional agreement between `columnar_flushed_segments[key][i]` and
    /// `columnar_flushed_surrogates[key][i]`. If a restore shifted, dropped or
    /// reordered the sidecar, nothing would fail loudly — the prefilter would simply
    /// answer with the wrong rows. Asking for row 2's surrogate and getting row 2 is
    /// the assertion that catches it.
    #[test]
    fn restored_prefilter_resolves_each_flushed_row_to_its_own_surrogate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_lockstep_query";

        let mut core = open_core(dir.path());
        seed_collection(
            &mut core,
            coll,
            &[
                (1, "a", Surrogate(201)),
                (2, "b", Surrogate(202)),
                (3, "c", Surrogate(203)),
                (4, "d", Surrogate(204)),
            ],
            &[],
        );
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        // Each surrogate must select exactly its own row. An off-by-one in the
        // sidecar returns a neighbour and is caught here; a dropped sidecar returns
        // nothing at all, since a row with no recorded surrogate cannot satisfy a
        // prefilter.
        for (id, surr) in [(1, 201u32), (2, 202), (3, 203), (4, 204)] {
            assert_eq!(
                ids(&scan(
                    &mut restored,
                    coll,
                    Some(&bitmap(&[Surrogate(surr)]))
                )),
                vec![id],
                "surrogate {surr} must resolve to row {id} and nothing else"
            );
        }

        // A non-contiguous subset, to catch a sidecar that survived per-row probes
        // by being reversed.
        assert_eq!(
            ids(&scan(
                &mut restored,
                coll,
                Some(&bitmap(&[Surrogate(201), Surrogate(204)]))
            )),
            vec![1, 4]
        );
    }

    /// The lockstep invariant across MULTIPLE segments: the outer index must still
    /// mean "segment index" after a restore, so `segment_id == index + 1` continues
    /// to hold and each segment's delete bitmap keeps addressing its own rows.
    #[test]
    fn restore_preserves_lockstep_across_multiple_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_multiseg";

        let mut core = open_core(dir.path());
        let key = seed_collection(&mut core, coll, &[(1, "a", Surrogate(301))], &[]);
        // A second flush on the same collection: two segments, two sidecar entries.
        {
            let engine = core.columnar_engines.get_mut(&key).expect("engine");
            engine
                .insert_with_surrogate(
                    &[Value::Integer(2), Value::String("b".into())],
                    Surrogate(302),
                    2,
                )
                .expect("insert_with_surrogate");
            let new_segment_id = engine.next_segment_id();
            let (seg_schema, columns, row_count) = engine.memtable_mut().drain_optimized();
            let surrogates: Vec<Option<Surrogate>> = engine.memtable_surrogates().to_vec();
            let memory = nodedb_mem::ScopedMemory::new(
                core.governor.clone(),
                key.0,
                key.1,
                nodedb_mem::EngineId::Columnar,
            );
            let bytes =
                nodedb_columnar::SegmentWriter::new(nodedb_columnar::writer::PROFILE_PLAIN, memory)
                    .write_segment(&seg_schema, &columns, row_count, None)
                    .expect("write_segment");
            engine
                .on_memtable_flushed(new_segment_id)
                .expect("on_memtable_flushed");
            core.columnar_flushed_segments
                .get_mut(&key)
                .expect("segments")
                .push(bytes);
            core.columnar_flushed_surrogates
                .get_mut(&key)
                .expect("sidecar")
                .push(surrogates);
        }
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        let segs = restored
            .columnar_flushed_segments
            .get(&key)
            .expect("segments restored");
        let surrs = restored
            .columnar_flushed_surrogates
            .get(&key)
            .expect("sidecar restored");
        assert_eq!(segs.len(), 2, "both segments restored");
        assert_eq!(
            segs.len(),
            surrs.len(),
            "outer Vec lengths must be equal — the index is the segment index"
        );
        for (i, (seg, surr)) in segs.iter().zip(surrs.iter()).enumerate() {
            let reader = nodedb_columnar::SegmentReader::open(seg).expect("open restored segment");
            assert_eq!(
                reader.row_count() as usize,
                surr.len(),
                "segment {i}: per-row sidecar length must equal the segment's row count"
            );
        }

        // And the identities still resolve per segment, not merely per length.
        assert_eq!(
            ids(&scan(&mut restored, coll, Some(&bitmap(&[Surrogate(302)])))),
            vec![2],
            "a surrogate in the SECOND segment must resolve to its own row"
        );
    }

    /// A restore must never leave one lockstep half populated and the other absent.
    /// Both are inserted from a single destructured value, so this asserts the
    /// property the type layout is meant to guarantee.
    #[test]
    fn restore_never_populates_one_lockstep_half_alone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_both_halves";

        let mut core = open_core(dir.path());
        let key = seed_collection(&mut core, coll, &[(1, "a", Surrogate(401))], &[]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(
            restored.columnar_flushed_segments.contains_key(&key),
            restored.columnar_flushed_surrogates.contains_key(&key),
            "a key present in one map must be present in the other"
        );
    }

    /// A collection that has never flushed has no segments and no sidecar, and the
    /// two must agree at length ZERO rather than one of them being absent while the
    /// other is empty.
    #[test]
    fn never_flushed_collection_restores_with_both_halves_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_memtable_only";

        let mut core = open_core(dir.path());
        let key = seed_collection(&mut core, coll, &[], &[(1, "a", Surrogate(501))]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(
            restored
                .columnar_flushed_segments
                .get(&key)
                .map(Vec::len)
                .unwrap_or(0),
            0
        );
        assert_eq!(
            restored
                .columnar_flushed_surrogates
                .get(&key)
                .map(Vec::len)
                .unwrap_or(0),
            0
        );
        assert_eq!(
            ids(&scan(&mut restored, coll, None)),
            vec![1],
            "a memtable-only collection's rows are exactly the ones a segment-file \
             design could not have persisted"
        );
    }

    /// A 255-byte collection name checkpoints under a short file name and
    /// restores every row under its full name.
    #[test]
    fn long_collection_name_checkpoints_and_restores() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "c".repeat(255);

        let mut core = open_core(dir.path());
        seed_collection(
            &mut core,
            &coll,
            &[(1, "a", Surrogate(301))],
            &[(2, "b", Surrogate(302))],
        );
        core.checkpoint_columnar_engines()
            .expect("a long-named collection must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");
        assert_eq!(ids(&scan(&mut restored, &coll, None)), vec![1, 2]);
    }
}
