// SPDX-License-Identifier: BUSL-1.1

//! The columnar checkpoint load path: decode the published generation whole,
//! install every engine with its flushed segments and surrogate sidecar, and
//! record the replay floor it authorises.

use tracing::info;

use super::format::{COLUMNAR_CKPT_FORMAT_VERSION, ColumnarCheckpointFile};
use super::paths::{columnar_ckpt_dir, columnar_ckpt_gen_dir, parse_columnar_ckpt_stem};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, Lsn, TenantId};

/// One collection's restored engine plus the two lockstep halves of its flushed
/// state, as handed back by `MutationEngine::from_snapshot`.
///
/// Kept as one struct all the way to installation so the segment blobs and their
/// surrogate table are never in scope as independently-movable values — the
/// lockstep invariant is preserved by never offering the code a way to install
/// one without the other.
struct RestoredColumnar {
    engine: nodedb_columnar::MutationEngine,
    segments: Vec<Vec<u8>>,
    surrogates: nodedb_columnar::mutation::snapshot::FlushedSurrogateTable,
}

/// Every collection in a decoded generation, keyed by engine key.
type DecodedColumnarGeneration = Vec<((DatabaseId, TenantId, String), RestoredColumnar)>;

impl CoreLoop {
    /// Load the columnar checkpoint from disk on startup, BEFORE WAL replay and
    /// BEFORE `seed_columnar_schemas`.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/columnar-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed — a core only ever sees its own collections.
    ///
    /// Ordering against the schema seed is what keeps the two from fighting:
    /// `seed_columnar_schemas` skips any collection that already has an engine,
    /// so running first means a restored engine keeps the exact schema it was
    /// exported with — including the bitemporal columns the seed would prepend —
    /// and the seed only creates engines for collections this restore did not
    /// cover. Running after the seed would instead leave the seed's empty engine
    /// in place and silently discard the restored rows.
    ///
    /// The manifest's LSN then becomes the replay floor: the columnar replay
    /// arms skip the records already folded in and apply everything above.
    ///
    /// # Fail-stop on corruption
    ///
    /// Columnar is memory-only on both halves — nothing writes the live
    /// memtables or flushed segment bytes to disk outside this checkpoint — so
    /// a published generation is the only non-WAL home of its rows once the
    /// WAL below its LSN has been truncated. A checkpoint that exists but
    /// cannot be read or decoded is therefore unrecoverable data loss: this
    /// returns `Err` in that case instead of skipping it, and the boot
    /// sequence refuses to bring the core up. An absent checkpoint directory
    /// is not an error — WAL replay reconstructs everything.
    pub fn load_columnar_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = columnar_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = self.read_columnar_manifest(&ckpt_dir)? else {
            return Ok(());
        };
        let gen_dir = columnar_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Decode the WHOLE generation before installing any of it. The manifest
        // promises a complete set at one LSN; installing a subset and claiming
        // that LSN would silently drop the collections that failed to decode,
        // and installing a subset WITHOUT the floor would re-apply every
        // non-idempotent Update record against the rows that did load. Either
        // way the failure must abort boot rather than restore nothing silently
        // — the WAL below this LSN may already be gone.
        let decoded = self.decode_columnar_generation(&gen_dir)?;

        let collections = decoded.len();
        let mut segments = 0usize;
        let mut geometry_rows = 0usize;
        for (key, restored) in decoded {
            let RestoredColumnar {
                engine,
                segments: blobs,
                surrogates,
            } = restored;

            // The R-tree entries for this collection's geometry columns are
            // rebuilt from the restored rows, not carried in the checkpoint —
            // see `geometry_restore.rs`. Done BEFORE the maps are populated so
            // it reads the restored engine and blobs directly and cannot see a
            // half-installed state.
            geometry_rows += self.restore_columnar_geometry_indexes(&key, &engine, &blobs);

            segments += blobs.len();
            // Both halves are installed from one destructured value, in the same
            // iteration, unconditionally. There is no branch on which one of the
            // two maps gets an entry, so the outer-index agreement that
            // `scan_flushed.rs` relies on cannot be broken here.
            self.columnar_flushed_segments.insert(key.clone(), blobs);
            self.columnar_flushed_surrogates
                .insert(key.clone(), surrogates);
            self.columnar_engines.insert(key, engine);
        }

        // Claimed only once every engine is in: the floor suppresses WAL
        // records, so claiming it over a half-restored generation would turn a
        // recoverable read failure into permanent data loss.
        self.floors
            .replay_floors
            .columnar
            .set(Lsn::new(manifest.durable_through_lsn));
        self.floors.columnar_durable_lsn = Lsn::new(manifest.durable_through_lsn);
        self.floors.columnar_published_lsn = Lsn::new(manifest.durable_through_lsn);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            collections,
            segments,
            geometry_rows,
            durable_through_lsn = manifest.durable_through_lsn,
            "columnar checkpoint restored"
        );
        Ok(())
    }

    /// Read and decode every collection file in a generation.
    ///
    /// `Err` if any file in the directory is unreadable, unparseable, or carries
    /// an unexpected format version — the caller then restores nothing.
    fn decode_columnar_generation(
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
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let (database_id, tenant_id, collection) =
                parse_columnar_ckpt_stem(stem).ok_or_else(|| {
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

/// Round-trip coverage for the columnar checkpoint.
///
/// The bar these tests hold to is that a RESTORED core answers QUERIES the way
/// the original did — not that its structs compare equal. A checkpoint whose
/// bytes round-trip perfectly but whose surrogate sidecar has slipped one index
/// against the segment bytes would pass a struct comparison and still answer
/// every cross-engine prefilter with the wrong rows, which is the failure this
/// module exists to make impossible.
///
/// Each test forces a REAL flush — drain the memtable, encode a `SegmentWriter`
/// segment, push bytes + captured surrogates in lockstep — mirroring the
/// production block in `handlers/columnar_write/flush.rs`, so the rows genuinely
/// live only in a flushed segment before the checkpoint runs.
#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_columnar::MutationEngine;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
    use crate::data::executor::columnar_checkpoint::paths;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::columnar_read::ColumnarScanParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    type EngineKey = (DatabaseId, TenantId, String);

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .expect("valid schema")
    }

    /// Open a core rooted at `dir`. Taking the dir rather than making one is what
    /// lets a test open a SECOND core over the first one's data — the only way to
    /// exercise the load path as boot actually runs it.
    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    fn engine_key(collection: &str) -> EngineKey {
        (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            collection.to_string(),
        )
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            // Plan is irrelevant: `execute_columnar_scan` is called directly with
            // `ColumnarScanParams` and only reads `database_id` / `tenant_id`.
            plan: PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Register an engine holding `flushed` rows in a real flushed segment and
    /// `resident` rows still in the live memtable.
    ///
    /// Both populations matter to this module's claim: a design that wrote only real
    /// on-disk segment files would carry `flushed` and silently lose `resident`.
    fn seed_collection(
        core: &mut CoreLoop,
        collection: &str,
        flushed: &[(i64, &str, Surrogate)],
        resident: &[(i64, &str, Surrogate)],
    ) -> EngineKey {
        let key = engine_key(collection);
        let mut engine = MutationEngine::new(collection.to_string(), schema());

        for (id, name, surr) in flushed {
            engine
                .insert_with_surrogate(&[Value::Integer(*id), Value::String((*name).into())], *surr)
                .expect("insert_with_surrogate");
        }

        if !flushed.is_empty() {
            // ── Mirror handlers/columnar_write/flush.rs ─────────────────────────
            let new_segment_id = engine.next_segment_id();
            let (seg_schema, columns, row_count) = engine.memtable_mut().drain_optimized();
            // Capture surrogates BEFORE `on_memtable_flushed` clears them.
            let flushed_surrogates: Vec<Option<Surrogate>> = engine.memtable_surrogates().to_vec();
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
            core.columnar_flushed_segments
                .entry(key.clone())
                .or_default()
                .push(bytes);
            core.columnar_flushed_surrogates
                .entry(key.clone())
                .or_default()
                .push(flushed_surrogates);
            engine
                .on_memtable_flushed(new_segment_id)
                .expect("on_memtable_flushed");
        }

        for (id, name, surr) in resident {
            engine
                .insert_with_surrogate(&[Value::Integer(*id), Value::String((*name).into())], *surr)
                .expect("insert_with_surrogate");
        }

        core.columnar_engines.insert(key.clone(), engine);
        key
    }

    /// Run a scan (optionally prefiltered) and return the decoded result rows.
    fn scan(
        core: &mut CoreLoop,
        collection: &str,
        prefilter: Option<&SurrogateBitmap>,
    ) -> Vec<serde_json::Value> {
        let task = make_task();
        let params = ColumnarScanParams {
            collection,
            projection: &[],
            limit: 0,
            filters: &[],
            rls_filters: &[],
            sort_keys: &[],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter,
            computed_columns: &[],
            txn_id: None,
        };
        let resp = core.execute_columnar_scan(&task, params);
        let decoded: Vec<nodedb_types::JsonValue> =
            zerompk::from_msgpack(resp.payload.as_bytes()).expect("decode scan payload");
        decoded.into_iter().map(|j| j.0).collect()
    }

    fn ids(rows: &[serde_json::Value]) -> Vec<i64> {
        let mut v: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.get("id").and_then(|x| x.as_i64()))
            .collect();
        v.sort_unstable();
        v
    }

    fn bitmap(surrogates: &[Surrogate]) -> SurrogateBitmap {
        let mut bm = SurrogateBitmap::new();
        for s in surrogates {
            bm.insert(*s);
        }
        bm
    }

    /// The headline claim: rows that existed ONLY in memory — in a flushed segment
    /// whose bytes no code path writes to disk, and in a live memtable — come back
    /// after a restart and are returned by a scan.
    ///
    /// This is the data-loss bug's direct regression: before the checkpoint, the WAL
    /// was the only copy of both populations while columnar writes advanced the
    /// watermark that authorised deleting it.
    #[test]
    fn restored_rows_are_visible_to_a_scan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_roundtrip";

        let mut core = open_core(dir.path());
        seed_collection(
            &mut core,
            coll,
            &[(1, "a", Surrogate(101)), (2, "b", Surrogate(102))],
            &[(3, "c", Surrogate(103))],
        );
        assert_eq!(ids(&scan(&mut core, coll, None)), vec![1, 2, 3]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        assert!(
            restored.columnar_engines.is_empty(),
            "a freshly opened core must hold nothing before the load runs"
        );
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(
            ids(&scan(&mut restored, coll, None)),
            vec![1, 2, 3],
            "every row must survive the restart: the flushed-segment rows AND the \
             rows still resident in the memtable"
        );
    }

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

    /// The reported LSN is a deletion authority: it must be the watermark on
    /// success, and it must come back as BOTH the restored durable LSN and the
    /// replay floor. Getting either wrong is silent — too high gates records that
    /// still needed replaying, too low replays records already folded in.
    #[test]
    fn reported_lsn_becomes_the_restored_floor_and_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_lsn";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(601))], &[]);
        core.watermark = Lsn::new(900);

        let reported = core
            .checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        assert_eq!(
            reported,
            Lsn::new(900),
            "a successful flush reports the watermark"
        );
        drop(core);

        let mut restored = open_core(dir.path());
        assert!(
            !restored.floors.replay_floors.columnar.covers(1),
            "before the load, no floor is set and nothing is gated"
        );
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(restored.floors.columnar_durable_lsn, Lsn::new(900));
        assert!(
            restored.floors.replay_floors.columnar.covers(900),
            "the stamped LSN is durable THROUGH, so its own record is folded in"
        );
        assert!(
            !restored.floors.replay_floors.columnar.covers(901),
            "a record above the stamp is NOT in the restored state and must replay"
        );
    }

    /// A flush that cannot write must surface a typed error rather than reporting
    /// an LSN. The caller's clamp depends on it: an `Ok(watermark)` here would
    /// authorise deleting the WAL segments holding the only copy of the rows this
    /// flush failed to write. The clamp itself is covered in
    /// `handlers/control/checkpoint_durable_lsn.rs`, where the contributor lives.
    #[test]
    fn a_failed_flush_errors_and_leaves_the_durable_lsn_untouched() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_fail";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(701))], &[]);
        core.watermark = Lsn::new(900);

        // Occupy the checkpoint directory's path with a FILE, so `create_dir_all`
        // fails and the flush cannot publish anything.
        std::fs::write(dir.path().join("columnar-ckpt"), b"not a directory")
            .expect("write blocking file");

        assert!(
            core.checkpoint_columnar_engines().is_err(),
            "a flush that cannot write must surface a typed error, never Ok"
        );
        assert_eq!(
            core.floors.columnar_durable_lsn,
            Lsn::ZERO,
            "a fresh core is durable through NOTHING, and a failed flush must not \
             advance that"
        );
    }

    /// A generation is only live once its manifest names it. Files written by a
    /// cycle that died before publishing must be inert: restoring them would install
    /// rows at an LSN nothing vouched for, and with them a floor that suppresses the
    /// records which would have corrected them.
    #[test]
    fn an_unpublished_generation_is_not_restored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_unpublished";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(801))], &[]);
        core.watermark = Lsn::new(500);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        // Remove the manifest, leaving the generation directory fully intact.
        let manifest = dir
            .path()
            .join("columnar-ckpt")
            .join("core-0")
            .join(paths::COLUMNAR_CKPT_MANIFEST);
        assert!(manifest.exists(), "the checkpoint published a manifest");
        std::fs::remove_file(&manifest).expect("remove manifest");

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert!(
            restored.columnar_engines.is_empty(),
            "without a manifest the generation is invisible"
        );
        assert!(
            !restored.floors.replay_floors.columnar.covers(1),
            "restoring nothing must gate nothing — replay falls back to the full WAL"
        );
    }

    /// Publishing a second generation must supersede the first, not merge with it.
    /// A restore that saw both would resurrect rows a later checkpoint recorded as
    /// deleted.
    #[test]
    fn a_republished_generation_supersedes_the_previous_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_regen";

        let mut core = open_core(dir.path());
        let key = seed_collection(
            &mut core,
            coll,
            &[],
            &[(1, "a", Surrogate(901)), (2, "b", Surrogate(902))],
        );
        core.watermark = Lsn::new(100);
        core.checkpoint_columnar_engines()
            .expect("first checkpoint must publish");

        // Delete a row, then publish again at a higher LSN.
        core.columnar_engines
            .get_mut(&key)
            .expect("engine")
            .delete(&Value::Integer(1))
            .expect("delete");
        core.watermark = Lsn::new(200);
        core.checkpoint_columnar_engines()
            .expect("second checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(
            ids(&scan(&mut restored, coll, None)),
            vec![2],
            "the live generation is the SECOND one; the deleted row must not return"
        );
        assert_eq!(restored.floors.columnar_durable_lsn, Lsn::new(200));
    }

    /// A restore must not be undone by the schema seed that runs after it.
    /// `seed_columnar_schemas` creates an EMPTY engine for every catalog collection
    /// and skips those that already exist — so the restored engine must be the one
    /// that survives, or every restored row is silently discarded at boot.
    #[test]
    fn the_schema_seed_does_not_replace_a_restored_engine() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_seed_order";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(1001))], &[]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");
        // Exactly the boot order in `data/runtime.rs`: load, then seed.
        restored.seed_columnar_schemas(&[(
            nodedb_types::DatabaseId::DEFAULT,
            TenantId::new(1),
            coll.to_string(),
            schema(),
        )]);

        assert_eq!(
            ids(&scan(&mut restored, coll, None)),
            vec![1],
            "the seed must skip the restored engine, not overwrite it with an empty one"
        );
    }

    /// A manifest that exists but is corrupt must abort boot, not be treated as an
    /// absent generation: the WAL below the LSN it names may already be truncated,
    /// so silently restoring nothing would be permanent, unannounced data loss.
    #[test]
    fn a_corrupt_manifest_fails_the_load_instead_of_restoring_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_corrupt_manifest";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(1101))], &[]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        // Overwrite the published manifest's bytes with garbage — a truncated /
        // corrupted frame, not a missing file.
        let manifest = dir
            .path()
            .join("columnar-ckpt")
            .join("core-0")
            .join(paths::COLUMNAR_CKPT_MANIFEST);
        assert!(manifest.exists(), "the checkpoint published a manifest");
        std::fs::write(&manifest, b"not a valid checkpoint frame").expect("corrupt manifest");

        let mut restored = open_core(dir.path());
        let err = restored
            .load_columnar_checkpoints()
            .expect_err("a corrupt manifest must fail the load, not silently skip it");
        let _ = err;
    }
}
