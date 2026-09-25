// SPDX-License-Identifier: BUSL-1.1

//! Array engine checkpoint flush for `CoreLoop`.
//!
//! ## What is at stake
//!
//! `ArrayStore::memtable` is a plain in-memory `Memtable` (see
//! `engine::array::memtable`). Every `INSERT INTO ARRAY` / `DELETE FROM ARRAY`
//! lands there and advances the core watermark (`dispatch::array::mutate`
//! calls `note_write_lsn` with the Control Plane's `wal_lsn`). The periodic
//! checkpoint reports those writes as durable, and the manager then removes
//! the `ArrayPut` / `ArrayDelete` records below that LSN. The checkpoint must
//! therefore flush every memtable first. An explicit `NDARRAY_FLUSH` and the
//! `flush_cell_threshold` flush in `dispatch::array::mutate` are not ordered
//! against that removal, so they cannot stand in for it.
//!
//! ## Why this reuses `ArrayEngine::flush` rather than writing a checkpoint blob
//!
//! Unlike KV / columnar / the sync gate, the array engine already has a real
//! durable form and a boot path that reads it:
//!
//! * `ArrayEngine::flush` drains the memtable into a compressed sparse segment,
//!   writes it with tmp+fsync+rename, installs it, and persists the per-array
//!   `manifest.ndam` — the manifest write being the commit point that makes the
//!   segment reachable.
//! * `ArrayStore::open` (reached from `ensure_array_open` on the first read after
//!   a restart, and from `ensure_array_open_for_replay` during WAL replay) loads
//!   that manifest and mmaps every segment it names.
//! * `ArrayStore::scan_tiles` and the bitemporal `scan_tiles_at` read segments
//!   and memtable together, so a restored segment answers a slice exactly as the
//!   memtable did before the flush.
//!
//! A checkpoint blob would therefore persist a second, redundant copy of state
//! the engine already knows how to write and read back — and a worse one: it
//! would bypass the tile compression, the per-tile MBR statistics the query
//! planner prunes on, and the compaction path that later merges those segments.
//! The checkpoint calls the existing flush instead.
//!
//! ## What a flush stamps
//!
//! Every flush publishes the core's replay stamp in the array's manifest: the
//! records this core applied, by prefix and ranges. This runs on the core's
//! own thread between tasks, and an applied write is noted before the next
//! task runs, so every record whose cells the memtable holds is named. A
//! record the stamp does not name was not applied yet, and replay applies it.
//!
//! The flush also reports the core watermark as this engine's durable point,
//! the LSN the checkpoint manager may truncate below.
//!
//! An array whose memtable is empty flushes nothing and leaves its manifest's
//! stamp where it stands. That is not a gap: an empty memtable means every
//! cell ever applied to this array is already in a segment, and the stamp of
//! the flush that wrote it named the record.
//!
//! ## Why the stamp is the manifest's, not `replay_floors.rs`
//!
//! Arrays flush independently of one another, so each array's manifest
//! carries the stamp of its own last flush. `replay_array_wal` skips a record
//! exactly when that stamp names it (`array_replay_skips`).
//!
//! Re-applying a named record is not merely redundant. Its cells would land in
//! the memtable while a segment already holds the same tile version, and the
//! next flush would write that version into a second segment. After a
//! bitemporal audit purge it is worse: the purge physically removes a
//! superseded tile-version from the flushed segment, and a still-retained
//! `ArrayPut` would re-materialise exactly the version the purge erased.

use nodedb_array::types::ArrayId;
use tracing::{info, warn};

use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush every array open on this core to disk and return the LSN the array
    /// engine is now durable through.
    ///
    /// Returns `Ok(watermark)` only once every array's segment AND manifest have
    /// landed. Any failure returns `Err` — the caller must then clamp the
    /// reported checkpoint LSN to the last LSN the arrays were known durable
    /// through, so a failed flush costs WAL growth instead of the cells it could
    /// not write.
    ///
    /// One array's failure does not abandon the others: every array is flushed
    /// and the first error is reported at the end. The reported LSN is clamped
    /// either way, so stopping early would buy nothing and cost the healthy
    /// arrays their durability — and, with a persistently broken array, cost it
    /// on every cycle from then on.
    ///
    /// Arrays not open on this core are not skipped state: a core only ever
    /// applied cells to arrays it opened, so an unopened array has no memtable
    /// here to lose. Its segments are on disk and its store is opened lazily by
    /// the first read (`ensure_array_open`) or by replay.
    pub(in crate::data::executor) fn checkpoint_array_engines(&mut self) -> crate::Result<Lsn> {
        let durable_through = self.watermark;
        let stamp = self.floors.applied_prefix.stamp()?;

        // Collected first: `flush` takes `&mut self.array_engine`, so the id
        // iterator cannot stay borrowed across the loop.
        let ids: Vec<ArrayId> = self.array_engine.array_ids().cloned().collect();
        if ids.is_empty() {
            return Ok(durable_through);
        }

        let mut flushed = 0usize;
        let mut first_error: Option<crate::Error> = None;
        for id in &ids {
            // `Ok(None)` = empty memtable, nothing to write; see the module docs
            // for why the watermark is still durable for that array.
            match self.array_engine.flush(id, stamp.clone()) {
                Ok(Some(_)) => flushed += 1,
                Ok(None) => {}
                Err(e) => {
                    let error = crate::Error::Storage {
                        engine: "array".to_string(),
                        detail: format!(
                            "array checkpoint: flush failed for tenant {} array {}: {e}",
                            id.tenant_id.as_u64(),
                            id.name
                        ),
                    };
                    // Every failure is logged where it happened; only the first
                    // is returned, since one clamp is all the caller can apply.
                    warn!(
                        core = self.core_id,
                        array = %id.name,
                        error = %error,
                        "array checkpoint flush failed for one array; continuing with \
                         the rest and clamping this core's checkpoint LSN"
                    );
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }

        info!(
            core = self.core_id,
            arrays = ids.len(),
            flushed,
            durable_through_lsn = durable_through.as_u64(),
            replay_prefix = stamp.prefix,
            applied_ranges = stamp.applied_above.len(),
            "array checkpoint flushed"
        );
        Ok(durable_through)
    }

    /// Flush `array_id` once its memtable reached the engine's cell
    /// threshold. [`Self::flush_array`] states what the flush stamps.
    pub(in crate::data::executor) fn flush_array_if_full(
        &mut self,
        array_id: &ArrayId,
    ) -> crate::Result<()> {
        let full = self
            .array_engine
            .needs_flush(array_id)
            .map_err(|e| array_flush_error(array_id, e))?;
        if full {
            self.flush_array(array_id)?;
        }
        Ok(())
    }

    /// Flush `array_id`, stamped with what this core applied.
    ///
    /// The caller noted every record it applied to the array before calling,
    /// so the stamp names each record whose cells the flush writes.
    pub(in crate::data::executor) fn flush_array(
        &mut self,
        array_id: &ArrayId,
    ) -> crate::Result<()> {
        let stamp = self.floors.applied_prefix.stamp()?;
        self.array_engine
            .flush(array_id, stamp)
            .map_err(|e| array_flush_error(array_id, e))?;
        Ok(())
    }
}

fn array_flush_error(
    array_id: &ArrayId,
    e: crate::engine::array::engine::ArrayEngineError,
) -> crate::Error {
    crate::Error::Storage {
        engine: "array".to_string(),
        detail: format!("flush of array '{}': {e}", array_id.name),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_array::query::slice::Slice as ArraySlice;
    use nodedb_array::schema::ArraySchema;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::ArrayOp;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::replay_stamp::{LsnRange, ReplayStamp};
    use crate::types::*;
    use nodedb_wal::record::{RecordType, WalRecordArgs};
    use nodedb_wal::{TombstoneSet, WalRecord};

    const TID: u64 = 1;

    /// A core over a caller-owned data dir, so a restart can be modelled by
    /// dropping one and opening the next over the same directory — the harness
    /// in `dispatch/array/tests_dispatch.rs` owns its tempdir and cannot.
    struct Core {
        core: CoreLoop,
        req_tx: Producer<BridgeRequest>,
        resp_rx: Consumer<BridgeResponse>,
        next_id: u64,
    }

    impl Core {
        fn open_at(dir: &std::path::Path) -> Self {
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
            let core = CoreLoop::open(
                0,
                req_rx,
                resp_tx,
                dir,
                Arc::new(nodedb_types::OrdinalClock::new()),
                crate::data::executor::core_loop::test_governor(),
            )
            .expect("CoreLoop::open");
            Self {
                core,
                req_tx,
                resp_rx,
                next_id: 1,
            }
        }

        fn send(&mut self, op: ArrayOp) -> Response {
            let id = self.next_id;
            self.next_id += 1;
            self.req_tx
                .try_push(BridgeRequest::unfloored(Request {
                    request_id: RequestId::new(id),
                    tenant_id: TenantId::new(TID),
                    database_id: DatabaseId::DEFAULT,
                    vshard_id: VShardId::new(0),
                    plan: PhysicalPlan::Array(op),
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
                    admission: crate::bridge::envelope::Admission::Admitted,
                }))
                .expect("push request");
            self.core.tick();
            self.resp_rx.try_pop().expect("response").inner
        }

        /// `OpenArray` — the same dispatch the Control Plane broadcasts on
        /// `CREATE ARRAY`, and (via the catalog) what a read auto-opens with
        /// after a restart.
        fn open_array(&mut self, id: &ArrayId) {
            let r = self.send(ArrayOp::OpenArray {
                array_id: id.clone(),
                schema_msgpack: zerompk::to_msgpack_vec(&schema()).expect("encode schema"),
                schema_hash: SCHEMA_HASH,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            });
            assert_eq!(r.status, Status::Ok, "open array: {r:?}");
        }

        fn put(&mut self, id: &ArrayId, x: i64, y: i64, v: i64, wal_lsn: u64) {
            self.put_at(id, x, y, v, 1, wal_lsn);
        }

        /// `put` with an explicit system time, for bitemporal versions.
        fn put_at(&mut self, id: &ArrayId, x: i64, y: i64, v: i64, sys_ms: i64, wal_lsn: u64) {
            let cells = vec![cell(x, y, v, sys_ms)];
            let r = self.send(ArrayOp::Put {
                array_id: id.clone(),
                cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
                wal_lsn,
                provenance: None,
            });
            assert_eq!(r.status, Status::Ok, "array put: {r:?}");
        }

        /// Every cell an unbounded slice returns, as `(x, y, v)`, sorted.
        fn slice_all(&mut self, id: &ArrayId) -> Vec<(i64, i64, i64)> {
            let slice = ArraySlice {
                dim_ranges: vec![None, None],
            };
            let r = self.send(ArrayOp::Slice {
                array_id: id.clone(),
                slice_msgpack: zerompk::to_msgpack_vec(&slice).expect("encode slice"),
                attr_projection: vec![],
                limit: 0,
                cell_filter: None,
                hilbert_range: None,
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            });
            assert_eq!(r.status, Status::Ok, "array slice: {r:?}");
            decode_cells(r.payload.as_bytes())
        }
    }

    const SCHEMA_HASH: u64 = 0xA55E7;

    fn aid() -> ArrayId {
        ArrayId::new(TenantId::new(TID), "grid")
    }

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4, 4])
            .build()
            .expect("build schema")
    }

    /// Decode a slice response into `(x, y, v)` triples.
    fn decode_cells(bytes: &[u8]) -> Vec<(i64, i64, i64)> {
        use crate::data::executor::response_codec::ArraySliceResponse;
        let envelope: ArraySliceResponse =
            zerompk::from_msgpack(bytes).expect("slice response envelope");
        let json = nodedb_types::msgpack_to_json_string(&envelope.rows_msgpack)
            .expect("slice rows msgpack to json");
        let rows: serde_json::Value = serde_json::from_str(&json).expect("slice rows json");
        let mut out: Vec<(i64, i64, i64)> = rows
            .as_array()
            .expect("slice rows array")
            .iter()
            .map(|row| {
                let coords = row["coords"].as_array().expect("coords");
                let attrs = row["attrs"].as_array().expect("attrs");
                (
                    coords[0].as_i64().expect("x"),
                    coords[1].as_i64().expect("y"),
                    attrs[0].as_i64().expect("v"),
                )
            })
            .collect();
        out.sort_unstable();
        out
    }

    /// The whole point of this checkpoint: cells written but never explicitly
    /// flushed must still answer a slice after a restart. Drives the real put
    /// and slice dispatch paths, and models the restart by dropping the core and
    /// opening a second one over the same data dir — its memtable starts empty,
    /// exactly as it does once truncation has deleted the `ArrayPut` records, so
    /// only the checkpoint's flush can make these assertions hold.
    #[test]
    fn checkpointed_cells_answer_a_slice_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();

        let mut before = Core::open_at(dir.path());
        before.open_array(&id);
        before.put(&id, 1, 2, 30, 10);
        // A second cell in a different tile (extents are 4x4), so the flush has
        // to carry more than one tile.
        before.put(&id, 9, 9, 40, 20);
        assert_eq!(
            before.slice_all(&id),
            vec![(1, 2, 30), (9, 9, 40)],
            "both cells must be live in the memtable before any flush"
        );
        before.core.advance_watermark(Lsn::new(20));

        let reported = before
            .core
            .checkpoint_array_engines()
            .expect("flush to a writable dir must succeed");
        assert_eq!(
            reported,
            Lsn::new(20),
            "the flush must report exactly the LSN it made durable — the manager \
             deletes WAL segments below whatever this returns"
        );

        drop(before);

        let mut after = Core::open_at(dir.path());
        // The catalog is per-process state the Control Plane seeds from
        // `_system.arrays`; re-opening here is what a real restart's first read
        // does via `ensure_array_open`.
        after.open_array(&id);
        assert_eq!(
            after.slice_all(&id),
            vec![(1, 2, 30), (9, 9, 40)],
            "every checkpointed cell must come back from its on-disk segment"
        );
    }

    /// A flush with an empty memtable must still report the watermark: every
    /// cell it holds is already in a segment, so clamping there would pin WAL
    /// truncation for no reason.
    #[test]
    fn empty_memtable_reports_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();

        let mut core = Core::open_at(dir.path());
        core.open_array(&id);
        core.put(&id, 1, 1, 7, 5);
        core.core.advance_watermark(Lsn::new(5));
        core.core.checkpoint_array_engines().expect("first flush");

        core.core.advance_watermark(Lsn::new(900));
        assert_eq!(
            core.core.checkpoint_array_engines().expect("second flush"),
            Lsn::new(900),
            "nothing was written since the last flush, so the array engine is \
             durable through the current watermark"
        );
    }

    /// A core with no arrays open reports the watermark rather than clamping —
    /// it holds no array state at all, so it can never be the reason the WAL
    /// must be kept.
    #[test]
    fn no_arrays_reports_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = Core::open_at(dir.path());
        core.core.advance_watermark(Lsn::new(42));
        assert_eq!(
            core.core.checkpoint_array_engines().expect("flush"),
            Lsn::new(42)
        );
    }

    /// A cell written AFTER the checkpoint stays in the memtable and must still
    /// be readable — the flush drains, it does not discard, and the segment plus
    /// memtable are read together.
    #[test]
    fn cells_written_after_the_flush_are_still_live() {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();

        let mut core = Core::open_at(dir.path());
        core.open_array(&id);
        core.put(&id, 1, 2, 30, 10);
        core.core.advance_watermark(Lsn::new(10));
        core.core.checkpoint_array_engines().expect("flush");
        core.put(&id, 3, 3, 50, 11);

        assert_eq!(
            core.slice_all(&id),
            vec![(1, 2, 30), (3, 3, 50)],
            "the flushed segment and the live memtable must read as one array"
        );
    }

    /// A committed record applied at an LSN the array's manifest stamp
    /// already names is flushed into a segment: restart replay skips it, so
    /// the segment is its only copy.
    #[test]
    fn a_committed_record_the_manifest_stamp_names_is_flushed() {
        use crate::data::executor::handlers::transaction::redo_apply::CommittedRedo;
        use crate::engine::array::wal::{ArrayPutPayload, encode_put_with_version};
        use crate::wal::{RedoRecord, RedoSubRecord};
        use nodedb_wal::record::RecordType;

        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();

        let mut before = Core::open_at(dir.path());
        before.open_array(&id);
        before.put(&id, 1, 2, 30, 100);
        before.core.advance_watermark(Lsn::new(100));
        // The outcome floor passed lsn 50 while that record was still on its
        // way to this core, so the flush's stamp prefix names it.
        before
            .core
            .floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(100));
        before
            .core
            .checkpoint_array_engines()
            .expect("flush stamped through lsn 100");

        let payload = encode_put_with_version(&ArrayPutPayload {
            array_id: id.clone(),
            cells: vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(9), CoordValue::Int64(9)],
                attrs: vec![CellValue::Int64(40)],
                surrogate: nodedb_types::Surrogate::ZERO,
                system_from_ms: 1,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            }],
            provenance: None,
        })
        .expect("encode put");
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::ArrayPut as u32,
                payload,
            }],
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo");
        let mut task = crate::data::executor::core_loop::tests::make_default_task();
        task.wal_lsn = Some(Lsn::new(50));
        let response = before.core.execute_apply_transaction_redo(
            &task,
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );
        assert_eq!(response.status, Status::Ok, "apply: {response:?}");
        drop(before);

        let mut after = Core::open_at(dir.path());
        after.open_array(&id);
        assert_eq!(
            after.slice_all(&id),
            vec![(1, 2, 30), (9, 9, 40)],
            "the cell committed at lsn 50 survives a restart that skips every \
             record the stamp names"
        );
    }

    fn cell(x: i64, y: i64, v: i64, sys_ms: i64) -> ArrayPutCell {
        ArrayPutCell {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            attrs: vec![CellValue::Int64(v)],
            surrogate: nodedb_types::Surrogate::ZERO,
            system_from_ms: sys_ms,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    /// The `ArrayPut` WAL record a live put of one cell appends.
    fn put_record(id: &ArrayId, x: i64, y: i64, v: i64, sys_ms: i64, lsn: u64) -> WalRecord {
        use crate::engine::array::wal::{ArrayPutPayload, encode_put_with_version};
        let payload = encode_put_with_version(&ArrayPutPayload {
            array_id: id.clone(),
            cells: vec![cell(x, y, v, sys_ms)],
            provenance: None,
        })
        .expect("encode put");
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::ArrayPut as u32,
            lsn,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// Restart the way `replay_all_wal` does: raise the floor to the WAL end,
    /// then replay `records` into the array.
    fn restart_and_replay(dir: &std::path::Path, id: &ArrayId, records: &[WalRecord]) -> Core {
        let mut core = Core::open_at(dir);
        core.open_array(id);
        let wal_end = records.iter().map(|r| r.header.lsn).max().unwrap_or(0);
        core.core
            .floors
            .applied_prefix
            .seed_replayed_through(Lsn::new(wal_end));
        core.core.replay_array_wal(records, 1, &TombstoneSet::new());
        core
    }

    /// Tiles over every segment the array's manifest names.
    fn segment_tiles(core: &Core, id: &ArrayId) -> u32 {
        core.core
            .array_engine
            .store(id)
            .expect("array open")
            .manifest()
            .segments
            .iter()
            .map(|s| s.tile_count)
            .sum()
    }

    /// The replay gate asks the stamp in the array's manifest, record by
    /// record. A record below the highest named LSN that the stamp does not
    /// name replays.
    #[test]
    fn array_replay_skips_exactly_the_records_the_stamp_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();
        let mut core = Core::open_at(dir.path());
        core.open_array(&id);
        core.core
            .floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(5));
        core.put(&id, 1, 1, 10, 10);
        core.core.checkpoint_array_engines().expect("flush");

        assert_eq!(
            core.core
                .array_engine
                .store(&id)
                .expect("open")
                .manifest()
                .replay,
            ReplayStamp {
                prefix: 5,
                applied_above: vec![LsnRange { start: 10, end: 10 }],
            }
        );
        for (lsn, skips) in [(3, true), (5, true), (7, false), (10, true), (11, false)] {
            assert_eq!(
                core.core.array_replay_skips(&id, lsn),
                skips,
                "record at lsn {lsn}"
            );
        }
    }

    /// A threshold flush stamps the record whose write filled the memtable.
    #[test]
    fn a_threshold_flush_stamps_the_write_that_filled_the_memtable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();
        let mut core = Core::open_at(dir.path());
        core.core.array_engine.set_flush_cell_threshold(1);
        core.open_array(&id);
        core.put(&id, 1, 1, 10, 40);

        let manifest = core.core.array_engine.store(&id).expect("open").manifest();
        assert_eq!(manifest.segments.len(), 1, "the put filled the memtable");
        assert!(
            manifest.replay.skips(40),
            "the threshold flush names the record it wrote: {:?}",
            manifest.replay
        );
    }

    /// Floor 10. B at lsn 30 applies, a checkpoint flushes it, then A at lsn
    /// 20 applies. A restart replays A once and never re-applies B, and the
    /// array reads as it did live.
    #[test]
    fn an_array_write_in_flight_at_a_checkpoint_replays_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();
        let records = [
            put_record(&id, 1, 1, 20, 1, 20),
            put_record(&id, 9, 9, 30, 1, 30),
        ];

        let mut before = Core::open_at(dir.path());
        before.open_array(&id);
        before
            .core
            .floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(10));
        before.put(&id, 9, 9, 30, 30);
        before.core.checkpoint_array_engines().expect("flush B");
        before.put(&id, 1, 1, 20, 20);
        let live = before.slice_all(&id);
        assert_eq!(live, vec![(1, 1, 20), (9, 9, 30)]);
        drop(before);

        let mut after = restart_and_replay(dir.path(), &id, &records);
        assert_eq!(after.slice_all(&id), live, "replay equals live");
        after.core.checkpoint_array_engines().expect("flush A");
        assert_eq!(
            segment_tiles(&after, &id),
            2,
            "B's tile is in the first segment only; the second holds A alone"
        );
        drop(after);

        // Every record is named now: a second restart applies nothing.
        let mut again = restart_and_replay(dir.path(), &id, &records);
        assert_eq!(again.slice_all(&id), live);
        assert_eq!(segment_tiles(&again, &id), 2);
        again.core.checkpoint_array_engines().expect("empty flush");
        assert_eq!(
            segment_tiles(&again, &id),
            2,
            "nothing replayed into memory"
        );
    }

    /// An audit purge removes a superseded tile version from a segment. The
    /// records that wrote it are named by the stamp, so replay does not bring
    /// the version back.
    #[test]
    fn replay_does_not_rematerialise_a_purged_version() {
        use nodedb_array::query::ceiling::CeilingResult;

        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();
        let records = [
            put_record(&id, 0, 0, 100, 100, 1),
            put_record(&id, 0, 0, 200, 200, 2),
            put_record(&id, 0, 0, 300, 300, 3),
        ];
        let ceiling = |core: &Core, sys: i64| {
            core.core
                .array_engine
                .store(&id)
                .expect("open")
                .ceiling_for_coord(&[CoordValue::Int64(0), CoordValue::Int64(0)], sys, None)
                .expect("ceiling")
        };

        let mut before = Core::open_at(dir.path());
        before.open_array(&id);
        for (v, lsn) in [(100, 1), (200, 2), (300, 3)] {
            before.put_at(&id, 0, 0, v, v, lsn);
            before
                .core
                .checkpoint_array_engines()
                .expect("flush one version");
        }
        let dropped = before
            .core
            .array_engine
            .temporal_purge(TenantId::new(TID), DatabaseId::DEFAULT, &id.name, 250)
            .expect("purge");
        assert!(dropped >= 1, "the purge dropped the superseded versions");
        assert!(matches!(ceiling(&before, 249), CeilingResult::NotFound));
        drop(before);

        let after = restart_and_replay(dir.path(), &id, &records);
        assert!(
            matches!(ceiling(&after, 249), CeilingResult::NotFound),
            "a purged version came back from replay: {:?}",
            ceiling(&after, 249)
        );
        assert!(matches!(ceiling(&after, i64::MAX), CeilingResult::Live(_)));
    }

    /// A restart opens only the arrays its WAL tail writes. The catalog still
    /// holds every array, as the Control Plane seeds it from `_system.arrays`
    /// at boot, so the first write to an array nothing reopened opens it from
    /// the catalog instead of failing as unknown.
    #[test]
    fn the_first_write_after_a_restart_opens_its_array_from_the_catalog() {
        use crate::control::array_catalog::ArrayCatalogEntry;

        let dir = tempfile::tempdir().expect("tempdir");
        let id = aid();

        let mut before = Core::open_at(dir.path());
        before.open_array(&id);
        before.put(&id, 1, 2, 30, 10);
        before.core.checkpoint_array_engines().expect("flush");
        drop(before);

        let mut after = Core::open_at(dir.path());
        after
            .core
            .array_catalog
            .write()
            .expect("catalog lock")
            .register(ArrayCatalogEntry {
                array_id: id.clone(),
                name: id.name.clone(),
                schema_msgpack: zerompk::to_msgpack_vec(&schema()).expect("encode schema"),
                schema_hash: SCHEMA_HASH,
                created_at_ms: 0,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            })
            .expect("seed the catalog");
        assert!(
            after.core.array_engine.store(&id).is_err(),
            "nothing opened the array on this core yet"
        );

        after.put(&id, 3, 3, 50, 20);
        assert_eq!(
            after.slice_all(&id),
            vec![(1, 2, 30), (3, 3, 50)],
            "the write lands beside the cells the array held before the restart"
        );
    }
}
