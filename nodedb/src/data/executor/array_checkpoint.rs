// SPDX-License-Identifier: BUSL-1.1

//! Array engine checkpoint flush for `CoreLoop`.
//!
//! ## What is at stake
//!
//! `ArrayStore::memtable` is a plain in-memory `Memtable` (see
//! `engine::array::memtable`). Every `INSERT INTO ARRAY` / `DELETE FROM ARRAY`
//! lands there and advances the core watermark
//! (`dispatch::array::mutate` calls `note_write_lsn` with the Control Plane's
//! `wal_lsn`), so the periodic checkpoint reported those writes as durable and
//! the manager truncated the `ArrayPut` / `ArrayDelete` records that were their
//! only copy. The memtable is drained to a segment only by an explicit
//! `NDARRAY_FLUSH` or by the `flush_cell_threshold` auto-flush — neither of
//! which is ordered against the truncation the checkpoint authorises. A restart
//! after truncation returned an array missing every cell written since the last
//! time a user happened to run `NDARRAY_FLUSH`.
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
//! The bug was never a missing format; it was that the existing flush was
//! reachable only by an explicit user command. Calling it here is the fix.
//!
//! ## What LSN is durable after a flush
//!
//! Segments carry a `flush_lsn` and the manifest a `durable_lsn = max(flush_lsn)`
//! — "every WAL record at or below this is already in a segment". Stamping the
//! flush with the core watermark makes that claim true: this runs on the core's
//! own thread between tasks, and an array write reaches `note_write_lsn` (which
//! raises the watermark) only after `put_cells` / `delete_cells` has already
//! stamped the memtable, so every cell with `lsn <= watermark` is in the drain.
//!
//! An array whose memtable is empty flushes nothing and leaves its manifest's
//! `durable_lsn` where it stands. That is not a gap: an empty memtable means
//! every cell ever applied to this array is already in a segment, so the
//! watermark is durable for it either way. The stamp is left alone rather than
//! advanced with an empty write because doing so would rewrite every array's
//! manifest on every cycle to record a claim nothing reads back.
//!
//! ## Why the floor is the manifest, not `replay_floors.rs`
//!
//! Arrays flush independently of one another, so the "already durable through"
//! watermark is per-array and already recorded: it is the `durable_lsn` this
//! flush stamps into each array's manifest. `replay_array_wal` skips every
//! record at or below it.
//!
//! Re-applying such a record would usually be merely redundant — the writes are
//! absolute and keyed by `(tile, coord)` with `system_from_ms` taken from the
//! WAL payload, so it writes the identical version back. It is not redundant
//! after a bitemporal audit purge: the purge physically removes a superseded
//! tile-version from the flushed segment, and a still-retained `ArrayPut` below
//! the watermark would re-materialise exactly the version the purge erased.

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
            match self.array_engine.flush(id, durable_through.as_u64()) {
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
            "array checkpoint flushed"
        );
        Ok(durable_through)
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
    use crate::types::*;

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
            let cells = vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
                attrs: vec![CellValue::Int64(v)],
                surrogate: nodedb_types::Surrogate::ZERO,
                system_from_ms: 1,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            }];
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
            "every checkpointed cell must come back from its on-disk segment — \
             pre-fix the checkpoint flushed nothing and both cells were gone"
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

    /// A committed record applied at an LSN the array's durable watermark
    /// already covers is flushed into a segment: restart replay skips it, so
    /// the segment is its only copy.
    #[test]
    fn a_committed_record_applied_below_the_durable_lsn_is_flushed() {
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
        before
            .core
            .checkpoint_array_engines()
            .expect("flush at lsn 100");

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
            "the cell committed at lsn 50 survives a restart that replays nothing \
             at or below lsn 100"
        );
    }
}
