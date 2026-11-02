// SPDX-License-Identifier: BUSL-1.1

//! Cursor-paginated raw columnar scan for the clone materializer.
//!
//! Returns `(surrogate_u32, value_bytes)` pairs plus the next-cursor in a
//! single msgpack payload so the Control Plane materializer can drive the scan
//! to completion in O(N / count) round-trips.
//!
//! The scan covers both in-memory memtable rows and flushed segment bytes so
//! it is complete regardless of whether the collection has been flushed. This
//! single handler covers all three columnar profiles — Plain, Timeseries, and
//! Spatial — because they share the same `MutationEngine` storage layer.
//!
//! Every collection emits, per primary key, the version a read at the cut
//! answers: the newest at or below it, and nothing for a key a delete
//! ended by then. Without a cut it emits current rows.
//!
//! ## Response payload (msgpack)
//! ```text
//! [ next_cursor: bin,
//!   entries: [ [surrogate: u32, value_bytes: bin, sys_from: int | nil], ... ] ]
//! ```
//! `sys_from` is the system-time ordinal of the row version read. A clone
//! copy installs at exactly that ordinal. A timeseries row without a stored
//! `_ts_system` carries nil.
//! `next_cursor` encodes the last-seen row position as an 8-byte big-endian
//! `(segment_id: u32, row_index: u32)` pair so the scan can resume across
//! round-trips. `segment_id == 0` means the row came from the active memtable.
//! Empty cursor = scan complete.

use nodedb_types::value::Value;

use nodedb_columnar::VersionCut;
use nodedb_columnar::pk_index::RowLocation;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::flushed_segment::SegmentBlocks;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a cursor-paginated raw columnar scan for the clone materializer.
    pub(in crate::data::executor) fn execute_columnar_materialize_scan(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        cursor: &[u8],
        count: usize,
        system_as_of_ms: Option<i64>,
    ) -> Response {
        let _scan_guard =
            match self.acquire_scan_guard(task, task.request.tenant_id.as_u64(), collection) {
                Ok(g) => g,
                Err(resp) => return resp,
            };

        let tid = task.request.tenant_id;
        let columnar_key = (task.request.database_id, tid, collection.to_string());

        let Some(engine) = self.columnar_engines.get(&columnar_key) else {
            // Not a plain/spatial collection. Check if it is a timeseries
            // collection (data lives in columnar_memtables / ts_registries).
            let has_ts_memtable = self
                .columnar_memtables
                .get(&columnar_key)
                .is_some_and(|mt| !mt.is_empty());
            let has_ts_partitions = self.ts_registries.contains_key(&columnar_key);

            if has_ts_memtable || has_ts_partitions {
                return self.execute_ts_materialize_scan(
                    task,
                    collection,
                    cursor,
                    count,
                    system_as_of_ms,
                );
            }

            // Empty collection — return zero entries with empty cursor.
            return build_response(self, task, Vec::new(), Vec::new());
        };

        let schema = engine.schema().clone();
        // The millisecond cutoff reads at the last ordinal of its
        // millisecond. Without one the clone reads current rows.
        let cut = system_as_of_ms
            .map(nodedb_types::ms_to_ordinal_upper)
            .map_or(VersionCut::Current, VersionCut::At);

        // Cursor encodes (segment_id: u32 BE, row_index: u32 BE).
        // segment_id == 0 means "memtable" (position within memtable rows).
        // segment_id >= 1 means "flushed segment N" (1-based).
        let (start_segment, start_row) = parse_cursor(cursor);

        let mut entries: Vec<(u32, Vec<u8>, Option<i64>)> = Vec::with_capacity(count.min(256));
        let mut last_segment: u32 = start_segment;
        let mut last_row: u32 = start_row;

        // ── Phase 1: flushed segments ────────────────────────────────────────
        // We scan flushed segments (segment_id >= 1) before the active memtable
        // because segments hold older rows and the cursor walks ascending segment
        // ids so restart-safety is trivial (the cursor always moves forward).
        let flushed: Vec<Vec<u8>> = self
            .columnar_flushed_segments
            .get(&columnar_key)
            .cloned()
            .unwrap_or_default();

        // A memtable cursor (`start_segment == 0`) resumes past every flushed
        // segment, so the segment phase reads nothing. Reading them again
        // would emit each flushed row once per page.
        let segments_to_scan = if start_segment == 0 {
            &flushed[..0]
        } else {
            &flushed[..]
        };
        'seg_loop: for (seg_idx, seg_bytes) in segments_to_scan.iter().enumerate() {
            let seg_id = (seg_idx as u32) + 1; // 1-based

            // Skip segments already fully consumed by a prior page, and
            // segments whose every row was created after the cut.
            if seg_id < start_segment || engine.segment_skippable(u64::from(seg_id), cut) {
                continue;
            }

            // A segment that does not read refuses the scan. Skipping it
            // would hand the materializer a clone without its rows.
            let segment = match self.decode_flushed_segment(
                &columnar_key,
                u64::from(seg_id),
                seg_bytes,
                schema.columns.len(),
                SegmentBlocks::Visible {
                    engine,
                    cut,
                    predicates: &[],
                },
                "columnar_materialize_scan",
            ) {
                Ok(segment) => segment,
                Err(e) => return self.response_error(task, e),
            };
            let row_count = segment.row_count();

            // Starting row within this segment.
            let first_row_in_seg = if seg_id == start_segment {
                start_row as usize
            } else {
                0
            };

            // Resolve the per-row surrogate sidecar for this segment.
            // `columnar_flushed_surrogates` is indexed by `seg_idx` (0-based),
            // in lockstep with `columnar_flushed_segments`.
            let seg_surrogates: Option<&Vec<Option<nodedb_types::Surrogate>>> = self
                .columnar_flushed_surrogates
                .get(&columnar_key)
                .and_then(|segs| segs.get(seg_idx));

            for row_idx in first_row_in_seg..row_count {
                // A row the cut does not see: ended for a current read,
                // outside its interval for a read at an ordinal. A block the
                // read did not decode holds no such row.
                if !segment.holds_row(row_idx)
                    || !engine.row_visible(u64::from(seg_id), row_idx as u32, cut)
                {
                    continue;
                }

                // Build a Value::Object for this row.
                let row = match segment.row(&schema, row_idx) {
                    Ok(row) => row,
                    Err(e) => return self.response_error(task, e),
                };
                let map: std::collections::HashMap<String, Value> = schema
                    .columns
                    .iter()
                    .map(|col_def| col_def.name.clone())
                    .zip(row)
                    .collect();

                // Encode as msgpack value bytes (the Insert handler reads this
                // format). A row that does not encode refuses the scan rather
                // than leaving it out of the clone.
                let value_bytes = match encode_row(&Value::Object(map)) {
                    Ok(b) => b,
                    Err(e) => return self.response_error(task, e),
                };

                // Emit the real per-row surrogate when available so the
                // Control Plane's tombstone/copyup probe keys on the same
                // value that was written at flush time. Fall back to the
                // synthetic pack only for rows that have no recorded surrogate
                // (e.g. segments restored from a pre-surrogate backup).
                let row_surrogate = seg_surrogates
                    .and_then(|s| s.get(row_idx))
                    .copied()
                    .flatten();
                let surrogate_u32: u32 = row_surrogate
                    .map(|s| s.as_u32())
                    .unwrap_or_else(|| encode_seg_row_as_u32(seg_id, row_idx as u32));

                entries.push((
                    surrogate_u32,
                    value_bytes,
                    Some(row_sys_from(engine, u64::from(seg_id), row_idx as u32)),
                ));
                last_segment = seg_id;
                last_row = (row_idx + 1) as u32; // exclusive next position

                if entries.len() >= count {
                    break 'seg_loop;
                }
            }
        }

        // ── Phase 2: active memtable rows ────────────────────────────────────
        // The segment loop stops early only when the page is full, so a page
        // with room left has read every flushed segment. The memtable then
        // starts at the cursor's row for a memtable cursor, else at its first
        // row. A run of tombstoned rows at the end of the segments does not
        // hold the scan back from the memtable.
        if entries.len() < count {
            let memtable_start_row = if start_segment == 0 {
                start_row as usize
            } else {
                0
            };

            // Rows the cut sees, each with its physical memtable index, so
            // the cursor resumes at the next physical row.
            let rows_with_surrogates: Vec<(u32, Option<nodedb_types::Surrogate>, Vec<Value>)> =
                match engine
                    .scan_memtable_versions(cut)
                    .filter(|scanned| {
                        !matches!(scanned, Ok((idx, _, _)) if (*idx as usize) < memtable_start_row)
                    })
                    .collect::<Result<_, _>>()
                {
                    Ok(rows) => rows,
                    Err(e) => return self.response_error(task, crate::Error::from(e)),
                };

            for (mt_idx, row_surrogate, row) in rows_with_surrogates.iter() {
                // Build Value::Object.
                let mut map = std::collections::HashMap::new();
                for (col_idx, col_def) in schema.columns.iter().enumerate() {
                    if col_idx < row.len() {
                        map.insert(col_def.name.clone(), row[col_idx].clone());
                    }
                }
                let value_bytes = match encode_row(&Value::Object(map)) {
                    Ok(b) => b,
                    Err(e) => return self.response_error(task, e),
                };

                let abs_row = *mt_idx as usize;
                let surrogate_u32: u32 = row_surrogate.map(|s| s.as_u32()).unwrap_or_else(|| {
                    // No recorded surrogate — use a hash-based synthetic value.
                    // segment_id=0 (memtable), row position in lower 24 bits.
                    (abs_row as u32) | 0x8000_0000
                });

                entries.push((
                    surrogate_u32,
                    value_bytes,
                    Some(row_sys_from(engine, engine.memtable_segment_id(), *mt_idx)),
                ));
                last_segment = 0;
                last_row = (abs_row + 1) as u32;

                if entries.len() >= count {
                    break;
                }
            }
        }

        // Build next-cursor: empty when fewer entries than requested (scan done).
        let next_cursor = if entries.len() < count {
            Vec::new()
        } else {
            encode_cursor(last_segment, last_row)
        };

        build_response(self, task, entries, next_cursor)
    }
}

/// The system-time ordinal of the row at `row` of segment `segment_id`. A
/// row with no recorded stamp reads at every cut, as `row_visible` judges
/// it, so it carries the lowest ordinal.
fn row_sys_from(engine: &nodedb_columnar::MutationEngine, segment_id: u64, row: u32) -> i64 {
    engine
        .sys_from_at(RowLocation {
            segment_id,
            row_index: row,
        })
        .unwrap_or(i64::MIN)
}

/// Encode one materialized row as MessagePack value bytes, the format the
/// Insert handler reads.
fn encode_row(row: &Value) -> crate::Result<Vec<u8>> {
    nodedb_types::value_to_msgpack(row).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("encode materialized columnar row: {e}"),
    })
}

/// Encode `(seg_id, row_idx)` as a compact 32-bit tag.
///
/// This is the tag for flushed-segment rows that have no recorded surrogate
/// in `columnar_flushed_surrogates`: rows of an insert that carried none.
/// Every other row carries a real surrogate that matches the
/// tombstone/copyup write side exactly.
///
/// Packs `seg_id` in the upper 16 bits and `row_idx` in the lower 16 bits.
pub(super) fn encode_seg_row_as_u32(seg_id: u32, row_idx: u32) -> u32 {
    (seg_id & 0xFFFF) << 16 | (row_idx & 0xFFFF)
}

/// Parse a cursor produced by a prior call. Returns (segment_id, row_index).
/// Empty cursor → (1, 0) which starts at the first flushed segment.
pub(super) fn parse_cursor(cursor: &[u8]) -> (u32, u32) {
    if cursor.len() < 8 {
        // Fresh scan: start with flushed segments first (segment_id = 1).
        // If there are none we move to memtable (segment_id = 0).
        // We use segment_id = 1 so the first iteration enters the flushed-
        // segment loop; the loop will simply produce nothing if len == 0.
        return (1, 0);
    }
    let seg = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    let row = u32::from_be_bytes([cursor[4], cursor[5], cursor[6], cursor[7]]);
    (seg, row)
}

/// Encode the resume cursor as 8 bytes.
pub(super) fn encode_cursor(segment_id: u32, row_index: u32) -> Vec<u8> {
    let mut c = Vec::with_capacity(8);
    c.extend_from_slice(&segment_id.to_be_bytes());
    c.extend_from_slice(&row_index.to_be_bytes());
    c
}

/// Serialize the result payload and wrap in a `Response`.
pub(super) fn build_response(
    core: &CoreLoop,
    task: &ExecutionTask,
    entries: Vec<(u32, Vec<u8>, Option<i64>)>,
    next_cursor: Vec<u8>,
) -> Response {
    let mut payload = Vec::with_capacity(
        entries.iter().map(|(_, v, _)| v.len() + 17).sum::<usize>() + next_cursor.len() + 16,
    );

    nodedb_query::msgpack_scan::write_array_header(&mut payload, 2);
    write_bin(&mut payload, &next_cursor);
    nodedb_query::msgpack_scan::write_array_header(&mut payload, entries.len());
    for (surrogate, value_bytes, sys_from) in &entries {
        nodedb_query::msgpack_scan::write_array_header(&mut payload, 3);
        write_u32(&mut payload, *surrogate);
        write_bin(&mut payload, value_bytes);
        match sys_from {
            Some(sys_from) => nodedb_query::msgpack_scan::write_i64(&mut payload, *sys_from),
            None => nodedb_query::msgpack_scan::write_null(&mut payload),
        }
    }

    if entries.is_empty() && next_cursor.is_empty() {
        // Nothing to return — status Ok with empty payload is the contract.
        return core.response_with_payload(task, payload);
    }

    if let Some(ref m) = core.metrics {
        m.record_query();
    }

    core.response_with_payload(task, payload)
}

/// Append a msgpack `bin` value to `out`.
fn write_bin(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= u8::MAX as usize {
        out.push(0xc4);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xc5);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xc6);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

/// Append a msgpack `u32` value to `out`.
fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.push(0xce);
    out.extend_from_slice(&v.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_columnar::MutationEngine;
    use nodedb_types::Surrogate;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("val", ColumnType::Int64),
        ])
        .expect("valid schema")
    }

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open");
        (core, dir)
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
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
            commit_hlc: None,
            system_ordinal: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Flush `rows` into a single flushed segment on `core`, mirroring the
    /// production flush block in `handlers/columnar_write/insert.rs`.
    fn insert_and_flush(
        core: &mut CoreLoop,
        collection: &str,
        rows: &[(i64, i64, Surrogate)],
    ) -> (DatabaseId, TenantId, String) {
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            collection.to_string(),
        );
        let mut engine = MutationEngine::new(collection.to_string(), schema());
        for (id, val, surr) in rows {
            engine
                .insert_with_surrogate(&[Value::Integer(*id), Value::Integer(*val)], *surr, *id)
                .expect("insert_with_surrogate");
        }

        let new_segment_id = engine.next_segment_id();
        let (seg_schema, columns, row_count) = engine.memtable_mut().drain_optimized();
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
        core.columnar_engines.insert(key.clone(), engine);
        key
    }

    /// Parse the response payload from `execute_columnar_materialize_scan`.
    /// Returns `(surrogates, next_cursor)`.
    fn parse_response(payload: &[u8]) -> (Vec<u32>, Vec<u8>) {
        use nodedb_query::msgpack_scan;

        if payload.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let (outer_len, mut off) = msgpack_scan::array_header(payload, 0).expect("outer array");
        assert_eq!(outer_len, 2);
        let next_cursor = msgpack_scan::read_bin_advance(payload, &mut off)
            .expect("cursor bin")
            .to_vec();
        let (entry_count, mut entry_off) =
            msgpack_scan::array_header(payload, off).expect("entries array");
        let mut surrogates = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let (pair_len, mut pair_off) =
                msgpack_scan::array_header(payload, entry_off).expect("pair array");
            assert_eq!(pair_len, 3);
            let surrogate =
                msgpack_scan::read_u32_advance(payload, &mut pair_off).expect("surrogate u32");
            let _value = msgpack_scan::read_bin_advance(payload, &mut pair_off).expect("value bin");
            assert!(
                msgpack_scan::read_i64(payload, pair_off).is_some(),
                "a columnar row carries its version stamp"
            );
            pair_off = msgpack_scan::skip_value(payload, pair_off).expect("stamp");
            surrogates.push(surrogate);
            entry_off = pair_off;
        }
        (surrogates, next_cursor)
    }

    /// Flushed columnar segment rows with recorded surrogates must emit those
    /// real surrogates — not the `encode_seg_row_as_u32` packed value — so that
    /// the Control Plane's tombstone/copyup probe keys on the same value written
    /// at flush time.
    #[test]
    fn flushed_segment_emits_real_surrogates() {
        let (mut core, _dir) = make_core();
        let coll = "mat_surr_test";

        let s10 = Surrogate::new(10);
        let s20 = Surrogate::new(20);
        let s30 = Surrogate::new(30);

        insert_and_flush(
            &mut core,
            coll,
            &[(1, 100, s10), (2, 200, s20), (3, 300, s30)],
        );

        let task = make_task();
        let resp = core.execute_columnar_materialize_scan(&task, coll, &[], 64, None);

        let (emitted_surrogates, next_cursor) = parse_response(resp.payload.as_bytes());

        assert!(
            next_cursor.is_empty(),
            "scan should be complete in one page"
        );
        assert_eq!(emitted_surrogates.len(), 3, "all three rows returned");

        // Real surrogates must match what was recorded at flush time.
        assert_eq!(emitted_surrogates[0], s10.as_u32());
        assert_eq!(emitted_surrogates[1], s20.as_u32());
        assert_eq!(emitted_surrogates[2], s30.as_u32());

        // Confirm none of the emitted values equal the synthetic fallback for
        // these positions: encode_seg_row_as_u32(seg_id=1, row_idx).
        let synthetic_row0 = super::encode_seg_row_as_u32(1, 0);
        let synthetic_row1 = super::encode_seg_row_as_u32(1, 1);
        let synthetic_row2 = super::encode_seg_row_as_u32(1, 2);
        assert_ne!(emitted_surrogates[0], synthetic_row0);
        assert_ne!(emitted_surrogates[1], synthetic_row1);
        assert_ne!(emitted_surrogates[2], synthetic_row2);
    }

    /// Paging across flushed segments and the memtable emits every row once.
    /// A memtable cursor re-read every flushed segment, so each later page
    /// repeated the flushed rows.
    #[test]
    fn memtable_cursor_does_not_reread_flushed_segments() {
        let (mut core, _dir) = make_core();
        let coll = "mat_cursor_test";
        let key = insert_and_flush(
            &mut core,
            coll,
            &[
                (1, 100, Surrogate::new(10)),
                (2, 200, Surrogate::new(20)),
                (3, 300, Surrogate::new(30)),
            ],
        );
        let engine = core.columnar_engines.get_mut(&key).expect("engine");
        for (id, surr) in [(4, 40), (5, 50)] {
            engine
                .insert_with_surrogate(
                    &[Value::Integer(id), Value::Integer(id * 100)],
                    Surrogate::new(surr),
                    1,
                )
                .expect("memtable insert");
        }

        let task = make_task();
        let mut cursor: Vec<u8> = Vec::new();
        let mut seen: Vec<u32> = Vec::new();
        for _page in 0..10 {
            let resp = core.execute_columnar_materialize_scan(&task, coll, &cursor, 2, None);
            let (page, next) = parse_response(resp.payload.as_bytes());
            seen.extend(page);
            if next.is_empty() {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen, vec![10, 20, 30, 40, 50]);
    }
}
