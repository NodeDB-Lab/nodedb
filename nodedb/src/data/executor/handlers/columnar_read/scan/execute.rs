// SPDX-License-Identifier: BUSL-1.1

//! The columnar base scan entry point.

use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::expr_eval::ComputedColumn;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::bitemporal::bitemporal_row_visible;
use crate::data::executor::handlers::columnar_read::convert::row_to_projected_value;
use crate::data::executor::handlers::columnar_read::filter::{
    decode_rls_filters, row_matches_filters_and_policy,
};
use crate::data::executor::handlers::columnar_read::scan_flushed::FlushedScanCtx;
use crate::data::executor::handlers::transaction::overlay::{
    ColumnarMatchedRow, ColumnarOverlayMergeParams,
};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

use super::block_skip::memtable_block_skipped;
use super::order::order_matched;
use super::params::ColumnarScanParams;

impl CoreLoop {
    /// Execute a base columnar scan: flushed segments first, then the live
    /// memtable. Flushed rows are delete-bitmap filtered. Surrogates are not
    /// stored in segment bytes but are retained in the in-memory
    /// `columnar_flushed_surrogates` sidecar (lockstep with the segment-bytes
    /// map); when a prefilter is active, flushed rows are filtered per-row by
    /// surrogate membership exactly like the live-memtable phase. See
    /// `scan_normalize::scan_columnar` for the parallel read path — keep both
    /// in sync on segment-iteration changes.
    ///
    /// Every result row is a `Value::Object` built by
    /// `row_to_projected_value` and encoded by `encode_value_vec`, so a
    /// declared `TIMESTAMP` / `TIMESTAMPTZ` cell reaches the client as a
    /// typed instant from either phase.
    pub(in crate::data::executor) fn execute_columnar_scan(
        &mut self,
        task: &ExecutionTask,
        params: ColumnarScanParams<'_>,
    ) -> Response {
        let ColumnarScanParams {
            collection,
            projection,
            limit,
            filters,
            rls_filters,
            sort_keys,
            system_time,
            valid_at_ms,
            prefilter,
            computed_columns,
            txn_id,
        } = params;

        use nodedb_types::SystemTimeScope;
        let all_versions = system_time.is_all_versions();
        // AS OF SYSTEM TIME NULL must surface every version: do not apply a
        // system-time cutoff. `AsOf(ms)` applies the ceiling; `Current` is
        // unconstrained.
        let system_as_of_ms = match system_time {
            SystemTimeScope::Current | SystemTimeScope::AllVersions => None,
            SystemTimeScope::AsOf(ms) => Some(ms),
        };

        let computed_cols: Vec<ComputedColumn> = if !computed_columns.is_empty() {
            match zerompk::from_msgpack(computed_columns) {
                Ok(cols) => cols,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("computed_columns decode: {e}"),
                        },
                    );
                }
            }
        } else {
            Vec::new()
        };
        // The read policy is decoded before any row is read: a payload the
        // Data Plane cannot decode fails the statement instead of admitting
        // the rows it governs.
        let rls_predicates: Vec<ScanFilter> = match decode_rls_filters(rls_filters) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };
        // A no-LIMIT SQL `SELECT * FROM <columnar>` arrives as
        // `limit == usize::MAX`. Capture that before the `limit == 0` rewrite
        // so the budget bound applies only to the unbounded path. Spatial
        // scans arrive with a finite `10000` and are therefore unaffected.
        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let unbounded = limit == usize::MAX;
        let limit = if limit == 0 {
            1000
        } else if unbounded {
            // Bound the materialized row count to a ceiling derived from the
            // memory budget (+1 row to detect "more exist") so the scan does
            // not pull the whole memtable into the `matched` Vec.
            crate::data::executor::handlers::scan_budget::fetch_limit_for(
                limit,
                0,
                scan_budget_bytes,
            )
        } else {
            limit
        };

        // Scan-quiesce gate.
        let _scan_guard =
            match self.acquire_scan_guard(task, task.request.tenant_id.as_u64(), collection) {
                Ok(g) => g,
                Err(resp) => return resp,
            };

        let engine_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );

        let engine = match self.columnar_engines.get(&engine_key) {
            Some(e) => e,
            None => {
                // Empty result for missing collection.
                return match response_codec::encode_value_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        let schema = engine.schema();

        let filter_predicates: Vec<ScanFilter> = if !filters.is_empty() {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed scan filters: {e}"),
                        },
                    );
                }
            }
        } else {
            Vec::new()
        };

        // Collect matched rows as (surrogate, row_values, projected object)
        // triples. The raw `Vec<Value>` is kept for sort-key comparison — the
        // projected object is emitted only after ORDER BY + limit are
        // applied. When no sort is requested the limit is enforced inside the
        // loop so the whole memtable is not materialised. The limit counts
        // rows that pass every predicate, so a row the WHERE clause or the
        // read policy excludes never consumes a slot.
        let mut matched: Vec<ColumnarMatchedRow> = Vec::new();
        // Resolve hidden bitemporal column positions once; `None` means
        // the collection is not bitemporal, so the per-row filter is a
        // no-op regardless of `system_as_of_ms` / `valid_at_ms` values.
        let ts_system_idx = schema.columns.iter().position(|c| c.name == TS_SYSTEM);
        let ts_valid_from_idx = schema.columns.iter().position(|c| c.name == TS_VALID_FROM);
        let ts_valid_until_idx = schema.columns.iter().position(|c| c.name == TS_VALID_UNTIL);

        // Block-boundary prefilter: skip the whole live memtable before any
        // row decoding when no recorded surrogate can be in the bitmap.
        let block_skipped = prefilter
            .is_some_and(|bitmap| memtable_block_skipped(bitmap, engine.memtable_surrogates()));

        // Deadline safe points for this scan: the phase boundary below, every
        // 1024th memtable row, and the stage boundary after the sort. The
        // flushed-segment pass in phase 1 runs to completion once entered, so a
        // statement is bounded to at most one such pass beyond its deadline.
        let deadline = crate::data::executor::deadline::DeadlineCheck::for_task(task);

        // ── Phase 1: flushed segments ───────────────────────────────────────
        // Read rows that were drained from the memtable during prior flushes.
        // These rows are older than anything in the current memtable.
        //
        // Surrogate note: surrogates are not serialised into segment bytes, but
        // they ARE retained in the in-memory `columnar_flushed_surrogates`
        // sidecar (kept in lockstep with `columnar_flushed_segments`). When a
        // surrogate prefilter is active we therefore scan flushed segments and
        // apply a per-row membership test below, mirroring the live-memtable
        // phase. See the method-level doc comment for the full rationale.
        if let Err(e) = self.scan_flushed_columnar_segments(
            FlushedScanCtx {
                collection,
                engine_key: &engine_key,
                schema,
                projection,
                limit,
                sort_keys,
                filter_predicates: &filter_predicates,
                rls_predicates: &rls_predicates,
                prefilter,
                computed_cols: &computed_cols,
                all_versions,
                system_as_of_ms,
                valid_at_ms,
                ts_system_idx,
                ts_valid_from_idx,
                ts_valid_until_idx,
            },
            &mut matched,
        ) {
            // `scan_flushed_columnar_segments` returns `crate::Result<()>`,
            // so `e` already carries the real typed error (e.g. a
            // division/modulo-by-zero in `filter_predicates`) — propagate
            // it as-is via `From<crate::Error> for ErrorCode` instead of
            // collapsing it to `Internal`/`XX000`, mirroring
            // `document/read/scan.rs`'s `Err(e) => self.response_error(task, e)`.
            return self.response_error(task, e);
        }

        // Safe point: phase 1 finished and nothing has been emitted, so the
        // statement stops here rather than paying for phase 2 as well.
        if deadline.expired_now() {
            return self.response_error(task, ErrorCode::DeadlineExceeded);
        }

        // ── Phase 2: live memtable ──────────────────────────────────────────
        // Rows still in the active memtable (not yet flushed). Skipped when
        // the flushed pass already filled an unsorted limit; otherwise the
        // loop stops at the limit or the deadline, like the flushed pass.
        if !block_skipped && (!sort_keys.is_empty() || matched.len() < limit) {
            for (row_surrogate, row) in engine.scan_memtable_rows_with_surrogates() {
                // Row-boundary prefilter: skip this row when its surrogate is
                // absent from the bitmap. Rows without a recorded surrogate
                // are always included when no prefilter is active; when a
                // prefilter is active they are excluded because the surrogate
                // identity is unknown.
                if let Some(bitmap) = prefilter {
                    match row_surrogate {
                        Some(s) if bitmap.contains(s) => {}
                        _ => continue,
                    }
                }

                if !bitemporal_row_visible(
                    &row,
                    ts_system_idx,
                    ts_valid_from_idx,
                    ts_valid_until_idx,
                    system_as_of_ms,
                    valid_at_ms,
                ) {
                    continue;
                }
                // The query's WHERE predicates, then the caller's read
                // policy: a row the policy excludes is dropped here, before
                // projection, sort, and limit, so a LIMIT counts admitted
                // rows only.
                match row_matches_filters_and_policy(
                    &row,
                    schema,
                    &filter_predicates,
                    &rls_predicates,
                ) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    // `EvalError` has exactly one variant and no direct
                    // `Into<ErrorCode>` — mirrors `stage_columnar_dml.rs`'s
                    // identical call site, which hardcodes the same typed
                    // code rather than collapsing to `Internal`/`XX000`.
                    Err(_e) => {
                        return self.response_error(task, ErrorCode::DivisionByZero);
                    }
                }
                let obj = match row_to_projected_value(
                    &row,
                    schema,
                    projection,
                    &computed_cols,
                    all_versions,
                ) {
                    Ok(v) => v,
                    // `row_to_projected_value` returns `crate::Result<_>`
                    // (unlike `row_matches_filters_and_policy` above) — its only
                    // fallible step is a computed-column expression eval,
                    // and `computed_cols` here is real, not `&[]` like the
                    // DML staging path, so propagate the actual typed error
                    // rather than assuming which one it is.
                    Err(e) => {
                        return self.response_error(task, e);
                    }
                };
                matched.push((row_surrogate, row, obj));
                if sort_keys.is_empty() && matched.len() >= limit {
                    break;
                }
                // Safe point: a row boundary. `expired` reads the clock once
                // per 1024 rows, so the per-row cost is a decrement.
                if deadline.expired() {
                    break;
                }
            }
            // The loop above stops mid-collection when the deadline passes, so
            // `matched` is a prefix of the answer. Fail the statement rather
            // than return a result set the client cannot tell from a complete
            // one.
            if deadline.tripped() {
                return self.response_error(task, ErrorCode::DeadlineExceeded);
            }
        }

        // In-transaction read-your-own-writes: fold this transaction's staged
        // `ColumnarOp::Insert` rows into the base result. Gated on `txn_id`
        // (autocommit reads never carry one) and skipped for temporal reads —
        // staged bodies represent the current version only, matching the
        // Document engine's overlay-merge convention.
        if let Some(txn_id) = txn_id
            && !all_versions
            && system_as_of_ms.is_none()
        {
            let coll_key = (
                task.request.database_id,
                task.request.tenant_id,
                collection.to_string(),
            );
            if let Err(e) = self.merge_overlay_into_columnar_scan(
                ColumnarOverlayMergeParams {
                    txn_id,
                    coll_key: &coll_key,
                    schema,
                    projection,
                    filter_predicates: &filter_predicates,
                    rls_predicates: &rls_predicates,
                    computed_cols: &computed_cols,
                    all_versions,
                },
                &mut matched,
            ) {
                // `merge_overlay_into_columnar_scan` returns
                // `crate::Result<()>` (the overlay's own residual-predicate
                // re-check can divide/modulo by zero) — propagate the typed
                // error directly, matching both `document/read/scan.rs`'s
                // generic `self.response_error(task, e)` pattern and
                // `stage_columnar_dml.rs`'s identical call to this same
                // function (`Err(e) => return Err(self.response_error(task, e))`).
                return self.response_error(task, e);
            }
        }

        if let Err(e) = order_matched(&mut matched, schema, sort_keys, all_versions, ts_system_idx)
        {
            return self.response_error(task, e);
        }
        // Safe point: the sort is the one stage whose cost grows with the
        // matched row count, and it has just finished. Nothing is emitted
        // yet, so stopping here costs the client only the error.
        if !sort_keys.is_empty() && deadline.expired_now() {
            return self.response_error(task, ErrorCode::DeadlineExceeded);
        }

        let results: Vec<Value> = matched
            .into_iter()
            .take(limit)
            .map(|(_, _, obj)| obj)
            .collect();

        let payload = match response_codec::encode_value_vec(&results) {
            Ok(payload) => payload,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Bound an unbounded (no-LIMIT) scan by the memory budget. The encoded
        // msgpack payload is the authoritative size of the materialized result;
        // surface a deterministic error if it exceeds the budget rather than
        // silently truncating. Spatial scans are bounded (finite limit) and so
        // skip this check.
        if unbounded
            && crate::data::executor::handlers::scan_budget::budget_exceeded(
                payload.len(),
                scan_budget_bytes,
            )
        {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        self.response_with_payload(task, payload)
    }
}

#[cfg(test)]
mod tests {
    //! Cross-engine prefilter coverage for FLUSHED plain-columnar segments.
    //!
    //! Rows that live only in a flushed segment (drained out of the live
    //! memtable) are visible to a prefiltered scan and are filtered per-row by
    //! their cross-engine surrogate, instead of being skipped wholesale. Each
    //! test forces a real flush — drain the memtable, encode a `SegmentWriter`
    //! segment, push the bytes + the captured surrogate sidecar in lockstep,
    //! then `on_memtable_flushed` clears the memtable — so the rows truly exist
    //! only in the flushed segment before the scan runs. This mirrors the
    //! production flush block in `handlers/columnar_write/insert.rs`.

    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_columnar::MutationEngine;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;
    use nodedb_types::{NdbDateTime, Surrogate, SurrogateBitmap};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    use super::ColumnarScanParams;

    const MICROS: i64 = 1_583_402_400_000_000;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("at", ColumnType::Timestamp),
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
            // Plan is irrelevant: `execute_columnar_scan` is called directly
            // with `ColumnarScanParams` and only reads `database_id`/`tenant_id`.
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

    fn engine_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            collection.to_string(),
        )
    }

    fn row(id: i64, name: &str) -> [Value; 3] {
        [
            Value::Integer(id),
            Value::String(name.into()),
            Value::NaiveDateTime(NdbDateTime::from_micros(MICROS)),
        ]
    }

    /// Run the EXACT production flush sequence on `engine` so its memtable
    /// rows end up only in a flushed segment, populating both lockstep maps
    /// on `core`.
    fn flush(
        core: &mut CoreLoop,
        key: &(DatabaseId, TenantId, String),
        engine: &mut MutationEngine,
    ) {
        // ── Mirror handlers/columnar_write/insert.rs flush block ────────────
        let new_segment_id = engine.next_segment_id();
        let (seg_schema, columns, row_count) = engine.memtable_mut().drain_optimized();
        // Capture surrogates BEFORE `on_memtable_flushed` clears them.
        let flushed_surrogates: Vec<Option<Surrogate>> = engine.memtable_surrogates().to_vec();
        assert_eq!(
            row_count,
            flushed_surrogates.len(),
            "drained row_count must equal captured surrogate count"
        );
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
        // Lockstep push: both maps, same key, same order.
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

        // After flush the memtable is empty: rows live ONLY in the segment.
        assert_eq!(
            engine.memtable_surrogates().len(),
            0,
            "memtable surrogates cleared after flush"
        );
    }

    /// Insert `(id, name, surrogate)` rows into a fresh engine, then flush so
    /// the rows end up only in a flushed segment. Registers the engine on
    /// `core`. Returns the engine key.
    fn insert_and_flush(
        core: &mut CoreLoop,
        collection: &str,
        rows: &[(i64, &str, Surrogate)],
    ) -> (DatabaseId, TenantId, String) {
        let key = engine_key(collection);
        let mut engine = MutationEngine::new(collection.to_string(), schema());
        for (id, name, surr) in rows {
            engine
                .insert_with_surrogate(&row(*id, name), *surr)
                .expect("insert_with_surrogate");
        }
        flush(core, &key, &mut engine);
        core.columnar_engines.insert(key.clone(), engine);
        key
    }

    fn scan_params<'a>(
        collection: &'a str,
        prefilter: Option<&'a SurrogateBitmap>,
    ) -> ColumnarScanParams<'a> {
        scan_params_with_rls(collection, prefilter, &[])
    }

    fn scan_params_with_rls<'a>(
        collection: &'a str,
        prefilter: Option<&'a SurrogateBitmap>,
        rls_filters: &'a [u8],
    ) -> ColumnarScanParams<'a> {
        ColumnarScanParams {
            collection,
            projection: &[],
            limit: 0,
            filters: &[],
            rls_filters,
            sort_keys: &[],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter,
            computed_columns: &[],
            txn_id: None,
        }
    }

    /// A read policy `name = <name>`, encoded the way the planner ships it.
    fn policy_name_eq(name: &str) -> Vec<u8> {
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "name".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: Value::String(name.into()),
            clauses: vec![],
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy")
    }

    fn decode_rows(payload: &[u8]) -> Vec<Value> {
        match nodedb_types::value_from_msgpack(payload).expect("decode scan payload") {
            Value::Array(rows) => rows,
            other => panic!("scan payload must be an array of rows: {other:?}"),
        }
    }

    /// Run a prefiltered scan and return the decoded result rows.
    fn scan_with_prefilter(
        core: &mut CoreLoop,
        collection: &str,
        prefilter: Option<&SurrogateBitmap>,
    ) -> Vec<Value> {
        let task = make_task();
        let resp = core.execute_columnar_scan(&task, scan_params(collection, prefilter));
        decode_rows(resp.payload.as_bytes())
    }

    fn field<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
        match row {
            Value::Object(map) => map.get(name),
            _ => None,
        }
    }

    fn ids(rows: &[Value]) -> Vec<i64> {
        let mut v: Vec<i64> = rows
            .iter()
            .filter_map(|r| match field(r, "id") {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            })
            .collect();
        v.sort_unstable();
        v
    }

    /// A prefilter that includes SOME flushed-row surrogates must return
    /// exactly those rows — proving flushed rows are prefiltered, not
    /// skipped wholesale.
    #[test]
    fn flushed_rows_are_prefiltered_not_skipped() {
        let (mut core, _dir) = make_core();
        let coll = "cf_some";
        insert_and_flush(
            &mut core,
            coll,
            &[
                (1, "a", Surrogate(101)),
                (2, "b", Surrogate(102)),
                (3, "c", Surrogate(103)),
            ],
        );

        let mut bitmap = SurrogateBitmap::new();
        bitmap.insert(Surrogate(101));
        bitmap.insert(Surrogate(103));

        let rows = scan_with_prefilter(&mut core, coll, Some(&bitmap));
        assert_eq!(
            ids(&rows),
            vec![1, 3],
            "only rows whose surrogate is in the bitmap come back"
        );
    }

    /// A prefilter that includes NONE of the surrogates must return zero rows.
    #[test]
    fn flushed_rows_empty_prefilter_returns_nothing() {
        let (mut core, _dir) = make_core();
        let coll = "cf_none";
        insert_and_flush(
            &mut core,
            coll,
            &[(1, "a", Surrogate(201)), (2, "b", Surrogate(202))],
        );

        let mut bitmap = SurrogateBitmap::new();
        bitmap.insert(Surrogate(999)); // no overlap

        let rows = scan_with_prefilter(&mut core, coll, Some(&bitmap));
        assert!(
            rows.is_empty(),
            "no surrogate matches => zero rows, got {}",
            rows.len()
        );
    }

    /// Sanity: with NO prefilter all flushed rows are returned (the gate only
    /// applies when a prefilter is present).
    #[test]
    fn flushed_rows_no_prefilter_returns_all() {
        let (mut core, _dir) = make_core();
        let coll = "cf_all";
        insert_and_flush(
            &mut core,
            coll,
            &[
                (1, "a", Surrogate(301)),
                (2, "b", Surrogate(302)),
                (3, "c", Surrogate(303)),
            ],
        );

        let rows = scan_with_prefilter(&mut core, coll, None);
        assert_eq!(ids(&rows), vec![1, 2, 3]);
    }

    /// A declared `TIMESTAMP` cell is the same naive instant whether the row
    /// is read from the live memtable or from a flushed segment.
    #[test]
    fn a_timestamp_cell_is_the_same_instant_before_and_after_flush() {
        let (mut core, _dir) = make_core();
        let coll = "cf_instant";
        let key = engine_key(coll);
        let mut engine = MutationEngine::new(coll.to_string(), schema());
        engine
            .insert_with_surrogate(&row(1, "live"), Surrogate(701))
            .expect("insert_with_surrogate");
        core.columnar_engines.insert(key.clone(), engine);

        let expected = Value::NaiveDateTime(NdbDateTime::from_micros(MICROS));
        let live = scan_with_prefilter(&mut core, coll, None);
        assert_eq!(live.len(), 1);
        assert_eq!(field(&live[0], "at"), Some(&expected), "live memtable cell");

        let mut engine = core
            .columnar_engines
            .remove(&key)
            .expect("engine registered");
        flush(&mut core, &key, &mut engine);
        core.columnar_engines.insert(key.clone(), engine);
        let flushed = scan_with_prefilter(&mut core, coll, None);
        assert_eq!(flushed.len(), 1);
        assert_eq!(
            field(&flushed[0], "at"),
            Some(&expected),
            "flushed segment cell"
        );
    }

    /// A read policy governs rows from the live memtable and from flushed
    /// segments alike: only the rows it admits come back.
    #[test]
    fn a_read_policy_admits_only_matching_rows_from_both_phases() {
        let (mut core, _dir) = make_core();
        let coll = "cf_rls_both";
        let key = insert_and_flush(
            &mut core,
            coll,
            &[(1, "mine", Surrogate(801)), (2, "theirs", Surrogate(802))],
        );
        let mut engine = core
            .columnar_engines
            .remove(&key)
            .expect("engine registered");
        engine
            .insert_with_surrogate(&row(3, "mine"), Surrogate(803))
            .expect("insert_with_surrogate");
        engine
            .insert_with_surrogate(&row(4, "theirs"), Surrogate(804))
            .expect("insert_with_surrogate");
        core.columnar_engines.insert(key, engine);

        let policy = policy_name_eq("mine");
        let task = make_task();
        let resp = core.execute_columnar_scan(&task, scan_params_with_rls(coll, None, &policy));
        assert_eq!(resp.status, crate::bridge::envelope::Status::Ok);
        assert_eq!(
            ids(&decode_rows(resp.payload.as_bytes())),
            vec![1, 3],
            "one flushed and one live row are admitted; the others are excluded"
        );
    }

    /// A read policy payload the Data Plane cannot decode fails the scan.
    /// The alternative — treating it as "no policy" — would return every
    /// row the policy exists to hide.
    #[test]
    fn a_malformed_read_policy_payload_fails_the_scan() {
        let (mut core, _dir) = make_core();
        let coll = "cf_rls_malformed";
        insert_and_flush(
            &mut core,
            coll,
            &[(1, "mine", Surrogate(901)), (2, "theirs", Surrogate(902))],
        );

        // `0xC1` is the one byte MessagePack reserves and never emits.
        let malformed = [0xC1u8];
        let task = make_task();
        let resp = core.execute_columnar_scan(&task, scan_params_with_rls(coll, None, &malformed));

        assert_eq!(
            resp.status,
            crate::bridge::envelope::Status::Error,
            "an unreadable policy must not admit rows"
        );
        assert!(
            resp.payload.is_empty(),
            "a failed scan carries no rows, got {} bytes",
            resp.payload.len()
        );
    }

    /// Build a task whose deadline is already in the past.
    fn expired_task() -> ExecutionTask {
        let mut task = make_task();
        task.request.deadline = Instant::now() - Duration::from_millis(1);
        task
    }

    fn scan_response(
        core: &mut CoreLoop,
        collection: &str,
        task: &ExecutionTask,
    ) -> crate::bridge::envelope::Response {
        core.execute_columnar_scan(task, scan_params(collection, None))
    }

    /// A statement over its deadline is stopped DURING execution, not merely
    /// refused before it starts.
    ///
    /// This calls the handler directly, so the core loop's pre-execution
    /// admission check never runs. The only code that can produce this response
    /// is a safe point inside the scan itself.
    #[test]
    fn an_expired_deadline_stops_the_scan_inside_the_handler() {
        let (mut core, _dir) = make_core();
        let coll = "cf_deadline";
        insert_and_flush(
            &mut core,
            coll,
            &[(1, "a", Surrogate(501)), (2, "b", Surrogate(502))],
        );

        let resp = scan_response(&mut core, coll, &expired_task());

        assert_eq!(
            resp.status,
            crate::bridge::envelope::Status::Error,
            "an expired statement must not return rows"
        );
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&crate::bridge::envelope::ErrorCode::DeadlineExceeded)
        );
        assert!(
            resp.payload.is_empty(),
            "a stopped scan carries no rows: a short result set is \
             indistinguishable from a complete one at the client"
        );
    }

    /// The companion negative: the same scan with a live deadline returns every
    /// row, so the check above is not firing on every scan.
    #[test]
    fn a_live_deadline_leaves_the_scan_alone() {
        let (mut core, _dir) = make_core();
        let coll = "cf_deadline_live";
        insert_and_flush(
            &mut core,
            coll,
            &[(1, "a", Surrogate(601)), (2, "b", Surrogate(602))],
        );

        let resp = scan_response(&mut core, coll, &make_task());
        assert_eq!(resp.status, crate::bridge::envelope::Status::Ok);
        let rows = decode_rows(resp.payload.as_bytes());
        assert_eq!(ids(&rows), vec![1, 2]);
    }

    /// Lockstep invariant: after a flush the two maps are equal-length per key
    /// and the inner surrogate Vec length equals the segment row count.
    #[test]
    fn lockstep_lengths_match_after_flush() {
        let (mut core, _dir) = make_core();
        let coll = "cf_lockstep";
        let rows = [
            (1, "a", Surrogate(401)),
            (2, "b", Surrogate(402)),
            (3, "c", Surrogate(403)),
            (4, "d", Surrogate(404)),
        ];
        let key = insert_and_flush(&mut core, coll, &rows);

        let segs = core
            .columnar_flushed_segments
            .get(&key)
            .expect("segments present");
        let surrs = core
            .columnar_flushed_surrogates
            .get(&key)
            .expect("surrogate sidecar present");
        assert_eq!(
            segs.len(),
            surrs.len(),
            "outer Vec lengths must be equal (lockstep)"
        );
        assert_eq!(segs.len(), 1, "exactly one flushed segment");
        assert_eq!(
            surrs[0].len(),
            rows.len(),
            "inner per-row surrogate Vec length must equal segment row count"
        );
    }
}
