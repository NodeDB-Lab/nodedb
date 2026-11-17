// SPDX-License-Identifier: BUSL-1.1

//! Shared fixtures for the columnar checkpoint round-trip tests in `load.rs`
//! and `restore.rs`.
//!
//! The bar those tests hold to is that a RESTORED core answers QUERIES the way
//! the original did — not that its structs compare equal. Each test forces a
//! REAL flush — drain the memtable, encode a `SegmentWriter` segment, push
//! bytes + captured surrogates in lockstep — mirroring the production block in
//! `handlers/columnar_write/flush.rs`, so the rows genuinely live only in a
//! flushed segment before the checkpoint runs.

use std::time::{Duration, Instant};

use nodedb_bridge::buffer::RingBuffer;
use nodedb_columnar::MutationEngine;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;
use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::ColumnarScanParams;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

pub(super) type EngineKey = (DatabaseId, TenantId, String);

pub(super) fn schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::required("name", ColumnType::String),
    ])
    .expect("valid schema")
}

/// Open a core rooted at `dir`. Taking the dir rather than making one is what
/// lets a test open a SECOND core over the first one's data — the only way to
/// exercise the load path as boot actually runs it.
pub(super) fn open_core(dir: &std::path::Path) -> CoreLoop {
    let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    CoreLoop::open(
        crate::data::executor::core_loop::CoreOpenParams::unencrypted(
            0,
            req_rx,
            resp_tx,
            dir,
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        ),
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
        commit_hlc: None,
        system_ordinal: None,
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
pub(super) fn seed_collection(
    core: &mut CoreLoop,
    collection: &str,
    flushed: &[(i64, &str, Surrogate)],
    resident: &[(i64, &str, Surrogate)],
) -> EngineKey {
    let key = engine_key(collection);
    let mut engine = MutationEngine::new(collection.to_string(), schema());

    for (id, name, surr) in flushed {
        engine
            .insert_with_surrogate(
                &[Value::Integer(*id), Value::String((*name).into())],
                *surr,
                *id,
            )
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
            .insert_with_surrogate(
                &[Value::Integer(*id), Value::String((*name).into())],
                *surr,
                *id,
            )
            .expect("insert_with_surrogate");
    }

    core.columnar_engines.insert(key.clone(), engine);
    key
}

/// Run a scan (optionally prefiltered) and return the decoded result rows.
pub(super) fn scan(
    core: &mut CoreLoop,
    collection: &str,
    prefilter: Option<&SurrogateBitmap>,
) -> Vec<serde_json::Value> {
    scan_at(
        core,
        collection,
        prefilter,
        nodedb_types::SystemTimeScope::Current,
    )
}

/// [`scan`] at a system-time scope.
pub(super) fn scan_at(
    core: &mut CoreLoop,
    collection: &str,
    prefilter: Option<&SurrogateBitmap>,
    system_time: nodedb_types::SystemTimeScope,
) -> Vec<serde_json::Value> {
    let task = make_task();
    let params = ColumnarScanParams {
        collection,
        projection: &[],
        // No LIMIT. `0` is `LIMIT 0`, which returns no rows.
        limit: usize::MAX,
        filters: &[],
        rls_filters: &[],
        sort_keys: &[],
        system_time,
        valid_at_ms: None,
        prefilter,
        computed_columns: &[],
        txn_id: None,
    };
    let resp = core.execute_columnar_scan(&task, params);
    let text = crate::data::executor::response_codec::decode_row_payload_to_json(
        resp.payload.as_bytes(),
        crate::data::executor::response_codec::RowContext::Flat,
    )
    .expect("decode scan payload");
    match serde_json::from_str(&text).expect("scan payload is JSON") {
        serde_json::Value::Array(rows) => rows,
        other => panic!("scan payload must be a row array, got {other}"),
    }
}

pub(super) fn ids(rows: &[serde_json::Value]) -> Vec<i64> {
    let mut v: Vec<i64> = rows
        .iter()
        .filter_map(|r| r.get("id").and_then(|x| x.as_i64()))
        .collect();
    v.sort_unstable();
    v
}

pub(super) fn bitmap(surrogates: &[Surrogate]) -> SurrogateBitmap {
    let mut bm = SurrogateBitmap::new();
    for s in surrogates {
        bm.insert(*s);
    }
    bm
}
