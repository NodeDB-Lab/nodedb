// SPDX-License-Identifier: BUSL-1.1

//! Bitmap-prefilter sub-plan execution.
//!
//! When `QueryOp::HashJoin` carries a `left_bitmap` or `right_bitmap`
//! sub-plan, or a vector search carries an inline prefilter plan, the
//! executor calls `run_bitmap_subplan` to execute that sub-plan and collect
//! the resulting surrogates into a `SurrogateBitmap`. The bitmap restricts
//! the consumer's candidates: an empty bitmap admits no row. A failing
//! sub-plan is an error, never an empty bitmap.

use nodedb_types::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::DocumentOp;

use super::materialize::collect_surrogates;

/// Execute a bitmap-producer sub-plan and return the resulting
/// `SurrogateBitmap`.
///
/// A sub-plan that fails returns its error. A sub-plan that succeeds with no
/// rows returns an empty bitmap, which admits no row.
pub(crate) fn run_bitmap_subplan(
    core: &mut CoreLoop,
    task: &ExecutionTask,
    sub_plan: &PhysicalPlan,
) -> crate::Result<SurrogateBitmap> {
    let sub_response = core.execute_plan(task, sub_plan);
    if sub_response.status == Status::Error {
        let code = sub_response.error_code.map_or_else(
            || ErrorCode::Internal {
                detail: "bitmap sub-plan failed without an error code".into(),
            },
            |code| *code,
        );
        return Err(crate::Error::DataPlane(code));
    }
    if sub_response.payload.is_empty() {
        return Ok(SurrogateBitmap::new());
    }
    let docs = crate::data::executor::response_codec::decode_response_to_docs(&sub_response)
        .ok_or_else(|| {
            crate::Error::DataPlane(ErrorCode::Internal {
                detail: "bitmap sub-plan returned an undecodable row payload".into(),
            })
        })?;
    Ok(collect_surrogates(&docs))
}

/// Build a `DocumentOp::Scan` physical plan with a surrogate prefilter injected.
///
/// Used by the hash-join executor for the probe side when a bitmap sub-plan
/// was provided. The scan skips rows whose surrogate is absent from `bitmap`,
/// pushing the filter into the document engine before any msgpack decode. An
/// empty `bitmap` scans no row.
pub(crate) fn prefiltered_scan_plan(
    collection: &str,
    limit: usize,
    bitmap: SurrogateBitmap,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        limit,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: Some(bitmap),
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;

    use super::*;
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn scan(collection: &str, filters: Vec<u8>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            filters,
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        })
    }

    fn task_for(plan: PhysicalPlan) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
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
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    /// A sub-plan that fails is an error, never an empty bitmap that would
    /// silently answer with zero rows.
    #[test]
    fn a_failing_sub_plan_is_an_error() {
        let mut harness = make_core();
        let sub_plan = scan("cells", vec![0xc1]);
        let task = task_for(sub_plan.clone());
        let result = run_bitmap_subplan(&mut harness.core, &task, &sub_plan);
        assert!(
            matches!(result, Err(crate::Error::DataPlane(_))),
            "expected the sub-plan error, got {result:?}"
        );
    }

    /// A sub-plan with no rows yields an empty bitmap, and the probe scan it
    /// builds admits no row.
    #[test]
    fn an_empty_sub_plan_admits_no_row() {
        let mut harness = make_core();
        let sub_plan = scan("cells", Vec::new());
        let task = task_for(sub_plan.clone());
        let bitmap = run_bitmap_subplan(&mut harness.core, &task, &sub_plan).expect("bitmap");
        assert!(bitmap.is_empty());
        match prefiltered_scan_plan("cells", 10, bitmap) {
            PhysicalPlan::Document(DocumentOp::Scan { prefilter, .. }) => {
                assert!(prefilter.is_some_and(|bm| bm.is_empty()));
            }
            other => panic!("expected a prefiltered scan, got {other:?}"),
        }
    }
}
