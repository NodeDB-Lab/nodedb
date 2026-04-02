//! Broadcast dispatch: fan a plan to all Data Plane cores and merge results.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::bridge::physical_plan::QueryOp;
use crate::control::arrow_convert;
use crate::control::state::SharedState;
use crate::types::{Lsn, ReadConsistency, RequestId, TenantId, VShardId};

static BROADCAST_COUNTER: AtomicU64 = AtomicU64::new(2_000_000);

/// Broadcast a physical plan to ALL Data Plane cores and merge responses.
///
/// Used for scans (DocumentScan, Aggregate, etc.) where data is distributed
/// across cores. Each core scans its local storage, and all results are
/// concatenated. The merged response payload is a JSON array of all results.
pub async fn broadcast_to_all_cores(
    shared: &SharedState,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: u64,
) -> crate::Result<Response> {
    let num_cores = match shared.dispatcher.lock() {
        Ok(d) => d.num_cores(),
        Err(p) => p.into_inner().num_cores(),
    };

    let mut receivers = Vec::with_capacity(num_cores);
    for core_id in 0..num_cores {
        let request_id = RequestId::new(BROADCAST_COUNTER.fetch_add(1, Ordering::Relaxed));
        let vshard_id = VShardId::new(core_id as u16);
        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan: plan.clone(),
            deadline: Instant::now()
                + Duration::from_secs(shared.tuning.network.default_deadline_secs),
            priority: Priority::Normal,
            trace_id,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        };

        let rx = shared.tracker.register_oneshot(request_id);
        match shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch_to_core(core_id, request)?,
            Err(p) => p.into_inner().dispatch_to_core(core_id, request)?,
        };
        receivers.push(rx);
    }

    // Await all responses and merge payloads.
    let mut merged_payload: Vec<u8> = Vec::new();
    let mut max_lsn = Lsn::ZERO;
    let mut had_error = false;
    let mut error_msg = String::new();

    merged_payload.push(b'[');
    let mut first = true;

    for rx in receivers {
        let resp = tokio::time::timeout(
            Duration::from_secs(shared.tuning.network.default_deadline_secs),
            rx,
        )
        .await
        .map_err(|_| crate::Error::Dispatch {
            detail: "broadcast timeout".into(),
        })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "broadcast channel closed".into(),
        })?;

        if resp.status == crate::bridge::envelope::Status::Error {
            if let Some(ref ec) = resp.error_code {
                match ec {
                    crate::bridge::envelope::ErrorCode::NotFound => continue,
                    _ => {
                        had_error = true;
                        error_msg = format!("{ec:?}");
                    }
                }
            }
            continue;
        }

        if resp.watermark_lsn > max_lsn {
            max_lsn = resp.watermark_lsn;
        }

        if !resp.payload.is_empty() {
            let json_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let json_bytes = json_text.as_bytes();

            if json_bytes.starts_with(b"[") && json_bytes.ends_with(b"]") {
                let inner = &json_bytes[1..json_bytes.len() - 1];
                if !inner.is_empty() {
                    if !first {
                        merged_payload.push(b',');
                    }
                    merged_payload.extend_from_slice(inner);
                    first = false;
                }
            } else if !json_bytes.is_empty() {
                if !first {
                    merged_payload.push(b',');
                }
                merged_payload.extend_from_slice(json_bytes);
                first = false;
            }
        }
    }

    merged_payload.push(b']');

    if had_error && first {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    // For aggregate plans, run Arrow SIMD final-aggregation post-processing on
    // the merged JSON. This converts the merged JSON rows into a columnar
    // RecordBatch and verifies that the Arrow kernels can operate on the
    // result (e.g. SUM/AVG/MIN/MAX over partial results from all cores).
    // The merged_payload itself is returned unchanged — the batch is used for
    // any Control-Plane-side window functions or secondary aggregations added
    // by the planner. Currently this validates the merge and logs schema info.
    if is_aggregate_plan(&plan)
        && let Ok(json_text) = std::str::from_utf8(&merged_payload)
        && let Some(batch) = arrow_convert::json_rows_to_record_batch(json_text)
    {
        tracing::trace!(
            rows = batch.num_rows(),
            columns = batch.num_columns(),
            "arrow aggregate post-processing: merged {} rows from {} cores",
            batch.num_rows(),
            num_cores,
        );
    }

    Ok(Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(merged_payload),
        watermark_lsn: max_lsn,
        error_code: None,
    })
}

/// Returns `true` if the plan is an aggregate kind that benefits from Arrow
/// SIMD post-processing on the Control Plane after multi-core merge.
fn is_aggregate_plan(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Query(QueryOp::Aggregate { .. })
            | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
    )
}
