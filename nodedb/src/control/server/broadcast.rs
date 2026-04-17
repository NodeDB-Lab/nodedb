//! Broadcast dispatch: fan a plan to all Data Plane cores and merge results.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nodedb_query::msgpack_scan;
use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::bridge::physical_plan::QueryOp;
use crate::control::arrow_convert;
use crate::control::state::SharedState;
use crate::types::{Lsn, ReadConsistency, RequestId, TenantId, VShardId};

static BROADCAST_COUNTER: AtomicU64 = AtomicU64::new(2_000_000);

/// Total number of `broadcast_to_all_cores` / `broadcast_count_to_all_cores`
/// invocations since process start. Exposed so callers (including test
/// harnesses) can assert O(hops) call-count budgets on batched BFS
/// paths. Separate from `BROADCAST_COUNTER`, which allocates request
/// IDs — that grows by `num_cores` per call, so it's a poor call-count
/// proxy.
static BROADCAST_CALLS: AtomicU64 = AtomicU64::new(0);

/// Read the total broadcast call count for observability / tests.
pub fn broadcast_call_count() -> u64 {
    BROADCAST_CALLS.load(Ordering::Relaxed)
}

/// Broadcast a physical plan to ALL Data Plane cores and merge responses.
///
/// Used for scans (DocumentScan, Aggregate, etc.) where data is distributed
/// across cores. Each core scans its local storage, and all results are
/// concatenated into one msgpack array.
pub async fn broadcast_to_all_cores(
    shared: &SharedState,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: u64,
) -> crate::Result<Response> {
    BROADCAST_CALLS.fetch_add(1, Ordering::Relaxed);
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

    // Await all responses and merge raw msgpack rows. Internal transport stays
    // msgpack; JSON conversion only happens at API boundaries.
    let mut all_elements: Vec<Vec<u8>> = Vec::new();
    let mut max_lsn = Lsn::ZERO;
    let mut had_error = false;
    let mut error_msg = String::new();

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

        if resp.payload.is_empty() {
            continue;
        }

        all_elements.extend(extract_msgpack_elements(&resp.payload));
    }

    let merged_payload = encode_msgpack_array(&all_elements);

    if had_error && all_elements.is_empty() {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    // For aggregate plans, run Arrow SIMD final-aggregation post-processing on
    // the merged msgpack rows. This converts the merged rows into a columnar
    // RecordBatch and verifies that the Arrow kernels can operate on the
    // result (e.g. SUM/AVG/MIN/MAX over partial results from all cores).
    // The merged_payload itself is returned unchanged — the batch is used for
    // any Control-Plane-side window functions or secondary aggregations added
    // by the planner. Currently this validates the merge and logs schema info.
    if is_aggregate_plan(&plan)
        && let Some(batch) = arrow_convert::msgpack_rows_to_record_batch(&merged_payload)
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

/// Broadcast a write-like plan to all cores and sum a numeric count field from
/// each response payload (for example `{"inserted": N}`).
pub async fn broadcast_count_to_all_cores(
    shared: &SharedState,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: u64,
    count_key: &str,
) -> crate::Result<Response> {
    BROADCAST_CALLS.fetch_add(1, Ordering::Relaxed);
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

    let mut total = 0usize;
    let mut max_lsn = Lsn::ZERO;
    let mut had_error = false;
    let mut error_msg = String::new();

    for rx in receivers {
        let resp = tokio::time::timeout(
            Duration::from_secs(shared.tuning.network.default_deadline_secs),
            rx,
        )
        .await
        .map_err(|_| crate::Error::Dispatch {
            detail: "broadcast count timeout".into(),
        })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "broadcast count channel closed".into(),
        })?;

        if resp.status == crate::bridge::envelope::Status::Error {
            had_error = true;
            if let Some(ref ec) = resp.error_code {
                error_msg = format!("{ec:?}");
            }
            continue;
        }

        if resp.watermark_lsn > max_lsn {
            max_lsn = resp.watermark_lsn;
        }

        total += decode_count_field(&resp.payload, count_key).unwrap_or(0);
    }

    if had_error && total == 0 {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    let mut map = std::collections::BTreeMap::new();
    map.insert(count_key, total);
    let payload = zerompk::to_msgpack_vec(&map).map_err(|e| crate::Error::Codec {
        detail: format!("count response serialization: {e}"),
    })?;

    Ok(Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(payload),
        watermark_lsn: max_lsn,
        error_code: None,
    })
}

/// Broadcast a plan to all cores and return raw binary payloads concatenated.
///
/// Unlike `broadcast_to_all_cores` (which merges as JSON), this returns the
/// raw response bytes. Each core's payload is appended as-is. Used by the
/// two-phase join: phase 1 scans the right collection across all cores and
/// collects raw msgpack, phase 2 passes it as `broadcast_data` to the join.
pub async fn broadcast_raw(
    shared: &SharedState,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: u64,
) -> crate::Result<Vec<u8>> {
    BROADCAST_CALLS.fetch_add(1, Ordering::Relaxed);
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
            deadline: std::time::Instant::now()
                + std::time::Duration::from_secs(shared.tuning.network.default_deadline_secs),
            priority: crate::bridge::envelope::Priority::Normal,
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

    let mut merged = Vec::new();
    for rx in receivers {
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(shared.tuning.network.default_deadline_secs),
            rx,
        )
        .await
        .map_err(|_| crate::Error::Dispatch {
            detail: "broadcast_raw timeout".into(),
        })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "broadcast_raw channel closed".into(),
        })?;

        if resp.status == crate::bridge::envelope::Status::Error {
            continue;
        }
        if !resp.payload.is_empty() {
            merged.extend_from_slice(resp.payload.as_ref());
        }
    }
    Ok(merged)
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

fn extract_msgpack_elements(payload: &[u8]) -> Vec<Vec<u8>> {
    if payload.is_empty() {
        return Vec::new();
    }

    let Some((count, mut pos)) = msgpack_scan::array_header(payload, 0) else {
        tracing::warn!(
            payload_len = payload.len(),
            "broadcast_to_all_cores: payload is not a msgpack array; treating as single row"
        );
        return vec![payload.to_vec()];
    };

    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        if pos >= payload.len() {
            break;
        }
        let start = pos;
        match msgpack_scan::skip_value(payload, pos) {
            Some(next) => {
                rows.push(payload[start..next].to_vec());
                pos = next;
            }
            None => {
                tracing::warn!(
                    pos,
                    payload_len = payload.len(),
                    "broadcast_to_all_cores: could not skip msgpack element; stopping early"
                );
                break;
            }
        }
    }
    rows
}

fn decode_count_field(payload: &[u8], key: &str) -> Option<usize> {
    if payload.is_empty() {
        return Some(0);
    }

    let json = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice::<serde_json::Value>(payload).ok())?;
    json.get(key).and_then(|v| v.as_u64()).map(|v| v as usize)
}

fn encode_msgpack_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let total_data: usize = rows.iter().map(|row| row.len()).sum();
    let mut out = Vec::with_capacity(total_data + 5);

    let row_count = rows.len();
    if row_count < 16 {
        out.push(0x90 | row_count as u8);
    } else if row_count <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(row_count as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(row_count as u32).to_be_bytes());
    }

    for row in rows {
        out.extend_from_slice(row);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{encode_msgpack_array, extract_msgpack_elements};

    #[test]
    fn extracts_raw_msgpack_rows_from_array_payload() {
        let payload =
            nodedb_types::json_to_msgpack(&serde_json::json!([{"id":"u1"},{"id":"u2"}])).unwrap();

        let rows = extract_msgpack_elements(&payload);

        assert_eq!(rows.len(), 2);
        assert_eq!(
            crate::data::executor::response_codec::decode_payload_to_json(&rows[0]),
            r#"{"id":"u1"}"#
        );
        assert_eq!(
            crate::data::executor::response_codec::decode_payload_to_json(&rows[1]),
            r#"{"id":"u2"}"#
        );
    }

    #[test]
    fn reencodes_merged_rows_as_msgpack_array() {
        let payload_a = nodedb_types::json_to_msgpack(&serde_json::json!([{"id":"u1"}])).unwrap();
        let payload_b = nodedb_types::json_to_msgpack(&serde_json::json!([{"id":"u2"}])).unwrap();

        let mut rows = extract_msgpack_elements(&payload_a);
        rows.extend(extract_msgpack_elements(&payload_b));
        let merged = encode_msgpack_array(&rows);

        assert_eq!(
            crate::data::executor::response_codec::decode_payload_to_json(&merged),
            r#"[{"id":"u1"},{"id":"u2"}]"#
        );
    }
}
