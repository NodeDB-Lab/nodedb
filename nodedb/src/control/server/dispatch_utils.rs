//! Shared dispatch utilities used by both the pgwire and native endpoints.
//!
//! Re-exports from focused sub-modules for backward compatibility.

use std::time::{Duration, Instant};

use crate::bridge::envelope::Payload;
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::bridge::physical_plan::{DocumentOp, KvOp, TimeseriesOp};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, TenantId, VShardId};

pub use super::broadcast::{broadcast_count_to_all_cores, broadcast_raw, broadcast_to_all_cores};
pub use super::graph_dispatch::{
    cross_core_bfs, cross_core_bfs_with_options, cross_core_shortest_path,
};
pub use super::wal_dispatch::wal_append_if_write;

#[derive(Debug)]
pub(crate) enum DispatchCollectError {
    OverBudget { bytes: usize },
    ChannelClosed,
}

/// Drain a dispatched request's bounded response channel, enforcing a
/// total-payload byte ceiling across streamed partials.
///
/// Returns the final Response (non-streaming: pass-through; streaming:
/// concatenated payload) or an error if the channel closed without a
/// final chunk or if the accumulated payload would exceed the ceiling.
pub(crate) async fn collect_bounded_response(
    rx: &mut tokio::sync::mpsc::Receiver<Response>,
    max_result_bytes: usize,
) -> Result<Response, DispatchCollectError> {
    let mut combined_payload: Vec<u8> = Vec::new();
    let mut final_response_meta: Option<Response> = None;
    let mut final_streaming = false;

    loop {
        let Some(resp) = rx.recv().await else { break };
        if resp.partial {
            combined_payload.extend_from_slice(&resp.payload);
            if combined_payload.len() > max_result_bytes {
                return Err(DispatchCollectError::OverBudget {
                    bytes: combined_payload.len(),
                });
            }
        } else if combined_payload.is_empty() {
            return Ok(resp);
        } else {
            combined_payload.extend_from_slice(&resp.payload);
            if combined_payload.len() > max_result_bytes {
                return Err(DispatchCollectError::OverBudget {
                    bytes: combined_payload.len(),
                });
            }
            final_response_meta = Some(resp);
            final_streaming = true;
            break;
        }
    }

    if final_streaming {
        let meta = final_response_meta.expect("final_streaming ⇒ meta set");
        return Ok(Response {
            payload: Payload::from_vec(combined_payload),
            ..meta
        });
    }
    Err(DispatchCollectError::ChannelClosed)
}

/// Current wall-clock time as milliseconds since Unix epoch.
///
/// Returns 0 if the system clock is before the epoch (should never happen
/// on correctly configured systems).
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Dispatch a physical plan to the Data Plane and await the response.
///
/// Creates a request envelope, registers with the tracker for correlation,
/// dispatches via the SPSC bridge, and awaits the response with a timeout.
pub async fn dispatch_to_data_plane(
    shared: &SharedState,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: u64,
) -> crate::Result<Response> {
    dispatch_to_data_plane_with_source(
        shared,
        tenant_id,
        vshard_id,
        plan,
        trace_id,
        crate::event::EventSource::User,
    )
    .await
}

/// Dispatch a physical plan to the Data Plane with an explicit event source.
///
/// Trigger-generated writes pass `EventSource::Trigger` so the Data Plane
/// emits WriteEvents with the correct source tag (preventing cascade
/// re-triggering in the Event Plane).
pub async fn dispatch_to_data_plane_with_source(
    shared: &SharedState,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: u64,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    // Extract write metadata before the plan is moved into the request.
    let is_columnar_collection = matches!(
        &plan,
        PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. })
    );
    let change_meta = extract_write_metadata(&plan, tenant_id);

    // Per-vShard QPS + latency timer. `dispatch_started` marks the
    // wall-clock moment the request enters the Control Plane dispatch
    // site; observation happens on every exit path (success, budget
    // over-run, timeout) so the histogram captures the true end-to-end
    // shape of the work routed to this vshard.
    let dispatch_started = Instant::now();
    let vshard_u16 = vshard_id.as_u16();

    let request_id = shared.next_request_id();
    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + Duration::from_secs(shared.tuning.network.default_deadline_secs),
        priority: Priority::Normal,
        trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source,
        user_roles: Vec::new(),
    };

    let mut rx = shared.tracker.register(request_id);

    match shared.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request)?,
        Err(poisoned) => poisoned.into_inner().dispatch(request)?,
    };

    // Collect response(s). For non-streaming queries, exactly one arrives.
    // For streaming queries, multiple partial chunks arrive before the final.
    // The mpsc channel is bounded (see `RequestTracker::register`); here we
    // additionally cap the *total* accumulated payload so a runaway scan
    // can't pin Control-Plane RAM — any query whose combined result
    // exceeds `tuning.network.max_query_result_bytes` is cancelled with
    // a typed `ExecutionLimitExceeded` error.
    let max_result_bytes = shared.tuning.network.max_query_result_bytes as usize;
    let observe = |shared: &SharedState| {
        let latency_us = dispatch_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        shared.per_vshard_metrics.observe(vshard_u16, latency_us);
    };
    let response = tokio::time::timeout(
        Duration::from_secs(shared.tuning.network.default_deadline_secs),
        collect_bounded_response(&mut rx, max_result_bytes),
    )
    .await
    .map_err(|_| {
        observe(shared);
        crate::Error::DeadlineExceeded { request_id }
    })?;

    let response = match response {
        Ok(r) => r,
        Err(DispatchCollectError::OverBudget { bytes }) => {
            shared.tracker.cancel(&request_id);
            observe(shared);
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "query result exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            });
        }
        Err(DispatchCollectError::ChannelClosed) => {
            observe(shared);
            return Err(crate::Error::Dispatch {
                detail: "response channel closed".into(),
            });
        }
    };

    // Publish change events for successful writes.
    if response.status == crate::bridge::envelope::Status::Ok
        && let Some((collection, doc_id, op)) = change_meta
    {
        // CDC opt-in check for timeseries: skip publishing unless cdc_enabled.
        // Document collections always publish (backward compatible).
        let should_publish = if is_columnar_collection {
            is_timeseries_cdc_enabled(shared, tenant_id, &collection)
        } else {
            true
        };

        if should_publish {
            use crate::control::change_stream::ChangeEvent;
            let event = ChangeEvent {
                lsn: response.watermark_lsn,
                tenant_id,
                collection,
                document_id: doc_id,
                operation: op,
                timestamp_ms: current_timestamp_ms(),
                after: None,
            };

            // Cluster-wide NOTIFY: broadcast to all peers via QUIC.
            if let (Some(transport), Some(topology)) =
                (&shared.cluster_transport, &shared.cluster_topology)
            {
                use std::sync::atomic::Ordering;
                static NOTIFY_SEQ: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(1);
                let seq = NOTIFY_SEQ.fetch_add(1, Ordering::Relaxed);
                crate::control::change_stream::broadcast_notify_to_cluster(
                    &event,
                    shared.node_id,
                    seq,
                    transport,
                    topology,
                );
            }

            shared.change_stream.publish(event);
        }
    }

    // Advance the tenant's observed write-HLC high-water on any
    // successful dispatch. Used by RESTORE staleness gate. Advance
    // on every success (not just writes) is intentionally
    // conservative — envelope.watermark is captured AFTER fan-out so
    // it always dominates the tenant_wm of a fresh backup.
    if response.status == crate::bridge::envelope::Status::Ok {
        shared.advance_tenant_write_hlc(tenant_id.as_u32());
    }

    observe(shared);
    Ok(response)
}

/// Extract write metadata from a physical plan for change event publishing.
///
/// `_tenant_id` is reserved for future tenant-scoped change stream filtering.
fn extract_write_metadata(
    plan: &PhysicalPlan,
    _tenant_id: TenantId,
) -> Option<(
    String,
    String,
    crate::control::change_stream::ChangeOperation,
)> {
    use crate::control::change_stream::ChangeOperation;
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Delete,
        )),
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Update,
        )),
        PhysicalPlan::Document(DocumentOp::Upsert {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Update))
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        // Timeseries ingest: batch write. CDC is opt-in for timeseries
        // collections (high-cardinality metrics would flood the bus).
        // The change event uses document_id="*" to indicate a batch.
        // Consumers can subscribe with collection_filter to get these events.
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Insert))
        }
        // KV engine write operations.
        PhysicalPlan::Kv(KvOp::Put {
            collection, key, ..
        }) => Some((
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::Kv(KvOp::Delete { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        PhysicalPlan::Kv(KvOp::FieldSet {
            collection, key, ..
        }) => Some((
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Update,
        )),
        PhysicalPlan::Kv(KvOp::BatchPut { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Insert))
        }
        PhysicalPlan::Kv(KvOp::Truncate { collection }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        PhysicalPlan::Kv(KvOp::Incr {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::IncrFloat {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::Cas {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::GetSet {
            collection, key, ..
        }) => Some((
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Update,
        )),
        _ => None,
    }
}

/// Check if a timeseries collection has CDC enabled.
///
/// Returns `false` (CDC off) by default for timeseries to prevent
/// high-cardinality metric streams from flooding the ChangeStream bus.
/// Users opt in via `CREATE TIMESERIES name WITH (cdc = 'true')`.
fn is_timeseries_cdc_enabled(shared: &SharedState, tenant_id: TenantId, collection: &str) -> bool {
    if let Some(catalog) = shared.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), collection)
        && coll.collection_type.is_timeseries()
    {
        if let Some(config) = coll.get_timeseries_config()
            && let Some(cdc_val) = config.get("cdc")
        {
            return cdc_val.as_str() == Some("true") || cdc_val.as_bool() == Some(true);
        }
        // Default: CDC off for timeseries.
        return false;
    }
    // Not timeseries or catalog unavailable — allow publishing.
    true
}

#[cfg(test)]
mod collect_budget_tests {
    use super::*;
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::{Lsn, RequestId};
    use tokio::sync::mpsc;

    fn partial(bytes: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(vec![0u8; bytes]),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
        }
    }

    fn final_resp(bytes: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(vec![0u8; bytes]),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
        }
    }

    #[tokio::test]
    async fn non_streaming_single_response_passes_through() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(final_resp(100)).await.unwrap();
        drop(tx);
        let resp = collect_bounded_response(&mut rx, 1024).await.unwrap();
        assert_eq!(resp.payload.len(), 100);
    }

    #[tokio::test]
    async fn streaming_under_budget_concatenates() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial(100)).await.unwrap();
        tx.send(partial(200)).await.unwrap();
        tx.send(final_resp(50)).await.unwrap();
        drop(tx);
        let resp = collect_bounded_response(&mut rx, 1024).await.unwrap();
        assert_eq!(resp.payload.len(), 350);
    }

    #[tokio::test]
    async fn streaming_over_budget_on_partial_aborts() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial(600)).await.unwrap();
        tx.send(partial(600)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1000).await.unwrap_err();
        match err {
            DispatchCollectError::OverBudget { bytes } => assert!(bytes > 1000),
            DispatchCollectError::ChannelClosed => panic!("expected OverBudget, got ChannelClosed"),
        }
    }

    #[tokio::test]
    async fn streaming_over_budget_on_final_chunk_aborts() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial(500)).await.unwrap();
        tx.send(final_resp(600)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1000).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::OverBudget { .. }));
    }

    #[tokio::test]
    async fn channel_closed_without_final_is_explicit_error() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial(10)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1024).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::ChannelClosed));
    }
}
