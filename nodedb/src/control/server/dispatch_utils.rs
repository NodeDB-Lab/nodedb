//! Shared dispatch utilities used by both the pgwire and native endpoints.
//!
//! Re-exports from focused sub-modules for backward compatibility.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::bridge::envelope::Payload;
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::bridge::physical_plan::{DocumentOp, TimeseriesOp};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

pub use super::broadcast::broadcast_to_all_cores;
pub use super::graph_dispatch::{cross_core_bfs, cross_core_bfs_with_options};
pub use super::wal_dispatch::wal_append_if_write;

static DISPATCH_COUNTER: AtomicU64 = AtomicU64::new(1_000_000);

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
    // Extract write metadata before the plan is moved into the request.
    let is_ts_collection = matches!(
        &plan,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. })
    );
    let change_meta = extract_write_metadata(&plan, tenant_id);

    let request_id = RequestId::new(DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed));
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
    };

    let mut rx = shared.tracker.register(request_id);

    match shared.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request)?,
        Err(poisoned) => poisoned.into_inner().dispatch(request)?,
    };

    // Collect response(s). For non-streaming queries, exactly one arrives.
    // For streaming queries, multiple partial chunks arrive before the final.
    // The mpsc channel is unbounded but safe: Data Plane sends at most
    // ceil(rows / STREAM_CHUNK_SIZE) partial messages, typically <100 chunks
    // for even large scans. The timeout bounds total wait time.
    let response = tokio::time::timeout(
        Duration::from_secs(shared.tuning.network.default_deadline_secs),
        async {
            let mut combined_payload: Vec<u8> = Vec::new();
            let mut final_response: Option<Response> = None;

            while let Some(resp) = rx.recv().await {
                if resp.partial {
                    // Partial chunk: accumulate payload.
                    combined_payload.extend_from_slice(&resp.payload);
                } else {
                    // Final response.
                    if combined_payload.is_empty() {
                        // Non-streaming: return directly.
                        final_response = Some(resp);
                    } else {
                        // Streaming: append final payload and return combined.
                        combined_payload.extend_from_slice(&resp.payload);
                        final_response = Some(Response {
                            payload: Payload::from_vec(combined_payload),
                            ..resp
                        });
                    }
                    break;
                }
            }

            final_response.ok_or(())
        },
    )
    .await
    .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
    .map_err(|_| crate::Error::Dispatch {
        detail: "response channel closed".into(),
    })?;

    // Publish change events for successful writes.
    if response.status == crate::bridge::envelope::Status::Ok
        && let Some((collection, doc_id, op)) = change_meta
    {
        // CDC opt-in check for timeseries: skip publishing unless cdc_enabled.
        // Document collections always publish (backward compatible).
        let should_publish = if is_ts_collection {
            is_timeseries_cdc_enabled(shared, tenant_id, &collection)
        } else {
            true
        };

        if should_publish {
            use crate::control::change_stream::ChangeEvent;
            shared.change_stream.publish(ChangeEvent {
                lsn: response.watermark_lsn,
                tenant_id,
                collection,
                document_id: doc_id,
                operation: op,
                timestamp_ms: current_timestamp_ms(),
                after: None,
            });
        }
    }

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
        PhysicalPlan::Document(DocumentOp::Truncate { collection }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        // Timeseries ingest: batch write. CDC is opt-in for timeseries
        // collections (high-cardinality metrics would flood the bus).
        // The change event uses document_id="*" to indicate a batch.
        // Consumers can subscribe with collection_filter to get these events.
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Insert))
        }
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
