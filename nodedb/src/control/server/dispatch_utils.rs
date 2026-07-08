// SPDX-License-Identifier: BUSL-1.1

//! Shared dispatch utilities used by both the pgwire and native endpoints.

use std::time::{Duration, Instant};

use crate::bridge::envelope::Payload;
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, KvOp, TimeseriesOp};

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
    // Each streamed chunk is its OWN msgpack array (`encode_raw_document_rows`
    // per chunk), so the chunks are accumulated separately and merged into a
    // single msgpack array at the end. Raw byte concatenation would leave every
    // chunk after the first as a trailing array that downstream single-array
    // decoders silently drop — truncating a streamed scan to `stream_chunk_size`
    // rows. The byte budget is enforced on the running total of raw chunk bytes
    // (the memory actually held), which is `>=` the merged-array size.
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut total_bytes: usize = 0;
    let mut final_response_meta: Option<Response> = None;

    loop {
        let Some(resp) = rx.recv().await else { break };
        if resp.partial {
            total_bytes = total_bytes.saturating_add(resp.payload.len());
            if total_bytes > max_result_bytes {
                return Err(DispatchCollectError::OverBudget { bytes: total_bytes });
            }
            chunks.push(resp.payload.to_vec());
        } else if chunks.is_empty() {
            // Non-streaming fast path: a single terminal frame is returned
            // unmodified (writes, point reads, DDL, counts, single-chunk scans).
            return Ok(resp);
        } else {
            total_bytes = total_bytes.saturating_add(resp.payload.len());
            if total_bytes > max_result_bytes {
                return Err(DispatchCollectError::OverBudget { bytes: total_bytes });
            }
            chunks.push(resp.payload.to_vec());
            final_response_meta = Some(resp);
            break;
        }
    }

    match final_response_meta {
        Some(meta) => Ok(Response {
            payload: Payload::from_vec(
                crate::control::server::payload_merge::merge_msgpack_arrays(&chunks),
            ),
            ..meta
        }),
        None => Err(DispatchCollectError::ChannelClosed),
    }
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
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Response> {
    dispatch_to_data_plane_with_source(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        crate::event::EventSource::User,
    )
    .await
}

/// Dispatch a physical plan to the Data Plane carrying an in-transaction id.
///
/// The gateway's local dispatch leg uses this for plans executing inside an
/// explicit transaction block: the id lands on the `Request` envelope so the
/// Data Plane keys the staging overlay for this transaction (staged writes
/// land under it; reads merge the transaction's own staged rows). Callers
/// with no transaction context stay on [`dispatch_to_data_plane`].
pub async fn dispatch_to_data_plane_in_txn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id,
            wal_lsn: None,
        },
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
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id: None,
            wal_lsn: None,
        },
    )
    .await
}

/// Dispatch a write to the Data Plane carrying the WAL LSN allocated for it.
///
/// Used by autocommit write endpoints that call `wal_append_if_write` and then
/// dispatch: the returned LSN is stamped onto the `Request` so the Data Plane
/// records the committed per-key / per-collection write version. The write's
/// identity and LSN travel in a [`WriteDispatch`] to keep the argument list
/// short; `wal_lsn` is `None` when the write was WAL-bypassed (e.g.
/// `timeseries` `wal=false`).
pub(crate) async fn dispatch_write_to_data_plane(
    shared: &SharedState,
    write: WriteDispatch,
) -> crate::Result<Response> {
    let WriteDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        wal_lsn,
    } = write;
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id,
            wal_lsn,
        },
    )
    .await
}

/// Identity + WAL LSN of a single autocommit write dispatched to the Data
/// Plane. Bundles the fields so [`dispatch_write_to_data_plane`] avoids a long
/// positional argument list.
pub(crate) struct WriteDispatch {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub plan: PhysicalPlan,
    pub trace_id: TraceId,
    pub event_source: crate::event::EventSource,
    pub txn_id: Option<crate::types::TxnId>,
    pub wal_lsn: Option<crate::types::Lsn>,
}

/// Dispatch a physical plan to the Data Plane carrying an explicit transaction
/// id so the Data Plane can resolve this transaction's staging overlay
/// (read-your-own-writes) and route `StageWrite`. Used by the native endpoint,
/// whose in-transaction tasks flow through this shared path.
pub async fn dispatch_to_data_plane_with_txn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id,
            // Staged in-transaction writes are not yet durably committed; the
            // committed write version is recorded at COMMIT via the batch funnel.
            wal_lsn: None,
        },
    )
    .await
}

/// Inputs for [`dispatch_to_data_plane_inner`]: the Data Plane request identity
/// plus the write's event source and optional owning transaction.
struct DataPlaneDispatch {
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: crate::event::EventSource,
    txn_id: Option<crate::types::TxnId>,
    /// WAL LSN allocated for this write (from `wal_append_if_write`), stamped
    /// onto the `Request` so the Data Plane records the committed write
    /// version. `None` for reads and control ops.
    wal_lsn: Option<crate::types::Lsn>,
}

async fn dispatch_to_data_plane_inner(
    shared: &SharedState,
    params: DataPlaneDispatch,
) -> crate::Result<Response> {
    let DataPlaneDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        wal_lsn,
    } = params;
    // Resolve any Exchange data-movement nodes before dispatch: a root-level
    // Gather fans the child to all cores and returns the merged response here;
    // a Broadcast join child is gathered and embedded so the plan reaching a
    // core is self-contained. Safe no-op for the many non-Exchange callers
    // (writes, metrics, triggers). Catalog materialization is identity-scoped
    // and already done upstream on the pgwire/native paths.
    // Internal funnel (COPY, cursors, materialized-view refresh, constraint
    // subqueries): not session-transaction-scoped, so `None`.
    let plan = match crate::control::server::exchange::resolve_exchange_in_plan(
        shared,
        database_id,
        tenant_id,
        plan,
        trace_id,
        None,
    )
    .await?
    {
        crate::control::server::exchange::Resolved::Gathered(resp, _shard_watermarks) => {
            return Ok(resp);
        }
        crate::control::server::exchange::Resolved::Plan(p) => p,
        // Internal funnel callers want a fully-collected Response, not a lazy
        // stream: materialize the stream into one merged-array Response,
        // preserving the prior gather-then-return behaviour on this path.
        crate::control::server::exchange::Resolved::Stream(s) => {
            return crate::control::server::exchange::gather::stream_to_response(s).await;
        }
    };

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
    let vshard_u32 = vshard_id.as_u32();

    let request_id = shared.next_request_id();
    let request = Request {
        request_id,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        deadline: Instant::now() + Duration::from_secs(shared.tuning.network.default_deadline_secs),
        priority: Priority::Normal,
        trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id,
        wal_lsn,
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
        shared.per_vshard_metrics.observe(vshard_u32, latency_us);
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
            is_timeseries_cdc_enabled(shared, database_id, tenant_id, &collection)
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
        shared.advance_tenant_write_hlc(tenant_id.as_u64());
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
fn is_timeseries_cdc_enabled(
    shared: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> bool {
    let catalog = shared.credentials.catalog();
    if let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), collection)
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

    use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};

    /// A standalone msgpack array of `n` one-byte elements — the shape a streamed
    /// scan chunk has (`encode_raw_document_rows` per chunk).
    fn array_payload(n: usize) -> Vec<u8> {
        let rows: Vec<Vec<u8>> = (0..n).map(|i| vec![(i % 128) as u8]).collect();
        encode_msgpack_array(&rows)
    }

    fn partial_rows(n: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(array_payload(n)),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
        }
    }

    fn final_rows(n: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(array_payload(n)),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
        }
    }

    /// Raw (non-array) payload, sized in bytes, for the budget-ceiling tests.
    fn partial_bytes(bytes: usize) -> Response {
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

    fn final_bytes(bytes: usize) -> Response {
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
        tx.send(final_bytes(100)).await.unwrap();
        drop(tx);
        // Single terminal frame returns unmodified — no merge, exact bytes.
        let resp = collect_bounded_response(&mut rx, 1024).await.unwrap();
        assert_eq!(resp.payload.len(), 100);
    }

    #[tokio::test]
    async fn streaming_merges_all_chunk_arrays() {
        // Three standalone array chunks must merge into ONE array with every
        // element — the regression: raw concatenation kept only the first array.
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial_rows(1000)).await.unwrap();
        tx.send(partial_rows(1000)).await.unwrap();
        tx.send(final_rows(500)).await.unwrap();
        drop(tx);
        let resp = collect_bounded_response(&mut rx, 1 << 20).await.unwrap();
        let elements = extract_msgpack_elements(resp.payload.as_ref());
        assert_eq!(
            elements.len(),
            2500,
            "streamed chunks must merge into one array of all rows, not just the first chunk"
        );
    }

    #[tokio::test]
    async fn streaming_over_budget_on_partial_aborts() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial_bytes(600)).await.unwrap();
        tx.send(partial_bytes(600)).await.unwrap();
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
        tx.send(partial_bytes(500)).await.unwrap();
        tx.send(final_bytes(600)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1000).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::OverBudget { .. }));
    }

    #[tokio::test]
    async fn channel_closed_without_final_is_explicit_error() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(partial_bytes(10)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1024).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::ChannelClosed));
    }
}
