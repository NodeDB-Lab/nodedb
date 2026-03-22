//! Shared dispatch utilities used by both the pgwire and HTTP endpoints.
//!
//! Centralizes WAL append logic and Data Plane request dispatch to prevent
//! duplication between the two query interfaces.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};
use crate::wal::manager::WalManager;

static DISPATCH_COUNTER: AtomicU64 = AtomicU64::new(1_000_000);

/// Default request deadline.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// Append a write operation to the WAL for single-node durability.
///
/// Serializes the write as MessagePack and appends to the appropriate
/// WAL record type. Read operations are no-ops (return Ok immediately).
pub fn wal_append_if_write(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    match plan {
        PhysicalPlan::PointPut {
            collection,
            document_id,
            value,
        } => {
            let entry = rmp_serde::to_vec(&(collection, document_id, value)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point put: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::PointDelete {
            collection,
            document_id,
        } => {
            let entry = rmp_serde::to_vec(&(collection, document_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::VectorInsert {
            collection,
            vector,
            dim,
            field_name: _,
        } => {
            let entry = rmp_serde::to_vec(&(collection, vector, dim)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector insert: {e}"),
                }
            })?;
            wal.append_vector_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::VectorBatchInsert {
            collection,
            vectors,
            dim,
        } => {
            let entry = rmp_serde::to_vec(&(collection, vectors, dim)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector batch insert: {e}"),
                }
            })?;
            wal.append_vector_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::VectorDelete {
            collection,
            vector_id,
        } => {
            let entry = rmp_serde::to_vec(&(collection, vector_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector delete: {e}"),
                }
            })?;
            wal.append_vector_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::CrdtApply { delta, .. } => {
            wal.append_crdt_delta(tenant_id, vshard_id, delta)?;
        }
        PhysicalPlan::EdgePut {
            src_id,
            label,
            dst_id,
            properties,
        } => {
            let entry = rmp_serde::to_vec(&(src_id, label, dst_id, properties)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal edge put: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::EdgeDelete {
            src_id,
            label,
            dst_id,
        } => {
            let entry = rmp_serde::to_vec(&(src_id, label, dst_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal edge delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::SetVectorParams {
            collection,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => {
            let entry = rmp_serde::to_vec(&(
                collection,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal set vector params: {e}"),
            })?;
            wal.append_vector_params(tenant_id, vshard_id, &entry)?;
        }
        // Read operations and control commands: no WAL needed.
        _ => {}
    }
    Ok(())
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
    let request_id = RequestId::new(DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed));
    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + DEFAULT_DEADLINE,
        priority: Priority::Normal,
        trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
    };

    let rx = shared.tracker.register(request_id);

    match shared.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request)?,
        Err(poisoned) => poisoned.into_inner().dispatch(request)?,
    };

    tokio::time::timeout(DEFAULT_DEADLINE, rx)
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "response channel closed".into(),
        })
}
