//! Shared dispatch utilities used by both the pgwire and HTTP endpoints.
//!
//! Centralizes WAL append logic and Data Plane request dispatch to prevent
//! duplication between the two query interfaces.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{Lsn, ReadConsistency, RequestId, TenantId, VShardId};
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
            doc_id: _,
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

    // Dispatch the plan to every core.
    let mut receivers = Vec::with_capacity(num_cores);
    for core_id in 0..num_cores {
        let request_id = RequestId::new(DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed));
        let vshard_id = VShardId::new(core_id as u16);
        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan: plan.clone(),
            deadline: Instant::now() + DEFAULT_DEADLINE,
            priority: Priority::Normal,
            trace_id,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
        };

        let rx = shared.tracker.register(request_id);
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

    // Start with '[' for JSON array.
    merged_payload.push(b'[');
    let mut first = true;

    for rx in receivers {
        let resp = tokio::time::timeout(DEFAULT_DEADLINE, rx)
            .await
            .map_err(|_| crate::Error::Dispatch {
                detail: "broadcast timeout".into(),
            })?
            .map_err(|_| crate::Error::Dispatch {
                detail: "broadcast channel closed".into(),
            })?;

        if resp.status == crate::bridge::envelope::Status::Error {
            // Non-fatal: some cores may not have data for this collection.
            // Only treat Internal errors as real failures.
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
            // Decode payload (may be MessagePack or JSON) to JSON text.
            let json_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let json_bytes = json_text.as_bytes();

            // If the core returned a JSON array, unwrap it and merge items.
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
        // All cores errored (non-NotFound).
        return Err(crate::Error::Dispatch { detail: error_msg });
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

/// Cross-core BFS orchestration for graph traversal.
///
/// Implements multi-hop BFS across all Data Plane cores. Each hop:
/// 1. Broadcasts GraphNeighbors for all frontier nodes to all cores
/// 2. Collects discovered neighbors from all cores
/// 3. Builds the next frontier (nodes not yet visited)
/// 4. Repeats until max_depth or no new nodes
///
/// Returns a JSON array of all discovered node IDs.
pub async fn cross_core_bfs(
    shared: &SharedState,
    tenant_id: TenantId,
    start_nodes: Vec<String>,
    edge_label: Option<String>,
    direction: crate::engine::graph::edge_store::Direction,
    max_depth: usize,
) -> crate::Result<Response> {
    use std::collections::HashSet;

    let mut visited: HashSet<String> = HashSet::new();
    let mut all_discovered: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = start_nodes.clone();

    // Mark start nodes as visited.
    for node in &start_nodes {
        visited.insert(node.clone());
        all_discovered.push(node.clone());
    }

    for _depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        // For each frontier node, broadcast GraphNeighbors to all cores.
        let mut next_frontier: Vec<String> = Vec::new();

        for node in &frontier {
            let plan = PhysicalPlan::GraphNeighbors {
                node_id: node.clone(),
                edge_label: edge_label.clone(),
                direction,
            };

            let resp = broadcast_to_all_cores(shared, tenant_id, plan, 0).await?;

            if !resp.payload.is_empty() {
                let json_text =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                // Parse neighbor results: [{"label":"X","node":"Y"}, ...]
                if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&json_text) {
                    for item in arr {
                        if let Some(neighbor) = item.get("node").and_then(|v| v.as_str())
                            && visited.insert(neighbor.to_string())
                        {
                            next_frontier.push(neighbor.to_string());
                            all_discovered.push(neighbor.to_string());
                        }
                    }
                }
            }
        }

        frontier = next_frontier;
    }

    // Serialize result as JSON array.
    let payload = serde_json::to_vec(&all_discovered).unwrap_or_else(|_| b"[]".to_vec());

    Ok(Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(payload),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
    })
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
