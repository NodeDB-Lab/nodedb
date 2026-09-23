// SPDX-License-Identifier: BUSL-1.1

//! Shared scaffolding for the array Raft apply paths.
//!
//! Holds the pieces reused across every per-concern apply module (op, schema,
//! cell): the committed-entry position identifier, the funnel submit shared by
//! both write paths, the Data-Plane `Request` builder and response-await helper
//! used by the array-open bootstrap, and the vShard derivation from an array
//! op's coordinate (its Hilbert-prefix tile placement).

use std::sync::Arc;
use std::time::Duration;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::distributed_applier::{AppliedWrite, ProposeResult};
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

/// Identifies a committed Raft entry within the apply loop.
///
/// Groups the three fields that always travel together: the Raft group, the
/// log index within that group, and the idempotency key extracted from the
/// `ReplicatedEntry` header. All three are forwarded together to
/// `ProposeTracker::complete` after each apply.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AppliedPosition {
    pub group_id: u64,
    pub log_index: u64,
    pub applied_key: u64,
}

/// One committed array write, ready for the Control-Plane write funnel.
pub(super) struct ArrayWriteSubmit {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard: VShardId,
    pub plan: PhysicalPlan,
    pub event_source: crate::event::EventSource,
    /// The instant the proposing node resolved for this entry, or `None` when
    /// the entry carries none. Passed through as the redo record's
    /// `now_override` so this replica records the value its peers recorded.
    pub resolved_now_ms: Option<u64>,
    /// The idempotency key of the committed entry, carried by the redo
    /// record's header.
    pub apply_key: u64,
    /// Contextual label for the error surfaced to the propose waiter.
    pub op_label: &'static str,
}

/// Submit a committed array write through the shared Control-Plane write funnel
/// and return the Data Plane's payload.
///
/// The funnel — not this module — appends the redo record, stamps the minted LSN
/// onto both the request and the plan, and fsyncs it before returning. That is
/// the whole reason both array write-apply paths route through here: the durable
/// applied floor the caller advances on success asserts exactly that this
/// entry's redo is fsync-durable, which is only true if a redo was minted at all.
///
/// An error-status response is surfaced as a typed error: a committed entry that
/// failed to apply must reach the propose waiter as a failure, not an empty
/// success, and must NOT advance the floor.
pub(super) async fn submit_array_write(
    state: &Arc<SharedState>,
    params: ArrayWriteSubmit,
) -> ProposeResult {
    let ArrayWriteSubmit {
        tenant_id,
        database_id,
        vshard,
        plan,
        event_source,
        resolved_now_ms,
        apply_key,
        op_label,
    } = params;

    let outcome = submit_write(
        state,
        SubmitWrite {
            tenant_id,
            database_id,
            vshard_id: vshard,
            plan,
            trace_id: TraceId::generate(),
            event_source,
            txn_id: None,
            // Auth ran on the node that proposed the entry; the committed entry
            // carries no session user.
            user_id: None,
            // The redo record is appended HERE, on this replica, from the
            // committed plan: the proposer's LSN is deliberately not carried on
            // the wire, and the array engine's tile state has no other
            // durability path than this record's replay.
            durability: WalDurability::AppendHere {
                now_override: resolved_now_ms,
                apply_key,
            },
            // Raft committed this entry at a fixed log index and every replica
            // applies it in that order; re-entering the write-admission gate
            // would re-decide an ordering that is already final.
            ordering: WriteOrdering::AlreadyOrdered,
            // An `ArrayOp::Put` / `Delete` does yield change metadata, but this
            // apply path runs on EVERY replica of the committed entry — the
            // node that proposed it owns the single publish. Emitting here
            // would give each subscriber one copy per replica plus a NOTIFY
            // fan-out from each. See [`ChangeFeedOwner`].
            change_feed: ChangeFeedOwner::Unowned,
        },
    )
    .await
    .map_err(|e| crate::Error::Internal {
        detail: format!("{op_label}: {e}"),
    })?;
    let response = outcome.response;

    if response.status != Status::Ok {
        let detail = response
            .error_code
            .as_ref()
            .map(|c| format!("{op_label} error: {c:?}"))
            .unwrap_or_else(|| format!("{op_label} returned error status"));
        return Err(crate::Error::Internal { detail });
    }
    // The response carries the write-version this replica stamped alongside the
    // payload; an array plan names no user collection, so it is `Lsn::ZERO` here
    // — reported honestly rather than substituted for.
    Ok(AppliedWrite::from_response(&response))
}

/// Derive the dispatch vShard for an array op from its coordinate.
///
/// When the array has known tile extents the coordinate is mapped to its tile
/// via the Hilbert-prefix routing (`vshard_for_array_coord`); otherwise the
/// array name alone selects the vShard. Shared by every array-op apply path so
/// each concern (op / cell) routes identically.
pub(super) fn vshard_for_array_op(
    state: &Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    op: &nodedb_array::sync::op::ArrayOp,
) -> VShardId {
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_cluster::array_routing::{array_vshard_for_name, vshard_for_array_coord};

    let tile_extents = state.array_sync_schemas.tile_extents_in_database(
        database_id,
        tenant_id.as_u64(),
        &op.header.array,
    );
    if let Some(extents) = tile_extents {
        let coord_u64: Vec<u64> = op
            .coord
            .iter()
            .map(|c| match c {
                CoordValue::Int64(v) | CoordValue::TimestampMs(v) => *v as u64,
                CoordValue::Float64(v) => v.to_bits(),
                CoordValue::String(_) => 0,
            })
            .collect();
        VShardId::new(vshard_for_array_coord(
            &op.header.array,
            &coord_u64,
            &extents,
        ))
    } else {
        VShardId::new(array_vshard_for_name(&op.header.array))
    }
}

/// Ensure the Data Plane has the array open before dispatching Put/Delete.
///
/// Looks up the catalog entry for `array_id.name`, then dispatches `OpenArray`
/// to the Data Plane. This is idempotent on the Data Plane side: if the array
/// is already open with the same schema hash, the handler returns `Ok`.
///
/// Returns an error if the catalog entry is missing (the array was never
/// registered on this node) or if the `OpenArray` dispatch fails.
pub(super) async fn ensure_array_open(
    state: &Arc<SharedState>,
    array_id: &nodedb_array::types::ArrayId,
    vshard: crate::types::VShardId,
    tenant_id: crate::types::TenantId,
    database_id: DatabaseId,
) -> crate::Result<()> {
    let (schema_msgpack, schema_hash, prefix_bits) = {
        let cat = state
            .array_catalog
            .read()
            .unwrap_or_else(|p| p.into_inner());
        match cat.lookup_by_id(array_id) {
            Some(entry) => (
                entry.schema_msgpack.clone(),
                entry.schema_hash,
                entry.prefix_bits,
            ),
            None => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "ensure_array_open: array '{}' not in catalog — register it before applying ops",
                        array_id.name
                    ),
                });
            }
        }
    };

    let open_plan = crate::bridge::envelope::PhysicalPlan::Array(
        nodedb_physical::physical_plan::ArrayOp::OpenArray {
            array_id: array_id.clone(),
            schema_msgpack,
            schema_hash,
            prefix_bits,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        },
    );
    let open_request = build_array_request(state, tenant_id, database_id, vshard, open_plan);
    let open_request_id = open_request.request_id;
    let mut open_rx = state.tracker.register(open_request_id);

    let dispatch_result = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(open_request),
        Err(poisoned) => poisoned.into_inner().dispatch(open_request),
    };

    if let Err(e) = dispatch_result {
        return Err(crate::Error::Internal {
            detail: format!("ensure_array_open: dispatch failed: {e}"),
        });
    }

    await_data_plane(async move { open_rx.recv().await.ok_or(()) }, "OpenArray")
        .await
        .map(|_| ())
}

/// Build a `Request` for an array apply/open with default deadline / priority.
///
/// Centralises the six boilerplate fields that are identical for every
/// Control-Plane → Data-Plane dispatch originating from the array apply path.
pub(super) fn build_array_request(
    state: &Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Request {
    Request {
        request_id: state.next_request_id(),
        tenant_id,
        database_id,
        vshard_id,
        plan,
        deadline: std::time::Instant::now()
            + Duration::from_secs(state.tuning.network.default_deadline_secs),
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::CrdtSync,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    }
}

/// Await a Data Plane response, mapping timeout / channel-closed / error-status
/// into `crate::Error::Internal` with a contextual `op_label`.
pub(super) async fn await_data_plane(
    rx: impl std::future::Future<Output = Result<Response, ()>>,
    op_label: &str,
) -> ProposeResult {
    match tokio::time::timeout(Duration::from_secs(30), rx).await {
        Ok(Ok(resp)) if resp.status == Status::Ok => Ok(AppliedWrite::from_response(&resp)),
        Ok(Ok(resp)) => {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{op_label} error: {c:?}"))
                .unwrap_or_else(|| format!("{op_label} returned error status"));
            Err(crate::Error::Internal { detail })
        }
        Ok(Err(_)) => Err(crate::Error::Internal {
            detail: format!("{op_label}: response channel closed"),
        }),
        Err(_) => Err(crate::Error::Internal {
            detail: format!("{op_label}: deadline exceeded"),
        }),
    }
}
