// SPDX-License-Identifier: BUSL-1.1

//! Write-fan-out broadcast helpers.
//!
//! These functions broadcast write-like or admin plans to every Data-Plane
//! core and merge a count or await acknowledgement from each.  They are the
//! write-replication path, not the read data-movement path — read data
//! movement is handled by `exchange::gather::gather_all_cores`.

use std::sync::atomic::{AtomicU64, Ordering};

use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::server::shared::session::statement_deadline;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

/// Total number of broadcast invocations (read + write) since process start.
///
/// Exposed so callers (including test harnesses) can assert O(hops)
/// call-count budgets on batched BFS paths.  `gather_all_cores` in
/// `exchange::gather` increments this counter via
/// `broadcast_call_count_increment`.
static BROADCAST_CALLS: AtomicU64 = AtomicU64::new(0);

/// Read the total broadcast call count for observability / tests.
pub fn broadcast_call_count() -> u64 {
    BROADCAST_CALLS.load(Ordering::Relaxed)
}

/// Increment the broadcast call counter.
///
/// Called by `exchange::gather::gather_all_cores` so all fan-out paths share
/// the same counter regardless of which helper is used.
pub(crate) fn broadcast_call_count_increment() {
    BROADCAST_CALLS.fetch_add(1, Ordering::Relaxed);
}

/// The typed error one core's refusal means to this barrier.
///
/// The code travels through the one Data-Plane-code conversion the crate
/// owns, so a core that ran out of time reports the statement's deadline and
/// a constraint refusal keeps its own SQLSTATE. A `{code:?}` dump into a
/// generic dispatch failure made every one of them read as internal.
fn typed_core_error(resp: &Response) -> crate::Error {
    match crate::control::local_dispatch::reject_data_plane_error(resp) {
        Err(error) => error,
        // `NotFound` is an empty observation elsewhere. A barrier takes any
        // error status as a core that did not acknowledge.
        Ok(()) => crate::Error::Dispatch {
            detail: "core returned an error status: NotFound".into(),
        },
    }
}

/// The `Admission` stamp for a per-core fan-out `Request`.
///
/// This fan-out builds its own per-core `Request` and enqueues directly (not
/// via the autocommit funnel). A cross-core fan-out write (e.g.
/// `INSERT ... SELECT`) is not a single-vShard point write, so the point-lock
/// fence does not apply — its isolation is enforced by collection-version
/// validation, not here. Write-class plans are stamped `Admitted`; DDL / reads
/// are `Exempt`.
fn broadcast_admission(plan: &PhysicalPlan) -> crate::bridge::envelope::Admission {
    if crate::control::server::shared::write_admission::plan_is_write(plan) {
        crate::bridge::envelope::Admission::Admitted
    } else {
        crate::bridge::envelope::Admission::Exempt(crate::bridge::envelope::ExemptReason::Read)
    }
}

/// Fan a read plan to every Data-Plane core and return the merged response.
///
/// Thin wrapper over the single gather primitive
/// [`crate::control::server::exchange::gather_all_cores`] for callers (e.g.
/// graph dispatch) that fan a plan to all cores and want the concatenated row
/// array. Join/catalog read data movement instead flows through
/// `exchange::resolve_and_materialize`; this entry stays for direct
/// all-core scans that are not expressed as an `Exchange` plan node.
pub async fn broadcast_to_all_cores(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Response> {
    // Graph/DDL all-core broadcast: not session-transaction-scoped, so `None`.
    broadcast_to_all_cores_txn(shared, tenant_id, database_id, plan, trace_id, None).await
}

/// [`broadcast_to_all_cores`], but session-transaction-scoped: `txn_id`
/// (when `Some`) is stamped onto every per-core request so a Data Plane
/// handler can merge that transaction's staged writes into its durable
/// result -- read-your-own-writes. Used by the GRAPH `Neighbors` single-hop
/// read so an in-transaction `GRAPH NEIGHBORS` observes the transaction's
/// own uncommitted edge writes (see `GraphTxnOverlay`).
pub async fn broadcast_to_all_cores_txn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    let outcome = crate::control::server::exchange::gather_all_cores(
        shared,
        tenant_id,
        database_id,
        plan,
        trace_id,
        txn_id,
    )
    .await?;
    Ok(Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(outcome.merged_array),
        watermark_lsn: outcome.watermark_lsn,
        error_code: None,
        stage_vote: None,
        read_versions: outcome.read_versions,
        write_set: Vec::new(),
    })
}

/// Broadcast a write-like plan to all cores and sum a numeric count field from
/// each response payload (for example `{"inserted": N}`).
pub async fn broadcast_count_to_all_cores(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    count_key: &str,
) -> crate::Result<Response> {
    // One statement, one deadline instant — every core waits to the same one.
    let deadline = statement_deadline(shared.tuning.network.default_deadline_secs);
    broadcast_count_to_all_cores_until(
        shared,
        tenant_id,
        database_id,
        plan,
        trace_id,
        count_key,
        deadline,
    )
    .await
}

/// [`broadcast_count_to_all_cores`] with every core bounded by `deadline`.
/// A caller outside the statement's task picks its own deadline.
pub(crate) async fn broadcast_count_to_all_cores_until(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    count_key: &str,
    deadline: std::time::Instant,
) -> crate::Result<Response> {
    BROADCAST_CALLS.fetch_add(1, Ordering::Relaxed);
    let num_cores = shared
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .num_cores();

    let mut receivers = Vec::with_capacity(num_cores);
    for core_id in 0..num_cores {
        let request_id = shared.next_request_id();
        let vshard_id = VShardId::new(core_id as u32);
        let admission = broadcast_admission(&plan);
        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id,
            plan: plan.clone(),
            deadline,
            priority: Priority::Normal,
            trace_id,
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
            entry_version: None,
            admission,
        };

        let rx = shared.tracker.register(request_id);
        let dispatched = shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request);
        if let Err(error) = dispatched {
            // No response ever arrives for a refused request.
            shared.tracker.cancel(&request_id);
            return Err(error);
        }
        receivers.push((request_id, rx));
    }

    let mut total = 0usize;
    let mut max_lsn = Lsn::ZERO;
    // First error seen across cores, kept as a TYPED error so a core that ran
    // out of time reports the statement's deadline rather than collapsing into
    // a generic dispatch failure.
    let mut first_error: Option<crate::Error> = None;

    for (request_id, mut rx) in receivers {
        let resp = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            rx.recv(),
        )
        .await
        {
            Ok(Some(resp)) => resp,
            // A producer that stopped after the deadline stopped because the
            // statement ran out of time; the closure is the symptom.
            Ok(None) if std::time::Instant::now() >= deadline => {
                return Err(crate::Error::DeadlineExceeded { request_id });
            }
            Ok(None) => {
                return Err(crate::Error::Dispatch {
                    detail: "broadcast count channel closed".into(),
                });
            }
            Err(_) => return Err(crate::Error::DeadlineExceeded { request_id }),
        };

        if resp.status == crate::bridge::envelope::Status::Error {
            if first_error.is_none() {
                first_error = Some(typed_core_error(&resp));
            }
            continue;
        }

        if resp.watermark_lsn > max_lsn {
            max_lsn = resp.watermark_lsn;
        }

        total += decode_count_field(&resp.payload, count_key).unwrap_or(0);
    }

    // A broadcast is an all-core barrier. Returning success after even one
    // error will let callers finalize control-plane state while that core
    // still retains the old Array store.
    if let Some(error) = first_error {
        return Err(error);
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
        stage_vote: None,
        read_versions: crate::types::ReadVersions::new(),
        write_set: Vec::new(),
    })
}

/// Broadcast a `DocumentOp::Register` plan to **every** Data Plane core
/// and await an acknowledgement from each core before returning.
///
/// This is the cross-core schema visibility barrier: callers (ALTER DDL,
/// collection post-apply hooks) must not return success to the client
/// until every core has applied the new schema.  Any core that returns an
/// error status or times out causes this function to return a typed error
/// — no warn-and-continue.
pub async fn broadcast_register_to_all_cores(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<()> {
    let num_cores = shared
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .num_cores();

    let mut receivers = Vec::with_capacity(num_cores);
    for core_id in 0..num_cores {
        let request_id = shared.next_request_id();
        let vshard_id = VShardId::new(core_id as u32);
        let admission = broadcast_admission(&plan);
        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id,
            plan: plan.clone(),
            deadline: statement_deadline(shared.tuning.network.default_deadline_secs),
            priority: Priority::Normal,
            trace_id,
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
            entry_version: None,
            admission,
        };

        let rx = shared.tracker.register(request_id);
        shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request)?;
        receivers.push((core_id, request_id, rx));
    }

    // One statement, one deadline instant — and every exit that ends on it
    // reports the deadline rather than the closure that followed from it.
    let deadline = statement_deadline(shared.tuning.network.default_deadline_secs);

    for (core_id, request_id, mut rx) in receivers {
        let resp = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            rx.recv(),
        )
        .await
        {
            Ok(Some(resp)) => resp,
            Ok(None) if std::time::Instant::now() >= deadline => {
                return Err(crate::Error::DeadlineExceeded { request_id });
            }
            Ok(None) => {
                return Err(crate::Error::Dispatch {
                    detail: format!("schema register barrier channel closed on core {core_id}"),
                });
            }
            Err(_) => return Err(crate::Error::DeadlineExceeded { request_id }),
        };

        if resp.status == crate::bridge::envelope::Status::Error {
            return Err(typed_core_error(&resp));
        }
    }

    tracing::info!(
        target: "nodedb::schema_barrier",
        num_cores,
        tenant = tenant_id.as_u64(),
        "schema_version_barrier_acquired",
    );
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::broadcast_call_count;

    #[test]
    fn call_count_readable() {
        let before = broadcast_call_count();
        assert!(before < u64::MAX);
    }
}
