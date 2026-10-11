// SPDX-License-Identifier: BUSL-1.1

//! Data Plane dispatch helpers used by the clone materializer.
//!
//! Lets the walker issue scans and writes against source/target collections
//! without pgwire. The materializer awaits these futures on the caller's
//! runtime.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, Priority, Request, Response};
use crate::control::gateway::core::QueryContext;
use crate::control::server::dispatch_utils::{OwnedRead, OwnedReadScope, route_owned_read};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;

/// Run a plan on the node that owns its vShard and return its payload.
///
/// The plan routes through the gateway like any statement: a scan reads the
/// owner's shard, and a write goes through the owner's replicated write
/// path, so every replica of the shard applies it.
///
/// A `NotFound` verdict returns an empty payload. A plan that fans out to
/// several shards is refused: a materializer cursor walks one shard.
pub(crate) async fn dispatch_to_owner(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection_qualified: &str,
    plan: PhysicalPlan,
) -> crate::Result<Vec<u8>> {
    let gateway = state.installed_gateway()?;
    let ctx = QueryContext {
        tenant_id,
        trace_id: TraceId::generate(),
        database_id,
        txn_id: None,
        linearizable: true,
    };
    let mut payloads = match gateway.execute_internal(&ctx, plan).await {
        Ok(payloads) => payloads,
        Err(crate::Error::DataPlane(ErrorCode::NotFound)) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    if payloads.len() > 1 {
        return Err(crate::Error::Storage {
            engine: "clone_materializer".into(),
            detail: format!(
                "plan on '{collection_qualified}' answered from {} shards; \
                 a materializer page walks one shard",
                payloads.len()
            ),
        });
    }
    Ok(payloads.pop().unwrap_or_default())
}

/// Dispatch a `PhysicalPlan` to the Data Plane and await the response.
///
/// A write bypasses routing and replication: it reaches only this node's copy
/// of the shard. A read runs where the owning group serves it. Materializer
/// scans and writes use [`dispatch_to_owner`].
///
/// `txn_id` stamps the request with the transaction whose staging overlay
/// the handler must fold in. Autocommit callers pass `None`; COMMIT-time
/// MERGE / `UPDATE ... FROM` expanders pass the transaction id so the
/// RESOLVE pass sees rows staged earlier in the same transaction.
pub(crate) async fn dispatch_local(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection_qualified: &str,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let vshard_id =
        nodedb_types::CollectionKey::from_qualified_str(database_id, collection_qualified)?
            .vshard();
    dispatch_local_on_vshard(state, tenant_id, database_id, vshard_id, plan, txn_id).await
}

/// Run a read-only resolve pass that carries a `Write` grant (columnar DML's
/// `ResolveDml`) on this node's cores. `MERGE` and `UPDATE ... FROM` resolve
/// through `DocumentOp::ResolveWrite`, a read, which [`dispatch_local`] routes
/// to the owner.
///
/// The gateway carries such a plan as a write, so the pass cannot be sent
/// to the owner. It runs here only when this node replicates the collection's
/// home group, confirmed, and is refused with `NotLeader` otherwise
/// (`dispatch_utils::prepare_local_pass`).
pub(crate) async fn dispatch_resolve_pass(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection_qualified: &str,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let vshard_id =
        nodedb_types::CollectionKey::from_qualified_str(database_id, collection_qualified)?
            .vshard();
    crate::control::server::dispatch_utils::prepare_local_pass(state, vshard_id, txn_id).await?;
    dispatch_local_on_vshard(state, tenant_id, database_id, vshard_id, plan, txn_id).await
}

/// [`dispatch_local`] for a plan whose home vShard is not derived from a
/// collection name.
///
/// A graph edge plan is key-homed on its endpoints, so the vShard that holds
/// the edge is `VShardId::from_key(src_id)` and not the collection's hash.
/// Routing such a plan by collection reads a shard that never held it.
///
/// A read of this node's copy feeds a write (MERGE and `UPDATE ... FROM`
/// resolve passes, predicate write resolution). A read runs where the group
/// that owns `vshard_id` serves it, confirmed (`dispatch_utils::owner_read`):
/// a stale or absent replica aims the write at the wrong rows.
pub(crate) async fn dispatch_local_on_vshard(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let plan = if plan_is_write(&plan) {
        plan
    } else {
        let scope = OwnedReadScope {
            tenant_id,
            database_id,
            vshard_id,
            trace_id: TraceId::ZERO,
            txn_id,
            linearizable: true,
        };
        match route_owned_read(state, scope, plan).await? {
            OwnedRead::Local(plan) => *plan,
            OwnedRead::Served(response) => return Ok(response),
        }
    };
    dispatch_on_this_node(state, tenant_id, database_id, vshard_id, plan, txn_id).await
}

/// Dispatch `plan` to this node's core for `vshard_id` and await the
/// response, with no routing: the plan runs against this node's own state.
pub(crate) async fn dispatch_on_this_node(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let req_id = RequestId::new(state.request_id_counter.fetch_add(1, Ordering::Relaxed));
    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline_dur = Duration::from_secs(deadline_secs);
    let req = Request {
        request_id: req_id,
        tenant_id,
        vshard_id,
        database_id,
        plan,
        deadline: Instant::now() + deadline_dur,
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id,
        wal_lsn: None,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };
    let mut rx = state.tracker.register(req_id);
    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(req)?,
        Err(p) => p.into_inner().dispatch(req)?,
    }
    tokio::time::timeout(deadline_dur, rx.recv())
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id: req_id })?
        .ok_or(crate::Error::Dispatch {
            detail: "clone materializer: response channel closed".into(),
        })
}
