// SPDX-License-Identifier: BUSL-1.1

//! Remote dispatch: send a plan to a remote node via `ExecuteRequest` RPC.
//!
//! Holds the one-shot path, [`dispatch_remote`], used by
//! [`super::dispatcher::dispatch_route`]. The streaming path lives in
//! [`super::dispatch_remote_stream`].

use std::sync::Arc;

use nodedb_cluster::rpc_codec::{ExecuteRequest, RaftRpc};
use tracing::debug;

use crate::Error;
use crate::control::security::identity::{Permission, required_permission};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::wire as plan_wire;

use super::cluster_error::{RemoteSend, map_typed_cluster_error, reply_wait_expired, send_error};
use super::dispatcher::DispatchOutcome;
use super::version_set::GatewayVersionSet;

/// Arguments for a remote dispatch call (bundles the parameters to stay
/// within clippy's `too_many_arguments` limit).
pub(super) struct RemoteDispatchArgs<'a> {
    pub plan: nodedb_physical::physical_plan::PhysicalPlan,
    pub shared: &'a Arc<SharedState>,
    pub node_id: u64,
    pub vshard_id: u64,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub trace_id: TraceId,
    /// Milliseconds left on the statement when this attempt starts.
    pub deadline_ms: u64,
    pub version_set: &'a GatewayVersionSet,
    /// Session-transaction context forwarded to the remote executor, or `None`
    /// for non-transactional dispatch.
    pub txn_id: Option<TxnId>,
    /// The route is a leg of a linearizable read. Exchange nodes resolved here
    /// before the plan ships inherit it.
    pub linearizable: bool,
    /// Groups the remote node confirms before it reads. Empty for a write or
    /// a weaker read.
    pub read_groups: Vec<u64>,
    /// Roles of the identity that wrote the statement. The remote node puts
    /// them on the replicated write so every replica judges them. Empty for
    /// a read or a system write.
    pub user_roles: Vec<String>,
}

/// Remote dispatch via `ExecuteRequest` RPC.
pub(super) async fn dispatch_remote(
    args: RemoteDispatchArgs<'_>,
) -> Result<DispatchOutcome, Error> {
    let RemoteDispatchArgs {
        plan,
        shared,
        node_id,
        vshard_id,
        tenant_id,
        database_id,
        trace_id,
        deadline_ms,
        version_set,
        txn_id,
        linearizable,
        read_groups,
        user_roles,
    } = args;
    // The statement's deadline, fixed before any wait of this attempt.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(deadline_ms);
    let transport = shared.cluster_transport.as_ref().ok_or(Error::Internal {
        detail: "gateway: cluster transport not available for remote dispatch".into(),
    })?;

    // Resolve any Exchange data-movement nodes BEFORE shipping to the remote
    // node. A Data-Plane core rejects any plan still containing an Exchange, so
    // the coordinator must gather/embed cross-node data here — symmetric with
    // the local path (`dispatch_local` → `dispatch_to_data_plane_with_source`,
    // which already resolves). A self-contained plan (no Exchange) is a no-op.
    // `resolve_exchange_in_plan` is identity-free; catalog materialization is
    // already done upstream on the pgwire/native paths that own the identity.
    // (`Box::pin` breaks the async-recursion cycle: resolving a Broadcast build
    // side calls `gather_all_vshards` → `gateway.execute` → routing → here.)
    // Cluster remote-dispatch: no session-transaction context crosses this
    // boundary yet, so `None`. TRACKED: cross-node in-transaction reads are a
    // known gap (see resolve/exchange.rs).
    let scope = crate::control::server::exchange::ReadScope {
        database_id,
        tenant_id,
        trace_id,
        txn_id: None,
        linearizable,
    };
    let plan = match Box::pin(crate::control::server::exchange::resolve_exchange_in_plan(
        shared, plan, scope,
    ))
    .await?
    {
        // A root-level Gather resolved entirely at the coordinator — its merged
        // response is ready; return it instead of shipping anything.
        crate::control::server::exchange::Resolved::Gathered(
            resp,
            shard_watermarks,
            _shuffle_reads,
        ) => {
            return Ok(DispatchOutcome {
                payloads: vec![resp.payload.to_vec()],
                shard_watermarks,
                // Coordinator-gathered at the exchange root: the gather folded
                // the read vShards across the responding shards and stamped
                // them on this response, so carry them through. This route
                // read no vShard of its own to report.
                read_vshards: resp.read_vshards,
                not_found: false,
            });
        }
        crate::control::server::exchange::Resolved::Plan(p) => *p,
        // Gateway path returns collected bytes: materialize the stream into one
        // merged-array payload. Key the collected watermark to the collection's
        // owning vShard this route dispatched to.
        crate::control::server::exchange::Resolved::Stream(s) => {
            let (merged, lsn) = crate::control::server::result_stream::materialize(s).await?;
            return Ok(DispatchOutcome {
                payloads: vec![merged],
                shard_watermarks: vec![(VShardId::new(vshard_id as u32), lsn)],
                // A materialized stream reports no read vShards: its frames
                // carry per-batch watermarks only. Empty is honest here rather
                // than lossy: the streaming branch is gated on
                // `txn_id.is_none()` (`resolve/exchange.rs`), so a stream never
                // serves an in-transaction read and no read-set entry consumes
                // this value.
                read_vshards: crate::types::ReadShards::new(),
                not_found: false,
            });
        }
    };

    // A read can be sent again after a lost answer. Any other plan can have
    // run on the target, so a lost answer is an unknown outcome.
    let send = RemoteSend {
        node_id,
        vshard_id,
        resend_safe: required_permission(&plan) == Permission::Read,
        deadline,
    };

    // Encode the plan.
    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("gateway: plan encode failed: {e}"),
    })?;

    // Build descriptor version entries.
    let descriptor_versions: Vec<nodedb_cluster::rpc_codec::DescriptorVersionEntry> = version_set
        .iter()
        .map(
            |(name, version)| nodedb_cluster::rpc_codec::DescriptorVersionEntry {
                collection: name.clone(),
                version: *version,
            },
        )
        .collect();

    let scoped_vshard = scoped_vshard(&plan, vshard_id)?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: tenant_id.as_u64(),
        database_id: database_id.as_u64(),
        deadline_remaining_ms: deadline_ms,
        trace_id: trace_id.0,
        descriptor_versions,
        txn_id,
        vshard_id: scoped_vshard,
        read_groups,
        user_roles,
    });

    debug!(
        node_id,
        vshard_id,
        tenant_id = tenant_id.as_u64(),
        "gateway: dispatching ExecuteRequest to remote node"
    );

    // The remote handler works within the statement's whole remaining budget.
    // A live node gets all of it. A dead node fails the send at the QUIC idle
    // timeout, and a read then retries elsewhere.
    let reply_wait = nodedb_cluster::rpc_codec::reply_wait(deadline_ms);
    let resp_rpc = match tokio::time::timeout(
        reply_wait,
        transport.send_rpc_with_read_timeout(node_id, req, reply_wait),
    )
    .await
    {
        Ok(answer) => answer.map_err(|e| send_error(e, &send))?,
        // The wait was the statement's whole budget: no retry has time left.
        Err(_) => return Err(reply_wait_expired(&send, reply_wait)),
    };

    match resp_rpc {
        RaftRpc::ExecuteResponse(resp) => {
            // A read that found no row reads back like a local miss: an
            // empty answer that keeps the `NotFound` verdict and the versions
            // the read observed. Every other error crosses as its typed error.
            let not_found = match resp.error {
                None => false,
                Some(err) => match map_typed_cluster_error(err, vshard_id) {
                    Error::DataPlane(crate::bridge::envelope::ErrorCode::NotFound) => true,
                    error => return Err(error),
                },
            };
            // Key the remote's read watermark to the collection's owning
            // vShard this route routed to (mirroring `dispatch_local`), NOT
            // the `vshard_id % COUNT` retry hint — the read-set validator
            // expects the true owning vShard as the entry key.
            Ok(DispatchOutcome {
                shard_watermarks: vec![(
                    VShardId::new(vshard_id as u32),
                    Lsn::new(resp.watermark_lsn),
                )],
                payloads: resp.payloads,
                read_vshards: crate::types::ReadShards::from_wire(&resp.read_vshards),
                not_found,
            })
        }
        other => Err(Error::Internal {
            detail: format!("gateway: unexpected RPC response variant: {other:?}"),
        }),
    }
}

/// The vShard a remote receiver must run `plan` on, or `None` when the plan
/// fans across the receiver's local cores.
///
/// A transaction meta-op names no collection, so only the route's own vShard
/// can pick the core that holds the transaction's staging overlay.
pub(super) fn scoped_vshard(
    plan: &nodedb_physical::physical_plan::PhysicalPlan,
    vshard_id: u64,
) -> Result<Option<VShardId>, Error> {
    if !super::router::is_task_vshard_scoped(plan) {
        return Ok(None);
    }
    let raw = u32::try_from(vshard_id)
        .ok()
        .filter(|raw| *raw < VShardId::COUNT)
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "gateway: vShard-scoped plan routed to out-of-range vShard {vshard_id}"
            ),
        })?;
    Ok(Some(VShardId::new(raw)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

    #[test]
    fn a_transaction_meta_op_carries_its_route_vshard() {
        let plan = PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
            txn_id: TxnId::new(4),
        });
        assert_eq!(scoped_vshard(&plan, 77).unwrap(), Some(VShardId::new(77)));
    }

    #[test]
    fn a_fanned_plan_carries_no_vshard() {
        let plan = PhysicalPlan::Meta(MetaOp::Checkpoint);
        assert_eq!(scoped_vshard(&plan, 77).unwrap(), None);
    }

    #[test]
    fn an_out_of_range_vshard_is_refused() {
        let plan = PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
            txn_id: TxnId::new(4),
        });
        assert!(scoped_vshard(&plan, u64::from(VShardId::COUNT)).is_err());
    }
}
