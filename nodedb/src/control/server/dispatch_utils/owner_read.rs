// SPDX-License-Identifier: BUSL-1.1

//! Serving a read from the group that owns its rows.
//!
//! A read that runs on this node's cores sees only the groups this node
//! replicates. A collection whose home group lives on other nodes has no rows
//! here, so a local read of it returns nothing. [`route_owned_read`] picks
//! where the read runs:
//!
//! - This node replicates the home group: the read runs here. A linearizable
//!   read confirms the group first.
//! - A read inside a transaction runs on the group's leader. The
//!   transaction's staging overlay lives there
//!   (`shared/session/leader_forward.rs`).
//! - Otherwise the read goes through the gateway to the group's leader. The
//!   gateway carries the same consistency, and the serving node confirms it.
//!
//! A graph or array plan spreads its rows across every hosted group. It runs
//! here and confirms the groups it reads (`graph_dispatch::read_groups`).

use futures::future::{BoxFuture, Either, Ready, ready};

use crate::bridge::envelope::{ErrorCode, Payload, PhysicalPlan, Response, Status};
use crate::control::cluster::linearizable_read::{
    confirm_linearizable_read, hosted_groups_of_vshards, statement_read_deadline,
};
use crate::control::gateway::RouteDecision;
use crate::control::gateway::core::QueryContext;
use crate::control::gateway::live_leaders::resolve_live_decision;
use crate::control::security::identity::{Permission, required_permission};
use crate::control::server::payload_merge::merge_msgpack_arrays;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, ReadVersions, RequestId, TenantId, TraceId, TxnId, VShardId};

/// Where one read runs.
pub(crate) struct OwnedReadScope {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The vShard that holds the rows the plan reads.
    pub vshard_id: VShardId,
    pub trace_id: TraceId,
    pub txn_id: Option<TxnId>,
    /// The read must observe every write committed before it began.
    pub linearizable: bool,
}

/// The outcome of [`route_owned_read`].
pub(crate) enum OwnedRead {
    /// The plan runs on this node's cores. Any confirm already ran. Boxed:
    /// a plan is far larger than a response.
    Local(Box<PhysicalPlan>),
    /// The owning group's leader answered the read.
    Served(Response),
}

/// Decide where `plan` runs, and run it at the owner when that is another
/// node. A plan that reads no user rows always runs here.
///
/// The return type is a named future, never an opaque `impl Future`. The
/// owner path enters the gateway, and the gateway dispatches back through
/// the funnel that awaits this function. An opaque type here puts that cycle
/// in the compiler's layout and `Send` checks, which then overflow or fail.
/// A plan that runs here without a confirm answers without a heap allocation.
pub(crate) fn route_owned_read(
    shared: &SharedState,
    scope: OwnedReadScope,
    plan: PhysicalPlan,
) -> Either<Ready<crate::Result<OwnedRead>>, BoxFuture<'_, crate::Result<OwnedRead>>> {
    if shared.cluster_routing.is_none() || !matches!(required_permission(&plan), Permission::Read) {
        return Either::Left(ready(Ok(OwnedRead::Local(Box::new(plan)))));
    }
    Either::Right(Box::pin(route_cluster_read(shared, scope, plan)))
}

/// [`route_owned_read`] for a read on a cluster node.
async fn route_cluster_read(
    shared: &SharedState,
    scope: OwnedReadScope,
    plan: PhysicalPlan,
) -> crate::Result<OwnedRead> {
    let deadline = statement_read_deadline(shared);
    if nodedb_physical::physical_plan::plan_contains_cluster_partitioned_leaf(&plan) {
        if scope.linearizable {
            let groups = crate::control::server::graph_dispatch::graph_read_groups(
                shared,
                scope.database_id,
                &plan,
            )?;
            confirm_linearizable_read(shared, &groups, deadline).await?;
        }
        return Ok(OwnedRead::Local(Box::new(plan)));
    }
    match read_placement(shared, scope.vshard_id, scope.txn_id)? {
        ReadPlacement::Here(hosted) => {
            if scope.linearizable {
                confirm_linearizable_read(shared, &hosted, deadline).await?;
            }
            Ok(OwnedRead::Local(Box::new(plan)))
        }
        ReadPlacement::Owner => {
            let gateway = shared.installed_gateway()?;
            let ctx = QueryContext {
                tenant_id: scope.tenant_id,
                trace_id: scope.trace_id,
                database_id: scope.database_id,
                txn_id: scope.txn_id,
                linearizable: scope.linearizable,
            };
            owner_response(gateway.execute_internal_outcome(&ctx, plan).await)
                .map(OwnedRead::Served)
        }
    }
}

/// Where a read of `vshard_id`'s rows runs.
pub(crate) enum ReadPlacement {
    /// On this node, which replicates these groups of the read.
    Here(Vec<u64>),
    /// On the group's leader, through the gateway.
    Owner,
}

/// Where a read of `vshard_id` runs: here when this node replicates the group
/// (a transaction's read also needs this node to lead it, since the staging
/// overlay lives on the leader), on the owner otherwise. Always here without
/// a cluster.
pub(crate) fn read_placement(
    shared: &SharedState,
    vshard_id: VShardId,
    txn_id: Option<TxnId>,
) -> crate::Result<ReadPlacement> {
    if shared.cluster_routing.is_none() {
        return Ok(ReadPlacement::Here(Vec::new()));
    }
    let hosted = hosted_groups_of_vshards(shared, [vshard_id.as_u32()])?;
    let serve_here = if txn_id.is_some() {
        leads_vshard(shared, vshard_id)
    } else {
        !hosted.is_empty()
    };
    Ok(if serve_here {
        ReadPlacement::Here(hosted)
    } else {
        ReadPlacement::Owner
    })
}

/// The owner's answer to a read, in the shape a local dispatch returns.
///
/// A read that found no row answers `NotFound` with the versions it
/// observed, the same as a local miss, so the read-set records the vShard's
/// real version.
pub(crate) fn owner_response(
    outcome: crate::Result<crate::control::gateway::outcome::GatewayOutcome>,
) -> crate::Result<Response> {
    let outcome = outcome?;
    let watermark_lsn = outcome
        .shard_watermarks
        .iter()
        .map(|(_, lsn)| *lsn)
        .max()
        .unwrap_or(Lsn::ZERO);
    if outcome.not_found {
        return Ok(response(
            Status::Error,
            Vec::new(),
            watermark_lsn,
            outcome.read_versions,
            Some(ErrorCode::NotFound),
        ));
    }
    let payloads = outcome.payloads;
    let payload = match payloads.len() {
        0 => Vec::new(),
        1 => payloads.into_iter().next().unwrap_or_default(),
        _ => merge_msgpack_arrays(&payloads),
    };
    Ok(response(
        Status::Ok,
        payload,
        watermark_lsn,
        outcome.read_versions,
        None,
    ))
}

/// Prepare a read-only pass that must run on this node's cores.
///
/// A columnar DML resolve pass (`ResolveDml`) reads rows but needs the
/// `Write` grant, so the gateway will carry it as a write. It runs
/// here, which is sound only where [`read_placement`] places the read: on a
/// node that replicates `vshard_id`'s group, and that leads it for a
/// transaction's pass. Otherwise the pass is refused with `NotLeader` naming
/// the leader, and no pass reads a replica that lacks its rows or overlay. A
/// served pass confirms the group first.
pub(crate) async fn prepare_local_pass(
    shared: &SharedState,
    vshard_id: VShardId,
    txn_id: Option<TxnId>,
) -> crate::Result<()> {
    match read_placement(shared, vshard_id, txn_id)? {
        ReadPlacement::Here(hosted) => {
            confirm_linearizable_read(shared, &hosted, statement_read_deadline(shared)).await
        }
        ReadPlacement::Owner => {
            let (leader_node, leader_term) = shared
                .cluster_routing
                .as_ref()
                .map(|lock| lock.read().unwrap_or_else(|p| p.into_inner()))
                .and_then(|routing| routing.leader_at_term_for_vshard(vshard_id.as_u32()).ok())
                .unwrap_or((0, 0));
            Err(crate::Error::NotLeader {
                vshard_id,
                leader_node,
                leader_addr: String::new(),
                leader_term,
            })
        }
    }
}

/// Whether live Raft names this node the leader of `vshard_id`'s group.
fn leads_vshard(shared: &SharedState, vshard_id: VShardId) -> bool {
    matches!(
        resolve_live_decision(shared, vshard_id.as_u32()),
        RouteDecision::Local
    )
}

/// A successful response carrying `payload`, in the shape a local dispatch
/// returns.
pub(crate) fn ok_payload_response(payload: Payload) -> Response {
    response(
        Status::Ok,
        payload.to_vec(),
        Lsn::ZERO,
        ReadVersions::new(),
        None,
    )
}

/// A `NotFound` refusal, the shape a local dispatch returns for a read that
/// found nothing.
pub(crate) fn not_found_response() -> Response {
    response(
        Status::Error,
        Vec::new(),
        Lsn::ZERO,
        ReadVersions::new(),
        Some(ErrorCode::NotFound),
    )
}

fn response(
    status: Status,
    payload: Vec<u8>,
    watermark_lsn: Lsn,
    read_versions: ReadVersions,
    error_code: Option<ErrorCode>,
) -> Response {
    Response {
        request_id: RequestId::new(0),
        status,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(payload),
        watermark_lsn,
        error_code: error_code.map(Box::new),
        stage_vote: None,
        read_versions,
        write_set: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::gateway::outcome::GatewayOutcome;

    fn outcome(not_found: bool) -> GatewayOutcome {
        GatewayOutcome {
            payloads: Vec::new(),
            shard_watermarks: vec![(VShardId::new(3), Lsn::new(7))],
            read_versions: ReadVersions::single(
                VShardId::new(3),
                nodedb_types::WriteVersion::logged(1, 40),
            ),
            not_found,
        }
    }

    /// A remote read that found no row answers like a local miss, with the
    /// version it observed for the read-set.
    #[test]
    fn an_owner_miss_keeps_its_verdict_and_its_version() {
        let response = owner_response(Ok(outcome(true))).expect("an owner miss answers");
        assert_eq!(response.status, Status::Error);
        assert_eq!(response.error_code.as_deref(), Some(&ErrorCode::NotFound));
        assert_eq!(
            response.read_versions.of(VShardId::new(3)),
            Some(nodedb_types::WriteVersion::logged(1, 40))
        );
    }

    #[test]
    fn an_owner_answer_keeps_its_version() {
        let response = owner_response(Ok(outcome(false))).expect("an owner answer");
        assert_eq!(response.status, Status::Ok);
        assert_eq!(
            response.read_versions.of(VShardId::new(3)),
            Some(nodedb_types::WriteVersion::logged(1, 40))
        );
    }
}
