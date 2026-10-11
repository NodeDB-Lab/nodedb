// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::HomeVersions` on this node: each core answers the probes of the
//! vShards it owns, under this node's leader lease on each probe's group.
//!
//! A write to a vShard runs on the one core the dispatcher's router assigns
//! it, and raises the vShard's versions there. So a probe is answered by that
//! core alone. A write to another vShard never moves the answer.
//!
//! A node that lost leadership of a group can miss writes a newer leader
//! committed. So a probe is answered only while this node holds its group's
//! leader lease, after this node applied the group through the lease read
//! index (`cluster::leased_read`). A probe of a group whose lease this node
//! does not hold is answered `HomeAnswer::NotLeader` with the leader and term
//! the routing table names, and the committer asks that leader.

use std::collections::{BTreeMap, HashMap};

use futures::future::try_join_all;
use nodedb_physical::physical_plan::{
    HomeAnswer, HomeVersion, HomeVersionProbe, MetaOp, PhysicalPlan,
};

use crate::control::cluster::leased_read::{LeaseRefusal, confirm_leased_read};
use crate::control::cluster::linearizable_read::statement_read_deadline;
use crate::control::server::exchange::owning_core::dispatch_single_owning_core;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, VShardId};

use super::dispatch::NodeLevelResult;

/// Answer the probes of a `HomeVersions` plan: refuse those of groups this
/// node holds no lease on, split the rest by owning core, ask each core for
/// its own, and return every answer as one msgpack array of `HomeVersion`.
pub(super) async fn fan_home_versions(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    let PhysicalPlan::Meta(MetaOp::HomeVersions { probes }) = plan else {
        return Err(crate::Error::Internal {
            detail: "fan_home_versions received a plan that is not HomeVersions".into(),
        });
    };
    let group_of = groups_of_probes(state, &probes)?;
    let mut groups: Vec<u64> = group_of.values().copied().collect();
    groups.sort_unstable();
    groups.dedup();
    let refusals: HashMap<u64, LeaseRefusal> =
        confirm_leased_read(state, &groups, statement_read_deadline(state))
            .await?
            .into_iter()
            .map(|refusal| (refusal.group_id, refusal))
            .collect();

    let mut answers = Vec::with_capacity(probes.len());
    let mut served = Vec::with_capacity(probes.len());
    for probe in probes {
        match group_of.get(&probe.vshard).and_then(|g| refusals.get(g)) {
            Some(refusal) => answers.push(HomeVersion {
                probe,
                answer: HomeAnswer::NotLeader {
                    leader_node: refusal.leader_node,
                    leader_term: refusal.leader_term,
                },
            }),
            None => served.push(probe),
        }
    }

    let by_core = probes_by_core(state, served)?;
    let asks =
        by_core.into_values().map(|probes| async move {
            // Every probe of the group lives on one core, so any of their
            // vShards routes the plan there.
            let vshard = probes.first().map(|p| p.vshard).unwrap_or_default();
            let response = dispatch_single_owning_core(
                state,
                tenant_id,
                database_id,
                PhysicalPlan::Meta(MetaOp::HomeVersions { probes }),
                VShardId::new(vshard),
                trace_id,
                None,
            )
            .await?;
            let answers: Vec<HomeVersion> = zerompk::from_msgpack(response.payload.as_ref())
                .map_err(|e| crate::Error::Internal {
                    detail: format!("home versions: a core's answer does not decode: {e}"),
                })?;
            Ok::<_, crate::Error>((answers, response.watermark_lsn))
        });
    let mut watermark_lsn = Lsn::ZERO;
    for (core_answers, watermark) in try_join_all(asks).await? {
        answers.extend(core_answers);
        watermark_lsn = watermark_lsn.max(watermark);
    }
    let payload = zerompk::to_msgpack_vec(&answers).map_err(|e| crate::Error::Internal {
        detail: format!("home versions: encoding the node's answer failed: {e}"),
    })?;
    Ok(NodeLevelResult {
        payload,
        watermark_lsn,
        read_versions: crate::types::ReadVersions::new(),
        not_found: false,
    })
}

/// The Raft group of each probed vShard. Empty on a node with no routing
/// table: a single node leads every vShard.
fn groups_of_probes(
    state: &SharedState,
    probes: &[HomeVersionProbe],
) -> crate::Result<HashMap<u32, u64>> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(HashMap::new());
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mut group_of = HashMap::with_capacity(probes.len());
    for probe in probes {
        let group_id =
            routing
                .group_for_vshard(probe.vshard)
                .map_err(|_| crate::Error::NoLeader {
                    vshard_id: VShardId::new(probe.vshard),
                })?;
        group_of.insert(probe.vshard, group_id);
    }
    Ok(group_of)
}

/// The probes grouped by the core the router assigns each probe's vShard.
fn probes_by_core(
    state: &SharedState,
    probes: Vec<HomeVersionProbe>,
) -> crate::Result<BTreeMap<usize, Vec<HomeVersionProbe>>> {
    let dispatcher = state.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
    let router = dispatcher.router();
    let mut by_core: BTreeMap<usize, Vec<HomeVersionProbe>> = BTreeMap::new();
    for probe in probes {
        let core =
            router
                .resolve(VShardId::new(probe.vshard))
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "home versions: no local core owns vShard {}; resend the check to the \
                     vShard's leader",
                        probe.vshard
                    ),
                })?;
        by_core.entry(core).or_default().push(probe);
    }
    Ok(by_core)
}
