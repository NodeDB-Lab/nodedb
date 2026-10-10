// SPDX-License-Identifier: BUSL-1.1

//! Commit-time validation of a read-only transaction's homed reads.
//!
//! A homed read is one vShard a cross-shard graph read observed (see
//! `session::graph_reads`). A read-write COMMIT goes through Calvin, whose
//! participants check it. A read-only COMMIT dispatches nothing, so this
//! module checks it: it asks each vShard's leader for the home's current
//! version and compares it to the recorded read version.
//!
//! The check sends one `MetaOp::HomeVersions` per leader node, carrying every
//! home that node leads. The leader hands each probe to the one core that owns
//! the probe's vShard (`exchange::all_cores::home_versions`), and that core
//! answers:
//!
//! - A read of one collection gets the collection's write floor on that core.
//! - A read of every collection gets the core watermark.
//!
//! The leader answers a home only under its leader lease on the home's group,
//! applied through the lease read index. A node that lost leadership refuses
//! the home with the leader and term it knows, and the check asks that leader.
//!
//! A version above the recorded read version means a write landed on that
//! core after the read. Single-shard SI (`conflict::si_conflict_abort`)
//! compares against this node's WAL, so it never judges a homed read: the
//! read's watermark can come from another node's WAL.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{HomeAnswer, HomeVersion, HomeVersionProbe, MetaOp};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::dispatcher::{
    DispatchRouteParams, dispatch_route, statement_deadline_ms,
};
use crate::control::gateway::live_leaders::resolve_live_decision;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::exchange::all_cores::execute_plan_all_local_cores;
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, VShardId};

use super::super::connection::SessionId;
use super::super::outcome::{AbortReason, CommitOutcome};
use super::super::read_set::ReadSetEntry;
use super::super::reservation_release;
use super::super::store::SessionStore;

/// The node that leads a home's vShard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Leader {
    Local,
    Remote(u64),
}

/// Rounds a home check asks again after a leader refused a home.
const MAX_CHECK_ROUNDS: u32 = 4;

/// The wait before a later round, times the round number.
const RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

/// One home read: its tenant scope, the vShard and collection it read, and
/// the node that served it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Home {
    database_id: u64,
    tenant_id: u64,
    probe: HomeVersionProbe,
    served_by: u64,
}

/// One check request: every home one leader leads, in one tenant scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CheckTarget {
    leader: Leader,
    database_id: u64,
    tenant_id: u64,
}

/// Abort the transaction when a homed read in `read_set` is no longer
/// current, or when a home cannot be checked. Releases the read
/// reservations and rolls the session back before returning the outcome.
/// `None` means every homed read still holds.
pub(super) async fn homed_read_abort(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    read_set: &[ReadSetEntry],
) -> Option<CommitOutcome> {
    let reason = match homed_reads_changed(state, read_set).await {
        Ok(false) => return None,
        Ok(true) => {
            super::super::hot_key::record_read_set_aborts(state, read_set);
            AbortReason::Serialization
        }
        Err(error) => AbortReason::Dispatch(error),
    };
    reservation_release::release_and_rollback(state, sessions, session_id).await;
    Some(CommitOutcome::Aborted { reason })
}

/// Whether any homed read in `read_set` is no longer current.
///
/// Entries without a home are skipped: SI validates them.
///
/// A read of every collection (a graph read with no collection scope) is
/// checked against the core watermark. That is the conservative rule: any
/// write the core took raises it, to any collection and to any vShard the core
/// owns. Such a check can abort a transaction whose read still holds. It never
/// misses a write that changed the read.
///
/// A leader answers only under its leader lease. A home it refuses is asked
/// again at the leader the refusal names, or at the routing table's leader
/// when it names none, for at most [`MAX_CHECK_ROUNDS`] rounds. A home still
/// unanswered then aborts the commit: no answer from a node that lost
/// leadership ever lets it through.
///
/// A version is a position in the answering node's WAL. A home answered by a
/// node other than the one that served the read counts as changed: its
/// versions do not compare with the read's.
async fn homed_reads_changed(
    state: &SharedState,
    read_set: &[ReadSetEntry],
) -> crate::Result<bool> {
    // The earliest read version per home is the one that must still hold.
    let mut homes: BTreeMap<Home, Lsn> = BTreeMap::new();
    for entry in read_set {
        let Some(vshard) = entry.home else {
            continue;
        };
        let home = Home {
            database_id: entry.database_id.as_u64(),
            tenant_id: entry.tenant_id.as_u64(),
            probe: HomeVersionProbe {
                vshard: vshard.as_u32(),
                collection: (!entry.collection.is_empty()).then(|| entry.collection.clone()),
            },
            served_by: entry.home_node,
        };
        let version = homes.entry(home).or_insert(entry.read_version_lsn);
        *version = (*version).min(entry.read_version_lsn);
    }
    if homes.is_empty() {
        return Ok(false);
    }
    let shared = gateway_shared(state)?;
    // Each pending home, with the leader the last refusal named.
    let mut pending: Vec<(Home, Option<Leader>)> =
        homes.keys().cloned().map(|home| (home, None)).collect();
    let mut last_refusal: Option<crate::Error> = None;
    for round in 0..MAX_CHECK_ROUNDS {
        if pending.is_empty() {
            return Ok(false);
        }
        if round > 0 {
            tokio::time::sleep(RETRY_BACKOFF * round).await;
        }
        let mut by_target: BTreeMap<CheckTarget, Vec<Home>> = BTreeMap::new();
        for (home, named) in pending.drain(..) {
            let leader = match named {
                Some(leader) => leader,
                None => leader_of(state, VShardId::new(home.probe.vshard))?,
            };
            by_target
                .entry(CheckTarget {
                    leader,
                    database_id: home.database_id,
                    tenant_id: home.tenant_id,
                })
                .or_default()
                .push(home);
        }
        let checks = by_target.into_iter().map(|(target, asked)| {
            let shared = shared.clone();
            async move {
                let mut probes: Vec<HomeVersionProbe> =
                    asked.iter().map(|home| home.probe.clone()).collect();
                probes.sort();
                probes.dedup();
                let answers = current_versions(&shared, target, probes).await?;
                Ok::<_, crate::Error>((target, asked, answers))
            }
        });
        for (target, asked, answers) in futures::future::try_join_all(checks).await? {
            let answered_by = match target.leader {
                Leader::Local => state.node_id,
                Leader::Remote(node_id) => node_id,
            };
            let by_probe: BTreeMap<HomeVersionProbe, HomeAnswer> = answers
                .into_iter()
                .map(|answer| (answer.probe, answer.answer))
                .collect();
            for home in asked {
                let read_version = *homes
                    .get(&home)
                    .ok_or_else(|| unasked_home(target, &home))?;
                let answer = *by_probe
                    .get(&home.probe)
                    .ok_or_else(|| unasked_home(target, &home))?;
                match answer {
                    // Versions are the answering node's WAL positions. Only the
                    // node that served the read numbers them in the read's own
                    // domain, so an answer from any other node counts as a
                    // change.
                    HomeAnswer::Version(_) if answered_by != home.served_by => {
                        return Ok(true);
                    }
                    HomeAnswer::Version(version) => {
                        if Lsn::new(version) > read_version {
                            return Ok(true);
                        }
                    }
                    HomeAnswer::NotLeader {
                        leader_node,
                        leader_term,
                    } => {
                        let named = (leader_node != 0).then_some(if leader_node == state.node_id {
                            Leader::Local
                        } else {
                            Leader::Remote(leader_node)
                        });
                        last_refusal = Some(crate::Error::NotLeader {
                            vshard_id: VShardId::new(home.probe.vshard),
                            leader_node,
                            leader_addr: String::new(),
                            leader_term,
                        });
                        pending.push((home, named));
                    }
                }
            }
        }
    }
    if pending.is_empty() {
        return Ok(false);
    }
    Err(last_refusal.unwrap_or_else(|| crate::Error::Internal {
        detail: "homed read check: a home stayed unanswered; retry the commit".into(),
    }))
}

/// The node leading `vshard`, by live Raft leadership where known.
fn leader_of(state: &SharedState, vshard: VShardId) -> crate::Result<Leader> {
    match resolve_live_decision(state, vshard.as_u32()) {
        RouteDecision::Local => Ok(Leader::Local),
        RouteDecision::Remote { node_id, .. } => Ok(Leader::Remote(node_id)),
        RouteDecision::LeaderUnknown { .. } => Err(crate::Error::NotLeader {
            vshard_id: vshard,
            leader_node: 0,
            leader_addr: String::new(),
            leader_term: 0,
        }),
        RouteDecision::Broadcast { .. } => Err(crate::Error::Internal {
            detail: format!(
                "homed read check: vShard {} resolved to a broadcast route",
                vshard.as_u32()
            ),
        }),
    }
}

/// The current version of each of `probes`, read on their leader in one
/// request.
async fn current_versions(
    shared: &std::sync::Arc<SharedState>,
    target: CheckTarget,
    probes: Vec<HomeVersionProbe>,
) -> crate::Result<Vec<HomeVersion>> {
    let tenant_id = TenantId::new(target.tenant_id);
    let database_id = DatabaseId::new(target.database_id);
    let route_vshard = probes.first().map(|p| p.vshard).unwrap_or_default();
    let asked = probes.len();
    let plan = PhysicalPlan::Meta(MetaOp::HomeVersions { probes });
    let payloads = match target.leader {
        // This node's cores answer directly. A gateway local route will run
        // the plan on the one core of its route vShard.
        Leader::Local => vec![
            execute_plan_all_local_cores(shared, tenant_id, database_id, plan, TraceId::ZERO, None)
                .await?
                .payload,
        ],
        Leader::Remote(node_id) => {
            let route = TaskRoute {
                plan,
                decision: RouteDecision::Remote {
                    node_id,
                    vshard_id: u64::from(route_vshard),
                },
                vshard_id: route_vshard,
            };
            dispatch_route(DispatchRouteParams {
                route,
                shared,
                tenant_id,
                database_id,
                trace_id: TraceId::ZERO,
                deadline_ms: statement_deadline_ms(shared),
                version_set: &GatewayVersionSet::from_pairs(Vec::new()),
                txn_id: None,
                // The leader proves its leadership itself: it answers each
                // home under its leader lease, or refuses it.
                linearizable: false,
            })
            .await?
            .payloads
        }
    };
    let mut answers = Vec::with_capacity(asked);
    for payload in payloads {
        let decoded: Vec<HomeVersion> =
            zerompk::from_msgpack(&payload).map_err(|e| crate::Error::Internal {
                detail: format!("homed read check: a leader's answer does not decode: {e}"),
            })?;
        answers.extend(decoded);
    }
    if answers.len() != asked {
        return Err(crate::Error::Internal {
            detail: format!(
                "homed read check: {:?} answered {} of {asked} homes; retry the commit",
                target.leader,
                answers.len()
            ),
        });
    }
    Ok(answers)
}

fn unasked_home(target: CheckTarget, home: &Home) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "homed read check: {:?} gave no answer for vShard {} ({:?}); retry the commit",
            target.leader, home.probe.vshard, home.probe.collection
        ),
    }
}
