// SPDX-License-Identifier: BUSL-1.1

//! RESTORE's staleness guard: the newest committed write of a tenant.
//!
//! The answer comes from replicated state. Every data group's replicas derive
//! the same durable per-tenant marks from the entries they apply, and a
//! committed Calvin transaction records its mark in the data group that homes
//! its vShard before its COMMIT is acknowledged. The guard reads the marks of
//! every data group:
//!
//! - a group this node replicates, here, once this node applied every entry
//!   the group committed before the read and every Calvin transaction
//!   sequenced before it installed;
//! - any other group, from a current replica, which does the same before it
//!   answers.
//!
//! A replica that left the group refuses with a typed `NotLeader`. The guard
//! then asks another replica, until one statement deadline shared by every
//! group. A group no replica answered for by then refuses the restore.
//!
//! A node that restarted, or that never applied the write, answers alike.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc, TypedClusterError};
use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan, wire as plan_wire};

use crate::Error;
use crate::control::security::auth_fence::cluster::{
    confirmed_read_index, hosts_group, routed_groups, wait_applied,
};
use crate::control::state::SharedState;
use crate::control::state::tenant_marks::{GroupMark, LOCAL_MARK_GROUP};
use crate::types::{DatabaseId, TraceId};

/// First wait before the guard asks again for the marks of groups whose
/// replica refused. Each round doubles it up to [`MAX_ASK_BACKOFF`].
const FIRST_ASK_BACKOFF: Duration = Duration::from_millis(10);

/// Longest wait between two rounds of asking.
const MAX_ASK_BACKOFF: Duration = Duration::from_millis(200);

/// The newest committed write of a tenant, and where it was recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NewestWrite {
    /// HLC wall time, in nanoseconds, of its commit.
    pub hlc: u64,
    /// The apply path that recorded it.
    pub site: String,
    /// The collection it named, when it named one.
    pub collection: Option<String>,
}

/// One group's mark on the wire: `(group_id, commit_hlc, site_code,
/// collection)`.
type WireMark = (u64, u64, u8, String);

/// The newest committed write of `tenant_id` across every data group, and
/// this node's own mark of writes no data group carries.
pub(super) async fn newest_committed_write(
    state: &Arc<SharedState>,
    tenant_id: u64,
) -> Result<Option<NewestWrite>, Error> {
    let mut newest = state.tenant_write_mark(tenant_id).map(|mark| NewestWrite {
        hlc: mark.hlc,
        site: mark.origin.site.to_owned(),
        collection: mark.origin.collection,
    });
    let mut consider = |mark: GroupMark| {
        if newest.as_ref().is_none_or(|current| mark.hlc > current.hlc) {
            newest = Some(NewestWrite {
                hlc: mark.hlc,
                site: mark.site.as_str().to_owned(),
                collection: mark.collection,
            });
        }
    };
    // The durable mark of this node's writes while it ran with no Raft groups.
    if let Some(mark) = state.tenant_marks.get(LOCAL_MARK_GROUP, tenant_id) {
        consider(mark);
    }

    // The statement deadline, shared by every group and every attempt.
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(state.tuning.network.default_deadline_secs);
    let groups: Vec<u64> = routed_groups(state)
        .into_iter()
        .filter(|group| *group != METADATA_GROUP_ID && *group != SEQUENCER_GROUP_ID)
        .collect();
    for (_, mark) in group_marks(state, tenant_id, groups, deadline).await? {
        consider(mark);
    }
    Ok(newest)
}

/// The marks of `tenant_id` in every group of `groups`, each from a current
/// replica of the group.
///
/// Each round reads the groups this node replicates here, and asks one
/// replica of every other group. A replica that refuses a group, because it
/// does not replicate it, is not asked for that group again until every known
/// replica refused. Every round ends by `deadline`. A group still unanswered
/// then fails the call with [`Error::GroupMarksUnavailable`]. Any other error
/// fails it at once.
async fn group_marks(
    state: &Arc<SharedState>,
    tenant_id: u64,
    groups: Vec<u64>,
    deadline: tokio::time::Instant,
) -> Result<Vec<(u64, GroupMark)>, Error> {
    let mut pending: BTreeMap<u64, GroupAsk> = groups
        .into_iter()
        .map(|group| (group, GroupAsk::default()))
        .collect();
    let mut marks = Vec::new();
    let mut backoff = FIRST_ASK_BACKOFF;
    loop {
        let (local, remote): (Vec<u64>, Vec<u64>) = pending
            .keys()
            .copied()
            .partition(|group| hosts_group(state, *group));
        if !local.is_empty() {
            marks.extend(local_tenant_marks(state, tenant_id, &local, deadline).await?);
            for group in &local {
                pending.remove(group);
            }
        }
        let mut by_node: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for group_id in remote {
            if let Some(ask) = pending.get_mut(&group_id)
                && let Some(node_id) = ask.next_target(state, group_id)
            {
                by_node.entry(node_id).or_default().push(group_id);
            }
        }
        let answers = futures::future::join_all(by_node.into_iter().map(|(node_id, groups)| {
            let asked = groups.clone();
            async move {
                let answer = remote_tenant_marks(state, node_id, tenant_id, groups, deadline).await;
                (node_id, asked, answer)
            }
        }))
        .await;
        for (node_id, asked, answer) in answers {
            match answer {
                Ok(answered) => {
                    marks.extend(answered);
                    for group in &asked {
                        pending.remove(group);
                    }
                }
                Err(RemoteMarksError::NotReplica { group_id, hint }) => {
                    if let Some(ask) = pending.get_mut(&group_id) {
                        ask.refused(node_id, hint);
                    }
                }
                Err(RemoteMarksError::Failed(error)) => return Err(error),
            }
        }
        let Some((&group_id, ask)) = pending.iter().next() else {
            return Ok(marks);
        };
        if tokio::time::Instant::now() + backoff >= deadline {
            return Err(Error::GroupMarksUnavailable {
                group_id,
                refused_by: ask.refused_by.iter().copied().collect(),
            });
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_ASK_BACKOFF);
    }
}

/// Which replicas of one group the guard asked, and which refused.
#[derive(Debug, Default)]
struct GroupAsk {
    /// Nodes that refused the group since the last time every known replica
    /// refused it.
    refused: BTreeSet<u64>,
    /// Every node that refused the group, for the deadline error.
    refused_by: BTreeSet<u64>,
    /// The replica a refusing node's routing table named.
    hint: Option<u64>,
}

impl GroupAsk {
    /// The node to ask next: the last refusal's hint, then the group's leader,
    /// voters and learners in this node's routing table, skipping this node
    /// and every node that refused. When every candidate refused, the refused
    /// set is cleared for the next round, since routing tables converge, and
    /// this round asks no node.
    fn next_target(&mut self, state: &SharedState, group_id: u64) -> Option<u64> {
        let candidates = self.hint.into_iter().chain(group_replicas(state, group_id));
        let mut any = false;
        for node in candidates {
            if node == 0 || node == state.node_id {
                continue;
            }
            any = true;
            if !self.refused.contains(&node) {
                return Some(node);
            }
        }
        if any {
            self.refused.clear();
            self.hint = None;
        }
        None
    }

    fn refused(&mut self, node_id: u64, hint: Option<u64>) {
        self.refused.insert(node_id);
        self.refused_by.insert(node_id);
        self.hint = hint.filter(|hint| !self.refused.contains(hint));
    }
}

/// The replicas of `group_id` in this node's routing table: its leader when
/// known, then its voters, then its learners.
fn group_replicas(state: &SharedState, group_id: u64) -> Vec<u64> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Vec::new();
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let Some(info) = routing.group_info(group_id) else {
        return Vec::new();
    };
    std::iter::once(info.leader)
        .chain(info.members.iter().copied())
        .chain(info.learners.iter().copied())
        .collect()
}

/// This node's marks of `tenant_id` in `group_ids`, once this node applied
/// every entry the groups committed before the call and every Calvin
/// transaction sequenced before it installed here. Every wait ends by
/// `deadline`.
pub(crate) async fn local_tenant_marks(
    state: &Arc<SharedState>,
    tenant_id: u64,
    group_ids: &[u64],
    deadline: tokio::time::Instant,
) -> Result<Vec<(u64, GroupMark)>, Error> {
    if !group_ids.is_empty() {
        let marker = state.hlc_clock.now().wall_ns;
        crate::control::backup::cut::cut_calvin(state, marker, deadline).await?;
    }
    let mut marks = Vec::new();
    for &group_id in group_ids {
        let index = confirmed_read_index(state, group_id, remaining(deadline)?).await?;
        wait_applied(state, group_id, index, remaining(deadline)?).await?;
        if let Some(mark) = state.tenant_marks.get(group_id, tenant_id) {
            marks.push((group_id, mark));
        }
    }
    Ok(marks)
}

/// Encode marks for the wire.
pub(crate) fn encode_marks(marks: &[(u64, GroupMark)]) -> Result<Vec<u8>, Error> {
    let wire: Vec<WireMark> = marks
        .iter()
        .map(|(group_id, mark)| {
            (
                *group_id,
                mark.hlc,
                mark.site.code(),
                mark.collection.clone().unwrap_or_default(),
            )
        })
        .collect();
    zerompk::to_msgpack_vec(&wire).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("tenant write marks: encode: {e}"),
    })
}

fn decode_marks(bytes: &[u8]) -> Result<Vec<(u64, GroupMark)>, Error> {
    let wire: Vec<WireMark> = zerompk::from_msgpack(bytes).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("tenant write marks: decode: {e}"),
    })?;
    Ok(wire
        .into_iter()
        .map(|(group_id, hlc, site, collection)| {
            (
                group_id,
                GroupMark {
                    hlc,
                    site: crate::control::state::tenant_marks::MarkSite::from_code(site),
                    collection: (!collection.is_empty()).then_some(collection),
                },
            )
        })
        .collect())
}

/// The time left before `deadline`, or the deadline error once it passed.
fn remaining(deadline: tokio::time::Instant) -> Result<Duration, Error> {
    let left = deadline.saturating_duration_since(tokio::time::Instant::now());
    if left.is_zero() {
        return Err(Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        });
    }
    Ok(left)
}

/// Why a replica gave no marks.
enum RemoteMarksError {
    /// The node does not replicate `group_id`. `hint` is the replica its
    /// routing table names.
    NotReplica { group_id: u64, hint: Option<u64> },
    /// Any other error. It ends the guard.
    Failed(Error),
}

impl From<Error> for RemoteMarksError {
    fn from(error: Error) -> Self {
        Self::Failed(error)
    }
}

/// Ask `node_id` for its marks of `tenant_id` in `group_ids`, within what
/// remains of `deadline`.
async fn remote_tenant_marks(
    state: &SharedState,
    node_id: u64,
    tenant_id: u64,
    group_ids: Vec<u64>,
    deadline: tokio::time::Instant,
) -> Result<Vec<(u64, GroupMark)>, RemoteMarksError> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "restore: node {node_id} replicates a data group of the tenant, but this node \
                 has no cluster transport to ask it for the group's newest write"
            ),
        })?;
    let plan = PhysicalPlan::ClusterEvent(ClusterEventOp::TenantWriteMarks {
        tenant_id,
        group_ids,
    });
    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("restore: encode the tenant write-mark request: {e}"),
    })?;
    let budget = remaining(deadline)?;
    let request = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        database_id: DatabaseId::DEFAULT.as_u64(),
        deadline_remaining_ms: u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
        trace_id: TraceId::generate().0,
        descriptor_versions: Vec::new(),
        txn_id: None,
    });
    let response = tokio::time::timeout_at(deadline, transport.send_rpc(node_id, request))
        .await
        .map_err(|_| Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        })?
        .map_err(|e| Error::Internal {
            detail: format!("restore: tenant write-mark request to node {node_id} failed: {e}"),
        })?;
    match response {
        RaftRpc::ExecuteResponse(ExecuteResponse {
            success: true,
            payloads,
            ..
        }) => match payloads.as_slice() {
            [payload] => Ok(decode_marks(payload)?),
            _ => Err(Error::Internal {
                detail: format!(
                    "restore: node {node_id} answered the tenant write-mark request with {} \
                     payloads, expected 1",
                    payloads.len()
                ),
            }
            .into()),
        },
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error:
                Some(TypedClusterError::NotLeader {
                    group_id,
                    leader_node_id,
                    ..
                }),
            ..
        }) => Err(RemoteMarksError::NotReplica {
            group_id,
            hint: leader_node_id,
        }),
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(error), ..
        }) => Err(Error::from(error).into()),
        RaftRpc::ExecuteResponse(ExecuteResponse { error: None, .. }) => Err(Error::Internal {
            detail: format!(
                "restore: node {node_id} failed the tenant write-mark request without an error"
            ),
        }
        .into()),
        other => Err(Error::Internal {
            detail: format!(
                "restore: unexpected reply to the tenant write-mark request from node \
                 {node_id}: {other:?}"
            ),
        }
        .into()),
    }
}
