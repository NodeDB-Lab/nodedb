// SPDX-License-Identifier: BUSL-1.1

//! A database backup's capture of every tenant at one cut.
//!
//! The coordinator picks the cut watermark and a request id, then takes the
//! cut with capturing barriers on its own Raft data groups, a one-node
//! cluster's included. Each group's leader places the group's one barrier. Every other source node receives the request, takes
//! the same cut on its groups, and answers with the captures it parked. Each
//! group's capture comes from its leader at the apply of the request's first
//! barrier. A group with no capture fails the backup with a retryable error
//! that names it: its leadership moved before the capture was collected.
//! The backup never falls back to a live read.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use nodedb_physical::physical_plan::{CutCaptureRequest, MetaOp};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan as BridgePlan;
use crate::control::backup::node_snapshot::{is_self, snapshot_remote};
use crate::control::backup::orchestrator::source_assignment;
use crate::control::backup::restore::sections::append_snapshot;
use crate::control::security::auth_fence::cluster::group_of_vshard;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantDataSnapshot};

use super::registry::GroupCapture;

/// One group's capture on the wire.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct GroupReply {
    pub group_id: u64,
    /// `(tenant_id, TenantDataSnapshot msgpack)`.
    pub tenants: Vec<(u64, Vec<u8>)>,
    /// The reason the capture failed, when it did.
    pub failure: Option<String>,
}

/// Every tenant of a database, captured at one cut.
pub(crate) struct DatabaseCapture {
    /// The cut's HLC wall time, in nanoseconds.
    pub cut: u64,
    /// Each tenant's rows, merged over every group.
    pub tenants: BTreeMap<u64, TenantDataSnapshot>,
}

/// Capture every tenant of `tenants` in `database_id` at one cut.
pub(crate) async fn capture_database(
    state: &Arc<SharedState>,
    database_id: DatabaseId,
    tenants: &BTreeSet<u64>,
) -> Result<DatabaseCapture, Error> {
    if state.cluster_routing.is_none() {
        return Err(Error::Internal {
            detail: "database backup: this node holds no routing table, so it cannot place \
                     a capturing cut in every data group"
                .into(),
        });
    }
    let cut = state.hlc_clock.now().wall_ns;
    let request = CutCaptureRequest {
        // The watermark is unique on this node. The node id keeps two
        // coordinators apart.
        request_id: (cut & !0xFFFF) | (state.node_id & 0xFFFF),
        database_id: database_id.as_u64(),
        tenants: tenants.iter().copied().collect(),
    };
    // The snapshot request names a tenant only to frame it.
    let framing = tenants.first().copied().unwrap_or(0);

    super::super::cut::cut_with_capture(state, cut, &request).await?;
    let mut replies = take_replies(state, request.request_id);
    let remote: Vec<u64> = source_assignment(state)?
        .into_iter()
        .map(|(node_id, _)| node_id)
        .filter(|node_id| !is_self(state, *node_id))
        .collect();
    let plan = BridgePlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id: framing,
        cut_watermark: Some(cut),
        cut_capture: Some(request.clone()),
        arrays: true,
    });
    let answers = futures::future::join_all(remote.iter().map(|&node_id| {
        let plan = &plan;
        async move { snapshot_remote(state, node_id, framing, database_id, plan).await }
    }))
    .await;
    for answer in answers {
        let decoded: Vec<GroupReply> =
            zerompk::from_msgpack(&answer?).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!("database backup: decode a node's cut captures: {e}"),
            })?;
        replies.extend(decoded);
    }
    // Ask again locally: a remote node's barrier can be the first one of a
    // group this node leads.
    replies.extend(take_replies(state, request.request_id));
    // A test parks the backup here: every capture is taken, so a write from
    // now on lies above the cut.
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::wait(
        crate::fail_point::FailScope::Node(state.node_id),
        "backup::database::after_cut",
    )
    .await;
    assemble(state, cut, replies)
}

/// The captures of `request` this node parked, as wire replies.
pub(crate) fn take_replies(state: &SharedState, request: u64) -> Vec<GroupReply> {
    state
        .cut_captures
        .take_all(request)
        .into_iter()
        .map(|(group_id, capture)| match capture {
            GroupCapture::Taken(tenants) => GroupReply {
                group_id,
                tenants,
                failure: None,
            },
            GroupCapture::Failed(reason) => GroupReply {
                group_id,
                tenants: Vec::new(),
                failure: Some(reason),
            },
        })
        .collect()
}

/// Take the cut on this node for a coordinator's `request`, and answer with
/// the encoded captures this node parked.
pub(crate) async fn cut_and_reply(
    state: &SharedState,
    watermark: u64,
    request: &CutCaptureRequest,
) -> Result<Vec<u8>, Error> {
    super::super::cut::cut_with_capture(state, watermark, request).await?;
    zerompk::to_msgpack_vec(&take_replies(state, request.request_id)).map_err(|e| {
        Error::Serialization {
            format: "msgpack".into(),
            detail: format!("cut capture: encode this node's captures: {e}"),
        }
    })
}

/// Every data group that homes a vShard, from this node's routing table.
fn data_groups(state: &SharedState) -> BTreeSet<u64> {
    (0..nodedb_cluster::routing::VSHARD_COUNT)
        .filter_map(|vshard| group_of_vshard(state, vshard).ok())
        .collect()
}

/// Check that every data group has one capture, and merge each tenant's rows
/// over the groups.
fn assemble(
    state: &SharedState,
    cut: u64,
    replies: Vec<GroupReply>,
) -> Result<DatabaseCapture, Error> {
    let mut by_group: BTreeMap<u64, GroupReply> = BTreeMap::new();
    for reply in replies {
        // Two leaders of one term cannot both apply a group's first barrier,
        // so two replies for a group hold the same capture.
        by_group.entry(reply.group_id).or_insert(reply);
    }
    let mut tenants: BTreeMap<u64, TenantDataSnapshot> = BTreeMap::new();
    for group_id in data_groups(state) {
        let reply = by_group
            .remove(&group_id)
            .ok_or(Error::BackupCaptureMoved { group_id })?;
        if let Some(reason) = reply.failure {
            return Err(Error::Internal {
                detail: format!(
                    "database backup: the capture of raft group {group_id} at the cut failed: \
                     {reason}"
                ),
            });
        }
        for (tenant_id, body) in reply.tenants {
            let snap: TenantDataSnapshot =
                zerompk::from_msgpack(&body).map_err(|e| Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "database backup: decode the tenant {tenant_id} capture of raft group \
                         {group_id}: {e}"
                    ),
                })?;
            append_snapshot(tenants.entry(tenant_id).or_default(), snap);
        }
    }
    Ok(DatabaseCapture { cut, tenants })
}
