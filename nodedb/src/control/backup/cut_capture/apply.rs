// SPDX-License-Identifier: BUSL-1.1

//! The capture a cut barrier takes in its group's apply path.
//!
//! The apply loop runs a capturing barrier with nothing else of its group in
//! flight: every earlier entry of the group finished on its core, and no later
//! entry starts until the capture returns. The snapshot therefore holds every
//! entry at or below the barrier and none above it. The snapshot reaches the
//! cores over the SPSC bridge, like every other Control-Plane request.

use std::collections::HashSet;
use std::time::Duration;

use nodedb_cluster::routing::VSHARD_COUNT;
use nodedb_physical::physical_plan::CutCaptureRequest;

use crate::Error;
use crate::control::backup::snapshot_keys::{
    StoredRecord, extract_db_tenant_scoped_collection, homes_of_stored,
    retain_tenant_data_for_vshards,
};
use crate::control::security::auth_fence::cluster::group_of_vshard;
use crate::control::server::exchange::snapshot_tenant_on_local_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantDataSnapshot, TenantId};

use super::registry::GroupCapture;

/// How long one tenant's snapshot on this node's cores can take.
const TENANT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Capture `request`'s tenants at a barrier of `group_id` this node applies.
///
/// Only the first barrier of the request in the group captures, and only on
/// the group's leader at that apply. The capture, or the reason it failed, is
/// parked for the backup to collect.
pub(crate) async fn capture_at_barrier(
    state: &SharedState,
    group_id: u64,
    request: &CutCaptureRequest,
) {
    if !state
        .cut_captures
        .first_barrier(request.request_id, group_id)
    {
        return;
    }
    if !leads_group(state, group_id) {
        return;
    }
    let capture = match capture_group(state, group_id, request).await {
        Ok(tenants) => GroupCapture::Taken(tenants),
        Err(error) => GroupCapture::Failed(error.to_string()),
    };
    state
        .cut_captures
        .park(request.request_id, group_id, capture);
}

/// Whether this node leads `group_id` now. A node with no Raft status leads
/// every group it applies.
fn leads_group(state: &SharedState, group_id: u64) -> bool {
    match state.raft_status_fn.get() {
        Some(status) => status()
            .iter()
            .any(|group| group.group_id == group_id && group.leader_id == state.node_id),
        None => true,
    }
}

/// Every vShard `group_id` homes. With no routing table this node's one
/// group homes them all.
pub(crate) fn group_vshards(state: &SharedState, group_id: u64) -> HashSet<u32> {
    if state.cluster_routing.is_none() {
        return (0..VSHARD_COUNT).collect();
    }
    (0..VSHARD_COUNT)
        .filter(|vshard| group_of_vshard(state, *vshard).ok() == Some(group_id))
        .collect()
}

/// Snapshot every tenant of `request` on this node's cores, and keep the
/// records `group_id` homes.
async fn capture_group(
    state: &SharedState,
    group_id: u64,
    request: &CutCaptureRequest,
) -> Result<Vec<(u64, Vec<u8>)>, Error> {
    let vshards = group_vshards(state, group_id);
    let database_id = DatabaseId::new(request.database_id);
    let mut tenants = Vec::with_capacity(request.tenants.len());
    for &tenant_id in &request.tenants {
        let body = snapshot_tenant_on_local_cores(
            state,
            TenantId::new(tenant_id),
            database_id,
            TENANT_SNAPSHOT_TIMEOUT,
            true,
        )
        .await?;
        let snapshot = keep_group_records(&body, tenant_id, database_id, &vshards)?;
        tenants.push((tenant_id, snapshot));
    }
    Ok(tenants)
}

/// Decode one tenant's snapshot and keep the records whose owner home is in
/// `vshards`.
pub(crate) fn keep_group_records(
    body: &[u8],
    tenant_id: u64,
    database_id: DatabaseId,
    vshards: &HashSet<u32>,
) -> Result<Vec<u8>, Error> {
    let mut snap: TenantDataSnapshot =
        zerompk::from_msgpack(body).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("cut capture: decode the tenant {tenant_id} snapshot: {e}"),
        })?;
    retain_tenant_data_for_vshards(&mut snap, tenant_id, vshards, |record| {
        homes_of_stored(database_id, record).map(Some)
    })?;
    // The vector build parameters are keyed like `vectors`. The shared filter
    // leaves them alone, and every group's capture carries them all.
    let owned = |key: &str| -> Result<bool, Error> {
        match extract_db_tenant_scoped_collection(key, tenant_id) {
            Some(collection) => Ok(
                homes_of_stored(database_id, StoredRecord::Row { collection })?.owned_by(vshards),
            ),
            None => Ok(false),
        }
    };
    let mut kept_params = Vec::with_capacity(snap.vector_params.len());
    for entry in std::mem::take(&mut snap.vector_params) {
        if owned(&entry.0)? {
            kept_params.push(entry);
        }
    }
    snap.vector_params = kept_params;
    let mut kept_configs = Vec::with_capacity(snap.index_configs.len());
    for entry in std::mem::take(&mut snap.index_configs) {
        if owned(&entry.0)? {
            kept_configs.push(entry);
        }
    }
    snap.index_configs = kept_configs;
    zerompk::to_msgpack_vec(&snap).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("cut capture: encode the tenant {tenant_id} snapshot: {e}"),
    })
}
