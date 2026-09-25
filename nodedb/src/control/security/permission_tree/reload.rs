// SPDX-License-Identifier: BUSL-1.1

//! Reload the permission cache from its source collections.
//!
//! A reload reads the emitted-event counter of every core, then scans every
//! governed collection and permission table on this node's own cores. A scan
//! runs on its core after every write whose event the counter already
//! counted, so the reloaded state covers each of those events. The reload
//! holds the cache's write lock throughout: the permission step waits, so no
//! event it applies falls between the counter read and the install.
//!
//! The scans read local state, never a leader's. The cache must match this
//! node's own apply position. The scans dispatch through the read-only local
//! path, never the write funnel.

use std::collections::HashMap;

use nodedb_types::{QualifiedCollection, TenantId};

use crate::control::server::dispatch_utils::{
    LocalRead, dispatch_local_read, reject_data_plane_error,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};

use super::cache::{PermissionCache, TreeSource, TreeSourceKind};
use super::event_handler::{extract_edge, extract_grant};
use super::types::PermissionGrant;

/// Edges and grants a reload read for one tenant.
#[derive(Default)]
struct TenantRows {
    edges: Vec<(String, String)>,
    grants: Vec<PermissionGrant>,
}

/// Reload every registered tree's sources, unless the cache already reflects
/// every event at or below `targets` (another reload got there first).
///
/// `targets` of `None` reloads unconditionally: no permission step runs, so
/// only a reload reflects writes.
pub async fn reload_all(state: &SharedState, targets: Option<&[u64]>) -> crate::Result<()> {
    let mut cache = state.permission_cache.write().await;
    if let Some(targets) = targets
        && cache.progress().caught_up(targets)
    {
        return Ok(());
    }
    reload_locked(state, &mut cache).await
}

/// Reload when the cache is stale: no reload covers the registered trees yet
/// (startup, or a tree definition changed), or a core lost an event.
///
/// Planning and the lease renewal call this. A writer waiting for its
/// acknowledgement never does: a stale cache is reloaded before the next
/// statement plans, and that reload reads the write.
pub async fn reload_if_stale(state: &SharedState) -> crate::Result<()> {
    // Checked under the read lock first: planning calls this for every
    // statement, and the write lock is needed only when the cache is stale.
    if !state.permission_cache.read().await.progress().is_stale() {
        return Ok(());
    }
    let mut cache = state.permission_cache.write().await;
    if !cache.progress().is_stale() {
        return Ok(());
    }
    reload_locked(state, &mut cache).await
}

/// Reload every registered tree's sources into `cache`, whose write lock the
/// caller holds.
async fn reload_locked(state: &SharedState, cache: &mut PermissionCache) -> crate::Result<()> {
    let fence = &state.authorization_fence;
    // Read under the write lock: every event the permission step applied is
    // counted, and none it applies later can be.
    let emitted = fence.emitted_snapshot().unwrap_or_default();

    let mut rows: HashMap<u64, TenantRows> = HashMap::new();
    for source in cache.tree_sources() {
        let docs = scan_source(state, &source).await?;
        let tenant = rows.entry(source.tenant_id).or_default();
        match source.kind {
            TreeSourceKind::Hierarchy => tenant.edges.extend(docs.iter().filter_map(|doc| {
                extract_edge(doc).map(|(child, parent)| (child.to_owned(), parent.to_owned()))
            })),
            TreeSourceKind::Grants => tenant.grants.extend(docs.iter().filter_map(extract_grant)),
        }
    }
    for (tenant_id, tenant) in rows {
        cache.replace_tenant_state(tenant_id, &tenant.edges, &tenant.grants);
    }
    cache.progress_mut().install_reload(&emitted);
    fence.permission_applied().notify_waiters();
    Ok(())
}

/// Every row of one source collection, read on this node's own core.
///
/// A source that no longer exists holds no rows. A source that is not a
/// document collection cannot carry edges or grants, and refuses the reload:
/// planning with it would silently grant or deny nothing.
async fn scan_source(
    state: &SharedState,
    source: &TreeSource,
) -> crate::Result<Vec<serde_json::Value>> {
    let database_id = DatabaseId::DEFAULT;
    let catalog = state.credentials.catalog();
    let stored = catalog.get_collection(database_id, source.tenant_id, &source.collection)?;
    let Some(stored) = stored.filter(|collection| collection.is_active) else {
        return Ok(Vec::new());
    };
    if !stored.collection_type.is_document() {
        return Err(crate::Error::FeatureNotSupported {
            detail: format!(
                "permission tree source '{}' is a {} collection; the hierarchy and the \
                 permission table must be document collections",
                source.collection, stored.collection_type
            ),
        });
    }

    // A read-only dispatch: a reload can run while a write waits for its
    // acknowledgement, and it must never issue a request through the write
    // path.
    let response = dispatch_local_read(
        state,
        TenantId::new(source.tenant_id),
        database_id,
        VShardId::from_collection_in_database(database_id, &source.collection),
        LocalRead::DocumentScan {
            collection: QualifiedCollection::new(database_id, &source.collection),
        },
    )
    .await?;
    reject_data_plane_error(&response)?;
    Ok(
        crate::data::executor::response_codec::decode_raw_scan_to_docs(response.payload.as_bytes())
            .into_iter()
            .filter_map(|(_, body)| nodedb_types::json_from_msgpack(&body).ok())
            .collect(),
    )
}
