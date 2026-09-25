// SPDX-License-Identifier: BUSL-1.1

//! Event Plane integration: the permission step.
//!
//! The Event Plane passes every event it takes off a core's ring through
//! [`apply_ring_events`], in ring order. An event for a permission table or a
//! governed collection updates the cache. Every event advances the core's
//! apply progress, so lease coverage can tell how far the cache reflects the
//! core's writes (see [`super::sync_state`]).
//!
//! Events rebuilt from the WAL during catch-up never pass through here. Their
//! ring copies do, or, when the ring dropped them, a reload covers their
//! writes.

use std::sync::Arc;

use tracing::debug;

use crate::event::types::WriteEvent;

use super::cache::PermissionCache;
use super::invalidation;
use super::types::PermissionGrant;

/// Apply the permission effect of `events`, taken off core `core_id`'s ring
/// in ring order, and advance that core's apply progress.
///
/// Holds the write lock for the whole batch and across no await. `notify`
/// wakes the coverage waits for this core.
pub async fn apply_ring_events(
    core_id: usize,
    events: &[WriteEvent],
    cache: &Arc<tokio::sync::RwLock<PermissionCache>>,
    notify: &tokio::sync::Notify,
) {
    if events.is_empty() {
        return;
    }
    // A test parks the permission step here to prove a permission-row write
    // is not acknowledged before the step applies it. The gate sits before
    // the lock, so a reload can still run.
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::wait("permission_tree::before_apply").await;
    {
        let mut guard = cache.write().await;
        for event in events {
            if event.op.is_data_event()
                && !guard.progress().covered_by_reload(core_id, event.sequence)
            {
                apply_event(&mut guard, event);
            }
            guard.progress_mut().note_consumed(core_id, event.sequence);
        }
    }
    notify.notify_waiters();
}

/// Apply one data event to the cache when its collection is a permission
/// table or a governed collection of a registered tree.
fn apply_event(cache: &mut PermissionCache, event: &WriteEvent) {
    let collection = event.collection.as_ref();
    let tenant_id = event.tenant_id.as_u64();

    let is_permission_table = cache.tree_defs_using_permission_table(tenant_id, collection);
    let is_resource_graph = cache.tree_defs_using_graph(tenant_id, collection);

    if !is_permission_table && !is_resource_graph {
        return;
    }

    let new_val = event
        .new_value
        .as_ref()
        .and_then(|b| nodedb_types::json_from_msgpack(b).ok());

    match event.op {
        crate::event::types::WriteOp::Insert | crate::event::types::WriteOp::Update => {
            if is_permission_table
                && let Some(ref val) = new_val
                && let Some(grant) = extract_grant(val)
            {
                invalidation::on_grant_upsert(cache, tenant_id, &grant);
            }
            if is_resource_graph
                && let Some(ref val) = new_val
                && let Some((child_id, parent_id)) = extract_edge(val)
            {
                invalidation::on_edge_upsert(cache, tenant_id, child_id, parent_id);
            }
        }
        crate::event::types::WriteOp::Delete => {
            let old_val = event
                .old_value
                .as_ref()
                .and_then(|b| nodedb_types::json_from_msgpack(b).ok());

            if is_permission_table
                && let Some(ref val) = old_val
                && let (Some(resource_id), Some(grantee)) = (
                    val.get("resource_id").and_then(|v| v.as_str()),
                    val.get("grantee").and_then(|v| v.as_str()),
                )
            {
                invalidation::on_grant_delete(cache, tenant_id, resource_id, grantee);
            }
            if is_resource_graph
                && let Some(ref val) = old_val
                && let Some(child_id) = val.get("id").and_then(|v| v.as_str())
            {
                invalidation::on_edge_delete(cache, tenant_id, child_id);
            }
        }
        crate::event::types::WriteOp::BulkInsert { .. }
        | crate::event::types::WriteOp::BulkDelete { .. }
        | crate::event::types::WriteOp::Heartbeat => {}
    }

    debug!(
        tenant_id,
        collection, "permission_tree: cache updated from a write event"
    );
}

/// Extract a `(child_id, parent_id)` edge from a governed collection's row.
pub(super) fn extract_edge(val: &serde_json::Value) -> Option<(&str, &str)> {
    Some((val.get("id")?.as_str()?, val.get("parent_id")?.as_str()?))
}

/// Extract a PermissionGrant from a JSON value (permission table row).
pub(super) fn extract_grant(val: &serde_json::Value) -> Option<PermissionGrant> {
    Some(PermissionGrant {
        resource_id: val.get("resource_id")?.as_str()?.to_owned(),
        grantee: val.get("grantee")?.as_str()?.to_owned(),
        level: val.get("level")?.as_str()?.to_owned(),
        inherited: val
            .get("inherited")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::event::types::{EventSource, RowId, WriteOp};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    const TENANT: u64 = 1;

    fn cache_with_tree() -> Arc<tokio::sync::RwLock<PermissionCache>> {
        let def = sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"docs_tree","permission_table":"grants"}"#,
        )
        .expect("tree def");
        let mut cache = PermissionCache::new();
        cache.register_tree_def(TENANT, "docs", def);
        cache.progress_mut().install_reload(&[0]);
        Arc::new(tokio::sync::RwLock::new(cache))
    }

    fn grant_event(sequence: u64, op: WriteOp) -> WriteEvent {
        let row = serde_json::json!({
            "resource_id": "d1",
            "grantee": "role_a",
            "level": "viewer",
            "inherited": false,
        });
        let body: Option<Arc<[u8]>> = Some(Arc::from(
            nodedb_types::json_to_msgpack(&row).expect("encode grant row"),
        ));
        let (new_value, old_value) = match op {
            WriteOp::Delete => (None, body),
            _ => (body, None),
        };
        WriteEvent {
            sequence,
            collection: Arc::from("grants"),
            op,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("g1")),
            lsn: Lsn::new(sequence),
            record: None,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TENANT),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value,
            old_value,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        }
    }

    #[tokio::test]
    async fn grant_event_applies_after_a_held_read_lock_is_released() {
        let cache = cache_with_tree();
        let notify = Arc::new(tokio::sync::Notify::new());
        let version_before = cache.read().await.tenant_version(TENANT);
        let reader = cache.read().await;

        let apply = {
            let cache = Arc::clone(&cache);
            let notify = Arc::clone(&notify);
            tokio::spawn(async move {
                apply_ring_events(0, &[grant_event(1, WriteOp::Insert)], &cache, &notify).await
            })
        };
        tokio::task::yield_now().await;
        assert!(!apply.is_finished(), "the update waits for the reader");
        drop(reader);
        apply.await.expect("apply task");

        let guard = cache.read().await;
        assert_eq!(
            guard.get_grant(TENANT, "d1", "role_a"),
            Some(("viewer", false))
        );
        assert_eq!(guard.tenant_version(TENANT), version_before + 1);
        assert!(guard.progress().caught_up(&[1]));
    }

    /// An event a reload already covers is not applied again: the ring may
    /// have dropped a later write to the same row.
    #[tokio::test]
    async fn an_event_covered_by_a_reload_is_not_applied_again() {
        let cache = cache_with_tree();
        let notify = tokio::sync::Notify::new();
        cache.write().await.progress_mut().install_reload(&[2]);

        apply_ring_events(0, &[grant_event(2, WriteOp::Insert)], &cache, &notify).await;
        assert!(
            cache
                .read()
                .await
                .get_grant(TENANT, "d1", "role_a")
                .is_none()
        );

        apply_ring_events(0, &[grant_event(3, WriteOp::Insert)], &cache, &notify).await;
        let guard = cache.read().await;
        assert!(guard.get_grant(TENANT, "d1", "role_a").is_some());
        assert!(guard.progress().caught_up(&[3]));
    }

    /// A gap in the ring leaves the core behind even though later events
    /// apply.
    #[tokio::test]
    async fn a_dropped_event_leaves_the_core_behind() {
        let cache = cache_with_tree();
        let notify = tokio::sync::Notify::new();
        apply_ring_events(
            0,
            &[
                grant_event(1, WriteOp::Insert),
                grant_event(3, WriteOp::Delete),
            ],
            &cache,
            &notify,
        )
        .await;
        let guard = cache.read().await;
        assert!(guard.get_grant(TENANT, "d1", "role_a").is_none());
        assert!(!guard.progress().caught_up(&[3]));
        assert!(guard.progress().needs_reload_for(&[3]));
    }
}
