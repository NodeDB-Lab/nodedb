// SPDX-License-Identifier: BUSL-1.1

//! Event Plane integration: process WriteEvents that affect permission trees.
//!
//! When a row is written to a collection that serves as a permission table
//! or resource hierarchy for some permission tree, update the in-memory cache.

use std::sync::Arc;

use tracing::debug;

use crate::event::types::WriteEvent;

use super::cache::PermissionCache;
use super::invalidation;
use super::types::PermissionGrant;

/// Process a WriteEvent and update the permission cache if relevant.
///
/// Called from the Event Plane consumer for every data event, on both the
/// Normal-mode and WAL-catchup paths. Checks if the event's collection is a
/// permission table or resource graph for any registered permission tree,
/// and updates the cache accordingly.
///
/// Waits for the write lock. Each grant or edge event is the only update for
/// that row, so a skipped event leaves the cache wrong with no later repair.
/// This function holds the lock across no await. The wait ends when the
/// planners that hold a read lock finish.
pub async fn handle_permission_event(
    event: &WriteEvent,
    cache: &Arc<tokio::sync::RwLock<PermissionCache>>,
) {
    let collection = event.collection.as_ref();
    let tenant_id = event.tenant_id.as_u64();

    let mut guard = cache.write().await;

    let is_permission_table = guard.tree_defs_using_permission_table(tenant_id, collection);
    let is_resource_graph = guard.tree_defs_using_graph(tenant_id, collection);

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
                invalidation::on_grant_upsert(&mut guard, tenant_id, &grant);
            }
            if is_resource_graph
                && let Some(ref val) = new_val
                && let (Some(child_id), Some(parent_id)) = (
                    val.get("id").and_then(|v| v.as_str()),
                    val.get("parent_id").and_then(|v| v.as_str()),
                )
            {
                invalidation::on_edge_upsert(&mut guard, tenant_id, child_id, parent_id);
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
                invalidation::on_grant_delete(&mut guard, tenant_id, resource_id, grantee);
            }
            if is_resource_graph
                && let Some(ref val) = old_val
                && let Some(child_id) = val.get("id").and_then(|v| v.as_str())
            {
                invalidation::on_edge_delete(&mut guard, tenant_id, child_id);
            }
        }
        _ => {}
    }

    debug!(
        tenant_id,
        collection, "permission_tree: cache updated from CDC event"
    );
}

/// Extract a PermissionGrant from a JSON value (permission table row).
fn extract_grant(val: &serde_json::Value) -> Option<PermissionGrant> {
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
        Arc::new(tokio::sync::RwLock::new(cache))
    }

    fn grant_insert() -> WriteEvent {
        let row = serde_json::json!({
            "resource_id": "d1",
            "grantee": "role_a",
            "level": "viewer",
            "inherited": false,
        });
        WriteEvent {
            sequence: 1,
            collection: Arc::from("grants"),
            op: WriteOp::Insert,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("g1")),
            lsn: Lsn::new(1),
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TENANT),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(
                nodedb_types::json_to_msgpack(&row).expect("encode grant row"),
            )),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        }
    }

    #[tokio::test]
    async fn grant_event_applies_after_a_held_read_lock_is_released() {
        let cache = cache_with_tree();
        let reader = cache.read().await;

        let apply = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { handle_permission_event(&grant_insert(), &cache).await })
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
        assert_eq!(guard.tenant_version(TENANT), 1);
    }
}
