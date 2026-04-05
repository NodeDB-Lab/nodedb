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
/// Called from the Event Plane consumer after CDC routing. Checks if the
/// event's collection is a permission table or resource graph for any
/// registered permission tree, and updates the cache accordingly.
pub fn handle_permission_event(
    event: &WriteEvent,
    cache: &Arc<tokio::sync::RwLock<PermissionCache>>,
) {
    let collection = event.collection.as_ref();
    let tenant_id = event.tenant_id.as_u32();

    // Non-blocking lock — skip if contended; next event will catch up.
    let mut guard = match cache.try_write() {
        Ok(g) => g,
        Err(_) => return,
    };

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
