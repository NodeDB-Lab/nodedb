//! Trigger dispatcher: bridges Event Plane events to Control Plane trigger fire.
//!
//! For each incoming WriteEvent with `source: User`, the dispatcher:
//! 1. Deserializes `new_value`/`old_value` from MessagePack to serde_json::Map
//! 2. Calls the existing `fire_after_insert/update/delete()` in `control::trigger::fire`
//! 3. On failure, enqueues into the retry queue (exponential backoff)
//! 4. After max retries, sends to the trigger DLQ

use std::sync::Arc;

use tracing::{debug, trace, warn};

use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::control::trigger::fire;
use crate::event::types::{EventSource, WriteEvent, WriteOp};
use crate::types::TenantId;

use super::retry::{RetryEntry, TriggerRetryQueue};

/// Dispatch a WriteEvent to matching AFTER triggers.
///
/// Skips events not from `EventSource::User` (cascade prevention).
/// Returns `Ok(())` even if trigger execution fails — failures are
/// handled via retry queue + DLQ, not propagated to the caller.
pub async fn dispatch_triggers(
    event: &WriteEvent,
    state: &Arc<SharedState>,
    retry_queue: &mut TriggerRetryQueue,
) {
    // Determine which trigger execution mode to fire based on event source.
    let mode_filter = match event.source {
        EventSource::User => Some(TriggerExecutionMode::Async),
        EventSource::Deferred => Some(TriggerExecutionMode::Deferred),
        // Trigger/RaftFollower/CrdtSync sources don't fire triggers.
        _ => {
            trace!(
                source = %event.source,
                collection = %event.collection,
                "skipping trigger dispatch for non-triggerable event source"
            );
            return;
        }
    };

    // Deserialize row data from the event payload.
    let new_fields = event
        .new_value
        .as_ref()
        .and_then(|v| deserialize_to_json_map(v));
    let old_fields = event
        .old_value
        .as_ref()
        .and_then(|v| deserialize_to_json_map(v));

    // Build a system identity for trigger execution (SECURITY DEFINER model).
    let identity = trigger_identity(event.tenant_id);

    let op_str = event.op.to_string();
    let result = match event.op {
        WriteOp::BulkInsert { .. } => {
            // BulkInsert events from WAL replay may not carry per-row payload
            // (new_value is None for KV batch puts). If we have row data,
            // try to deserialize as an array and fire per-row in chunks of 1024.
            if let Some(ref new) = new_fields {
                fire_for_operation(
                    "INSERT",
                    state,
                    &identity,
                    event.tenant_id,
                    &event.collection,
                    Some(new),
                    None,
                    0,
                    mode_filter,
                )
                .await
            } else {
                // No row data in bulk event (e.g., KV batch from WAL replay).
                // Individual row events are emitted separately on the ring buffer path.
                Ok(())
            }
        }
        WriteOp::BulkDelete { .. } => {
            // Same as BulkInsert: if we have old_value, fire per-row.
            if let Some(ref old) = old_fields {
                fire_for_operation(
                    "DELETE",
                    state,
                    &identity,
                    event.tenant_id,
                    &event.collection,
                    None,
                    Some(old),
                    0,
                    mode_filter,
                )
                .await
            } else {
                Ok(())
            }
        }
        _ => {
            fire_for_operation(
                &op_str,
                state,
                &identity,
                event.tenant_id,
                &event.collection,
                new_fields.as_ref(),
                old_fields.as_ref(),
                0, // cascade_depth starts at 0 for Event Plane dispatch
                mode_filter,
            )
            .await
        }
    };

    if let Err(e) = result {
        warn!(
            collection = %event.collection,
            op = %event.op,
            error = %e,
            "trigger execution failed, enqueuing for retry"
        );
        retry_queue.enqueue(RetryEntry {
            tenant_id: event.tenant_id.as_u32(),
            collection: event.collection.to_string(),
            row_id: event.row_id.as_str().to_string(),
            operation: event.op.to_string(),
            trigger_name: String::new(), // Registry matched multiple — generic entry
            new_fields,
            old_fields,
            attempts: 0,
            last_error: e.to_string(),
            next_retry_at: std::time::Instant::now(),
            source_lsn: event.lsn.as_u64(),
            source_sequence: event.sequence,
            cascade_depth: 0,
        });
    }
}

/// Retry a single entry. On failure, re-enqueues into the retry queue.
///
/// Called from the consumer loop where the DLQ mutex is NOT held,
/// avoiding holding a MutexGuard across await points.
pub async fn retry_single(
    entry: &RetryEntry,
    state: &Arc<SharedState>,
    retry_queue: &mut TriggerRetryQueue,
) {
    let identity = trigger_identity(TenantId::new(entry.tenant_id));
    let result = retry_fire(entry, state, &identity).await;

    if let Err(e) = result {
        debug!(
            trigger = %entry.trigger_name,
            attempt = entry.attempts,
            error = %e,
            "trigger retry failed, re-enqueuing"
        );
        let mut re = entry.clone();
        re.last_error = e.to_string();
        retry_queue.enqueue(re);
    }
}

/// Re-fire a single trigger entry during retry.
async fn retry_fire(
    entry: &RetryEntry,
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
) -> crate::Result<()> {
    fire_for_operation(
        entry.operation.as_str(),
        state,
        identity,
        TenantId::new(entry.tenant_id),
        &entry.collection,
        entry.new_fields.as_ref(),
        entry.old_fields.as_ref(),
        entry.cascade_depth,
        Some(TriggerExecutionMode::Async), // Retries are always ASYNC
    )
    .await
}

/// Shared trigger fire logic: routes to the correct fire_after_* function.
///
/// Used by both initial dispatch (from WriteEvent) and retry (from RetryEntry).
/// `mode_filter` controls which execution mode triggers are fired.
#[allow(clippy::too_many_arguments)]
async fn fire_for_operation(
    operation: &str,
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    new_fields: Option<&serde_json::Map<String, serde_json::Value>>,
    old_fields: Option<&serde_json::Map<String, serde_json::Value>>,
    cascade_depth: u32,
    mode_filter: Option<TriggerExecutionMode>,
) -> crate::Result<()> {
    match operation {
        "INSERT" => {
            if let Some(new) = new_fields {
                fire::fire_after_insert(
                    state,
                    identity,
                    tenant_id,
                    collection,
                    new,
                    cascade_depth,
                    mode_filter,
                )
                .await
            } else {
                Ok(())
            }
        }
        "UPDATE" => {
            if let (Some(old), Some(new)) = (old_fields, new_fields) {
                fire::fire_after_update(
                    state,
                    identity,
                    tenant_id,
                    collection,
                    old,
                    new,
                    cascade_depth,
                    mode_filter,
                )
                .await
            } else {
                Ok(())
            }
        }
        "DELETE" => {
            if let Some(old) = old_fields {
                fire::fire_after_delete(
                    state,
                    identity,
                    tenant_id,
                    collection,
                    old,
                    cascade_depth,
                    mode_filter,
                )
                .await
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

/// Deserialize a MessagePack payload (or raw JSON) into a serde_json::Map.
///
/// WriteEvent payloads are stored in the same format as the WAL payload,
/// which is MessagePack for schemaless documents and Binary Tuple for strict.
/// We try MessagePack first, then JSON fallback.
fn deserialize_to_json_map(bytes: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    // Try MessagePack → serde_json::Value → Map.
    if let Ok(serde_json::Value::Object(map)) = rmp_serde::from_slice::<serde_json::Value>(bytes) {
        return Some(map);
    }

    // Try raw JSON.
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_slice::<serde_json::Value>(bytes) {
        return Some(map);
    }

    None
}

/// Build a system identity for trigger execution (SECURITY DEFINER model).
///
/// Trigger bodies execute with superuser privileges — they are database-defined
/// code, not user-submitted queries. The trigger creator's identity is stored
/// in `StoredTrigger.owner` but for now we use a system identity.
fn trigger_identity(tenant_id: TenantId) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: "_system_trigger".into(),
        tenant_id,
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_json_payload() {
        let json = serde_json::json!({"id": 1, "name": "test"});
        let bytes = serde_json::to_vec(&json).unwrap();
        let map = deserialize_to_json_map(&bytes).unwrap();
        assert_eq!(map.get("id").unwrap(), &serde_json::json!(1));
        assert_eq!(map.get("name").unwrap(), &serde_json::json!("test"));
    }

    #[test]
    fn deserialize_msgpack_payload() {
        let json = serde_json::json!({"status": "active", "count": 42});
        let bytes = rmp_serde::to_vec(&json).unwrap();
        let map = deserialize_to_json_map(&bytes).unwrap();
        assert_eq!(map.get("status").unwrap(), &serde_json::json!("active"));
    }

    #[test]
    fn deserialize_non_object_returns_none() {
        let bytes = serde_json::to_vec(&serde_json::json!([1, 2, 3])).unwrap();
        assert!(deserialize_to_json_map(&bytes).is_none());
    }

    #[test]
    fn trigger_identity_is_superuser() {
        let id = trigger_identity(TenantId::new(5));
        assert!(id.is_superuser);
        assert_eq!(id.tenant_id, TenantId::new(5));
    }
}
