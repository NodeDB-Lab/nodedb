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
use crate::event::types::{EventSource, WriteEvent, WriteOp, deserialize_event_payload};
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
        .and_then(|v| deserialize_event_payload(v));
    let old_fields = event
        .old_value
        .as_ref()
        .and_then(|v| deserialize_event_payload(v));

    // Build a system identity for trigger execution (SECURITY DEFINER model).
    let identity = trigger_identity(event.tenant_id);

    let op_str = event.op.to_string();
    let result = match event.op {
        WriteOp::BulkInsert { .. } | WriteOp::BulkDelete { .. } => {
            // Bulk events are only created during WAL replay (wal_replay.rs)
            // and always carry new_value: None / old_value: None — they are
            // aggregate metadata (count of affected rows), not per-row payloads.
            //
            // The Data Plane ring buffer path emits individual Insert/Delete
            // events for each row in a batch, so triggers fire on those
            // individual events. Bulk events are safe to skip for ROW triggers.
            //
            // However, STATEMENT-level triggers fire on bulk events since they
            // represent a complete DML statement.
            let dml_event = match event.op {
                WriteOp::BulkInsert { .. } => crate::control::trigger::DmlEvent::Insert,
                _ => crate::control::trigger::DmlEvent::Delete,
            };
            crate::control::trigger::fire_statement::fire_after_statement(
                state,
                &identity,
                event.tenant_id,
                &event.collection,
                dml_event,
                0,
                mode_filter,
            )
            .await
        }
        _ => {
            // Fire ROW-level triggers for individual events.
            let row_result = fire_for_operation(
                &op_str,
                state,
                &identity,
                event.tenant_id,
                &event.collection,
                new_fields.as_ref(),
                old_fields.as_ref(),
                0,
                mode_filter,
            )
            .await;

            // Also fire STATEMENT-level triggers for individual point operations
            // (a point INSERT/UPDATE/DELETE is also a complete statement).
            if row_result.is_ok() {
                let dml_event = match event.op {
                    WriteOp::Insert => crate::control::trigger::DmlEvent::Insert,
                    WriteOp::Update => crate::control::trigger::DmlEvent::Update,
                    WriteOp::Delete => crate::control::trigger::DmlEvent::Delete,
                    _ => return, // Heartbeat, etc.
                };
                if let Err(e) = crate::control::trigger::fire_statement::fire_after_statement(
                    state,
                    &identity,
                    event.tenant_id,
                    &event.collection,
                    dml_event,
                    0,
                    mode_filter,
                )
                .await
                {
                    warn!(
                        collection = %event.collection,
                        op = %event.op,
                        error = %e,
                        "AFTER STATEMENT trigger failed, enqueuing for retry"
                    );
                    // Statement trigger failures also go through retry.
                    retry_queue.enqueue(RetryEntry {
                        tenant_id: event.tenant_id.as_u32(),
                        collection: event.collection.to_string(),
                        row_id: String::new(),
                        operation: op_str.clone(),
                        trigger_name: String::new(),
                        new_fields: None,
                        old_fields: None,
                        attempts: 0,
                        last_error: e.to_string(),
                        next_retry_at: std::time::Instant::now(),
                        source_lsn: event.lsn.as_u64(),
                        source_sequence: event.sequence,
                        cascade_depth: 0,
                    });
                }
            }

            row_result
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

/// Dispatch a batch of trigger rows.
///
/// For `BatchSafe` triggers, fires each matching trigger once per row in the batch
/// (but with the optimization of pre-filtering WHEN clauses across the whole batch).
/// For `RowAtATime` triggers, fires per-row as usual.
///
/// This is called by the consumer loop after the batch collector yields a full batch.
pub async fn dispatch_trigger_batch(
    batch: &crate::control::trigger::batch::collector::TriggerBatch,
    state: &Arc<SharedState>,
    retry_queue: &mut TriggerRetryQueue,
) {
    use crate::control::security::catalog::trigger_types::{TriggerGranularity, TriggerTiming};
    use crate::control::trigger::batch::when_filter;
    use crate::control::trigger::fire_common;
    use crate::control::trigger::registry::DmlEvent;

    let tenant_id = TenantId::new(batch.tenant_id);
    let identity = trigger_identity(tenant_id);
    let mode_filter = Some(TriggerExecutionMode::Async);

    let dml_event = match batch.operation.as_str() {
        "INSERT" => DmlEvent::Insert,
        "UPDATE" => DmlEvent::Update,
        "DELETE" => DmlEvent::Delete,
        _ => return,
    };

    let triggers =
        state
            .trigger_registry
            .get_matching(batch.tenant_id, &batch.collection, dml_event);

    let after_row_triggers: Vec<_> = triggers
        .iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_row_triggers.is_empty() {
        return;
    }

    for trigger in &after_row_triggers {
        // Pre-filter the batch by WHEN clause.
        let mask = when_filter::filter_batch_by_when(
            &batch.rows,
            &batch.collection,
            &batch.operation,
            trigger.when_condition.as_deref(),
        );

        let passing = when_filter::count_passing(&mask);
        if passing == 0 {
            continue;
        }

        // For each passing row, fire the trigger body.
        // BatchSafe triggers could in the future dispatch a single bulk DML
        // with all passing rows. For now, they still fire per-row but with
        // the WHEN clause pre-filtered (avoiding parse+eval overhead on
        // non-matching rows).
        for (row, &passes) in batch.rows.iter().zip(mask.iter()) {
            if !passes {
                continue;
            }

            let bindings =
                when_filter::build_row_bindings(row, &batch.collection, &batch.operation);

            let result = fire_common::fire_triggers(
                state,
                &identity,
                tenant_id,
                &batch.collection,
                std::slice::from_ref(trigger),
                &bindings,
                0,
            )
            .await;

            if let Err(e) = result {
                warn!(
                    trigger = %trigger.name,
                    collection = %batch.collection,
                    row_id = %row.row_id,
                    error = %e,
                    "batch trigger fire failed, enqueuing row for retry"
                );
                retry_queue.enqueue(RetryEntry {
                    tenant_id: batch.tenant_id,
                    collection: batch.collection.clone(),
                    row_id: row.row_id.clone(),
                    operation: batch.operation.clone(),
                    trigger_name: trigger.name.clone(),
                    new_fields: row.new_fields.clone(),
                    old_fields: row.old_fields.clone(),
                    attempts: 0,
                    last_error: e.to_string(),
                    next_retry_at: std::time::Instant::now(),
                    source_lsn: 0,
                    source_sequence: 0,
                    cascade_depth: 0,
                });
            }
        }
    }
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
        let map = deserialize_event_payload(&bytes).unwrap();
        assert_eq!(map.get("id").unwrap(), &serde_json::json!(1));
        assert_eq!(map.get("name").unwrap(), &serde_json::json!("test"));
    }

    #[test]
    fn deserialize_msgpack_payload() {
        let json = serde_json::json!({"status": "active", "count": 42});
        let bytes = rmp_serde::to_vec(&json).unwrap();
        let map = deserialize_event_payload(&bytes).unwrap();
        assert_eq!(map.get("status").unwrap(), &serde_json::json!("active"));
    }

    #[test]
    fn deserialize_non_object_returns_none() {
        let bytes = serde_json::to_vec(&serde_json::json!([1, 2, 3])).unwrap();
        assert!(deserialize_event_payload(&bytes).is_none());
    }

    #[test]
    fn trigger_identity_is_superuser() {
        let id = trigger_identity(TenantId::new(5));
        assert!(id.is_superuser);
        assert_eq!(id.tenant_id, TenantId::new(5));
    }
}
