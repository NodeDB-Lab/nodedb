// SPDX-License-Identifier: BUSL-1.1

//! Single-event trigger dispatch: one `WriteEvent` → matching AFTER triggers.
//!
//! For each incoming `WriteEvent` with a triggerable source, this path:
//! 1. Deserializes `new_value` / `old_value` from MessagePack to
//!    `HashMap<String, nodedb_types::Value>`
//! 2. Fires every matching AFTER trigger through `control::trigger::fire`
//! 3. Queues one retry record per trigger that failed
//!
//! Every matching trigger fires even when one of them fails. These triggers
//! share no transaction, so a failure carries no reason to cancel the rest,
//! and a retry re-runs only the trigger named on its record — a sibling
//! skipped here would never run at all.

use std::sync::Arc;

use tracing::trace;

use crate::control::planner::procedural::executor::core::CrossShardOrigin;
use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::control::trigger::TriggerScope;
use crate::control::trigger::fire;
use crate::control::trigger::fire_common::{FireErrorPolicy, FireReport};
use crate::control::trigger::fire_statement::{FireAfterStatementParams, fire_after_statement};
use crate::control::trigger::row_identity::inject_row_identity;
use crate::event::action::ActionRetryQueue;
use crate::event::types::{EventSource, WriteEvent, WriteOp, deserialize_event_payload};
use crate::types::TenantId;

use super::enqueue::{ActionSource, record_row_failures, record_statement_failures};
use super::identity::trigger_identity;

/// Dispatch a `WriteEvent` to matching AFTER triggers.
///
/// Fires only for `EventSource::User` / `Deferred`. Trigger and replicated
/// writes are skipped to prevent cascades. Restored rows are skipped because
/// their triggers fired at original write time. Trigger failures never
/// propagate to the caller. They are queued for retry and, once out of
/// attempts, routed to the DLQ.
pub async fn dispatch_triggers(
    event: &WriteEvent,
    state: &Arc<SharedState>,
    queue: &mut ActionRetryQueue,
) {
    let mode_filter = match event.source {
        EventSource::User => Some(TriggerExecutionMode::Async),
        EventSource::Deferred => Some(TriggerExecutionMode::Deferred),
        EventSource::Trigger
        | EventSource::RaftFollower
        | EventSource::CrdtSync
        | EventSource::Restore => {
            trace!(
                source = %event.source,
                collection = %event.collection,
                "skipping trigger dispatch for non-triggerable event source"
            );
            return;
        }
    };

    let identity = trigger_identity(event.tenant_id);
    let op_str = event.op.to_string();
    let source = ActionSource {
        database_id: event.database_id,
        tenant_id: event.tenant_id.as_u64(),
        collection: &event.collection,
        row_id: event.row_id.as_str(),
        operation: &op_str,
        source_lsn: event.lsn.as_u64(),
        source_sequence: event.sequence,
        source_vshard: event.vshard_id.as_u32(),
        cascade_depth: 0,
    };

    // Bulk events are only created during WAL replay and always carry
    // `new_value: None` / `old_value: None` — they are aggregate metadata (a
    // count of affected rows), not per-row payloads. The Data Plane ring
    // buffer emits an individual Insert/Delete event per row, so ROW triggers
    // fire on those. A bulk event still represents one complete statement, so
    // STATEMENT triggers do fire on it.
    let is_bulk = matches!(
        event.op,
        WriteOp::BulkInsert { .. } | WriteOp::BulkDelete { .. }
    );

    // A row image that does not decode refuses the ROW pass: firing with a
    // missing NEW or OLD row would skip every trigger without a trace.
    let fields = if is_bulk {
        None
    } else {
        match event_row_fields(event) {
            Ok(fields) => Some(fields),
            Err(error) => {
                record_row_failures(
                    &source,
                    FireReport::from_precondition(error),
                    None,
                    None,
                    queue,
                );
                None
            }
        }
    };
    if let Some((new_fields, old_fields)) = fields {
        let report = fire_for_operation(FireForOperationParams {
            operation: &op_str,
            state,
            identity: &identity,
            database_id: event.database_id,
            tenant_id: event.tenant_id,
            collection: &event.collection,
            new_fields: new_fields.as_ref(),
            old_fields: old_fields.as_ref(),
            cascade_depth: 0,
            mode_filter,
            cross_shard_origin: Some(CrossShardOrigin {
                source_lsn: event.lsn.as_u64(),
                source_sequence: event.sequence,
                source_vshard: event.vshard_id.as_u32(),
                source_collection: event.collection.to_string(),
            }),
            on_error: FireErrorPolicy::Continue,
            only_trigger: None,
        })
        .await;
        record_row_failures(
            &source,
            report,
            new_fields.as_ref(),
            old_fields.as_ref(),
            queue,
        );
    }

    // STATEMENT triggers are a separate action from the ROW triggers of the
    // same write, so they fire whether or not a ROW trigger failed.
    let Some(dml_event) = dml_event_of(&event.op) else {
        return;
    };
    let report = fire_after_statement(FireAfterStatementParams {
        state,
        identity: &identity,
        scope: TriggerScope {
            database_id: event.database_id,
            tenant_id: event.tenant_id,
        },
        collection: &event.collection,
        event: dml_event,
        cascade_depth: 0,
        mode_filter,
        on_error: FireErrorPolicy::Continue,
        only_trigger: None,
    })
    .await;
    record_statement_failures(&source, report, queue);
}

/// The NEW and OLD row fields of `event`, with the row identity injected.
///
/// Every engine emits its row images as the decoded row map a read returns.
/// A KV row is the `{key, value}` row the Data Plane shapes at emission.
fn event_row_fields(event: &WriteEvent) -> crate::Result<(RowFields, RowFields)> {
    let row_id = event.row_id.as_str();
    Ok((
        row_fields(event.new_value.as_deref(), row_id)?,
        row_fields(event.old_value.as_deref(), row_id)?,
    ))
}

type RowFields = Option<std::collections::HashMap<String, nodedb_types::Value>>;

/// One row image as fields. `None` when the event carries no image. An image
/// that is not a row map is an error.
fn row_fields(payload: Option<&[u8]>, row_id: &str) -> crate::Result<RowFields> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let map = deserialize_event_payload(payload).ok_or_else(|| crate::Error::Internal {
        detail: format!("trigger dispatch: the row image of '{row_id}' is not a row map"),
    })?;
    let mut fields: std::collections::HashMap<String, nodedb_types::Value> = map
        .into_iter()
        .map(|(k, v)| (k, nodedb_types::Value::from(v)))
        .collect();
    inject_row_identity(&mut fields, row_id);
    Ok(Some(fields))
}

/// The statement-level DML event a write op represents, if any.
fn dml_event_of(op: &WriteOp) -> Option<crate::control::trigger::DmlEvent> {
    match op {
        WriteOp::Insert | WriteOp::BulkInsert { .. } => {
            Some(crate::control::trigger::DmlEvent::Insert)
        }
        WriteOp::Update => Some(crate::control::trigger::DmlEvent::Update),
        WriteOp::Delete | WriteOp::BulkDelete { .. } => {
            Some(crate::control::trigger::DmlEvent::Delete)
        }
        _ => None,
    }
}

/// Parameters for [`fire_for_operation`].
pub(super) struct FireForOperationParams<'a> {
    /// DML operation string (`"INSERT"` / `"UPDATE"` / `"DELETE"`).
    pub operation: &'a str,
    /// Shared server state (trigger registry, block cache).
    pub state: &'a Arc<SharedState>,
    /// Effective identity used to fire the trigger.
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: crate::types::DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// NEW row fields, when the operation carries a NEW row.
    pub new_fields: Option<&'a std::collections::HashMap<String, nodedb_types::Value>>,
    /// OLD row fields, when the operation carries an OLD row.
    pub old_fields: Option<&'a std::collections::HashMap<String, nodedb_types::Value>>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Source-write context, so a trigger body writing to a remote-homed
    /// collection is dispatched to the owning node instead of the local core.
    pub cross_shard_origin: Option<CrossShardOrigin>,
    /// What a failing trigger does to the triggers queued behind it.
    pub on_error: FireErrorPolicy,
    /// Restricts firing to the one named trigger; `None` fires every match.
    pub only_trigger: Option<&'a str>,
}

/// Shared trigger fire logic: routes to the correct `fire_after_*` function.
///
/// Used by both initial dispatch (from a `WriteEvent`) and retry (from a
/// queued action).
pub(super) async fn fire_for_operation(params: FireForOperationParams<'_>) -> FireReport {
    let FireForOperationParams {
        operation,
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        new_fields,
        old_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
        on_error,
        only_trigger,
    } = params;

    match operation {
        "INSERT" => match new_fields {
            Some(new) => {
                fire::fire_after_insert(fire::FireAfterInsertParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection,
                    new_fields: new,
                    cascade_depth,
                    mode_filter,
                    cross_shard_origin,
                    on_error,
                    only_trigger,
                })
                .await
            }
            None => FireReport::default(),
        },
        "UPDATE" => match (old_fields, new_fields) {
            (Some(old), Some(new)) => {
                fire::fire_after_update(fire::FireAfterUpdateParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection,
                    old_fields: old,
                    new_fields: new,
                    cascade_depth,
                    mode_filter,
                    cross_shard_origin,
                    on_error,
                    only_trigger,
                })
                .await
            }
            _ => FireReport::default(),
        },
        "DELETE" => match old_fields {
            Some(old) => {
                fire::fire_after_delete(fire::FireAfterDeleteParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection,
                    old_fields: old,
                    cascade_depth,
                    mode_filter,
                    cross_shard_origin,
                    on_error,
                    only_trigger,
                })
                .await
            }
            None => FireReport::default(),
        },
        _ => FireReport::default(),
    }
}

#[cfg(test)]
mod tests {
    use crate::event::types::deserialize_event_payload;

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
        let bytes = nodedb_types::json_to_msgpack(&json).unwrap();
        let map = deserialize_event_payload(&bytes).unwrap();
        assert_eq!(map.get("status").unwrap(), &serde_json::json!("active"));
    }

    #[test]
    fn deserialize_non_object_returns_none() {
        let bytes = serde_json::to_vec(&serde_json::json!([1, 2, 3])).unwrap();
        assert!(deserialize_event_payload(&bytes).is_none());
    }

    #[test]
    fn a_shaped_kv_row_binds_its_key_and_value() {
        let row = nodedb_query::msgpack_scan::kv_row_msgpack("a", b"x");
        let fields = super::row_fields(Some(&row), "a")
            .expect("a KV row decodes")
            .expect("an image is present");
        assert_eq!(
            fields.get("key"),
            Some(&nodedb_types::Value::String("a".into()))
        );
        assert_eq!(
            fields.get("value"),
            Some(&nodedb_types::Value::String("x".into()))
        );
    }

    #[test]
    fn a_document_image_that_is_not_a_map_is_an_error() {
        assert!(super::row_fields(Some(b"x"), "a").is_err());
        assert!(super::row_fields(None, "a").expect("no image").is_none());
    }
}
