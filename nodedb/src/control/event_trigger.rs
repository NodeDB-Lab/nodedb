// SPDX-License-Identifier: BUSL-1.1

//! DEFINE EVENT trigger processing in the WAL-recoverable Event Plane.
//!
//! Each data [`WriteEvent`] is processed by the Event Plane consumer after it
//! has been ordered by the durable WAL. This deliberately has no ChangeStream
//! subscription or independent checkpoint: the Event Plane watermark covers
//! both normal delivery and restart catchup.

use std::sync::Arc;

use tracing::{debug, error, info, trace};

use crate::control::event_action_error::{TriggerActionError, TriggerRenderError};
use crate::control::planner::context::QueryContext;
use crate::control::state::SharedState;
use crate::event::action::{
    ActionContext, ActionId, ActionKey, ActionPayload, ActionRetryQueue, FailedAction,
};
use crate::event::types::{EventSource, WriteEvent, WriteOp};
use crate::types::{DatabaseId, TenantId};

/// Process one WAL-derived data event against matching EventDefinitions.
///
/// The caller awaits this before advancing its durable consumer watermark, so
/// DEFINE EVENT actions share the Event Plane's normal/replay delivery and
/// recovery guarantees. Heartbeats are intentionally not trigger input.
pub async fn process_write_event(
    shared: Arc<SharedState>,
    event: &WriteEvent,
    queue: &mut ActionRetryQueue,
) {
    if !event.op.is_data_event() {
        return;
    }

    // An action's own writes come back through the Event Plane. Firing event
    // definitions on them lets an action that writes to the collection it
    // watches re-trigger itself without bound, so only the same sources that
    // fire triggers fire event definitions.
    if !matches!(event.source, EventSource::User | EventSource::Deferred) {
        trace!(
            source = %event.source,
            collection = %event.collection,
            "skipping event definitions for a non-triggerable event source"
        );
        return;
    }

    let catalog = shared.credentials.catalog();
    let coll = match catalog.get_collection(
        event.database_id,
        event.tenant_id.as_u64(),
        &event.collection,
    ) {
        Ok(Some(collection)) => collection,
        _ => return,
    };

    if coll.event_defs.is_empty() {
        return;
    }

    let op_str = event_operation(event.op);
    for (index, event_def) in coll.event_defs.iter().enumerate() {
        let when_upper = event_def.when_condition.to_uppercase();
        let matches = match when_upper.as_str() {
            "INSERT" => matches!(event.op, WriteOp::Insert | WriteOp::BulkInsert { .. }),
            "UPDATE" => event.op == WriteOp::Update,
            "DELETE" => matches!(event.op, WriteOp::Delete | WriteOp::BulkDelete { .. }),
            "ANY" | "*" | "TRUE" => true,
            compound => compound.contains(op_str),
        };
        if !matches {
            continue;
        }

        debug!(
            event = event_def.name,
            collection = %event.collection,
            document_id = ?event.row_id,
            operation = op_str,
            action = event_def.then_action,
            "event trigger fired"
        );

        let rendered = render_then_action_sql(&event_def.then_action, event);
        let outcome = match &rendered {
            Ok(sql) => {
                run_event_action_sql(
                    Arc::clone(&shared),
                    event.database_id,
                    event.tenant_id,
                    sql,
                    &event_def.name,
                )
                .await
            }
            Err(source) => Err(TriggerActionError::Rejected {
                source: source.clone(),
            }),
        };

        // The audit record is this path's durable account of what the trigger
        // did, so a failed action is recorded as failed rather than as a fired
        // action. A failure ends this action; the remaining event definitions
        // are independent triggers and plan against fresh catalog state.
        let (source, detail) = match &outcome {
            Ok(()) => (
                "event_trigger",
                format!(
                    "event '{}' on '{}': doc={}, op={}, action={}",
                    event_def.name, event.collection, event.row_id, op_str, event_def.then_action
                ),
            ),
            Err(error) => (
                "event_trigger_failed",
                format!(
                    "event '{}' on '{}': doc={}, op={}, action={}, error={error}",
                    event_def.name, event.collection, event.row_id, op_str, event_def.then_action
                ),
            ),
        };
        shared.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(event.tenant_id),
            source,
            &detail,
        );
        if let Err(error) = outcome {
            error!(
                trigger = event_def.name,
                collection = %event.collection,
                retryable = error.is_retryable(),
                error = %error,
                "event trigger action failed"
            );
            // Only an action that applied nothing can be re-run. A malformed
            // template will never render, and a part-applied action would
            // duplicate the tasks that already landed.
            if let (true, Ok(sql)) = (error.is_retryable(), &rendered) {
                queue.enqueue(FailedAction {
                    key: ActionKey {
                        source_lsn: event.lsn.as_u64(),
                        source_sequence: event.sequence,
                        source_vshard: event.vshard_id.as_u32(),
                        action: ActionId::EventAction {
                            event_name: event_def.name.clone(),
                            index,
                        },
                    },
                    payload: ActionPayload::EventAction { sql: sql.clone() },
                    context: ActionContext {
                        database_id: event.database_id,
                        tenant_id: event.tenant_id.as_u64(),
                        collection: event.collection.to_string(),
                        row_id: event.row_id.as_str().to_owned(),
                        cascade_depth: 0,
                    },
                    attempts: 0,
                    last_error: error.to_string(),
                });
            }
        }
    }
}

fn event_operation(op: WriteOp) -> &'static str {
    match op {
        WriteOp::Insert | WriteOp::BulkInsert { .. } => "INSERT",
        WriteOp::Update => "UPDATE",
        WriteOp::Delete | WriteOp::BulkDelete { .. } => "DELETE",
        WriteOp::Heartbeat => "HEARTBEAT",
    }
}

fn contains_trigger_placeholder(text: &str) -> bool {
    ["$document_id", "$collection", "$operation"]
        .iter()
        .any(|placeholder| text.contains(placeholder))
}

fn quoted_region_end(sql: &str, start: usize, quote: u8, backslash_escapes: bool) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if backslash_escapes && bytes[cursor] == b'\\' {
            cursor = cursor.checked_add(2)?;
            continue;
        }
        if bytes[cursor] == quote {
            if bytes.get(cursor + 1) == Some(&quote) {
                cursor += 2;
                continue;
            }
            return Some(cursor + 1);
        }
        cursor += 1;
    }
    None
}

fn dollar_delimiter(sql: &str, start: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut cursor = start + 1;
    while bytes
        .get(cursor)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        cursor += 1;
    }
    (bytes.get(cursor) == Some(&b'$')).then(|| &sql[start..=cursor])
}

fn canonical_trigger_template_sql(fragment: &str) -> &str {
    fragment
}

fn render_then_action_sql(action: &str, event: &WriteEvent) -> Result<String, TriggerRenderError> {
    let mut rendered = String::with_capacity(action.len());
    let mut cursor = 0;
    while cursor < action.len() {
        let rest = &action[cursor..];
        if rest.starts_with("--") {
            let end = rest
                .find('\n')
                .map_or(action.len(), |offset| cursor + offset);
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if rest.starts_with("/*") {
            let bytes = action.as_bytes();
            let mut end = cursor + 2;
            let mut depth = 1usize;
            while end < bytes.len() && depth > 0 {
                if bytes[end..].starts_with(b"/*") {
                    depth += 1;
                    end += 2;
                } else if bytes[end..].starts_with(b"*/") {
                    depth -= 1;
                    end += 2;
                } else {
                    end += 1;
                }
            }
            if depth != 0 {
                return Err(TriggerRenderError::UnterminatedBlockComment);
            }
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if rest.starts_with('\'') || rest.starts_with('"') {
            let quote = action.as_bytes()[cursor];
            let backslash_escapes = quote == b'\''
                && cursor > 0
                && matches!(action.as_bytes()[cursor - 1], b'E' | b'e')
                && (cursor == 1
                    || !matches!(
                        action.as_bytes()[cursor - 2],
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'$'
                    ));
            let end = quoted_region_end(action, cursor, quote, backslash_escapes)
                .ok_or(TriggerRenderError::UnterminatedQuote)?;
            if contains_trigger_placeholder(&action[cursor..end]) {
                return Err(TriggerRenderError::QuotedPlaceholder);
            }
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if let Some(delimiter) = dollar_delimiter(action, cursor) {
            let body_start = cursor + delimiter.len();
            let relative_end = action[body_start..]
                .find(delimiter)
                .ok_or(TriggerRenderError::UnterminatedDollarQuote)?;
            let end = body_start + relative_end + delimiter.len();
            if contains_trigger_placeholder(&action[cursor..end]) {
                return Err(TriggerRenderError::QuotedPlaceholder);
            }
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if rest.starts_with("$document_id") {
            rendered.push_str(&::nodedb_types::quote_literal(event.row_id.as_str()));
            cursor += "$document_id".len();
        } else if rest.starts_with("$collection") {
            rendered.push_str(&::nodedb_types::quote_ident(&event.collection));
            cursor += "$collection".len();
        } else if rest.starts_with("$operation") {
            let operation = event_operation(event.op);
            rendered.push_str(&::nodedb_types::quote_literal(operation));
            cursor += "$operation".len();
        } else {
            let ch = rest
                .chars()
                .next()
                .ok_or(TriggerRenderError::InvalidUtf8Boundary)?;
            rendered.push(ch);
            cursor += ch.len_utf8();
        }
    }
    Ok(rendered)
}

/// Plan and run one already-rendered THEN action.
///
/// Takes rendered SQL rather than a template because a retry runs long after
/// its event is gone: the record stores the substituted statement, and this
/// re-plans it against the catalog as it stands now. Template variables are
/// substituted as complete canonical SQL tokens before execution and
/// therefore must not be manually quoted:
/// - `$document_id` → a string literal containing the affected document ID
/// - `$collection` → a quoted collection identifier
/// - `$operation` → an `INSERT`, `UPDATE`, or `DELETE` string literal
pub async fn run_event_action_sql(
    shared: Arc<SharedState>,
    database_id: DatabaseId,
    tenant_id: TenantId,
    sql: &str,
    trigger_name: &str,
) -> Result<(), TriggerActionError> {
    let query_ctx = QueryContext::for_state(&shared);
    // A trigger action is database-defined code with no external requester, so
    // it plans as the system — the same SECURITY DEFINER model the trigger
    // dispatcher already uses for the identity it executes under.
    let security = crate::control::planner::context::SystemPlanSecurity::new(
        tenant_id,
        "_system_event_trigger",
    );

    let (tasks, _output_schema, versions, _) = query_ctx
        .plan_sql_with_rls_and_versions(
            sql,
            tenant_id,
            database_id,
            &security.context(&shared),
            None,
        )
        .await
        .map_err(|source| TriggerActionError::Plan { source })?;

    // Keep the Arc and lease scope alive through the whole action. Admission
    // is fail-closed while a descriptor drains.
    let lease_scope = Arc::new(
        Arc::clone(&shared)
            .acquire_plan_lease_scope(&versions)
            .map_err(|source| TriggerActionError::LeaseAdmission { source })?,
    );

    // The action's tasks commit as one transaction. An action that dispatched
    // its tasks one by one could stop half-applied, and re-running a
    // half-applied action repeats the tasks that already landed — which is
    // what makes a retry queue unsafe to point at it.
    let identity = event_action_identity(tenant_id);
    crate::control::system_txn::run_tasks_atomically(
        &shared,
        &identity,
        tasks,
        lease_scope,
        crate::event::EventSource::Trigger,
    )
    .await
    .map_err(|source| TriggerActionError::Transaction { source })?;

    info!(
        trigger = trigger_name,
        sql = sql,
        "event trigger action executed"
    );
    Ok(())
}

/// Identity a DEFINE EVENT action executes under.
///
/// A THEN action is database-defined code with no external requester, so it
/// runs as the system — the same SECURITY DEFINER model the trigger
/// dispatcher applies to trigger bodies.
fn event_action_identity(
    tenant_id: TenantId,
) -> crate::control::security::identity::AuthenticatedIdentity {
    use crate::control::security::identity::{AuthenticatedIdentity, DatabaseSet, Role};
    AuthenticatedIdentity::new_internal_service(
        0,
        "_system_event_trigger",
        tenant_id,
        vec![Role::Superuser],
        true,
        None,
        DatabaseSet::All,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{TriggerRenderError, event_operation, render_then_action_sql};
    use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    fn hostile_event() -> WriteEvent {
        WriteEvent {
            sequence: 1,
            collection: Arc::from("odd\"; DROP TABLE audit; --"),
            op: WriteOp::Insert,
            row_id: RowId::new("doc'; DELETE FROM audit; --"),
            lsn: Lsn::new(1),
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        }
    }

    #[test]
    fn wal_write_operations_preserve_event_condition_operation_names() {
        assert_eq!(event_operation(WriteOp::Insert), "INSERT");
        assert_eq!(event_operation(WriteOp::BulkInsert { count: 2 }), "INSERT");
        assert_eq!(event_operation(WriteOp::Update), "UPDATE");
        assert_eq!(event_operation(WriteOp::BulkDelete { count: 2 }), "DELETE");
        assert_eq!(event_operation(WriteOp::Heartbeat), "HEARTBEAT");
    }

    #[test]
    fn trigger_placeholders_render_as_canonical_sql_tokens() {
        let sql = render_then_action_sql(
            "INSERT INTO $collection (id, op) VALUES ($document_id, $operation)",
            &hostile_event(),
        )
        .expect("render trigger SQL");
        assert_eq!(
            sql,
            "INSERT INTO \"odd\"\"; DROP TABLE audit; --\" (id, op) VALUES ('doc''; DELETE FROM audit; --', 'INSERT')"
        );
    }

    #[test]
    fn trigger_placeholders_reject_manual_quoting_and_preserve_opaque_comments() {
        assert!(render_then_action_sql("SELECT '$document_id'", &hostile_event()).is_err());
        assert_eq!(
            render_then_action_sql("SELECT 1 -- $document_id\n", &hostile_event())
                .expect("comment is opaque"),
            "SELECT 1 -- $document_id\n"
        );
    }

    #[test]
    fn trigger_renderer_preserves_escape_string_literal_boundaries() {
        assert_eq!(
            render_then_action_sql(r"SELECT E'escaped \' quote'", &hostile_event())
                .expect("escape string is opaque"),
            r"SELECT E'escaped \' quote'"
        );
        assert_eq!(
            render_then_action_sql(r"SELECT e'escaped \' $document_id'", &hostile_event()),
            Err(TriggerRenderError::QuotedPlaceholder)
        );
    }
}
