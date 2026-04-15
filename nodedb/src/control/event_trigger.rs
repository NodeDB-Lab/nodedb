//! Background event trigger processor.
//!
//! Subscribes to the ChangeStream and evaluates DEFINE EVENT triggers
//! for each write. When a WHEN condition matches, the THEN action is
//! executed as a SQL statement via the Control Plane query path.

use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::control::change_stream::{ChangeEvent, ChangeOperation};
use crate::control::planner::context::QueryContext;
use crate::control::state::SharedState;

/// Spawn the event trigger processor as a background tokio task
/// registered with the shutdown loop registry. The processor
/// exits on shutdown signal or when the change-stream
/// broadcast channel closes.
pub fn spawn_event_trigger_processor(shared: Arc<SharedState>) {
    let mut subscription = shared.change_stream.subscribe(None, None);
    let registry = Arc::clone(&shared.loop_registry);
    let watch = Arc::clone(&shared.shutdown);

    crate::control::shutdown::spawn_loop(
        &registry,
        &watch,
        "event_trigger_processor",
        move |mut shutdown| async move {
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => {
                        debug!("event trigger processor shutting down");
                        break;
                    }
                    msg = subscription.recv_filtered() => match msg {
                        Ok(event) => process_event(&shared, &event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(
                                lagged = n,
                                "event trigger processor fell behind; skipped {n} events"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            debug!("change stream closed; event trigger processor stopping");
                            break;
                        }
                    },
                }
            }
        },
    );
}

/// Process a single change event against all matching EventDefinitions.
async fn process_event(shared: &SharedState, event: &ChangeEvent) {
    let catalog = match shared.credentials.catalog() {
        Some(c) => c,
        None => return,
    };

    let coll = match catalog.get_collection(event.tenant_id.as_u32(), &event.collection) {
        Ok(Some(c)) => c,
        _ => return,
    };

    if coll.event_defs.is_empty() {
        return;
    }

    let op_str = event.operation.as_str();

    for event_def in &coll.event_defs {
        let when_upper = event_def.when_condition.to_uppercase();
        let matches = match when_upper.as_str() {
            "INSERT" => event.operation == ChangeOperation::Insert,
            "UPDATE" => event.operation == ChangeOperation::Update,
            "DELETE" => event.operation == ChangeOperation::Delete,
            "ANY" | "*" | "TRUE" => true,
            compound => compound.contains(op_str),
        };

        if !matches {
            continue;
        }

        debug!(
            event = event_def.name,
            collection = event.collection,
            document_id = event.document_id,
            operation = op_str,
            action = event_def.then_action,
            "event trigger fired"
        );

        // Execute the THEN action as SQL.
        execute_then_action(shared, event, &event_def.then_action, &event_def.name).await;

        shared.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(event.tenant_id),
            "event_trigger",
            &format!(
                "event '{}' on '{}': doc={}, op={}, action={}",
                event_def.name, event.collection, event.document_id, op_str, event_def.then_action
            ),
        );
    }
}

/// Execute a THEN action string as SQL.
///
/// Template variables are substituted before execution:
/// - `$document_id` → the affected document ID
/// - `$collection` → the collection name
/// - `$operation` → "insert", "update", or "delete"
async fn execute_then_action(
    shared: &SharedState,
    event: &ChangeEvent,
    action: &str,
    trigger_name: &str,
) {
    let sql = action
        .replace("$document_id", &event.document_id)
        .replace("$collection", &event.collection)
        .replace("$operation", event.operation.as_str());

    let query_ctx = QueryContext::for_state(shared);

    match query_ctx.plan_sql(&sql, event.tenant_id).await {
        Ok(tasks) => {
            for task in tasks {
                match crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    shared,
                    task.tenant_id,
                    task.vshard_id,
                    task.plan,
                    0,
                )
                .await
                {
                    Ok(_) => {
                        info!(
                            trigger = trigger_name,
                            sql = sql,
                            "event trigger action executed"
                        );
                    }
                    Err(e) => {
                        warn!(
                            trigger = trigger_name,
                            sql = sql,
                            error = %e,
                            "event trigger action dispatch failed"
                        );
                    }
                }
            }
        }
        Err(e) => {
            warn!(
                trigger = trigger_name,
                sql = sql,
                error = %e,
                "event trigger action plan failed"
            );
        }
    }
}
