//! AFTER trigger firing logic.
//!
//! Called after DML operations to fire matching AFTER ROW triggers.
//! Matches triggers by (collection, event), evaluates WHEN clauses,
//! and invokes the statement executor for each matching trigger body.
//!
//! Supports three execution modes via `mode_filter`:
//! - `Some(Sync)`: fire in the Control Plane write path (same transaction)
//! - `Some(Async)`: fire from Event Plane (eventually consistent)
//! - `Some(Deferred)`: fire at COMMIT time (same transaction, batched)
//! - `None`: fire all AFTER triggers regardless of mode (legacy behavior)

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::security::catalog::trigger_types::{TriggerExecutionMode, TriggerTiming};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use std::collections::HashMap;

use super::fire_common::{check_cascade_depth, fire_triggers};
use super::registry::DmlEvent;

/// Fire AFTER ROW triggers for an INSERT operation.
///
/// Called after a successful INSERT dispatch. `new_fields` contains the
/// inserted row's field values. The trigger body's DML is dispatched through
/// the normal plan+SPSC path, executing in the same logical transaction context.
///
/// `mode_filter` selects which execution mode to fire:
/// - `Some(Sync)`: only fire SYNC triggers (called from write path)
/// - `Some(Async)`: only fire ASYNC triggers (called from Event Plane)
/// - `None`: fire all AFTER triggers regardless of mode (legacy behavior)
pub async fn fire_after_insert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    new_fields: &HashMap<String, nodedb_types::Value>,
    cascade_depth: u32,
    mode_filter: Option<TriggerExecutionMode>,
) -> crate::Result<()> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Insert);

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::after_insert(collection, new_fields.clone());

    fire_triggers(
        state,
        identity,
        tenant_id,
        collection,
        &after_triggers,
        &bindings,
        cascade_depth,
    )
    .await
}

/// Fire AFTER ROW triggers for an UPDATE operation.
///
/// `old_fields` is the row before the update, `new_fields` is after.
/// Both are available as OLD.field and NEW.field in the trigger body.
#[allow(clippy::too_many_arguments)]
pub async fn fire_after_update(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
    new_fields: &HashMap<String, nodedb_types::Value>,
    cascade_depth: u32,
    mode_filter: Option<TriggerExecutionMode>,
) -> crate::Result<()> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Update);

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::after_update(collection, old_fields.clone(), new_fields.clone());

    fire_triggers(
        state,
        identity,
        tenant_id,
        collection,
        &after_triggers,
        &bindings,
        cascade_depth,
    )
    .await
}

/// Fire AFTER ROW triggers for a DELETE operation.
///
/// `old_fields` is the deleted row. Available as OLD.field in the trigger body.
pub async fn fire_after_delete(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
    cascade_depth: u32,
    mode_filter: Option<TriggerExecutionMode>,
) -> crate::Result<()> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Delete);

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::after_delete(collection, old_fields.clone());

    fire_triggers(
        state,
        identity,
        tenant_id,
        collection,
        &after_triggers,
        &bindings,
        cascade_depth,
    )
    .await
}

/// Execute raw SQL in a trigger-like context (no row bindings).
///
/// Used by the cross-shard receiver to execute trigger-originated DML
/// on the target node. The SQL is parsed and executed through the normal
/// Control Plane → Data Plane path with cascade depth tracking.
pub async fn fire_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    sql: &str,
    cascade_depth: u32,
) -> crate::Result<()> {
    use crate::control::planner::procedural::executor::bindings::RowBindings;
    use crate::control::planner::procedural::executor::core::{
        MAX_CASCADE_DEPTH, StatementExecutor,
    };

    if cascade_depth >= MAX_CASCADE_DEPTH {
        return Err(crate::Error::BadRequest {
            detail: format!("cross-shard cascade depth exceeded ({MAX_CASCADE_DEPTH})"),
        });
    }

    let block = crate::control::planner::procedural::parse_block(sql).map_err(|e| {
        crate::Error::BadRequest {
            detail: format!("cross-shard SQL parse error: {e}"),
        }
    })?;

    let executor = StatementExecutor::with_source(
        state,
        identity.clone(),
        tenant_id,
        cascade_depth,
        crate::event::EventSource::Trigger,
    );
    let bindings = RowBindings::empty();

    executor
        .execute_block(&block, &bindings)
        .await
        .map_err(|e| crate::Error::BadRequest {
            detail: format!("cross-shard SQL execution failed: {e}"),
        })
}
