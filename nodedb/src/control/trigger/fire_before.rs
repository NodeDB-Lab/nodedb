// SPDX-License-Identifier: BUSL-1.1

//! BEFORE trigger firing logic.
//!
//! BEFORE triggers fire synchronously in the Control Plane BEFORE the row
//! mutation is dispatched to the Data Plane. They can:
//! - Validate and reject the DML via RAISE EXCEPTION
//! - Modify the NEW row (for INSERT/UPDATE) before it reaches storage
//! - Execute side-effect DML (dispatched through normal plan+SPSC path)
//!
//! BEFORE triggers are ALWAYS synchronous — there is no ASYNC or DEFERRED
//! variant. Every body joins the triggering statement's transaction.

use std::collections::HashMap;

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::security::catalog::trigger_types::{TriggerGranularity, TriggerTiming};

use super::SyncFire;
use super::fire_common::{
    BeforeTriggersMutationParams, FireErrorPolicy, FireTriggersParams, check_cascade_depth,
    fire_before_triggers_with_mutation, fire_triggers,
};
use super::registry::DmlEvent;

/// Fire BEFORE ROW triggers for an INSERT operation.
///
/// Returns the (possibly modified) NEW fields. The caller MUST use the returned
/// fields for the actual PointPut dispatch, not the original input — a BEFORE
/// trigger can have normalized or enriched the row.
///
/// If a trigger raises an exception, the error propagates and the INSERT is aborted.
pub async fn fire_before_insert(
    fire: SyncFire<'_>,
    collection: &str,
    new_fields: &HashMap<String, nodedb_types::Value>,
) -> crate::Result<HashMap<String, nodedb_types::Value>> {
    let SyncFire {
        state,
        identity,
        scope,
        cascade_depth,
        txn,
    } = fire;
    let triggers = state.trigger_registry.get_matching(
        scope.database_id,
        scope.tenant_id.as_u64(),
        collection,
        DmlEvent::Insert,
    );

    let before_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .collect();

    if before_triggers.is_empty() {
        return Ok(new_fields.clone());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_insert(collection, new_fields.clone());

    let result = fire_before_triggers_with_mutation(BeforeTriggersMutationParams {
        state,
        identity,
        tenant_id: scope.tenant_id,
        collection,
        triggers: &before_triggers,
        bindings: &bindings,
        cascade_depth,
        new_fields: Some(new_fields.clone()),
        joined: txn,
    })
    .await?;

    // Return the (possibly mutated) NEW fields. If None somehow, return original.
    Ok(result.unwrap_or_else(|| new_fields.clone()))
}

/// Fire BEFORE ROW triggers for an UPDATE operation.
///
/// Returns the (possibly modified) NEW fields. `old_fields` is the row before
/// the update (read-only in the trigger body as OLD.*).
///
/// If a trigger raises an exception, the UPDATE is aborted.
pub async fn fire_before_update(
    fire: SyncFire<'_>,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
    new_fields: &HashMap<String, nodedb_types::Value>,
) -> crate::Result<HashMap<String, nodedb_types::Value>> {
    let SyncFire {
        state,
        identity,
        scope,
        cascade_depth,
        txn,
    } = fire;
    let triggers = state.trigger_registry.get_matching(
        scope.database_id,
        scope.tenant_id.as_u64(),
        collection,
        DmlEvent::Update,
    );

    let before_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .collect();

    if before_triggers.is_empty() {
        return Ok(new_fields.clone());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_update(collection, old_fields.clone(), new_fields.clone());

    let result = fire_before_triggers_with_mutation(BeforeTriggersMutationParams {
        state,
        identity,
        tenant_id: scope.tenant_id,
        collection,
        triggers: &before_triggers,
        bindings: &bindings,
        cascade_depth,
        new_fields: Some(new_fields.clone()),
        joined: txn,
    })
    .await?;

    Ok(result.unwrap_or_else(|| new_fields.clone()))
}

/// Fire BEFORE ROW triggers for a DELETE operation.
///
/// BEFORE DELETE triggers cannot modify the row (there is no NEW). They can
/// only validate and reject via RAISE EXCEPTION, or execute side-effect DML.
///
/// If a trigger raises an exception, the DELETE is aborted.
pub async fn fire_before_delete(
    fire: SyncFire<'_>,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
) -> crate::Result<()> {
    let SyncFire {
        state,
        identity,
        scope,
        cascade_depth,
        txn,
    } = fire;
    let triggers = state.trigger_registry.get_matching(
        scope.database_id,
        scope.tenant_id.as_u64(),
        collection,
        DmlEvent::Delete,
    );

    let before_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .collect();

    if before_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_delete(collection, old_fields.clone());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id: scope.tenant_id,
        collection,
        triggers: &before_triggers,
        bindings: &bindings,
        cascade_depth,
        // BEFORE triggers run in the caller's write context, not the
        // Event-Plane async cross-shard sender path.
        cross_shard_origin: None,
        on_error: FireErrorPolicy::Abort,
        joined: Some(txn),
    })
    .await
    .into_result()
}
