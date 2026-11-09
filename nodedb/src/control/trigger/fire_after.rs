// SPDX-License-Identifier: BUSL-1.1

//! AFTER trigger firing logic.
//!
//! Called after DML operations to fire matching AFTER ROW triggers.
//! Matches triggers by (collection, event), evaluates WHEN clauses,
//! and invokes the statement executor for each matching trigger body.
//!
//! Supports three execution modes via `mode_filter`:
//! - `Some(Sync)`: fire in the Control Plane write path
//! - `Some(Async)`: fire from Event Plane (eventually consistent)
//! - `Some(Deferred)`: fire at COMMIT time, batched
//! - `None`: fire all AFTER triggers regardless of mode
//!
//! A SYNC body joins the transaction of the write that fired it (`joined`):
//! the write and the body commit together or not at all. An ASYNC or
//! DEFERRED body runs in its own transaction: its writes commit together when
//! it succeeds and none apply when it fails.

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::CrossShardOrigin;
use crate::control::security::catalog::trigger_types::{
    TriggerExecutionMode, TriggerGranularity, TriggerTiming,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use std::collections::HashMap;

use super::fire_common::{
    FireErrorPolicy, FireReport, FireTriggersParams, check_cascade_depth, fire_triggers,
};
use super::registry::DmlEvent;

/// Parameters for [`fire_after_insert`].
pub struct FireAfterInsertParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// Inserted row fields (bound as `NEW.*`).
    pub new_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Cross-shard origin context (Event-Plane fire path only).
    pub cross_shard_origin: Option<CrossShardOrigin>,
    /// What a failing trigger does to the triggers queued behind it.
    pub on_error: FireErrorPolicy,
    /// Restricts firing to the one named trigger; `None` fires every match.
    ///
    /// A retry sets this. Without it a retry re-fires every trigger matching
    /// the operation, re-running the siblings that already succeeded.
    pub only_trigger: Option<&'a str>,
    /// The triggering statement's transaction, which a SYNC body joins.
    /// `None` on the Event Plane: each body runs its own transaction.
    pub joined: Option<&'a DmlTxnCtx<'a>>,
}

/// Fire AFTER ROW triggers for an INSERT operation.
///
/// Called after a successful INSERT dispatch. `new_fields` contains the
/// inserted row's field values. The trigger body's DML is planned through
/// the normal path and commits as the body's own transaction.
///
/// `mode_filter` selects which execution mode to fire:
/// - `Some(Sync)`: only fire SYNC triggers (called from write path)
/// - `Some(Async)`: only fire ASYNC triggers (called from Event Plane)
/// - `None`: fire all AFTER triggers regardless of mode
pub async fn fire_after_insert(params: FireAfterInsertParams<'_>) -> FireReport {
    let FireAfterInsertParams {
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        new_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
        on_error,
        only_trigger,
        joined,
    } = params;

    let triggers = state.trigger_registry.get_matching(
        database_id,
        tenant_id.as_u64(),
        collection,
        DmlEvent::Insert,
    );

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .filter(|t| only_trigger.is_none_or(|name| t.name == name))
        .collect();

    if after_triggers.is_empty() {
        return FireReport::default();
    }

    if let Err(error) = check_cascade_depth(cascade_depth, collection) {
        return FireReport::from_precondition(error);
    }

    let bindings = RowBindings::after_insert(collection, new_fields.clone());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id,
        collection,
        triggers: &after_triggers,
        bindings: &bindings,
        cascade_depth,
        cross_shard_origin,
        on_error,
        joined,
    })
    .await
}

/// Parameters for [`fire_after_update`].
pub struct FireAfterUpdateParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// Row fields before the update (bound as `OLD.*`).
    pub old_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Row fields after the update (bound as `NEW.*`).
    pub new_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Cross-shard origin context (Event-Plane fire path only).
    pub cross_shard_origin: Option<CrossShardOrigin>,
    /// What a failing trigger does to the triggers queued behind it.
    pub on_error: FireErrorPolicy,
    /// Restricts firing to the one named trigger; `None` fires every match.
    ///
    /// A retry sets this. Without it a retry re-fires every trigger matching
    /// the operation, re-running the siblings that already succeeded.
    pub only_trigger: Option<&'a str>,
    /// The triggering statement's transaction, which a SYNC body joins.
    /// `None` on the Event Plane: each body runs its own transaction.
    pub joined: Option<&'a DmlTxnCtx<'a>>,
}

/// Fire AFTER ROW triggers for an UPDATE operation.
///
/// `old_fields` is the row before the update, `new_fields` is after.
/// Both are available as OLD.field and NEW.field in the trigger body.
pub async fn fire_after_update(params: FireAfterUpdateParams<'_>) -> FireReport {
    let FireAfterUpdateParams {
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        old_fields,
        new_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
        on_error,
        only_trigger,
        joined,
    } = params;

    let triggers = state.trigger_registry.get_matching(
        database_id,
        tenant_id.as_u64(),
        collection,
        DmlEvent::Update,
    );

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .filter(|t| only_trigger.is_none_or(|name| t.name == name))
        .collect();

    if after_triggers.is_empty() {
        return FireReport::default();
    }

    if let Err(error) = check_cascade_depth(cascade_depth, collection) {
        return FireReport::from_precondition(error);
    }

    let bindings = RowBindings::after_update(collection, old_fields.clone(), new_fields.clone());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id,
        collection,
        triggers: &after_triggers,
        bindings: &bindings,
        cascade_depth,
        cross_shard_origin,
        on_error,
        joined,
    })
    .await
}

/// Parameters for [`fire_after_delete`].
pub struct FireAfterDeleteParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// Deleted row fields (bound as `OLD.*`).
    pub old_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Cross-shard origin context (Event-Plane fire path only).
    pub cross_shard_origin: Option<CrossShardOrigin>,
    /// What a failing trigger does to the triggers queued behind it.
    pub on_error: FireErrorPolicy,
    /// Restricts firing to the one named trigger; `None` fires every match.
    ///
    /// A retry sets this. Without it a retry re-fires every trigger matching
    /// the operation, re-running the siblings that already succeeded.
    pub only_trigger: Option<&'a str>,
    /// The triggering statement's transaction, which a SYNC body joins.
    /// `None` on the Event Plane: each body runs its own transaction.
    pub joined: Option<&'a DmlTxnCtx<'a>>,
}

/// Fire AFTER ROW triggers for a DELETE operation.
///
/// `old_fields` is the deleted row. Available as OLD.field in the trigger body.
pub async fn fire_after_delete(params: FireAfterDeleteParams<'_>) -> FireReport {
    let FireAfterDeleteParams {
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        old_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
        on_error,
        only_trigger,
        joined,
    } = params;

    let triggers = state.trigger_registry.get_matching(
        database_id,
        tenant_id.as_u64(),
        collection,
        DmlEvent::Delete,
    );

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .filter(|t| only_trigger.is_none_or(|name| t.name == name))
        .collect();

    if after_triggers.is_empty() {
        return FireReport::default();
    }

    if let Err(error) = check_cascade_depth(cascade_depth, collection) {
        return FireReport::from_precondition(error);
    }

    let bindings = RowBindings::after_delete(collection, old_fields.clone());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id,
        collection,
        triggers: &after_triggers,
        bindings: &bindings,
        cascade_depth,
        cross_shard_origin,
        on_error,
        joined,
    })
    .await
}

/// One block of trigger DML shipped from another node, applied here.
pub struct ShippedBlock<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// One procedural block.
    pub sql: &'a str,
    pub cascade_depth: u32,
    /// The vShard the request addresses. The block's writes and every write
    /// they derive commit in one transaction, whichever vShards they span.
    /// The request's key rides this vShard's redo record.
    pub target_vshard: u32,
    /// The request's dedup key, recorded by the commit's redo record.
    pub applied_key: crate::wal::CrossShardAppliedKey,
}

/// Execute raw SQL in a trigger-like context (no row bindings).
///
/// Used by the cross-shard receiver to execute trigger-originated DML
/// on the target node. The block's statements commit as one transaction
/// whose redo record carries its applied key, with cascade depth tracking.
pub async fn fire_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    shipped: ShippedBlock<'_>,
) -> crate::Result<()> {
    use crate::control::planner::procedural::executor::bindings::RowBindings;
    use crate::control::planner::procedural::executor::core::{
        AtomicBody, MAX_CASCADE_DEPTH, StatementExecutor,
    };

    let ShippedBlock {
        tenant_id,
        database_id,
        sql,
        cascade_depth,
        target_vshard,
        applied_key,
    } = shipped;
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

    let executor = StatementExecutor::with_source_in_database(
        state,
        identity.clone(),
        tenant_id,
        database_id,
        cascade_depth,
        crate::event::EventSource::Trigger,
    )
    .with_atomic_body(AtomicBody::CrossShardApply)
    .with_applied_key(applied_key, target_vshard);
    let bindings = RowBindings::empty();

    // The block has no post-commit effects: it refuses PUBLISH, and with no
    // cross-shard origin every statement stages here.
    executor
        .execute_block(&block, &bindings)
        .await
        .map_err(|e| crate::Error::BadRequest {
            detail: format!("cross-shard SQL execution failed: {e}"),
        })
}
