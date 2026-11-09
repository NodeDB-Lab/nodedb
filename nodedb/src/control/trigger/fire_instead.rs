// SPDX-License-Identifier: BUSL-1.1

//! INSTEAD OF trigger firing logic.
//!
//! INSTEAD OF triggers replace the DML operation entirely. When an INSTEAD OF
//! trigger exists for a collection+event, the original DML is NOT dispatched
//! to the Data Plane. Instead, the trigger body executes and is responsible
//! for performing whatever writes are needed.
//!
//! Primary use case: updatable views and custom write routing.
//!
//! INSTEAD OF triggers are always synchronous (no ASYNC/DEFERRED variants).
//! Every body joins the triggering statement's transaction.

use std::collections::HashMap;

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerGranularity, TriggerTiming,
};

use super::SyncFire;
use super::fire_common::{FireErrorPolicy, FireTriggersParams, check_cascade_depth, fire_triggers};
use super::registry::DmlEvent;

/// Result of checking for INSTEAD OF triggers.
pub enum InsteadOfResult {
    /// No INSTEAD OF trigger exists — proceed with normal DML dispatch.
    NoTrigger,
    /// An INSTEAD OF trigger fired and handled the DML.
    /// The caller MUST NOT dispatch the original DML to the Data Plane.
    Handled,
}

/// Check for and fire INSTEAD OF triggers for an INSERT operation.
///
/// Returns `InsteadOfResult::Handled` if an INSTEAD OF trigger fired
/// (caller must skip normal dispatch). Returns `NoTrigger` otherwise.
pub async fn fire_instead_of_insert(
    fire: SyncFire<'_>,
    collection: &str,
    new_fields: &HashMap<String, nodedb_types::Value>,
) -> crate::Result<InsteadOfResult> {
    let bindings = || RowBindings::before_insert(collection, new_fields.clone());
    fire_instead_of(fire, collection, DmlEvent::Insert, bindings).await
}

/// Check for and fire INSTEAD OF triggers for an UPDATE operation.
pub async fn fire_instead_of_update(
    fire: SyncFire<'_>,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
    new_fields: &HashMap<String, nodedb_types::Value>,
) -> crate::Result<InsteadOfResult> {
    let bindings =
        || RowBindings::before_update(collection, old_fields.clone(), new_fields.clone());
    fire_instead_of(fire, collection, DmlEvent::Update, bindings).await
}

/// Check for and fire INSTEAD OF triggers for a DELETE operation.
pub async fn fire_instead_of_delete(
    fire: SyncFire<'_>,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
) -> crate::Result<InsteadOfResult> {
    let bindings = || RowBindings::before_delete(collection, old_fields.clone());
    fire_instead_of(fire, collection, DmlEvent::Delete, bindings).await
}

/// Fire the INSTEAD OF triggers matching `event` on `collection`, with the
/// bindings `bindings` builds once a trigger matches.
async fn fire_instead_of(
    fire: SyncFire<'_>,
    collection: &str,
    event: DmlEvent,
    bindings: impl FnOnce() -> RowBindings,
) -> crate::Result<InsteadOfResult> {
    let SyncFire {
        state,
        identity,
        scope,
        cascade_depth,
        txn,
    } = fire;
    let instead_triggers: Vec<StoredTrigger> = state
        .trigger_registry
        .get_matching(
            scope.database_id,
            scope.tenant_id.as_u64(),
            collection,
            event,
        )
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::InsteadOf)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .collect();

    if instead_triggers.is_empty() {
        return Ok(InsteadOfResult::NoTrigger);
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = bindings();
    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id: scope.tenant_id,
        collection,
        triggers: &instead_triggers,
        bindings: &bindings,
        cascade_depth,
        // INSTEAD OF triggers replace the base DML in the caller's context;
        // they are not part of the Event-Plane async cross-shard sender path.
        cross_shard_origin: None,
        on_error: FireErrorPolicy::Abort,
        joined: Some(txn),
    })
    .await
    .into_result()?;

    Ok(InsteadOfResult::Handled)
}
