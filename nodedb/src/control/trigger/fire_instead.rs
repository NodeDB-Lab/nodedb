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

use std::collections::HashMap;

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::security::catalog::trigger_types::TriggerTiming;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::fire_common::{check_cascade_depth, fire_triggers};
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
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    new_fields: &HashMap<String, nodedb_types::Value>,
    cascade_depth: u32,
) -> crate::Result<InsteadOfResult> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Insert);

    let instead_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::InsteadOf)
        .collect();

    if instead_triggers.is_empty() {
        return Ok(InsteadOfResult::NoTrigger);
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_insert(collection, new_fields.clone());

    fire_triggers(
        state,
        identity,
        tenant_id,
        collection,
        &instead_triggers,
        &bindings,
        cascade_depth,
    )
    .await?;

    Ok(InsteadOfResult::Handled)
}

/// Check for and fire INSTEAD OF triggers for an UPDATE operation.
#[allow(clippy::too_many_arguments)]
pub async fn fire_instead_of_update(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
    new_fields: &HashMap<String, nodedb_types::Value>,
    cascade_depth: u32,
) -> crate::Result<InsteadOfResult> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Update);

    let instead_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::InsteadOf)
        .collect();

    if instead_triggers.is_empty() {
        return Ok(InsteadOfResult::NoTrigger);
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_update(collection, old_fields.clone(), new_fields.clone());

    fire_triggers(
        state,
        identity,
        tenant_id,
        collection,
        &instead_triggers,
        &bindings,
        cascade_depth,
    )
    .await?;

    Ok(InsteadOfResult::Handled)
}

/// Check for and fire INSTEAD OF triggers for a DELETE operation.
pub async fn fire_instead_of_delete(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    old_fields: &HashMap<String, nodedb_types::Value>,
    cascade_depth: u32,
) -> crate::Result<InsteadOfResult> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Delete);

    let instead_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::InsteadOf)
        .collect();

    if instead_triggers.is_empty() {
        return Ok(InsteadOfResult::NoTrigger);
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_delete(collection, old_fields.clone());

    fire_triggers(
        state,
        identity,
        tenant_id,
        collection,
        &instead_triggers,
        &bindings,
        cascade_depth,
    )
    .await?;

    Ok(InsteadOfResult::Handled)
}
