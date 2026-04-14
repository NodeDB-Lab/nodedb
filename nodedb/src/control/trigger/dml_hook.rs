//! DML trigger hook: intercepts write dispatches to fire BEFORE/AFTER/INSTEAD OF triggers.
//!
//! Sits between the Control Plane query router and the Data Plane dispatch.
//! For each DML write task:
//! 1. Classify the operation (INSERT/UPDATE/DELETE) and extract collection + doc ID
//! 2. Fetch OLD row data for UPDATE/DELETE (needed for OLD.* bindings)
//! 3. Fire INSTEAD OF triggers — if handled, skip normal dispatch
//! 4. Fire BEFORE triggers — may abort the DML via RAISE EXCEPTION
//! 5. Dispatch to Data Plane (normal write path)
//! 6. Fire SYNC AFTER triggers (same logical transaction)
//!
//! ASYNC AFTER triggers are handled by the Event Plane via WriteEvents — not here.

use std::collections::HashMap;

use sonic_rs;

use crate::bridge::physical_plan::DocumentOp;
use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::fire_after;
use super::fire_before;
use super::fire_instead::InsteadOfResult;
use super::fire_statement;
use super::registry::DmlEvent;

/// Classification of a DML write for trigger purposes.
#[derive(Debug)]
pub struct DmlWriteInfo {
    /// Collection name targeted by this write.
    pub collection: String,
    /// Document ID (for point operations). None for bulk operations.
    pub document_id: Option<String>,
    /// DML event type.
    pub event: DmlEvent,
    /// NEW row fields extracted from the write plan. None for DELETE.
    pub new_fields: Option<HashMap<String, nodedb_types::Value>>,
}

/// Attempt to classify a PhysicalPlan as a document DML write.
///
/// Returns `None` for non-write operations (reads, DDL, scans, etc.)
/// and for non-document engines (vector, graph, etc. — those emit WriteEvents
/// for ASYNC triggers but don't participate in the BEFORE/SYNC AFTER path).
pub fn classify_dml_write(plan: &crate::bridge::envelope::PhysicalPlan) -> Option<DmlWriteInfo> {
    match plan {
        crate::bridge::envelope::PhysicalPlan::Document(doc_op) => classify_document_op(doc_op),
        // KV, Vector, Graph, etc. writes emit WriteEvents for ASYNC triggers
        // but don't participate in BEFORE/SYNC AFTER trigger hooks.
        // Those engines handle triggers via Event Plane only.
        _ => None,
    }
}

fn classify_document_op(op: &DocumentOp) -> Option<DmlWriteInfo> {
    match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            value,
        } => {
            let new_fields = deserialize_value_to_fields(value);
            Some(DmlWriteInfo {
                collection: collection.clone(),
                document_id: Some(document_id.clone()),
                event: DmlEvent::Insert,
                new_fields: Some(new_fields),
            })
        }
        DocumentOp::Upsert {
            collection,
            document_id,
            value,
            ..
        } => {
            let new_fields = deserialize_value_to_fields(value);
            Some(DmlWriteInfo {
                collection: collection.clone(),
                document_id: Some(document_id.clone()),
                event: DmlEvent::Insert, // Upsert treated as INSERT for trigger purposes
                new_fields: Some(new_fields),
            })
        }
        DocumentOp::PointDelete {
            collection,
            document_id,
        } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: Some(document_id.clone()),
            event: DmlEvent::Delete,
            new_fields: None,
        }),
        DocumentOp::PointUpdate {
            collection,
            document_id,
            ..
        } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: Some(document_id.clone()),
            event: DmlEvent::Update,
            new_fields: None, // NEW fields computed after applying updates to OLD
        }),
        DocumentOp::BatchInsert { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Insert,
            new_fields: None, // Batch — individual rows not available here
        }),
        DocumentOp::BulkUpdate { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Update,
            new_fields: None,
        }),
        DocumentOp::BulkDelete { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Delete,
            new_fields: None,
        }),
        DocumentOp::Truncate { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Delete,
            new_fields: None,
        }),
        DocumentOp::InsertSelect {
            target_collection, ..
        } => Some(DmlWriteInfo {
            collection: target_collection.clone(),
            document_id: None,
            event: DmlEvent::Insert,
            new_fields: None,
        }),
        // Not a write operation.
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::EstimateCount { .. } => None,
    }
}

/// Deserialize a MessagePack/JSON value blob into a HashMap for trigger bindings.
fn deserialize_value_to_fields(value: &[u8]) -> HashMap<String, nodedb_types::Value> {
    // Try MessagePack first (primary format), fall back to JSON.
    if let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(value) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(value) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }
    HashMap::new()
}

/// Patch a `PhysicalTask` with mutated fields from a BEFORE trigger.
///
/// Serializes the mutated fields to MessagePack and replaces the value
/// payload in the underlying `PointPut` or `Upsert` operation.
/// For `PointUpdate`, the updates are re-derived from the mutated fields.
pub fn patch_task_with_mutated_fields(
    task: &mut crate::control::planner::physical::PhysicalTask,
    mutated: &HashMap<String, nodedb_types::Value>,
) {
    use crate::bridge::envelope::PhysicalPlan;

    let json_obj: serde_json::Map<String, serde_json::Value> = mutated
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
        .collect();
    let json_val = serde_json::Value::Object(json_obj);
    let new_bytes = match nodedb_types::value_to_msgpack(&nodedb_types::Value::from(json_val)) {
        Ok(b) => b,
        Err(_) => return,
    };

    match &mut task.plan {
        PhysicalPlan::Document(DocumentOp::PointPut { value, .. })
        | PhysicalPlan::Document(DocumentOp::Upsert { value, .. }) => {
            *value = new_bytes;
        }
        PhysicalPlan::Document(DocumentOp::PointUpdate { updates, .. }) => {
            // Re-derive field-level updates from the full mutated row. Trigger
            // mutations are fully-evaluated post-trigger values, so they ship
            // as `UpdateValue::Literal`.
            *updates = mutated
                .iter()
                .filter_map(|(k, v)| {
                    nodedb_types::value_to_msgpack(v).ok().map(|b| {
                        (
                            k.clone(),
                            crate::bridge::physical_plan::UpdateValue::Literal(b),
                        )
                    })
                })
                .collect();
        }
        _ => {}
    }
}

/// Fetch the current document as a field map (for OLD row bindings).
///
/// Issues a PointGet to the Data Plane and deserializes the response.
/// Returns an empty map if the document doesn't exist or can't be read.
pub async fn fetch_old_row(
    state: &SharedState,
    tenant_id: TenantId,
    collection: &str,
    document_id: &str,
) -> HashMap<String, nodedb_types::Value> {
    let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::PointGet {
        collection: collection.to_string(),
        document_id: document_id.to_string(),
        rls_filters: Vec::new(),
    });
    let vshard_id = crate::types::VShardId::from_key(document_id.as_bytes());

    let resp = match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard_id, plan, 0,
    )
    .await
    {
        Ok(r) => r,
        Err(_) => return HashMap::new(),
    };

    if resp.payload.is_empty() {
        return HashMap::new();
    }

    // Decode the response payload (MessagePack or JSON).
    let bytes = resp.payload.as_ref();
    if let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(bytes) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }

    HashMap::new()
}

/// Check if any triggers exist for this collection+event combination.
///
/// Quick check to avoid fetch_old_row and other overhead when no triggers are defined.
pub fn has_triggers(state: &SharedState, tenant_id: TenantId, collection: &str) -> bool {
    let tid = tenant_id.as_u32();
    !state
        .trigger_registry
        .get_matching(tid, collection, DmlEvent::Insert)
        .is_empty()
        || !state
            .trigger_registry
            .get_matching(tid, collection, DmlEvent::Update)
            .is_empty()
        || !state
            .trigger_registry
            .get_matching(tid, collection, DmlEvent::Delete)
            .is_empty()
}

/// Result of firing BEFORE + INSTEAD OF triggers before dispatch.
pub enum PreDispatchResult {
    /// INSTEAD OF trigger handled the write — skip normal dispatch.
    Handled,
    /// Proceed with dispatch. If a BEFORE trigger mutated the row,
    /// `mutated_fields` contains the new fields to use instead of the original.
    Proceed {
        mutated_fields: Option<HashMap<String, nodedb_types::Value>>,
    },
}

/// Fire BEFORE + INSTEAD OF triggers for a point write.
///
/// Returns `PreDispatchResult::Proceed` if the caller should dispatch normally.
/// The `mutated_fields` inside may contain fields modified by a BEFORE trigger —
/// the caller MUST use these to patch the task before dispatch.
///
/// Returns `PreDispatchResult::Handled` if an INSTEAD OF trigger handled the write.
///
/// On BEFORE trigger error (RAISE EXCEPTION), the error propagates and
/// the caller should abort the write.
#[allow(clippy::too_many_arguments)]
pub async fn fire_pre_dispatch_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    info: &DmlWriteInfo,
    old_row: &Option<HashMap<String, nodedb_types::Value>>,
    cascade_depth: u32,
) -> crate::Result<PreDispatchResult> {
    // Check INSTEAD OF first — if it handles the write, skip everything else.
    match info.event {
        DmlEvent::Insert => {
            if let Some(ref new_fields) = info.new_fields {
                match super::fire_instead::fire_instead_of_insert(
                    state,
                    identity,
                    tenant_id,
                    &info.collection,
                    new_fields,
                    cascade_depth,
                )
                .await?
                {
                    InsteadOfResult::Handled => return Ok(PreDispatchResult::Handled),
                    InsteadOfResult::NoTrigger => {}
                }
            }
        }
        DmlEvent::Update => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            let new_fields = info.new_fields.as_ref().unwrap_or(&empty);
            match super::fire_instead::fire_instead_of_update(
                state,
                identity,
                tenant_id,
                &info.collection,
                old_fields,
                new_fields,
                cascade_depth,
            )
            .await?
            {
                InsteadOfResult::Handled => return Ok(PreDispatchResult::Handled),
                InsteadOfResult::NoTrigger => {}
            }
        }
        DmlEvent::Delete => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            match super::fire_instead::fire_instead_of_delete(
                state,
                identity,
                tenant_id,
                &info.collection,
                old_fields,
                cascade_depth,
            )
            .await?
            {
                InsteadOfResult::Handled => return Ok(PreDispatchResult::Handled),
                InsteadOfResult::NoTrigger => {}
            }
        }
    }

    // Fire BEFORE triggers — capture mutated fields from INSERT/UPDATE.
    let mutated_fields = match info.event {
        DmlEvent::Insert => {
            if let Some(ref new_fields) = info.new_fields {
                let mutated = fire_before::fire_before_insert(
                    state,
                    identity,
                    tenant_id,
                    &info.collection,
                    new_fields,
                    cascade_depth,
                )
                .await?;
                if mutated != *new_fields {
                    Some(mutated)
                } else {
                    None
                }
            } else {
                None
            }
        }
        DmlEvent::Update => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            let new_fields = info.new_fields.as_ref().unwrap_or(&empty);
            let mutated = fire_before::fire_before_update(
                state,
                identity,
                tenant_id,
                &info.collection,
                old_fields,
                new_fields,
                cascade_depth,
            )
            .await?;
            if mutated != *new_fields {
                Some(mutated)
            } else {
                None
            }
        }
        DmlEvent::Delete => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            fire_before::fire_before_delete(
                state,
                identity,
                tenant_id,
                &info.collection,
                old_fields,
                cascade_depth,
            )
            .await?;
            None
        }
    };

    Ok(PreDispatchResult::Proceed { mutated_fields })
}

/// Fire SYNC AFTER ROW + SYNC AFTER STATEMENT triggers post-dispatch.
///
/// Called after the Data Plane has committed the write. Only fires triggers
/// with `execution_mode = Sync`. ASYNC triggers are handled by the Event Plane.
#[allow(clippy::too_many_arguments)]
pub async fn fire_post_dispatch_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    info: &DmlWriteInfo,
    old_row: &Option<HashMap<String, nodedb_types::Value>>,
    cascade_depth: u32,
) -> crate::Result<()> {
    let empty = HashMap::new();

    // Fire SYNC AFTER ROW triggers.
    match info.event {
        DmlEvent::Insert => {
            if let Some(ref new_fields) = info.new_fields {
                fire_after::fire_after_insert(
                    state,
                    identity,
                    tenant_id,
                    &info.collection,
                    new_fields,
                    cascade_depth,
                    Some(TriggerExecutionMode::Sync),
                )
                .await?;
            }
        }
        DmlEvent::Update => {
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            let new_fields = info.new_fields.as_ref().unwrap_or(&empty);
            fire_after::fire_after_update(
                state,
                identity,
                tenant_id,
                &info.collection,
                old_fields,
                new_fields,
                cascade_depth,
                Some(TriggerExecutionMode::Sync),
            )
            .await?;
        }
        DmlEvent::Delete => {
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            fire_after::fire_after_delete(
                state,
                identity,
                tenant_id,
                &info.collection,
                old_fields,
                cascade_depth,
                Some(TriggerExecutionMode::Sync),
            )
            .await?;
        }
    }

    // Fire SYNC AFTER STATEMENT triggers (once per DML statement, not per row).
    fire_statement::fire_after_statement(
        state,
        identity,
        tenant_id,
        &info.collection,
        info.event,
        cascade_depth,
        Some(TriggerExecutionMode::Sync),
    )
    .await?;

    Ok(())
}
