// SPDX-License-Identifier: BUSL-1.1

//! DML trigger hook: intercepts write dispatches to fire BEFORE/AFTER/INSTEAD OF triggers.
//!
//! Sits between the Control Plane router and Data Plane dispatch: classify the
//! op, fetch OLD row data, fire INSTEAD OF/BEFORE/SYNC AFTER triggers. ASYNC
//! AFTER triggers run on the Event Plane via WriteEvents, not here.

use std::collections::HashMap;

use sonic_rs;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::registry::DmlEvent;

/// Classification of a DML write for trigger purposes.
#[derive(Debug)]
pub struct DmlWriteInfo {
    /// Collection name targeted by this write.
    pub collection: String,
    /// Document ID (for point operations). None for bulk operations.
    pub document_id: Option<String>,
    /// DML event type. For UPSERT this is a best guess; routing overrides it
    /// after probing the pre-write row via `fetch_old_row`.
    pub event: DmlEvent,
    /// NEW row fields extracted from the write plan. None for DELETE.
    pub new_fields: Option<HashMap<String, nodedb_types::Value>>,
    /// True when the real event type depends on row existence (UPSERT / INSERT
    /// ... ON CONFLICT), forcing a pre-dispatch existence probe.
    pub needs_existence_probe: bool,
}

/// Attempt to classify a PhysicalPlan as a document DML write. `None` for
/// non-write ops and non-document engines (those use WriteEvents only).
pub fn classify_dml_write(plan: &crate::bridge::envelope::PhysicalPlan) -> Option<DmlWriteInfo> {
    match plan {
        crate::bridge::envelope::PhysicalPlan::Document(doc_op) => classify_document_op(doc_op),
        // KV/Vector/Graph writes emit WriteEvents for ASYNC triggers only.
        _ => None,
    }
}

fn classify_document_op(op: &DocumentOp) -> Option<DmlWriteInfo> {
    match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            ..
        }
        | DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            ..
        } => {
            let new_fields = deserialize_value_to_fields(value);
            Some(DmlWriteInfo {
                collection: collection.to_string(),
                document_id: Some(document_id.clone()),
                event: DmlEvent::Insert,
                new_fields: Some(new_fields),
                needs_existence_probe: false,
            })
        }
        DocumentOp::Upsert {
            collection,
            document_id,
            value,
            ..
        } => {
            // Depends on whether the PK already exists; `event` starts at Insert
            // as a harmless default, the probe result overrides it.
            let new_fields = deserialize_value_to_fields(value);
            Some(DmlWriteInfo {
                collection: collection.to_string(),
                document_id: Some(document_id.clone()),
                event: DmlEvent::Insert,
                new_fields: Some(new_fields),
                needs_existence_probe: true,
            })
        }
        DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        } => Some(DmlWriteInfo {
            collection: collection.to_string(),
            document_id: Some(document_id.clone()),
            event: DmlEvent::Delete,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::PointUpdate {
            collection,
            document_id,
            ..
        } => Some(DmlWriteInfo {
            collection: collection.to_string(),
            document_id: Some(document_id.clone()),
            event: DmlEvent::Update,
            new_fields: None, // NEW fields computed after applying updates to OLD
            needs_existence_probe: false,
        }),
        DocumentOp::BatchInsert { collection, .. } => Some(DmlWriteInfo {
            collection: collection.to_string(),
            document_id: None,
            event: DmlEvent::Insert,
            new_fields: None, // Batch — individual rows not available here
            needs_existence_probe: false,
        }),
        // Each row inserts or updates by whether its key is stored, so the
        // page names both events.
        DocumentOp::BatchUpsert { collection, .. } => Some(DmlWriteInfo {
            collection: collection.to_string(),
            document_id: None,
            event: DmlEvent::Insert,
            new_fields: None,
            needs_existence_probe: true,
        }),
        DocumentOp::BulkUpdate { collection, .. } => Some(DmlWriteInfo {
            collection: collection.to_string(),
            document_id: None,
            event: DmlEvent::Update,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::BulkDelete { collection, .. } => Some(DmlWriteInfo {
            collection: collection.to_string(),
            document_id: None,
            event: DmlEvent::Delete,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::Truncate { collection, .. } => Some(DmlWriteInfo {
            collection: collection.to_string(),
            document_id: None,
            event: DmlEvent::Delete,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::InsertSelect {
            target_collection, ..
        } => Some(DmlWriteInfo {
            collection: target_collection.to_string(),
            document_id: None,
            event: DmlEvent::Insert,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::UpdateFromJoin {
            target_collection, ..
        } => Some(DmlWriteInfo {
            collection: target_collection.to_string(),
            document_id: None,
            event: DmlEvent::Update,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::Merge {
            target_collection, ..
        } => Some(DmlWriteInfo {
            collection: target_collection.to_string(),
            document_id: None,
            event: DmlEvent::Update,
            new_fields: None,
            needs_existence_probe: false,
        }),
        // Not a write operation.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        // Spans N rows across a resolved mutation list this single-tuple shape can't name;
        // trigger intent was already reported by the intercepted statement.
        | DocumentOp::ResolvedWrite { .. }
        // A derived balance write carries no user DML intent — the causing
        // statement already fired its own triggers on the source row.
        | DocumentOp::ApplyBalanceDelta { .. } => None,
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

/// Patch a `PhysicalTask` with the NEW row a BEFORE trigger left.
///
/// A `PointPut`, `PointInsert` or `Upsert` carries the whole row, so it takes
/// `mutated` whole. A `PointUpdate`, and an `Upsert` with `ON CONFLICT DO
/// UPDATE` assignments, keeps its own assignments and gains a literal
/// assignment for each field the trigger changed: `before` is the NEW row the
/// trigger bound, so a field equal in both keeps the statement's own
/// assignment.
pub fn patch_task_with_mutated_fields(
    task: &mut nodedb_physical::physical_task::PhysicalTask,
    before: &HashMap<String, nodedb_types::Value>,
    mutated: &HashMap<String, nodedb_types::Value>,
) -> crate::Result<()> {
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::UpdateValue;

    let encode = |value: &nodedb_types::Value| {
        nodedb_types::value_to_msgpack(value).map_err(|error| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("BEFORE trigger NEW row: {error}"),
        })
    };
    // Each field the trigger changed, as a literal assignment: a trigger's NEW
    // field is a fully evaluated value.
    let assign_changed = |updates: &mut Vec<(String, UpdateValue)>| -> crate::Result<()> {
        for (field, value) in mutated {
            if before.get(field) == Some(value) {
                continue;
            }
            let literal = UpdateValue::Literal(encode(value)?);
            match updates.iter_mut().find(|(name, _)| name == field) {
                Some((_, rhs)) => *rhs = literal,
                None => updates.push((field.clone(), literal)),
            }
        }
        Ok(())
    };
    match &mut task.plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { value, .. } | DocumentOp::PointInsert { value, .. },
        ) => {
            *value = encode(&nodedb_types::Value::Object(mutated.clone()))?;
        }
        PhysicalPlan::Document(DocumentOp::Upsert {
            value,
            on_conflict_updates,
            ..
        }) => {
            *value = encode(&nodedb_types::Value::Object(mutated.clone()))?;
            if !on_conflict_updates.is_empty() {
                assign_changed(on_conflict_updates)?;
            }
        }
        PhysicalPlan::Document(DocumentOp::PointUpdate { updates, .. }) => {
            assign_changed(updates)?;
        }
        _ => {}
    }
    Ok(())
}

/// The NEW row a `PointUpdate` writes: `old` with each assignment applied.
/// A literal assignment decodes its value. An expression evaluates against
/// `old`, as the Data Plane evaluates it against the stored row.
pub fn update_new_row(
    old: &HashMap<String, nodedb_types::Value>,
    updates: &[(String, nodedb_physical::physical_plan::UpdateValue)],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<HashMap<String, nodedb_types::Value>> {
    use nodedb_physical::physical_plan::UpdateValue;

    let old_row = nodedb_types::Value::Object(old.clone());
    let mut new = old.clone();
    for (field, rhs) in updates {
        let value = match rhs {
            UpdateValue::Literal(bytes) => {
                nodedb_types::value_from_msgpack(bytes).map_err(|error| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("UPDATE assignment to '{field}': {error}"),
                    }
                })?
            }
            UpdateValue::Expr(expr) => expr.eval(&old_row, crs).map_err(crate::Error::from)?,
        };
        new.insert(field.clone(), value);
    }
    Ok(new)
}

/// Fetch the current document as a field map (for OLD row bindings).
/// Authorizes `READ` before touching catalog state, injects RLS. An empty map
/// means the row is absent; other failures propagate.
///
/// `collection` is typed [`nodedb_types::QualifiedCollection`] so each caller
/// states, and the compiler checks, whether it holds a bare or a qualified
/// name — a bare `&str` let a caller pass either silently.
///
/// `txn_id` names the statement's transaction: the read sees its staged
/// writes, so a trigger binds the row as the transaction left it.
pub async fn fetch_old_row(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    auth: &AuthContext,
    collection: &nodedb_types::QualifiedCollection,
    document_id: &str,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<HashMap<String, nodedb_types::Value>> {
    let tenant_id = identity.tenant_id;
    if auth.tenant_id != tenant_id || auth.database_id != Some(database_id) {
        return Err(crate::Error::RejectedAuthz {
            tenant_id,
            resource: "OLD-row fetch auth context is not aligned to the selected database"
                .to_owned(),
        });
    }

    // Catalog, permission, surrogate and vShard lookups use the canonical
    // key: the bare name plus a separate `database_id`.
    let key =
        nodedb_types::CollectionKey::from_qualified(database_id, collection).map_err(|error| {
            crate::Error::RejectedAuthz {
                tenant_id,
                resource: format!("OLD-row fetch: {error}"),
            }
        })?;
    let bare_collection = key.name();

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        bare_collection,
        Permission::Read,
        &state.permissions,
        &state.roles,
        &audit,
    )?;

    let pk_bytes = document_id.as_bytes().to_vec();
    let bound = match state
        .surrogate_assigner
        .lookup_bound(key, tenant_id, &pk_bytes)?
    {
        Some(surrogate) => Some(surrogate),
        None => {
            crate::control::server::surrogate_exchange::lookup_surrogate_routed(
                state,
                key,
                tenant_id,
                &pk_bytes,
                crate::types::TraceId::ZERO,
            )
            .await?
        }
    };
    let Some(surrogate) = bound else {
        return Ok(HashMap::new());
    };
    let mut plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::PointGet {
        collection: collection.clone(),
        document_id: document_id.to_string(),
        surrogate: Some(surrogate),
        pk_bytes,
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        database_id,
        &mut plan,
        &state.rls,
        state.credentials.catalog(),
        auth,
    )?;
    crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        database_id,
        auth,
        &state.redaction,
    )?;

    let task = PhysicalTask {
        tenant_id,
        database_id,
        // A document is homed by its collection, never by its own key.
        vshard_id: key.vshard(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id,
    };
    let resp = crate::control::server::shared::clone_write::intercept_authorize_and_dispatch(
        crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
            state,
            task,
            identity,
            tenant_id,
            permissions: &state.permissions,
            roles: &state.roles,
            emitter: &audit,
        },
        TraceId::ZERO,
    )
    .await?;

    if resp.payload.is_empty() {
        return Ok(HashMap::new());
    }

    // A non-object payload is a transport failure, not evidence the row is absent.
    let bytes = resp.payload.as_ref();
    if let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(bytes) {
        return Ok(map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect());
    }
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        return Ok(map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect());
    }

    Err(crate::Error::PlanError {
        detail: format!("invalid OLD-row response payload for collection '{collection}'"),
    })
}

/// Check if any triggers exist for this collection+event combination.
///
/// Quick check to avoid fetch_old_row and other overhead when no triggers are defined.
pub fn has_triggers(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> bool {
    let tid = tenant_id.as_u64();
    !state
        .trigger_registry
        .get_matching(database_id, tid, collection, DmlEvent::Insert)
        .is_empty()
        || !state
            .trigger_registry
            .get_matching(database_id, tid, collection, DmlEvent::Update)
            .is_empty()
        || !state
            .trigger_registry
            .get_matching(database_id, tid, collection, DmlEvent::Delete)
            .is_empty()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::control::state::SharedState;
    use crate::types::{DatabaseId, TenantId};
    use crate::wal::WalManager;

    use super::fetch_old_row;

    fn regular_identity(database_id: DatabaseId) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            42,
            "trigger-reader",
            TenantId::new(7),
            AuthMethod::Trust,
            vec![Role::Custom("trigger_observer".into())],
            Some(database_id),
            DatabaseSet::Some(smallvec::smallvec![database_id]),
        )
    }

    #[tokio::test]
    async fn fetch_old_row_denies_unreadable_collection_before_lookup_or_dispatch() {
        let directory = tempfile::tempdir().expect("create trigger hook test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("trigger-hook.wal"))
                .expect("open trigger hook test WAL"),
        );
        let (dispatcher, mut data_sides) = Dispatcher::new(1, 1);
        let state = SharedState::new(dispatcher, wal).expect("construct trigger hook state");
        let database_id = DatabaseId::new(17);
        let identity = regular_identity(database_id);
        let mut auth = AuthContext::from_identity(&identity, "trigger-hook-session".into());
        auth.database_id = Some(database_id);
        let collection = "orders";
        let document_id = "order-42";
        let initial_hwm = state
            .surrogate_registry
            .read()
            .expect("read surrogate registry")
            .current_hwm();

        let error = fetch_old_row(
            &state,
            &identity,
            database_id,
            &auth,
            &nodedb_types::QualifiedCollection::new(database_id, collection),
            document_id,
            None,
        )
        .await
        .expect_err("custom role without READ grant must not fetch OLD row");

        assert!(matches!(
            error,
            crate::Error::RejectedAuthz { tenant_id, resource }
                if tenant_id == TenantId::new(7)
                    && resource == "permission denied: user 'trigger-reader' lacks Read permission on 'orders'"
        ));
        assert_eq!(
            state
                .surrogate_registry
                .read()
                .expect("read surrogate registry")
                .current_hwm(),
            initial_hwm,
            "authorization denial must not allocate a surrogate"
        );
        assert_eq!(
            state
                .surrogate_assigner
                .lookup_bound(
                    nodedb_types::CollectionKey::from_bare(database_id, collection),
                    identity.tenant_id,
                    document_id.as_bytes()
                )
                .expect("inspect surrogate binding after denial"),
            None,
            "authorization denial must not create a surrogate binding"
        );
        assert!(
            data_sides.remove(0).request_rx.try_pop().is_err(),
            "authorization denial must not dispatch an OLD-row read"
        );
    }

    #[tokio::test]
    async fn fetch_old_row_rejects_misaligned_auth_context_before_authorization() {
        let directory = tempfile::tempdir().expect("create trigger hook test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("trigger-hook.wal"))
                .expect("open trigger hook test WAL"),
        );
        let (dispatcher, mut data_sides) = Dispatcher::new(1, 1);
        let state = SharedState::new(dispatcher, wal).expect("construct trigger hook state");
        let database_id = DatabaseId::new(17);
        let identity = regular_identity(database_id);
        let mut auth = AuthContext::from_identity(&identity, "trigger-hook-session".into());
        auth.database_id = Some(DatabaseId::new(18));
        let initial_hwm = state
            .surrogate_registry
            .read()
            .expect("read surrogate registry")
            .current_hwm();

        let error = fetch_old_row(
            &state,
            &identity,
            database_id,
            &auth,
            &nodedb_types::QualifiedCollection::new(database_id, "orders"),
            "order-42",
            None,
        )
        .await
        .expect_err("misaligned auth context must be rejected before authorization");

        assert!(matches!(
            error,
            crate::Error::RejectedAuthz { tenant_id, resource }
                if tenant_id == TenantId::new(7)
                    && resource == "OLD-row fetch auth context is not aligned to the selected database"
        ));
        assert_eq!(
            state
                .surrogate_registry
                .read()
                .expect("read surrogate registry")
                .current_hwm(),
            initial_hwm,
            "misaligned context must not allocate a surrogate"
        );
        assert!(
            state.audit.lock().expect("read audit log").is_empty(),
            "context rejection must occur before collection authorization"
        );
        assert!(
            data_sides.remove(0).request_rx.try_pop().is_err(),
            "context rejection must not dispatch an OLD-row read"
        );
    }
}
