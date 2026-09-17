// SPDX-License-Identifier: BUSL-1.1

//! Operator access to the trigger dead-letter queue: SHOW TRIGGER DLQ and
//! REQUEUE TRIGGER DLQ <entry_id>.
//!
//! An action reaches the DLQ after it has spent its retries. What failed it —
//! a dropped collection, an offline shard, a constraint since relaxed — is
//! fixed outside the database, so putting the action back is an operator
//! decision and needs an operator-facing surface. Without one the DLQ is
//! write-only and the work in it is unreachable.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// Rows a `SHOW TRIGGER DLQ` returns when nothing is listed.
const COLUMNS: [&str; 7] = [
    "entry_id",
    "tenant_id",
    "collection",
    "row_id",
    "owner",
    "error",
    "retry_count",
];

fn column_types() -> Vec<DdlColType> {
    vec![
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
    ]
}

/// SHOW TRIGGER DLQ [LIMIT <n>] — list dead-lettered actions.
///
/// Scoped to the caller's tenant unless the caller is a superuser, so an
/// error string from another tenant's trigger is never disclosed.
pub fn show_trigger_dlq(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    limit: usize,
) -> Result<Vec<DdlResult>, DdlError> {
    let Some(dlq) = state.trigger_dlq.get() else {
        return Err(ddl_err(
            "55000",
            "the event plane is not running on this node, so it has no trigger DLQ",
        ));
    };
    let dlq = dlq.lock().unwrap_or_else(|poison| poison.into_inner());

    let visible =
        |tenant_id: u64| identity.is_superuser || tenant_id == identity.tenant_id.as_u64();
    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();
    for entry in dlq.list().filter(|e| !e.resolved && visible(e.tenant_id())) {
        let mut row = Map::new();
        row.insert("entry_id".into(), JsonValue::from(entry.entry_id));
        row.insert("tenant_id".into(), JsonValue::from(entry.tenant_id()));
        row.insert("collection".into(), JsonValue::from(entry.collection()));
        row.insert(
            "row_id".into(),
            JsonValue::from(entry.action.context.row_id.clone()),
        );
        row.insert("owner".into(), JsonValue::from(entry.owner()));
        row.insert("error".into(), JsonValue::from(entry.error()));
        row.insert("retry_count".into(), JsonValue::from(entry.retry_count()));
        rows.push(row);
        if rows.len() >= limit {
            break;
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        COLUMNS.iter().map(|c| (*c).to_owned()).collect(),
        column_types(),
        rows,
    ))])
}

/// REQUEUE TRIGGER DLQ <entry_id> — hand one dead-lettered action back to the
/// Event Plane for another attempt.
///
/// The entry is marked resolved as it is taken, so the same work cannot be
/// requeued twice while it is in flight.
pub fn requeue_trigger_dlq(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    entry_id: u64,
) -> Result<Vec<DdlResult>, DdlError> {
    let Some(dlq) = state.trigger_dlq.get() else {
        return Err(ddl_err(
            "55000",
            "the event plane is not running on this node, so it has no trigger DLQ",
        ));
    };
    let Some(inbox) = state.action_requeue.get() else {
        return Err(ddl_err(
            "55000",
            "the event plane is not running on this node, so there is nothing to requeue onto",
        ));
    };

    // Take under the lock, then release it before touching the inbox — the
    // consumer collecting requeued work must never wait on the DLQ mutex.
    let action = {
        let mut dlq = dlq.lock().unwrap_or_else(|poison| poison.into_inner());
        let owner_tenant = dlq
            .list()
            .find(|e| e.entry_id == entry_id)
            .map(|e| e.tenant_id());
        match owner_tenant {
            Some(tenant_id)
                if !identity.is_superuser && tenant_id != identity.tenant_id.as_u64() =>
            {
                // Report exactly as an absent entry: whether another tenant
                // holds this id is not this caller's to learn.
                return Err(ddl_err("42704", format!("no dead-letter entry {entry_id}")));
            }
            _ => {}
        }
        dlq.take_for_requeue(entry_id)
            .map_err(|e| ddl_err(requeue_take_sqlstate(&e), e.to_string()))?
    };

    let owner = action.owner().to_owned();
    let core_id = inbox
        .submit(action)
        .map_err(|e| ddl_err("55000", e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        "trigger_dlq_requeue",
        &format!("dead-letter entry {entry_id} ({owner}) requeued onto core {core_id}"),
    );

    Ok(vec![DdlResult::Status {
        command: format!("REQUEUE TRIGGER DLQ {entry_id} ({owner})"),
        rows_affected: Some(1),
    }])
}

/// Map a take refusal onto the SQLSTATE it reports as.
fn requeue_take_sqlstate(error: &crate::event::trigger::RequeueTakeError) -> &'static str {
    use crate::event::trigger::RequeueTakeError as E;
    match error {
        // No such object.
        E::NotFound { .. } => "42704",
        // The object exists but is not in a state this action accepts.
        E::AlreadyResolved { .. } => "55000",
    }
}
