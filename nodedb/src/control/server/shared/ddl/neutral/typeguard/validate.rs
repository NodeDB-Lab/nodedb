// SPDX-License-Identifier: BUSL-1.1

//! `VALIDATE TYPEGUARD ON <collection>` — scan existing documents and report violations.
//!
//! Scans all documents in the collection, evaluates each against the active
//! type guards, and returns a result set of violations (field, document_id, detail).
//! Does NOT modify or reject data — read-only audit.
//!
//! The scan planning, Data Plane dispatch, JSON decode, and per-guard
//! enforcement logic run here. The result is the protocol-neutral
//! [`DdlResult`] / [`DdlError`].

use nodedb_types::{DatabaseId, Value};

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{RowContext, decode_row_payload_value};
use crate::types::TraceId;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `VALIDATE TYPEGUARD ON <collection>`.
///
/// Scans all documents, validates each against type guards, and returns
/// a table of violations: `(document_id, field, violation)`.
/// Returns zero rows if all documents are valid.
pub async fn validate_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let coll_name = super::parse::extract_collection_name(sql)?;
    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();

    let coll = catalog
        .get_collection(database_id, tenant_id.as_u64(), &coll_name)
        .map_err(|e| DdlError::from_error_in_context("catalog error", &e))?
        .ok_or_else(|| {
            super::parse::err("42P01", &format!("collection '{coll_name}' not found"))
        })?;

    let columns = vec![
        "document_id".to_string(),
        "field".to_string(),
        "violation".to_string(),
    ];

    if coll.type_guards.is_empty() {
        // No type guards — return empty result.
        return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
            columns,
            Vec::new(),
        ))]);
    }

    // Every guard is parsed once for the whole scan.
    let guards = crate::data::executor::enforcement::typeguard::CompiledTypeGuard::compile_all(
        &coll_name,
        &coll.type_guards,
    );

    // Scan all documents.
    let scan_sql = format!("SELECT * FROM {}", ::nodedb_types::quote_ident(&coll_name));
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            &scan_sql,
            database_id,
        )
        .await
        .map_err(|error| super::parse::err(&error.sqlstate, &error.message))?;

    // Each task's rows decode typed, under the row shape its plan produces,
    // so a guard checks the stored value's own type, plain or native.
    let mut chunks: Vec<(RowContext, Value)> = Vec::new();
    for task in tasks {
        let row_context = crate::bridge::envelope::describe_row_context(&task.plan);
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        let resp = crate::control::server::shared::clone_write::intercept_authorize_and_dispatch(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state,
                task,
                identity,
                tenant_id,
                permissions: &state.permissions,
                roles: &state.roles,
                emitter: &emitter,
            },
            TraceId::ZERO,
        )
        .await
        .map_err(|e| DdlError::from_error_in_context("scan dispatch failed", &e))?;

        if !resp.payload.is_empty() {
            let decoded = decode_row_payload_value(&resp.payload, row_context)
                .map_err(|e| DdlError::from_error_in_context("scan rows do not decode", &e))?;
            chunks.push((row_context, decoded));
        }
    }

    // Validate each document.
    let mut violations = Vec::new();
    let crs = crate::control::crs_context::crs_context_for(state);
    let text = |v: Option<&Value>| match v {
        Some(Value::String(s) | Value::Uuid(s) | Value::Ulid(s)) => Some(s.clone()),
        _ => None,
    };

    for (row_context, chunk) in chunks {
        let Value::Array(rows) = chunk else {
            continue;
        };
        for row in rows {
            // Scan responses wrap documents as {"id": "...", "data": {...}}.
            // The outer "id" is the substrate row key (a surrogate hex
            // string); the user-visible primary key lives inside the
            // document body. Prefer the body's PK so violation reports
            // reference the identifier the caller wrote.
            let envelope_data = match (&row_context, row.get("data")) {
                (RowContext::ScanEnvelope, Some(data @ Value::Object(_))) => Some(data.clone()),
                _ => None,
            };
            let (doc_id, doc) = match envelope_data {
                Some(data) => {
                    let id = text(data.get("id"))
                        .or_else(|| text(row.get("id")))
                        .unwrap_or_else(|| "unknown".to_string());
                    (id, data)
                }
                None => {
                    let id = text(row.get("id")).unwrap_or_else(|| "unknown".to_string());
                    (id, row)
                }
            };

            // Validate against each guard individually to collect ALL violations.
            for guard in &guards {
                if let Err(e) = crate::data::executor::enforcement::typeguard::check_type_guards(
                    std::slice::from_ref(guard),
                    &doc,
                    None,
                    &crs,
                ) {
                    let detail = match &e {
                        crate::bridge::envelope::ErrorCode::TypeGuardViolation {
                            detail, ..
                        } => detail.clone(),
                        other => format!("{other:?}"),
                    };
                    violations.push((doc_id.clone(), guard.field.clone(), detail));
                }
            }
        }
    }

    // Build result set.
    let rows: Vec<Map<String, JsonValue>> = violations
        .into_iter()
        .map(|(doc_id, field, detail)| {
            let mut row = Map::new();
            row.insert("document_id".to_string(), JsonValue::String(doc_id));
            row.insert("field".to_string(), JsonValue::String(field));
            row.insert("violation".to_string(), JsonValue::String(detail));
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
