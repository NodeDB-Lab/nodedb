// SPDX-License-Identifier: BUSL-1.1

//! `VALIDATE TYPEGUARD ON <collection>` — scan existing documents and report violations.
//!
//! Scans all documents in the collection, evaluates each against the active
//! type guards, and returns a result set of violations (field, document_id, detail).
//! Does NOT modify or reject data — read-only audit.
//!
//! Ported from the pgwire `ddl::typeguard::validate` handler. The scan planning,
//! Data Plane dispatch, JSON decode, and per-guard enforcement logic are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] / [`DdlError`].

use nodedb_types::DatabaseId;

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
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
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let coll_name = super::parse::extract_collection_name(sql)?;
    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();

    let coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &coll_name)
        .map_err(|e| super::parse::err("XX000", &format!("catalog error: {e}")))?
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

    let guards = coll.type_guards.clone();

    // Scan all documents.
    let scan_sql = format!("SELECT * FROM {}", ::nodedb_types::quote_ident(&coll_name));
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            &scan_sql,
            DatabaseId::DEFAULT,
        )
        .await
        .map_err(|error| super::parse::err(&error.sqlstate, &error.message))?;

    let mut json_chunks = Vec::new();
    for task in tasks {
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
        .map_err(|e| super::parse::err("XX000", &format!("scan dispatch failed: {e}")))?;

        if !resp.payload.is_empty() {
            let json = crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            if !json.is_empty() {
                json_chunks.push(json);
            }
        }
    }

    // Parse JSON rows and validate each document.
    let mut violations = Vec::new();

    for chunk in &json_chunks {
        if let Ok(serde_json::Value::Array(rows)) = sonic_rs::from_str::<serde_json::Value>(chunk) {
            for row in rows {
                // Scan responses wrap documents as {"id": "...", "data": {...}}.
                // The outer "id" is the substrate row key (a surrogate hex
                // string); the user-visible primary key lives inside the
                // document body. Prefer the body's PK so violation reports
                // reference the identifier the caller wrote.
                let (doc_id, inner) = if let Some(data) = row.get("data") {
                    let body_id = data.get("id").and_then(|v| v.as_str());
                    let outer_id = row.get("id").and_then(|v| v.as_str());
                    let id = body_id.or(outer_id).unwrap_or("unknown").to_string();
                    (id, data.clone())
                } else {
                    let id = row
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    (id, row.clone())
                };

                let doc = nodedb_types::Value::from(inner);

                // Validate against each guard individually to collect ALL violations.
                for guard in &guards {
                    if let Err(e) = crate::data::executor::enforcement::typeguard::check_type_guards(
                        &coll_name,
                        std::slice::from_ref(guard),
                        &doc,
                        None,
                    ) {
                        let detail = match &e {
                            crate::bridge::envelope::ErrorCode::TypeGuardViolation {
                                detail,
                                ..
                            } => detail.clone(),
                            other => format!("{other:?}"),
                        };
                        violations.push((doc_id.clone(), guard.field.clone(), detail));
                    }
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
