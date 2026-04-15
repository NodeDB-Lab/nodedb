//! `VALIDATE TYPEGUARD ON <collection>` — scan existing documents and report violations.
//!
//! Scans all documents in the collection, evaluates each against the active
//! type guards, and returns a result set of violations (field, document_id, detail).
//! Does NOT modify or reject data — read-only audit.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::text_field;
use crate::control::state::SharedState;

/// Handle `VALIDATE TYPEGUARD ON <collection>`.
///
/// Scans all documents, validates each against type guards, and returns
/// a table of violations: `(document_id, field, violation)`.
/// Returns zero rows if all documents are valid.
pub async fn validate_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let coll_name = super::parse::extract_collection_name(sql)?;
    let tenant_id = identity.tenant_id;

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| super::parse::err("08000", "catalog not available"))?;

    let coll = catalog
        .get_collection(tenant_id.as_u32(), &coll_name)
        .map_err(|e| super::parse::err("XX000", &format!("catalog error: {e}")))?
        .ok_or_else(|| {
            super::parse::err("42P01", &format!("collection '{coll_name}' not found"))
        })?;

    if coll.type_guards.is_empty() {
        // No type guards — return empty result.
        let schema = Arc::new(vec![
            text_field("document_id"),
            text_field("field"),
            text_field("violation"),
        ]);
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(Vec::new()),
        ))]);
    }

    let guards = coll.type_guards.clone();

    // Scan all documents.
    let scan_sql = format!("SELECT * FROM {coll_name}");
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let tasks = query_ctx
        .plan_sql(&scan_sql, tenant_id)
        .await
        .map_err(|e| super::parse::err("XX000", &format!("scan planning failed: {e}")))?;

    let mut json_chunks = Vec::new();
    for task in tasks {
        let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            task.vshard_id,
            task.plan,
            0,
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
                // Extract the inner document for validation.
                let (doc_id, inner) = if let Some(data) = row.get("data") {
                    let id = row
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
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
    let schema = Arc::new(vec![
        text_field("document_id"),
        text_field("field"),
        text_field("violation"),
    ]);

    let rows: Vec<_> = violations
        .into_iter()
        .map(|(doc_id, field, detail)| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&doc_id);
            let _ = encoder.encode_field(&field);
            let _ = encoder.encode_field(&detail);
            Ok(encoder.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
