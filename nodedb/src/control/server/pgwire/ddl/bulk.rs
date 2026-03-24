//! Bulk data import: COPY <collection> FROM '<path>' [WITH (FORMAT csv|json|ndjson)]
//!
//! Reads a file from the local filesystem and inserts documents into a collection.
//! Each line/record becomes a document. The first column or "id" field is the doc ID.

use std::io::BufRead;
use std::time::Duration;

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;
use super::user::extract_quoted_string;

const COPY_DEADLINE: Duration = Duration::from_secs(120);

/// COPY <collection> FROM '<path>' [WITH (FORMAT csv|json|ndjson)]
pub async fn copy_from(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // COPY <collection> FROM '<path>' [WITH (FORMAT ...)]
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: COPY <collection> FROM '<path>' [WITH (FORMAT csv|json|ndjson)]",
        ));
    }

    let collection = parts[1];

    let from_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| sqlstate_error("42601", "expected FROM keyword"))?;

    let path = extract_quoted_string(parts, from_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "path must be a single-quoted string"))?;

    // Detect format (default: ndjson).
    let format = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FORMAT"))
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.to_lowercase().trim_matches(')').to_string())
        .unwrap_or_else(|| "ndjson".into());

    let tenant_id = identity.tenant_id;

    // Open file.
    let file = std::fs::File::open(&path)
        .map_err(|e| sqlstate_error("XX000", &format!("failed to open '{path}': {e}")))?;
    let reader = std::io::BufReader::new(file);

    let mut count = 0usize;
    let mut errors = 0usize;

    match format.as_str() {
        "ndjson" | "json" | "jsonl" => {
            for line in reader.lines() {
                let line = line.map_err(|e| {
                    sqlstate_error("XX000", &format!("read error at line {count}: {e}"))
                })?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let doc: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                    sqlstate_error("22P02", &format!("invalid JSON at line {}: {e}", count + 1))
                })?;

                // Extract document ID from "id" field, or auto-generate.
                let doc_id = doc
                    .get("id")
                    .and_then(|v| match v {
                        serde_json::Value::String(s) => Some(s.clone()),
                        serde_json::Value::Number(n) => Some(n.to_string()),
                        _ => None,
                    })
                    .unwrap_or_else(nodedb_types::id_gen::uuid_v7);

                let value = serde_json::to_vec(&doc).unwrap_or_default();

                let plan = PhysicalPlan::PointPut {
                    collection: collection.to_string(),
                    document_id: doc_id,
                    value,
                };

                if super::sync_dispatch::dispatch_async(
                    state,
                    tenant_id,
                    collection,
                    plan,
                    COPY_DEADLINE,
                )
                .await
                .is_ok()
                {
                    count += 1;
                } else {
                    errors += 1;
                }
            }
        }
        "csv" => {
            let mut lines = reader.lines();
            // First line is header.
            let header_line = lines
                .next()
                .ok_or_else(|| sqlstate_error("XX000", "CSV file is empty"))?
                .map_err(|e| sqlstate_error("XX000", &format!("read header: {e}")))?;
            let headers: Vec<&str> = header_line.split(',').map(|h| h.trim()).collect();

            for line in lines {
                let line = line.map_err(|e| {
                    sqlstate_error("XX000", &format!("read error at line {count}: {e}"))
                })?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let values: Vec<&str> = line.split(',').map(|v| v.trim()).collect();
                let mut obj = serde_json::Map::new();
                for (i, header) in headers.iter().enumerate() {
                    let val = values.get(i).unwrap_or(&"");
                    // Try to parse as number, bool, or fall back to string.
                    let json_val = if let Ok(n) = val.parse::<i64>() {
                        serde_json::Value::Number(n.into())
                    } else if let Ok(f) = val.parse::<f64>() {
                        serde_json::Number::from_f64(f)
                            .map(serde_json::Value::Number)
                            .unwrap_or_else(|| serde_json::Value::String(val.to_string()))
                    } else if *val == "true" || *val == "false" {
                        serde_json::Value::Bool(*val == "true")
                    } else {
                        serde_json::Value::String(val.to_string())
                    };
                    obj.insert(header.to_string(), json_val);
                }

                let doc_id = obj
                    .get("id")
                    .and_then(|v| match v {
                        serde_json::Value::String(s) => Some(s.clone()),
                        serde_json::Value::Number(n) => Some(n.to_string()),
                        _ => None,
                    })
                    .unwrap_or_else(nodedb_types::id_gen::uuid_v7);

                let value = serde_json::to_vec(&obj).unwrap_or_default();

                let plan = PhysicalPlan::PointPut {
                    collection: collection.to_string(),
                    document_id: doc_id,
                    value,
                };

                if super::sync_dispatch::dispatch_async(
                    state,
                    tenant_id,
                    collection,
                    plan,
                    COPY_DEADLINE,
                )
                .await
                .is_ok()
                {
                    count += 1;
                } else {
                    errors += 1;
                }
            }
        }
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!("unsupported format: '{other}'. Use csv, json, or ndjson."),
            ));
        }
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("COPY {collection} FROM '{path}': {count} rows imported, {errors} errors"),
    );

    Ok(vec![Response::Execution(Tag::new(&format!(
        "COPY {count}"
    )))])
}
