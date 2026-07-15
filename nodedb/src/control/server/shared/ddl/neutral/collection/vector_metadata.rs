// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handlers for vector model metadata.
//!
//! - `ALTER COLLECTION x SET VECTOR METADATA ON column (model = '...', dimensions = N, ...)`
//! - `SHOW VECTOR MODELS` — catalog view of all vector columns with model metadata
//! - `SELECT VECTOR_METADATA('collection', 'column')` — inline JSON query
//!
//! Ported verbatim from the pgwire `ddl::collection::vector_metadata`
//! handlers; only the result construction changed from pgwire `Response` /
//! `QueryResponse` (text fields) to the protocol-neutral [`DdlResult`] over
//! [`ShapedRows`] (all-text columns — every field the pgwire handlers encoded
//! via `text_field`, including `dimensions` / `strict_dimensions`, was a text
//! column). The parsing, catalog reads/writes, `chrono_format_utc` default,
//! SQLSTATE codes / messages, and the `ALTER COLLECTION` command tag are
//! unchanged.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::DatabaseId;
use serde_json::{Map, Value as JsonValue};

use nodedb_types::{VectorModelEntry, VectorModelMetadata};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Handle `ALTER COLLECTION x SET VECTOR METADATA ON column (model = '...', dimensions = N, ...)`.
pub fn handle_set_vector_metadata(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    // Parse: ALTER COLLECTION <name> SET VECTOR METADATA ON <column> (...)
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 8 {
        return Err(err(
            "42601",
            "syntax: ALTER COLLECTION <name> SET VECTOR METADATA ON <column> (model = '...', dimensions = N)",
        ));
    }

    let collection = parts[2].to_lowercase();
    // Find column name after " ON " (the second ON after "METADATA ON").
    let metadata_on_pos = find_ascii_case_insensitive(sql, "METADATA ON ")
        .ok_or_else(|| err("42601", "expected METADATA ON <column>"))?;
    let after_on = sql[metadata_on_pos + "METADATA ON ".len()..].trim();
    let column = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| err("42601", "expected column name after ON"))?
        .to_lowercase();

    // Verify collection exists.
    if state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id, &collection)
        .ok()
        .flatten()
        .is_none()
    {
        return Err(err(
            "42P01",
            format!("collection \"{collection}\" does not exist"),
        ));
    }

    // Parse parenthesized key-value pairs.
    let paren_start = sql
        .find('(')
        .ok_or_else(|| err("42601", "expected (...) with model metadata"))?;
    let paren_end = sql
        .rfind(')')
        .ok_or_else(|| err("42601", "expected closing ) for metadata"))?;

    let inner = &sql[paren_start + 1..paren_end];
    let mut model = String::new();
    let mut dimensions = 0usize;
    let mut created_at = String::new();
    let mut strict_dimensions = false;

    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some((key, val)) = pair.split_once('=') {
            let key = key.trim().to_lowercase();
            let val = val.trim().trim_matches('\'').trim_matches('"');
            match key.as_str() {
                "model" => model = val.to_string(),
                "dimensions" => {
                    dimensions = val
                        .parse()
                        .map_err(|_| err("22023", format!("invalid dimensions: {val}")))?;
                }
                "created_at" => created_at = val.to_string(),
                "strict_dimensions" => {
                    strict_dimensions =
                        matches!(val.to_uppercase().as_str(), "TRUE" | "1" | "ON" | "YES");
                }
                other => {
                    return Err(err(
                        "42601",
                        format!(
                            "unknown metadata key '{other}'; supported: model, dimensions, created_at, strict_dimensions"
                        ),
                    ));
                }
            }
        }
    }

    if model.is_empty() {
        return Err(err("42601", "model is required"));
    }
    if dimensions == 0 {
        return Err(err("42601", "dimensions is required and must be > 0"));
    }

    // Default created_at to now.
    if created_at.is_empty() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        created_at = chrono_format_utc(now);
    }

    let entry = VectorModelEntry {
        tenant_id,
        collection: collection.clone(),
        column: column.clone(),
        metadata: VectorModelMetadata {
            model,
            dimensions,
            created_at,
            strict_dimensions,
        },
    };

    let catalog = state.credentials.catalog();

    catalog
        .put_vector_model(&entry)
        .map_err(|e| err("XX000", e.to_string()))?;

    tracing::info!(
        %collection,
        %column,
        model = %entry.metadata.model,
        dimensions = entry.metadata.dimensions,
        strict = entry.metadata.strict_dimensions,
        "vector model metadata set"
    );

    Ok(vec![DdlResult::Status {
        command: "ALTER COLLECTION".to_string(),
        rows_affected: None,
    }])
}

/// Handle `SHOW VECTOR MODELS` — list all vector columns with model metadata.
pub fn handle_show_vector_models(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state.credentials.catalog();

    let entries = catalog
        .list_vector_models(tenant_id)
        .map_err(|e| err("XX000", e.to_string()))?;

    let columns = vec![
        "collection".to_string(),
        "column".to_string(),
        "model".to_string(),
        "dimensions".to_string(),
        "created_at".to_string(),
        "strict_dimensions".to_string(),
    ];

    let rows: Vec<Map<String, JsonValue>> = entries
        .iter()
        .map(|e| {
            let mut row = Map::new();
            row.insert(
                "collection".to_string(),
                JsonValue::String(e.collection.clone()),
            );
            row.insert("column".to_string(), JsonValue::String(e.column.clone()));
            row.insert(
                "model".to_string(),
                JsonValue::String(e.metadata.model.clone()),
            );
            row.insert(
                "dimensions".to_string(),
                JsonValue::String(e.metadata.dimensions.to_string()),
            );
            row.insert(
                "created_at".to_string(),
                JsonValue::String(e.metadata.created_at.clone()),
            );
            row.insert(
                "strict_dimensions".to_string(),
                JsonValue::String(e.metadata.strict_dimensions.to_string()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types: ShapedRows::text_types(6),
        rows,
        notice: None,
    })])
}

/// Handle `SELECT VECTOR_METADATA('collection', 'column')` — return JSON.
pub fn handle_vector_metadata_query(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    column: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state.credentials.catalog();

    let entry = catalog
        .get_vector_model(tenant_id, collection, column)
        .map_err(|e| err("XX000", e.to_string()))?;

    let json = match entry {
        Some(e) => {
            format!(
                r#"{{"model":"{}","dimensions":{},"created_at":"{}","strict_dimensions":{}}}"#,
                e.metadata.model,
                e.metadata.dimensions,
                e.metadata.created_at,
                e.metadata.strict_dimensions
            )
        }
        None => "null".to_string(),
    };

    let mut row = Map::new();
    row.insert("vector_metadata".to_string(), JsonValue::String(json));

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec!["vector_metadata".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: vec![row],
        notice: None,
    })])
}

/// Format a Unix timestamp (seconds) as an ISO-8601 UTC date string (YYYY-MM-DD).
fn chrono_format_utc(epoch_secs: u64) -> String {
    // Correct Gregorian calendar conversion without external crate.
    let mut remaining_days = (epoch_secs / 86400) as i64;

    // Civil calendar algorithm (Howard Hinnant).
    // https://howardhinnant.github.io/date_algorithms.html#civil_from_days
    remaining_days += 719_468; // shift epoch from 1970-01-01 to 0000-03-01
    let era = if remaining_days >= 0 {
        remaining_days
    } else {
        remaining_days - 146_096
    } / 146_097;
    let doe = (remaining_days - era * 146_097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // year of era [0, 399]
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month index [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chrono_format_epoch() {
        assert_eq!(chrono_format_utc(0), "1970-01-01");
    }

    #[test]
    fn chrono_format_known_date() {
        // 2001-09-09 01:46:40 UTC = 1_000_000_000 seconds
        assert_eq!(chrono_format_utc(1_000_000_000), "2001-09-09");
    }

    #[test]
    fn chrono_format_leap_year() {
        // 2024-02-29 = 1709164800 seconds
        assert_eq!(chrono_format_utc(1_709_164_800), "2024-02-29");
    }

    #[test]
    fn chrono_format_end_of_year() {
        // 2023-12-31 = 1703980800 seconds
        assert_eq!(chrono_format_utc(1_703_980_800), "2023-12-31");
    }
}
