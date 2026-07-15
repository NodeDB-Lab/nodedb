// SPDX-License-Identifier: BUSL-1.1

//! `SHOW CONSTRAINTS ON <collection>` — unified view of all constraint kinds.
//!
//! Ported from the pgwire `ddl::constraint::show`. The per-row content is
//! preserved verbatim; only the result construction changed from a pgwire
//! `QueryResponse` (4 text columns via `DataRowEncoder`) to a protocol-neutral
//! [`DdlResult::Rows`] carrying the same four text columns.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::DatabaseId;
use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::support::err;

/// Handle `SHOW CONSTRAINTS ON <collection>`.
///
/// Returns a unified view of all constraint kinds:
/// - `transition` — state transition constraints (ON COLUMN ... TRANSITIONS)
/// - `transition_check` — OLD/NEW predicate constraints
/// - `typeguard` — per-field type + CHECK constraints
/// - `check` — general CHECK constraints (may have subqueries)
pub fn show_constraints(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let coll_name = extract_collection_after_on(sql)?;

    let catalog = state.credentials.catalog();

    let tenant_id = identity.tenant_id.as_u64();
    let coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let columns = vec![
        "name".to_string(),
        "kind".to_string(),
        "field".to_string(),
        "detail".to_string(),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();

    // State transition constraints.
    for sc in &coll.state_constraints {
        let detail = sc
            .transitions
            .iter()
            .map(|t| {
                if let Some(role) = &t.required_role {
                    format!("'{}' -> '{}' BY ROLE '{}'", t.from, t.to, role)
                } else {
                    format!("'{}' -> '{}'", t.from, t.to)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        rows.push(constraint_row(&sc.name, "transition", &sc.column, &detail));
    }

    // Transition check constraints.
    for tc in &coll.transition_checks {
        rows.push(constraint_row(
            &tc.name,
            "transition_check",
            "",
            &format!("{:?}", tc.predicate),
        ));
    }

    // Typeguard constraints.
    for guard in &coll.type_guards {
        let detail = {
            let mut parts = Vec::new();
            parts.push(format!("type={}", guard.type_expr));
            if guard.required {
                parts.push("REQUIRED".to_string());
            }
            if let Some(check) = &guard.check_expr {
                parts.push(format!("CHECK ({check})"));
            }
            parts.join(", ")
        };
        let auto_name = format!("_guard_{}", guard.field);
        rows.push(constraint_row(
            &auto_name,
            "typeguard",
            &guard.field,
            &detail,
        ));
    }

    // General CHECK constraints.
    for cc in &coll.check_constraints {
        let kind_str = if cc.has_subquery {
            "check (subquery)"
        } else {
            "check"
        };
        rows.push(constraint_row(&cc.name, kind_str, "", &cc.check_sql));
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}

/// Build a single `SHOW CONSTRAINTS` row keyed by the four text columns.
fn constraint_row(name: &str, kind: &str, field: &str, detail: &str) -> Map<String, JsonValue> {
    let mut row = Map::new();
    row.insert("name".to_string(), JsonValue::String(name.to_string()));
    row.insert("kind".to_string(), JsonValue::String(kind.to_string()));
    row.insert("field".to_string(), JsonValue::String(field.to_string()));
    row.insert("detail".to_string(), JsonValue::String(detail.to_string()));
    row
}

/// Extract collection name from `SHOW CONSTRAINTS ON <collection>`.
fn extract_collection_after_on(sql: &str) -> Result<String, DdlError> {
    let on_pos = find_ascii_case_insensitive(sql, " ON ")
        .ok_or_else(|| err("42601", "SHOW CONSTRAINTS requires ON <collection>"))?;
    let after = sql[on_pos + 4..].trim();
    let end = after
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(after.len());
    let name = after[..end].trim().to_lowercase();
    if name.is_empty() {
        return Err(err("42601", "missing collection name after ON"));
    }
    Ok(name)
}
