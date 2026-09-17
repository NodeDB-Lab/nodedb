// SPDX-License-Identifier: BUSL-1.1

//! Scope introspection: `SHOW SCOPES` / `SHOW SCOPE '<name>'`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// SHOW SCOPES / SHOW SCOPE '<name>' / SHOW SCOPES FOR <type> '<id>'
pub fn show_scopes(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // SHOW SCOPE '<name>' — resolve a single scope.
    if parts.len() >= 3 && parts[1].to_uppercase() == "SCOPE" && parts[2].to_uppercase() != "GRANTS"
    {
        let name = parts[2].trim_matches('\'');
        let resolved = state.scope_defs.resolve(name);
        let columns = vec!["permission".to_string(), "collection".to_string()];
        let rows: Vec<_> = resolved
            .iter()
            .map(|(perm, coll)| {
                let mut row = Map::new();
                row.insert("permission".to_string(), JsonValue::String(perm.clone()));
                row.insert("collection".to_string(), JsonValue::String(coll.clone()));
                row
            })
            .collect();
        return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))]);
    }

    // SHOW SCOPES — list all scope definitions.
    let scopes = state.scope_defs.list();
    let columns = vec![
        "name".to_string(),
        "grants".to_string(),
        "includes".to_string(),
        "created_by".to_string(),
    ];

    let rows: Vec<_> = scopes
        .iter()
        .map(|s| {
            let grants_str: Vec<String> = s
                .grants
                .iter()
                .map(|(p, c)| format!("{p} ON {c}"))
                .collect();
            let mut row = Map::new();
            row.insert("name".to_string(), JsonValue::String(s.name.clone()));
            row.insert(
                "grants".to_string(),
                JsonValue::String(grants_str.join(", ")),
            );
            row.insert(
                "includes".to_string(),
                JsonValue::String(s.includes.join(", ")),
            );
            row.insert(
                "created_by".to_string(),
                JsonValue::String(s.created_by.clone()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
