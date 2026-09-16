// SPDX-License-Identifier: BUSL-1.1

//! `SHOW FUNCTIONS` DDL handler.
//!
//! Lists all user-defined functions for the current tenant, plus system functions.
//!
//! Ported from the pgwire `ddl::function::show` handler. The catalog reads and
//! row ordering (user-defined functions first, then system functions) are
//! preserved verbatim; only the result construction changed from a pgwire
//! `QueryResponse` (6 text columns) to a protocol-neutral [`DdlResult::Rows`]
//! carrying the same columns and per-row values.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW FUNCTIONS`
///
/// Returns a result set with columns: name, type, parameters, return_type, volatility, owner.
pub fn show_functions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let columns = vec![
        "name".to_string(),
        "type".to_string(),
        "parameters".to_string(),
        "return_type".to_string(),
        "volatility".to_string(),
        "owner".to_string(),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();

    // User-defined functions from catalog.
    let catalog = state.credentials.catalog();
    if let Ok(functions) = catalog.load_functions_in_database(database_id, tenant_id) {
        for func in &functions {
            let params_str = func
                .parameters
                .iter()
                .map(|p| format!("{} {}", p.name, p.data_type))
                .collect::<Vec<_>>()
                .join(", ");

            let mut row = Map::new();
            row.insert("name".to_string(), JsonValue::String(func.name.clone()));
            row.insert(
                "type".to_string(),
                JsonValue::String("expression".to_string()),
            );
            row.insert("parameters".to_string(), JsonValue::String(params_str));
            row.insert(
                "return_type".to_string(),
                JsonValue::String(func.return_type.clone()),
            );
            row.insert(
                "volatility".to_string(),
                JsonValue::String(func.volatility.as_str().to_string()),
            );
            row.insert("owner".to_string(), JsonValue::String(func.owner.clone()));
            rows.push(row);
        }
    }

    // System functions (built-in UDFs) — listed with type "system".
    for name in crate::control::planner::context::SYSTEM_FUNCTION_NAMES {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(name.to_string()));
        row.insert("type".to_string(), JsonValue::String("system".to_string()));
        row.insert("parameters".to_string(), JsonValue::String(String::new())); // params vary
        row.insert("return_type".to_string(), JsonValue::String(String::new())); // return type varies
        row.insert(
            "volatility".to_string(),
            JsonValue::String("immutable".to_string()),
        );
        row.insert("owner".to_string(), JsonValue::String("system".to_string()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
