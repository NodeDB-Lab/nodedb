// SPDX-License-Identifier: BUSL-1.1

//! `SHOW PROCEDURES` DDL handler.
//!
//! Ported from the pgwire `ddl::procedure::show` handler. The catalog read and
//! per-row parameter formatting are preserved verbatim; only the result
//! construction changed from a pgwire `QueryResponse` (5 text columns) to a
//! protocol-neutral [`DdlResult::Rows`] carrying the same columns and per-row
//! values.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW PROCEDURES`
///
/// Returns a result set with columns: name, parameters, max_iterations,
/// timeout_secs, owner.
pub fn show_procedures(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let columns = vec![
        "name".to_string(),
        "parameters".to_string(),
        "max_iterations".to_string(),
        "timeout_secs".to_string(),
        "owner".to_string(),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();
    let catalog = state.credentials.catalog();
    if let Ok(procs) = catalog.load_procedures_in_database(database_id, tenant_id) {
        for p in &procs {
            let params_str = p
                .parameters
                .iter()
                .map(|param| {
                    format!(
                        "{} {} {}",
                        param.direction.as_str(),
                        param.name,
                        param.data_type
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");

            let mut row = Map::new();
            row.insert("name".to_string(), JsonValue::String(p.name.clone()));
            row.insert("parameters".to_string(), JsonValue::String(params_str));
            row.insert(
                "max_iterations".to_string(),
                JsonValue::String(p.max_iterations.to_string()),
            );
            row.insert(
                "timeout_secs".to_string(),
                JsonValue::String(p.timeout_secs.to_string()),
            );
            row.insert("owner".to_string(), JsonValue::String(p.owner.clone()));
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
