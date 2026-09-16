// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW SYNONYM GROUPS` handler.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW SYNONYM GROUPS`.
///
/// Lists only the groups of the session's database. A group of another
/// database expands nothing here, so listing it misstates what a query does.
pub fn show_synonym_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id_u64 = identity.tenant_id.as_u64();
    let groups = state
        .synonym_registry
        .list_for_tenant(database_id.as_u64(), tenant_id_u64);

    let columns = vec!["name".to_string(), "terms".to_string()];

    let mut rows = Vec::with_capacity(groups.len());
    for g in &groups {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(g.name.clone()));
        let terms_csv = g.terms.join(", ");
        row.insert("terms".to_string(), JsonValue::String(terms_csv));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
