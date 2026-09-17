// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW SCHEMA VERSION` — current descriptor version
//! visible on this node.
//!
//! Ported from the pgwire `ddl::cluster::schema_version` handler. The
//! schema-version / metadata-cache reads are preserved verbatim; only the
//! result construction changed from pgwire `Response` / `QueryResponse` to
//! the protocol-neutral `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// SHOW SCHEMA VERSION — report the current descriptor version
/// counter and per-collection metadata if available.
pub fn show_schema_version(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view schema version",
        ));
    }

    let columns = vec!["property".to_string(), "value".to_string()];

    let mut rows = Vec::new();

    let version = state.schema_version.current();
    let mut row = Map::new();
    row.insert(
        "property".to_string(),
        JsonValue::String("schema_version".to_string()),
    );
    row.insert("value".to_string(), JsonValue::String(version.to_string()));
    rows.push(row);

    let applied_index = {
        let cache = state
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.applied_index
    };
    let mut row = Map::new();
    row.insert(
        "property".to_string(),
        JsonValue::String("metadata_applied_index".to_string()),
    );
    row.insert(
        "value".to_string(),
        JsonValue::String(applied_index.to_string()),
    );
    rows.push(row);

    let mut row = Map::new();
    row.insert(
        "property".to_string(),
        JsonValue::String("node_id".to_string()),
    );
    row.insert(
        "value".to_string(),
        JsonValue::String(state.node_id.to_string()),
    );
    rows.push(row);

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
