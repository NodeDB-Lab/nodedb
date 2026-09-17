// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW CHANGE STREAMS` DDL handler.
//!
//! Ported from the pgwire `ddl::change_stream::show` handler. The tenant
//! scoping and the per-stream field extraction are preserved verbatim; only the
//! result construction changed from a pgwire `QueryResponse` to the
//! protocol-neutral [`DdlResult::Rows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW CHANGE STREAMS`
pub fn show_change_streams(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let columns = vec![
        "name".to_string(),
        "collection".to_string(),
        "include".to_string(),
        "format".to_string(),
        "owner".to_string(),
        "created_at".to_string(),
    ];

    let streams = state
        .stream_registry
        .list_for_database_tenant(database_id, tenant_id);

    let mut rows = Vec::with_capacity(streams.len());
    for s in &streams {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(s.name.clone()));
        row.insert(
            "collection".to_string(),
            JsonValue::String(s.collection.clone()),
        );
        row.insert(
            "include".to_string(),
            JsonValue::String(s.op_filter.display()),
        );
        row.insert(
            "format".to_string(),
            JsonValue::String(s.format.as_str().to_string()),
        );
        row.insert("owner".to_string(), JsonValue::String(s.owner.clone()));
        row.insert(
            "created_at".to_string(),
            JsonValue::String(s.created_at.to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
