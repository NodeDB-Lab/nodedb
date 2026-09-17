// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral SHOW INDEXES DDL.
//!
//! Reads the catalog index registry — the one place every
//! `CREATE ... INDEX` path registers into — so the listing and
//! `DROP INDEX` always agree on which indexes exist. Indexes of a
//! soft-dropped collection are retained for `UNDROP` but not listed.
//!
//! `ON <collection>` filters by the collection each index is actually
//! attached to, not by whether the index name has the collection as a
//! prefix — a name-prefix filter both hides correctly-named indexes and
//! shows unrelated ones.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::catalog::StoredIndexRecord;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// SHOW INDEXES [ON <collection>]
///
/// Lists indexes for the current tenant (optionally filtered by collection).
pub fn show_indexes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    // Parse optional ON <collection> filter.
    let filter_collection = if parts.len() >= 4
        && parts[1].eq_ignore_ascii_case("INDEXES")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        Some(parse_ident_token(parts[3])?)
    } else {
        None
    };

    let columns = vec![
        "index_name".to_string(),
        "type".to_string(),
        "collection".to_string(),
        "fields".to_string(),
        "owner".to_string(),
    ];
    let mut records = state
        .credentials
        .catalog()
        .list_index_records(database_id.as_u64(), tenant_id.as_u64())
        .map_err(|e| DdlError::new("XX000", e.to_string()))?;
    records.retain(StoredIndexRecord::is_visible);
    if let Some(collection) = filter_collection.as_deref() {
        records.retain(|r| r.collection == collection);
    }
    // Group the kinds together, as the four separate ownership ledgers used
    // to, and keep the order inside a kind stable.
    records.sort_by(|a, b| (a.kind.display_type(), &a.name).cmp(&(b.kind.display_type(), &b.name)));

    let rows = records
        .into_iter()
        .map(|record| {
            // Ownership lives in the owner ledger, the single source of truth
            // for every object's owner; the registry never duplicates it.
            let owner = state
                .permissions
                .get_owner(
                    record.kind.owner_object_type(),
                    database_id.as_u64(),
                    tenant_id,
                    &record.name,
                )
                .unwrap_or_default();
            let mut row = Map::new();
            row.insert("index_name".to_string(), JsonValue::String(record.name));
            row.insert(
                "type".to_string(),
                JsonValue::String(record.kind.display_type().to_string()),
            );
            row.insert(
                "collection".to_string(),
                JsonValue::String(record.collection),
            );
            row.insert(
                "fields".to_string(),
                JsonValue::String(record.fields.join(", ")),
            );
            row.insert("owner".to_string(), JsonValue::String(owner));
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
