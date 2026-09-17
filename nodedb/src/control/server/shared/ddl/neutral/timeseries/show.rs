// SPDX-License-Identifier: BUSL-1.1

//! `SHOW PARTITIONS FOR <name>`
//!
//! Reachability note: the `SHOW PARTITIONS ` prefix is claimed by the
//! consumer-group handler earlier in the neutral router (mirroring the pgwire
//! streaming router, which ran before engine_ops), so this handler is shadowed
//! for that prefix exactly as it was on the pgwire path. It is ported here to
//! preserve the engine_ops handler set verbatim.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use nodedb_types::DatabaseId;
use serde_json::{Map, Value as JsonValue};

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::{ddl_err, format_bytes};

/// SHOW PARTITIONS FOR <name>
pub fn show_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 {
        return Err(ddl_err("42601", "syntax: SHOW PARTITIONS FOR <collection>"));
    }

    let name = parse_ident_token(parts[3])?;
    let tenant_id = identity.tenant_id;

    // Verify collection exists and is timeseries.
    {
        let catalog = state.credentials.catalog();
        match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &name) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(ddl_err(
                    "42809",
                    format!("'{name}' is not a timeseries collection"),
                ));
            }
            _ => {
                return Err(ddl_err(
                    "42P01",
                    format!("collection '{name}' does not exist"),
                ));
            }
        }
    }

    let mut rows = Vec::new();

    if let Some(registries) = state.timeseries_registries() {
        let regs = crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
        let key = format!("{}:{}", tenant_id.as_u64(), name);
        if let Some(registry) = regs.get(&key) {
            for (_, entry) in registry.iter() {
                if !entry.meta.is_queryable() {
                    continue;
                }
                let mut row = Map::new();
                row.insert(
                    "partition".to_string(),
                    JsonValue::String(entry.dir_name.clone()),
                );
                row.insert(
                    "min_ts".to_string(),
                    JsonValue::String(entry.meta.min_ts.to_string()),
                );
                row.insert(
                    "max_ts".to_string(),
                    JsonValue::String(entry.meta.max_ts.to_string()),
                );
                row.insert(
                    "rows".to_string(),
                    JsonValue::String((entry.meta.row_count as i64).to_string()),
                );
                row.insert(
                    "size".to_string(),
                    JsonValue::String(format_bytes(entry.meta.size_bytes)),
                );
                row.insert(
                    "state".to_string(),
                    JsonValue::String(format!("{:?}", entry.meta.state)),
                );
                rows.push(row);
            }
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        vec![
            "partition".to_string(),
            "min_ts".to_string(),
            "max_ts".to_string(),
            "rows".to_string(),
            "size".to_string(),
            "state".to_string(),
        ],
        vec![
            DdlColType::Text,
            DdlColType::Int8,
            DdlColType::Int8,
            DdlColType::Int8,
            DdlColType::Text,
            DdlColType::Text,
        ],
        rows,
    ))])
}
