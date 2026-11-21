// SPDX-License-Identifier: BUSL-1.1

//! `ALTER TIMESERIES <name> SET (key = 'value', ...)`

use nodedb_types::DatabaseId;
use serde_json::{Map, Value as JsonValue};

use crate::control::catalog_entry::persist_collection_replicated;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::ddl_err;

const SYNTAX: &str = "syntax: ALTER TIMESERIES <name> SET (key = 'value', ...)";

/// ALTER TIMESERIES <name> SET (key = 'value', ...)
///
/// Each named key replaces its value in the collection's timeseries config.
/// Every other key keeps its value.
pub async fn alter_timeseries(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 5 || parts[3].to_uppercase() != "SET" {
        return Err(ddl_err("42601", SYNTAX));
    }

    let name = parse_ident_token(parts[2])?;
    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &name,
        "alter timeseries collections",
    )?;
    let tenant_id = identity.tenant_id;
    let updates = parse_set_clause(&parts[4..])?;
    let partition_by = match updates.get("partition_by").and_then(JsonValue::as_str) {
        Some(text) => {
            nodedb_types::timeseries::PartitionInterval::parse(text)
                .map_err(|e| ddl_err("22023", format!("invalid partition_by '{text}': {e}")))?;
            Some(text.to_string())
        }
        None => None,
    };

    {
        let catalog = state.credentials.catalog();
        let mut coll = catalog
            .get_collection(database_id, tenant_id.as_u64(), &name)
            .map_err(|e| DdlError::from_error(&e))?
            .ok_or_else(|| ddl_err("42P01", format!("collection '{name}' does not exist")))?;

        if !coll.collection_type.is_timeseries() {
            return Err(ddl_err(
                "42809",
                format!("'{name}' is not a timeseries collection"),
            ));
        }

        let mut config = match coll.get_timeseries_config() {
            Some(JsonValue::Object(existing)) => existing,
            _ => Map::new(),
        };
        config.extend(updates);
        coll.timeseries_config = Some(JsonValue::Object(config).to_string());
        // The collection type's interval is the one a flush reads. It governs
        // only flushes after this ALTER: a sealed partition keeps its cut.
        if let Some(text) = partition_by
            && let nodedb_types::CollectionType::Columnar(
                nodedb_types::ColumnarProfile::Timeseries { interval, .. },
            ) = &mut coll.collection_type
        {
            *interval = text;
        }

        let outcome = persist_collection_replicated(state, &coll)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        // A proposal buffered in an open transaction runs no post-apply, so
        // the Data Plane learns the new interval here.
        super::super::collection::register_proposed_collection(state, outcome, &coll)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
    }

    tracing::info!(collection = name, "timeseries config updated");

    Ok(vec![DdlResult::Status {
        command: "ALTER TIMESERIES".to_string(),
        rows_affected: None,
    }])
}

/// Parse the `(key = 'value', ...)` list after `SET`. Keys are lower-cased and
/// values unquoted. An empty list or a pair without `=` is refused.
fn parse_set_clause(tokens: &[&str]) -> Result<Map<String, JsonValue>, DdlError> {
    let clause = tokens.join(" ");
    let inner = clause
        .trim()
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| ddl_err("42601", SYNTAX))?;
    let mut config = Map::new();
    for pair in inner.split(',') {
        let pair = pair.trim();
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| ddl_err("42601", format!("expected key = 'value', got '{pair}'")))?;
        let key = key.trim().to_lowercase();
        if key.is_empty() {
            return Err(ddl_err("42601", format!("missing key in '{pair}'")));
        }
        let value = value.trim().trim_matches('\'').trim_matches('"');
        config.insert(key, JsonValue::String(value.to_string()));
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Result<Map<String, JsonValue>, DdlError> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        parse_set_clause(&parts[4..])
    }

    #[test]
    fn the_set_list_is_parsed() {
        let config =
            parse("ALTER TIMESERIES m SET (partition_by = '3d', Retention_Period = '30d')")
                .expect("parses");
        assert_eq!(config["partition_by"], "3d");
        assert_eq!(config["retention_period"], "30d");
    }

    #[test]
    fn a_pair_without_equals_is_refused() {
        assert!(parse("ALTER TIMESERIES m SET (partition_by)").is_err());
    }

    #[test]
    fn a_list_without_parentheses_is_refused() {
        assert!(parse("ALTER TIMESERIES m SET partition_by = '3d'").is_err());
    }
}
