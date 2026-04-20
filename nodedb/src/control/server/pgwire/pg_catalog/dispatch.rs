//! pg_catalog query interception and dispatch.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::dropped_collections::dropped_collections;
use super::tables;

/// Try to handle a SQL query as a pg_catalog virtual-table lookup.
///
/// Returns `Some(Ok(response))` if the query targets a known
/// pg_catalog table, `None` if the query should fall through to the
/// normal planner. The `upper` argument is the uppercased SQL.
pub fn try_pg_catalog(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    upper: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let table = extract_pg_catalog_table(upper)?;
    let result = match table {
        "pg_database" => tables::pg_database(),
        "pg_namespace" => tables::pg_namespace(),
        "pg_type" => tables::pg_type(),
        "pg_class" => tables::pg_class(state, identity),
        "pg_attribute" => tables::pg_attribute(state, identity),
        "pg_index" => tables::pg_index(),
        "pg_authid" => tables::pg_authid(state, identity),
        "_system.dropped_collections" => dropped_collections(state, identity),
        _ => return None,
    };
    Some(result)
}

/// Static schema for a pg_catalog virtual table, for use at Parse/Describe
/// time in the extended-query protocol. Returns `None` for unknown tables.
///
/// The schema MUST match the `DataRowEncoder` schema used by the matching
/// row generator in `tables.rs`.
pub fn pg_catalog_schema(table: &str) -> Option<Vec<pgwire::api::results::FieldInfo>> {
    use crate::control::server::pgwire::types::{bool_field, int4_field, int8_field, text_field};
    let fields = match table {
        "pg_database" => vec![
            int8_field("oid"),
            text_field("datname"),
            text_field("datdba"),
            text_field("encoding"),
        ],
        "pg_namespace" => vec![
            int8_field("oid"),
            text_field("nspname"),
            int8_field("nspowner"),
        ],
        "pg_type" => vec![
            int8_field("oid"),
            text_field("typname"),
            int8_field("typnamespace"),
            int4_field("typlen"),
            text_field("typtype"),
        ],
        "pg_class" => vec![
            int8_field("oid"),
            text_field("relname"),
            int8_field("relnamespace"),
            text_field("relkind"),
            int8_field("relowner"),
        ],
        "pg_attribute" => vec![
            int8_field("attrelid"),
            text_field("attname"),
            int8_field("atttypid"),
            int4_field("attnum"),
            int4_field("attlen"),
            bool_field("attnotnull"),
        ],
        "pg_index" => vec![
            int8_field("indexrelid"),
            int8_field("indrelid"),
            bool_field("indisunique"),
            bool_field("indisprimary"),
        ],
        "pg_authid" => vec![
            int8_field("oid"),
            text_field("rolname"),
            bool_field("rolsuper"),
            bool_field("rolcanlogin"),
        ],
        "_system.dropped_collections" => vec![
            int8_field("tenant_id"),
            text_field("name"),
            text_field("owner"),
            text_field("engine_type"),
            int8_field("deactivated_at_ns"),
            int8_field("retention_expires_at_ns"),
        ],
        _ => return None,
    };
    Some(fields)
}

/// Extract the first `pg_catalog.<table>` or bare `pg_<table>`
/// reference from a FROM clause. Returns the lowercase table name
/// if found.
pub fn extract_pg_catalog_table(upper: &str) -> Option<&'static str> {
    // NodeDB-native `_system.*` views: match the full qualified form
    // (no bare alias — `dropped_collections` alone is ambiguous and
    // could shadow a user-created collection).
    if upper.contains("_SYSTEM.DROPPED_COLLECTIONS") {
        return Some("_system.dropped_collections");
    }
    let known = [
        "pg_database",
        "pg_namespace",
        "pg_type",
        "pg_class",
        "pg_attribute",
        "pg_index",
        "pg_authid",
    ];
    for table in &known {
        let qualified = format!("PG_CATALOG.{}", table.to_uppercase());
        let bare = table.to_uppercase();
        if upper.contains(&qualified) || upper.contains(&bare) {
            return Some(table);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_qualified_table() {
        let sql = "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_class")
        );
    }

    #[test]
    fn extracts_bare_table() {
        let sql = "SELECT oid, typname FROM pg_type";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_type")
        );
    }

    #[test]
    fn no_match_for_regular_query() {
        let sql = "SELECT * FROM users WHERE id = 1";
        assert_eq!(extract_pg_catalog_table(&sql.to_uppercase()), None);
    }

    #[test]
    fn handles_join_with_pg_catalog() {
        let sql =
            "SELECT c.oid FROM pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid";
        assert_eq!(
            extract_pg_catalog_table(&sql.to_uppercase()),
            Some("pg_namespace")
        );
    }
}
