//! DDL handler for CONVERT COLLECTION.
//!
//! Syntax:
//! - `CONVERT COLLECTION <name> TO document`
//! - `CONVERT COLLECTION <name> TO strict (col1 TYPE, col2 TYPE, ...)`
//! - `CONVERT COLLECTION <name> TO kv`

use std::time::Duration;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::MetaOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

/// CONVERT COLLECTION <name> TO <target_type> [(<col_defs>)]
pub async fn convert_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, target_type, explicit_columns) = parse_convert_sql(sql)?;
    let tenant_id = identity.tenant_id;

    // Validate collection exists.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };

    let mut coll = catalog
        .get_collection(tenant_id.as_u32(), &collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| {
            sqlstate_error(
                "42P01",
                &format!("collection '{collection}' does not exist"),
            )
        })?;

    // Build columns before dispatch — needed for both Data Plane and catalog.
    let columns: Option<Vec<nodedb_types::columnar::ColumnDef>> = match target_type.as_str() {
        "strict" | "kv" => {
            let cols = if let Some(cols) = explicit_columns {
                cols
            } else if !coll.type_guards.is_empty() {
                typeguards_to_column_defs(&coll.type_guards)?
            } else {
                return Err(sqlstate_error(
                    "42601",
                    "CONVERT TO strict requires column definitions or active typeguards",
                ));
            };
            Some(cols)
        }
        _ => None,
    };

    let schema_json_for_dp = if let Some(ref cols) = columns {
        sonic_rs::to_string(cols)
            .map_err(|e| sqlstate_error("XX000", &format!("schema serialization: {e}")))?
    } else {
        String::new()
    };

    // Dispatch to Data Plane: re-encode if needed (strict = Binary Tuple).
    let plan = PhysicalPlan::Meta(MetaOp::ConvertCollection {
        collection: collection.clone(),
        target_type: target_type.clone(),
        schema_json: schema_json_for_dp,
    });

    super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        &collection,
        plan,
        Duration::from_secs(60),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("conversion failed: {e}")))?;

    // Update catalog collection type.
    let new_type = match target_type.as_str() {
        "document" => nodedb_types::CollectionType::document(),
        "strict" | "kv" => {
            let columns = columns.unwrap(); // safe: validated above
            let schema = nodedb_types::columnar::StrictSchema {
                columns,
                version: 1,
                dropped_columns: Vec::new(),
            };
            if target_type == "kv" {
                nodedb_types::CollectionType::kv(schema)
            } else {
                nodedb_types::CollectionType::strict(schema)
            }
        }
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("unsupported target type: {target_type}"),
            ));
        }
    };

    coll.collection_type = new_type;

    // CONVERT TO strict: if collection had typeguards, carry over CHECK constraints
    // and drop typeguard definitions (strict schema subsumes type checking).
    if target_type == "strict" && !coll.type_guards.is_empty() {
        for guard in &coll.type_guards {
            if let Some(ref check_expr) = guard.check_expr {
                // Avoid duplicate names.
                let name = format!("_guard_{}", guard.field);
                if !coll.check_constraints.iter().any(|c| c.name == name) {
                    coll.check_constraints.push(
                        crate::control::security::catalog::types::CheckConstraintDef {
                            name,
                            check_sql: check_expr.clone(),
                            has_subquery: false,
                        },
                    );
                }
            }
        }
        coll.type_guards.clear();
    }

    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    tracing::info!(
        %collection,
        target_type,
        tenant = tenant_id.as_u32(),
        "collection converted"
    );

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "CONVERT COLLECTION",
    ))])
}

// ── SQL Parsing ──────────────────────────────────────────────────────────

/// Parse CONVERT COLLECTION SQL.
///
/// Returns `(collection_name, target_type, explicit_columns)`.
/// `explicit_columns` is `None` for `TO document` or `TO strict` without parens.
fn parse_convert_sql(
    sql: &str,
) -> PgWireResult<(
    String,
    String,
    Option<Vec<nodedb_types::columnar::ColumnDef>>,
)> {
    let upper = sql.to_uppercase();

    // Extract collection name: CONVERT COLLECTION <name> TO ...
    let coll_pos = upper
        .find("COLLECTION ")
        .ok_or_else(|| sqlstate_error("42601", "expected COLLECTION keyword"))?;
    let after_coll = sql[coll_pos + 11..].trim_start();
    let collection = after_coll
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    // Extract target type: TO <type>
    let to_pos = upper[coll_pos + 11..]
        .find(" TO ")
        .ok_or_else(|| sqlstate_error("42601", "expected TO <type> clause"))?;
    let after_to = sql[coll_pos + 11 + to_pos + 4..].trim_start();
    let target_type = after_to
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing target type after TO"))?
        .to_lowercase()
        .trim_matches('(')
        .to_string();

    match target_type.as_str() {
        "document" => Ok((collection, "document".into(), None)),
        "strict" | "kv" => {
            if after_to.contains('(') {
                let cols = parse_column_defs(after_to)?;
                Ok((collection, target_type, Some(cols)))
            } else {
                Ok((collection, target_type, None))
            }
        }
        other => Err(sqlstate_error(
            "42601",
            &format!("unsupported target type: {other} (expected document, strict, kv)"),
        )),
    }
}

/// Parse `(col1 TYPE, col2 TYPE, ...)` into `Vec<ColumnDef>`.
fn parse_column_defs(s: &str) -> PgWireResult<Vec<nodedb_types::columnar::ColumnDef>> {
    use nodedb_types::columnar::ColumnDef;

    let open = s
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected (column definitions) after type"))?;
    let close = s
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", "missing closing parenthesis"))?;
    if close <= open {
        return Err(sqlstate_error("42601", "empty column definitions"));
    }

    let inner = &s[open + 1..close];
    let mut columns: Vec<ColumnDef> = Vec::new();

    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(sqlstate_error(
                "42601",
                &format!("expected 'name TYPE' in column def: {part}"),
            ));
        }
        let col_name = tokens[0].to_lowercase();
        let col_type = tokens[1].to_uppercase();
        let nullable = !tokens
            .windows(2)
            .any(|w| w[0].eq_ignore_ascii_case("NOT") && w[1].eq_ignore_ascii_case("NULL"))
            && !tokens.iter().any(|t| t.eq_ignore_ascii_case("NOTNULL"));
        let primary_key = tokens
            .windows(2)
            .any(|w| w[0].eq_ignore_ascii_case("PRIMARY") && w[1].eq_ignore_ascii_case("KEY"));

        let ct = sql_type_to_column_type(&col_type);
        let mut col = if nullable {
            ColumnDef::nullable(col_name, ct)
        } else {
            ColumnDef::required(col_name, ct)
        };
        if primary_key {
            col = col.with_primary_key();
        }
        columns.push(col);
    }

    if columns.is_empty() {
        return Err(sqlstate_error("42601", "at least one column required"));
    }

    Ok(columns)
}

/// Map SQL type names to ColumnType.
fn sql_type_to_column_type(sql_type: &str) -> nodedb_types::columnar::ColumnType {
    use nodedb_types::columnar::ColumnType;
    match sql_type {
        "INT" | "INTEGER" | "INT8" | "BIGINT" | "INT4" | "INT2" | "SMALLINT" => ColumnType::Int64,
        "FLOAT" | "FLOAT8" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => ColumnType::Float64,
        "BOOL" | "BOOLEAN" => ColumnType::Bool,
        "TIMESTAMP" | "TIMESTAMPTZ" => ColumnType::Timestamp,
        "BLOB" | "BYTEA" | "BINARY" => ColumnType::Bytes,
        "UUID" => ColumnType::Uuid,
        "JSON" | "JSONB" => ColumnType::Json,
        "GEOMETRY" => ColumnType::Geometry,
        _ => ColumnType::String, // VARCHAR, TEXT, STRING, and anything else
    }
}

/// Convert typeguard field definitions to strict schema column definitions.
///
/// Each typeguard field becomes a column. The type expression is mapped to ColumnType.
/// REQUIRED fields become NOT NULL. DEFAULT expressions carry over.
fn typeguards_to_column_defs(
    guards: &[nodedb_types::TypeGuardFieldDef],
) -> PgWireResult<Vec<nodedb_types::columnar::ColumnDef>> {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    // Always include an `id` column as primary key.
    let mut columns = vec![ColumnDef::required("id", ColumnType::String).with_primary_key()];

    for guard in guards {
        // Skip dot-path fields (nested) — strict schema is flat.
        if guard.field.contains('.') {
            continue;
        }
        // Skip if already added (e.g., "id").
        if guard.field == "id" || columns.iter().any(|c| c.name == guard.field) {
            continue;
        }

        let ct = typeguard_type_to_column_type(&guard.type_expr);
        let mut col = if guard.required {
            ColumnDef::required(guard.field.clone(), ct)
        } else {
            ColumnDef::nullable(guard.field.clone(), ct)
        };
        col.default = guard.default_expr.clone().or(guard.value_expr.clone());
        columns.push(col);
    }

    Ok(columns)
}

/// Map a typeguard type expression string to a ColumnType.
fn typeguard_type_to_column_type(type_expr: &str) -> nodedb_types::columnar::ColumnType {
    use nodedb_types::columnar::ColumnType;

    // Strip union types — take the first non-NULL type.
    let base = type_expr
        .split('|')
        .map(|s| s.trim())
        .find(|s| !s.eq_ignore_ascii_case("NULL"))
        .unwrap_or(type_expr)
        .to_uppercase();

    // Strip generic parameters: ARRAY<STRING> → ARRAY, SET<INT> → SET.
    let base = base.split('<').next().unwrap_or(&base);

    match base {
        "INT" | "INTEGER" | "BIGINT" | "INT64" => ColumnType::Int64,
        "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => ColumnType::Float64,
        "STRING" | "TEXT" | "VARCHAR" => ColumnType::String,
        "BOOL" | "BOOLEAN" => ColumnType::Bool,
        "BYTES" | "BYTEA" | "BLOB" => ColumnType::Bytes,
        "TIMESTAMP" | "TIMESTAMPTZ" => ColumnType::Timestamp,
        "DECIMAL" | "NUMERIC" => ColumnType::Decimal,
        "UUID" => ColumnType::Uuid,
        "GEOMETRY" => ColumnType::Geometry,
        "JSON" | "JSONB" | "OBJECT" => ColumnType::Json,
        _ => ColumnType::String, // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_convert_to_document() {
        let (coll, target, cols) =
            parse_convert_sql("CONVERT COLLECTION users TO document").unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "document");
        assert!(cols.is_none());
    }

    #[test]
    fn parse_convert_to_strict() {
        let sql = "CONVERT COLLECTION users TO strict (name VARCHAR, age INT, active BOOLEAN)";
        let (coll, target, cols) = parse_convert_sql(sql).unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "strict");

        let cols = cols.unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "name");
        assert!(matches!(
            cols[0].column_type,
            nodedb_types::columnar::ColumnType::String
        ));
        assert_eq!(cols[1].name, "age");
        assert!(matches!(
            cols[1].column_type,
            nodedb_types::columnar::ColumnType::Int64
        ));
        assert_eq!(cols[2].name, "active");
        assert!(matches!(
            cols[2].column_type,
            nodedb_types::columnar::ColumnType::Bool
        ));
    }

    #[test]
    fn parse_convert_to_kv() {
        let sql = "CONVERT COLLECTION cache TO kv (key VARCHAR, value BLOB)";
        let (coll, target, cols) = parse_convert_sql(sql).unwrap();
        assert_eq!(coll, "cache");
        assert_eq!(target, "kv");
        assert!(cols.is_some());
    }

    #[test]
    fn parse_convert_not_null_constraint() {
        let sql = "CONVERT COLLECTION users TO strict (id INT NOT NULL, name VARCHAR)";
        let (_, _, cols) = parse_convert_sql(sql).unwrap();
        let cols = cols.unwrap();
        assert!(!cols[0].nullable);
        assert!(cols[1].nullable);
    }

    #[test]
    fn parse_convert_missing_to_errors() {
        assert!(parse_convert_sql("CONVERT COLLECTION users").is_err());
    }

    #[test]
    fn parse_convert_unknown_type_errors() {
        assert!(parse_convert_sql("CONVERT COLLECTION users TO graph").is_err());
    }
}
