// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handler for CONVERT COLLECTION.
//!
//! Syntax:
//! - `CONVERT COLLECTION <name> TO document`
//! - `CONVERT COLLECTION <name> TO strict (col1 TYPE, col2 TYPE, ...)`
//! - `CONVERT COLLECTION <name> TO kv`
//!
//! Ported from the pgwire `ddl::convert` handler. The accepted-target
//! validation (document_schemaless / document_strict / kv — columnar /
//! timeseries / spatial rejected), the catalog read + write, the Data Plane
//! conversion dispatch, and the typeguard → CHECK-constraint carry-over are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};
use nodedb_types::DatabaseId;
use std::time::Duration;

use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch::dispatch_async;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::MetaOp;

use super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: &str) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.to_string(),
    }
}

/// CONVERT COLLECTION <name> TO <target_type> [(<col_defs>)]
pub async fn convert_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, target_type, explicit_columns) = parse_convert_sql(sql)?;
    let tenant_id = identity.tenant_id;

    // Validate collection exists.
    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| {
            err(
                "42P01",
                &format!("collection '{collection}' does not exist"),
            )
        })?;

    // Build columns before dispatch — needed for both Data Plane and catalog.
    let columns: Option<Vec<nodedb_types::columnar::ColumnDef>> = match target_type.as_str() {
        "document_strict" | "kv" => {
            let cols = if let Some(cols) = explicit_columns {
                cols
            } else if !coll.type_guards.is_empty() {
                typeguards_to_column_defs(&coll.type_guards)?
            } else {
                return Err(err(
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
            .map_err(|e| err("XX000", &format!("schema serialization: {e}")))?
    } else {
        String::new()
    };

    // Dispatch to Data Plane: re-encode if needed (strict = Binary Tuple).
    let plan = PhysicalPlan::Meta(MetaOp::ConvertCollection {
        collection: collection.clone(),
        target_type: target_type.clone(),
        schema_json: schema_json_for_dp,
    });

    dispatch_async(
        state,
        tenant_id,
        database_id,
        &collection,
        plan,
        Duration::from_secs(60),
    )
    .await
    .map_err(|e| err("XX000", &format!("conversion failed: {e}")))?;

    // Update catalog collection type.
    let new_type = match target_type.as_str() {
        "document_schemaless" => nodedb_types::CollectionType::document(),
        "document_strict" | "kv" => {
            let columns = columns.expect(
                "invariant: columns is Some for document_strict/kv targets, validated above",
            );
            let schema = nodedb_types::columnar::StrictSchema {
                columns,
                version: 1,
                dropped_columns: Vec::new(),
                bitemporal: false,
            };
            if target_type == "kv" {
                nodedb_types::CollectionType::kv(schema)
            } else {
                nodedb_types::CollectionType::strict(schema)
            }
        }
        _ => {
            return Err(err(
                "42601",
                &format!("unsupported target type: {target_type}"),
            ));
        }
    };

    coll.collection_type = new_type;

    // CONVERT TO document_strict: if collection had typeguards, carry over CHECK constraints
    // and drop typeguard definitions (strict schema subsumes type checking).
    if target_type == "document_strict" && !coll.type_guards.is_empty() {
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
        .put_collection(database_id, &coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    tracing::info!(
        %collection,
        target_type,
        tenant = tenant_id.as_u64(),
        "collection converted"
    );

    Ok(vec![DdlResult::Status {
        command: "CONVERT COLLECTION".to_string(),
        rows_affected: None,
    }])
}

// ── SQL Parsing ──────────────────────────────────────────────────────────

/// Parse CONVERT COLLECTION SQL.
///
/// Returns `(collection_name, target_type, explicit_columns)`.
/// `explicit_columns` is `None` for `TO document` or `TO strict` without parens.
fn parse_convert_sql(
    sql: &str,
) -> Result<
    (
        String,
        String,
        Option<Vec<nodedb_types::columnar::ColumnDef>>,
    ),
    DdlError,
> {
    // Extract collection name: CONVERT COLLECTION <name> TO ...
    let coll_pos = find_ascii_case_insensitive(sql, "COLLECTION ")
        .ok_or_else(|| err("42601", "expected COLLECTION keyword"))?;
    let after_coll = sql[coll_pos + 11..].trim_start();
    let collection = after_coll
        .split_whitespace()
        .next()
        .ok_or_else(|| err("42601", "missing collection name"))?
        .to_lowercase();

    // Extract target type: TO <type>
    let to_pos = find_ascii_case_insensitive_from(sql, " TO ", coll_pos + 11)
        .ok_or_else(|| err("42601", "expected TO <type> clause"))?;
    let after_to = sql[to_pos + 4..].trim_start();
    let target_type = after_to
        .split_whitespace()
        .next()
        .ok_or_else(|| err("42601", "missing target type after TO"))?
        .to_lowercase()
        .trim_matches('(')
        .to_string();

    match target_type.as_str() {
        "document_schemaless" => Ok((collection, "document_schemaless".into(), None)),
        "document_strict" | "kv" => {
            if after_to.contains('(') {
                let cols = parse_column_defs(after_to)?;
                Ok((collection, target_type, Some(cols)))
            } else {
                Ok((collection, target_type, None))
            }
        }
        "document" | "doc" => Err(err(
            "42601",
            "deprecated target type 'document'; use 'document_schemaless'",
        )),
        "strict" => Err(err(
            "42601",
            "deprecated target type 'strict'; use 'document_strict'",
        )),
        "key_value" | "keyvalue" => Err(err("42601", "deprecated target type; use 'kv'")),
        other => Err(err(
            "42601",
            &format!(
                "unsupported target type: '{other}' \
                 (expected document_schemaless, document_strict, kv)"
            ),
        )),
    }
}

/// Parse `(col1 TYPE, col2 TYPE, ...)` into `Vec<ColumnDef>`.
fn parse_column_defs(s: &str) -> Result<Vec<nodedb_types::columnar::ColumnDef>, DdlError> {
    use nodedb_types::columnar::ColumnDef;

    let open = s
        .find('(')
        .ok_or_else(|| err("42601", "expected (column definitions) after type"))?;
    let close = s
        .rfind(')')
        .ok_or_else(|| err("42601", "missing closing parenthesis"))?;
    if close <= open {
        return Err(err("42601", "empty column definitions"));
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
            return Err(err(
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
        return Err(err("42601", "at least one column required"));
    }

    Ok(columns)
}

/// Map SQL type names to ColumnType.
fn sql_type_to_column_type(sql_type: &str) -> nodedb_types::columnar::ColumnType {
    use nodedb_types::columnar::ColumnType;
    match sql_type {
        "INT" | "INTEGER" | "INT8" | "BIGINT" | "INT4" | "INT2" | "SMALLINT" => ColumnType::Int64,
        "FLOAT" | "FLOAT8" | "DOUBLE" | "REAL" => ColumnType::Float64,
        "NUMERIC" | "DECIMAL" => ColumnType::Decimal {
            precision: 38,
            scale: 10,
        },
        "BOOL" | "BOOLEAN" => ColumnType::Bool,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => ColumnType::Timestamp,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => ColumnType::Timestamptz,
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
) -> Result<Vec<nodedb_types::columnar::ColumnDef>, DdlError> {
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
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => ColumnType::Timestamp,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => ColumnType::Timestamptz,
        "DECIMAL" | "NUMERIC" => ColumnType::Decimal {
            precision: 38,
            scale: 10,
        },
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
    fn parse_convert_to_document_schemaless() {
        let (coll, target, cols) =
            parse_convert_sql("CONVERT COLLECTION users TO document_schemaless").unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "document_schemaless");
        assert!(cols.is_none());
    }

    #[test]
    fn parse_convert_deprecated_document_rejected() {
        assert!(parse_convert_sql("CONVERT COLLECTION users TO document").is_err());
    }

    #[test]
    fn parse_convert_to_document_strict() {
        let sql =
            "CONVERT COLLECTION users TO document_strict (name VARCHAR, age INT, active BOOLEAN)";
        let (coll, target, cols) = parse_convert_sql(sql).unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "document_strict");

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
    fn parse_convert_deprecated_strict_rejected() {
        let sql = "CONVERT COLLECTION users TO strict (name VARCHAR)";
        assert!(parse_convert_sql(sql).is_err());
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
        let sql = "CONVERT COLLECTION users TO document_strict (id INT NOT NULL, name VARCHAR)";
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

    #[test]
    fn sql_type_to_column_type_distinguishes_timestamp_from_timestamptz() {
        use nodedb_types::columnar::ColumnType;
        assert!(matches!(
            sql_type_to_column_type("TIMESTAMP"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            sql_type_to_column_type("TIMESTAMP WITHOUT TIME ZONE"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            sql_type_to_column_type("TIMESTAMPTZ"),
            ColumnType::Timestamptz
        ));
        assert!(matches!(
            sql_type_to_column_type("TIMESTAMP WITH TIME ZONE"),
            ColumnType::Timestamptz
        ));
    }

    #[test]
    fn typeguard_type_to_column_type_distinguishes_timestamp_from_timestamptz() {
        use nodedb_types::columnar::ColumnType;
        assert!(matches!(
            typeguard_type_to_column_type("TIMESTAMP"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            typeguard_type_to_column_type("TIMESTAMP WITHOUT TIME ZONE"),
            ColumnType::Timestamp
        ));
        assert!(matches!(
            typeguard_type_to_column_type("TIMESTAMPTZ"),
            ColumnType::Timestamptz
        ));
        assert!(matches!(
            typeguard_type_to_column_type("TIMESTAMP WITH TIME ZONE"),
            ColumnType::Timestamptz
        ));
    }
}
