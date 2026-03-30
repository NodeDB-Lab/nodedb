//! Shared parsing helpers for function DDL statements.
//!
//! SQL type mapping, identifier validation, parameter parsing, and
//! utility functions used by both CREATE and DROP handlers.

use datafusion::arrow::datatypes::DataType;
use pgwire::error::PgWireResult;

use crate::control::security::catalog::FunctionParam;

use super::super::super::types::sqlstate_error;

// ─── SQL type mapping ────────────────────────────────────────────────────────

/// Map SQL type name to Arrow DataType.
pub(crate) fn sql_type_to_arrow(sql_type: &str) -> Option<DataType> {
    match sql_type.to_uppercase().as_str() {
        "TEXT" | "VARCHAR" | "STRING" => Some(DataType::Utf8),
        "INT" | "INT4" | "INTEGER" => Some(DataType::Int32),
        "INT2" | "SMALLINT" => Some(DataType::Int16),
        "INT8" | "BIGINT" => Some(DataType::Int64),
        "FLOAT" | "FLOAT4" | "REAL" => Some(DataType::Float32),
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => Some(DataType::Float64),
        "BOOL" | "BOOLEAN" => Some(DataType::Boolean),
        "BYTEA" | "BINARY" => Some(DataType::Binary),
        _ => None,
    }
}

/// Map SQL type to DataFusion SQL syntax (for CREATE TABLE statements).
pub(super) fn sql_type_to_arrow_sql(sql_type: &str) -> &'static str {
    match sql_type.to_uppercase().as_str() {
        "TEXT" | "VARCHAR" | "STRING" => "VARCHAR",
        "INT" | "INT4" | "INTEGER" => "INT",
        "INT2" | "SMALLINT" => "SMALLINT",
        "INT8" | "BIGINT" => "BIGINT",
        "FLOAT" | "FLOAT4" | "REAL" => "REAL",
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => "DOUBLE",
        "BOOL" | "BOOLEAN" => "BOOLEAN",
        "BYTEA" | "BINARY" => "BYTEA",
        _ => "VARCHAR", // fallback
    }
}

/// Check if a SQL type name is recognized.
pub(super) fn is_valid_sql_type(t: &str) -> bool {
    sql_type_to_arrow(t).is_some()
}

/// Check if two Arrow DataTypes are compatible for return type validation.
///
/// Allows numeric promotions (e.g. Int32 body → Float64 declared).
pub(super) fn types_compatible(actual: &DataType, expected: &DataType) -> bool {
    if actual == expected {
        return true;
    }
    // Allow Utf8 ↔ LargeUtf8.
    if matches!(
        (actual, expected),
        (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8)
    ) {
        return true;
    }
    // Allow numeric promotions.
    if actual.is_numeric() && expected.is_numeric() {
        return true;
    }
    false
}

/// Generate default literal values for parameters (used in the dummy CREATE TABLE).
pub(super) fn default_values_for_params(params: &[FunctionParam]) -> String {
    params
        .iter()
        .map(|p| match p.data_type.to_uppercase().as_str() {
            "TEXT" | "VARCHAR" | "STRING" => "'x'".to_string(),
            "INT" | "INT4" | "INTEGER" | "INT2" | "SMALLINT" | "INT8" | "BIGINT" => "0".to_string(),
            "FLOAT" | "FLOAT4" | "REAL" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => {
                "0.0".to_string()
            }
            "BOOL" | "BOOLEAN" => "true".to_string(),
            _ => "NULL".to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// ─── Identifier & parameter parsing ──────────────────────────────────────────

/// Validate that a name is a legal SQL identifier (alphanumeric + underscore).
pub(super) fn validate_identifier(name: &str) -> PgWireResult<()> {
    if name.is_empty() {
        return Err(sqlstate_error("42601", "identifier cannot be empty"));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(sqlstate_error(
            "42601",
            &format!("invalid identifier: '{name}'"),
        ));
    }
    if name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Err(sqlstate_error(
            "42601",
            &format!("identifier cannot start with digit: '{name}'"),
        ));
    }
    Ok(())
}

/// Parse comma-separated parameter list: `"email TEXT, threshold FLOAT"`.
pub(super) fn parse_parameters(params_str: &str) -> PgWireResult<Vec<FunctionParam>> {
    let trimmed = params_str.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut params = Vec::new();
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(sqlstate_error(
                "42601",
                &format!("parameter must have name and type: '{part}'"),
            ));
        }
        let param_name = tokens[0].to_lowercase();
        validate_identifier(&param_name)?;
        // Type may be multi-word (e.g., "DOUBLE PRECISION", "FLOAT[]").
        let param_type = tokens[1..].join(" ").to_uppercase();
        if !is_valid_sql_type(&param_type) {
            return Err(sqlstate_error(
                "42601",
                &format!("unsupported parameter type: '{param_type}'"),
            ));
        }
        params.push(FunctionParam {
            name: param_name,
            data_type: param_type,
        });
    }
    Ok(params)
}

/// Find the matching closing paren for the open paren at `start`.
pub(super) fn find_matching_paren(s: &str, start: usize) -> Option<usize> {
    super::super::parse_utils::find_matching_paren(s, start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_type_mapping() {
        assert_eq!(sql_type_to_arrow("TEXT"), Some(DataType::Utf8));
        assert_eq!(sql_type_to_arrow("INT"), Some(DataType::Int32));
        assert_eq!(sql_type_to_arrow("BIGINT"), Some(DataType::Int64));
        assert_eq!(sql_type_to_arrow("FLOAT"), Some(DataType::Float32));
        assert_eq!(sql_type_to_arrow("DOUBLE"), Some(DataType::Float64));
        assert_eq!(sql_type_to_arrow("BOOLEAN"), Some(DataType::Boolean));
        assert_eq!(sql_type_to_arrow("NONSENSE"), None);
    }

    #[test]
    fn type_compatibility() {
        assert!(types_compatible(&DataType::Utf8, &DataType::Utf8));
        assert!(types_compatible(&DataType::Int32, &DataType::Float64));
        assert!(!types_compatible(&DataType::Utf8, &DataType::Int32));
    }

    #[test]
    fn valid_identifiers() {
        assert!(validate_identifier("foo").is_ok());
        assert!(validate_identifier("foo_bar").is_ok());
        assert!(validate_identifier("x1").is_ok());
    }

    #[test]
    fn invalid_identifiers() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("1bad").is_err());
        assert!(validate_identifier("a-b").is_err());
    }

    #[test]
    fn parse_params() {
        let params = parse_parameters("email TEXT, score FLOAT").unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, "email");
        assert_eq!(params[0].data_type, "TEXT");
        assert_eq!(params[1].name, "score");
        assert_eq!(params[1].data_type, "FLOAT");
    }

    #[test]
    fn parse_empty_params() {
        assert!(parse_parameters("").unwrap().is_empty());
        assert!(parse_parameters("  ").unwrap().is_empty());
    }

    #[test]
    fn matching_parens() {
        assert_eq!(find_matching_paren("(a, b)", 0), Some(5));
        assert_eq!(find_matching_paren("((a))", 0), Some(4));
        assert_eq!(find_matching_paren("(", 0), None);
    }
}
