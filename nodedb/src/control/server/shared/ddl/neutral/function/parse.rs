// SPDX-License-Identifier: BUSL-1.1

//! Shared parsing helpers for function DDL statements.
//!
//! SQL type mapping, identifier validation, parameter parsing, and
//! utility functions used by both CREATE and DROP handlers.
//!
//! Ported verbatim from the pgwire `ddl::function::parse` helpers; only the
//! error type changed from pgwire `PgWireError` to the protocol-neutral
//! [`DdlError`]. `find_matching_paren` is inlined here (the pgwire helper
//! delegated to the pgwire-private `ddl::parse_utils`) to keep this family
//! self-contained.

use arrow::datatypes::DataType;
use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::security::catalog::FunctionParam;

use super::super::super::result::DdlError;

// ─── SQL type mapping ────────────────────────────────────────────────────────

/// Map SQL type name to Arrow DataType.
pub(super) fn sql_type_to_arrow(sql_type: &str) -> Option<DataType> {
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

// ─── Identifier & parameter parsing ──────────────────────────────────────────

/// Validate that a name is a legal SQL identifier (alphanumeric + underscore).
pub(super) fn validate_identifier(name: &str) -> Result<(), DdlError> {
    if name.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "identifier cannot be empty".to_string(),
        });
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: format!("invalid identifier: '{name}'"),
        });
    }
    if name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: format!("identifier cannot start with digit: '{name}'"),
        });
    }
    Ok(())
}

/// Parse comma-separated parameter list: `"email TEXT, threshold FLOAT"`.
pub(super) fn parse_parameters(params_str: &str) -> Result<Vec<FunctionParam>, DdlError> {
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
            return Err(DdlError {
                sqlstate: "42601".to_string(),
                message: format!("parameter must have name and type: '{part}'"),
            });
        }
        let param_name = tokens[0].to_lowercase();
        validate_identifier(&param_name)?;
        // Type may be multi-word (e.g., "DOUBLE PRECISION", "FLOAT[]").
        let param_type = tokens[1..].join(" ").to_uppercase();
        if sql_type_to_arrow(&param_type).is_none() {
            return Err(DdlError {
                sqlstate: "42601".to_string(),
                message: format!("unsupported parameter type: '{param_type}'"),
            });
        }
        params.push(FunctionParam {
            name: param_name,
            data_type: param_type,
        });
    }
    Ok(params)
}

/// Find the matching closing paren for the open paren at `start`.
///
/// Returns the index of the closing `)`, or `None` if unmatched.
pub(super) fn find_matching_paren(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate().skip(start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

// ─── Shared CREATE FUNCTION header parsing ──────────────────────────────────

/// Result of parsing `CREATE [OR REPLACE] FUNCTION name(params) RETURNS type`.
pub(super) struct FunctionHeader {
    pub or_replace: bool,
    pub name: String,
    pub parameters: Vec<FunctionParam>,
    pub return_type: String,
    /// The remainder of the SQL string after the return type.
    pub rest: String,
}

/// Parse the common header of a CREATE FUNCTION statement:
/// `CREATE [OR REPLACE] FUNCTION <name>(<params>) RETURNS <type>`
///
/// `terminators` are keywords (with leading space) that end the return type
/// (e.g. `[" AS ", " IMMUTABLE ", " LANGUAGE "]`).
///
/// Returns the parsed header and the remaining SQL after the return type,
/// which varies by language (volatility+AS for SQL, LANGUAGE WASM for WASM).
pub(super) fn parse_function_header(
    sql: &str,
    terminators: &[&str],
) -> Result<FunctionHeader, DdlError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    let (or_replace, after) = if upper.starts_with("CREATE OR REPLACE FUNCTION ") {
        (true, &trimmed["CREATE OR REPLACE FUNCTION ".len()..])
    } else if upper.starts_with("CREATE FUNCTION ") {
        (false, &trimmed["CREATE FUNCTION ".len()..])
    } else {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected CREATE FUNCTION".to_string(),
        });
    };

    // Name
    let paren_open = after.find('(').ok_or_else(|| DdlError {
        sqlstate: "42601".to_string(),
        message: "expected '(' after function name".to_string(),
    })?;
    let name = after[..paren_open].trim().to_lowercase();
    if name.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "function name is required".to_string(),
        });
    }
    validate_identifier(&name)?;

    // Parameters
    let paren_close = find_matching_paren(after, paren_open).ok_or_else(|| DdlError {
        sqlstate: "42601".to_string(),
        message: "unmatched '(' in parameter list".to_string(),
    })?;
    let params_str = &after[paren_open + 1..paren_close];
    let parameters = parse_parameters(params_str)?;

    // RETURNS <type>
    let after_params = after[paren_close + 1..].trim();
    let after_params_upper = after_params.to_uppercase();
    if !after_params_upper.starts_with("RETURNS ") {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected RETURNS <type>".to_string(),
        });
    }
    let after_returns = after_params["RETURNS ".len()..].trim();

    // Find the earliest terminator keyword to delimit the return type.
    let after_returns_upper = after_returns.to_uppercase();
    let mut earliest = after_returns.len();
    for term in terminators {
        if let Some(pos) = find_ascii_case_insensitive(after_returns, term) {
            earliest = earliest.min(pos);
        }
        // Handle keyword at end of string (no trailing space).
        let trimmed_term = term.trim();
        if after_returns_upper.ends_with(trimmed_term) {
            let pos = after_returns.len() - trimmed_term.len();
            earliest = earliest.min(pos);
        }
    }

    let return_type = after_returns[..earliest].trim().to_uppercase();
    let rest = after_returns[earliest..].trim().to_string();

    if return_type.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "return type is required".to_string(),
        });
    }

    Ok(FunctionHeader {
        or_replace,
        name,
        parameters,
        return_type,
        rest,
    })
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
