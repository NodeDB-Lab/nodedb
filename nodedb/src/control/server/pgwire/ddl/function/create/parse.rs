//! Parse a `CREATE [OR REPLACE] FUNCTION` statement into a
//! typed `ParsedCreateFunction`.

use pgwire::error::PgWireResult;

use crate::control::security::catalog::{FunctionParam, FunctionVolatility};

use super::super::super::super::types::sqlstate_error;
use super::super::parse::parse_function_header;

/// Parsed components of a `CREATE FUNCTION` statement.
pub struct ParsedCreateFunction {
    pub or_replace: bool,
    pub name: String,
    pub parameters: Vec<FunctionParam>,
    pub return_type: String,
    pub volatility: FunctionVolatility,
    pub body_sql: String,
}

/// Parse a CREATE [OR REPLACE] FUNCTION statement.
///
/// Grammar:
/// ```text
/// CREATE [OR REPLACE] FUNCTION <name>(<param_name> <type> [, ...])
///   RETURNS <type>
///   [IMMUTABLE | STABLE | VOLATILE]
///   AS <sql_expression> ;
/// ```
pub fn parse_create_function(sql: &str) -> PgWireResult<ParsedCreateFunction> {
    // Use shared header parser — SQL functions terminate return type at AS/volatility.
    let header = parse_function_header(sql, &[" AS ", " IMMUTABLE ", " STABLE ", " VOLATILE "])?;

    let (volatility, body_part) = extract_volatility_and_body(&header.rest)?;

    let body_sql = body_part.trim().trim_end_matches(';').trim().to_string();
    if body_sql.is_empty() {
        return Err(sqlstate_error("42601", "function body is empty"));
    }

    Ok(ParsedCreateFunction {
        or_replace: header.or_replace,
        name: header.name,
        parameters: header.parameters,
        return_type: header.return_type,
        volatility,
        body_sql,
    })
}

/// Extract optional volatility keyword and the body after AS.
fn extract_volatility_and_body(s: &str) -> PgWireResult<(FunctionVolatility, &str)> {
    let upper = s.to_uppercase();
    let mut rest = s;
    let mut volatility = FunctionVolatility::Immutable; // default

    for kw in ["IMMUTABLE", "STABLE", "VOLATILE"] {
        if upper.starts_with(kw) {
            volatility = FunctionVolatility::parse(kw).unwrap_or_default();
            rest = s[kw.len()..].trim();
            break;
        }
    }

    let rest_upper = rest.to_uppercase();
    if !rest_upper.starts_with("AS ") && !rest_upper.starts_with("AS\n") {
        if rest_upper.starts_with("AS") {
            return Err(sqlstate_error("42601", "expected function body after AS"));
        }
        return Err(sqlstate_error("42601", "expected AS <body>"));
    }
    let body = rest["AS".len()..].trim();

    Ok((volatility, body))
}
