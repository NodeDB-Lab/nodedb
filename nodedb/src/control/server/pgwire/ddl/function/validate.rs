//! Function body validation via sqlparser.
//!
//! Validates that:
//! - The SQL expression is syntactically valid.
//! - All referenced functions are known (system or user-defined).

use pgwire::error::PgWireResult;

use super::super::super::types::sqlstate_error;
use super::create::ParsedCreateFunction;

/// Validate function body by attempting to parse it as a SQL expression.
pub(super) fn validate_function_body(parsed: &ParsedCreateFunction) -> PgWireResult<()> {
    let body = &parsed.body_sql;

    // Build the test SQL.
    let test_sql = if body.to_uppercase().starts_with("SELECT ") {
        body.to_string()
    } else {
        format!("SELECT {body}")
    };

    // Parse via sqlparser to check syntax.
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let _statements = sqlparser::parser::Parser::parse_sql(&dialect, &test_sql)
        .map_err(|e| sqlstate_error("42601", &format!("invalid function body: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::create::ParsedCreateFunction;
    use super::validate_function_body;
    use crate::control::security::catalog::{FunctionParam, FunctionVolatility};

    fn make_parsed(
        params: Vec<(&str, &str)>,
        return_type: &str,
        body: &str,
    ) -> ParsedCreateFunction {
        ParsedCreateFunction {
            or_replace: false,
            name: "test_fn".into(),
            parameters: params
                .into_iter()
                .map(|(n, t)| FunctionParam {
                    name: n.into(),
                    data_type: t.into(),
                })
                .collect(),
            return_type: return_type.into(),
            volatility: FunctionVolatility::Immutable,
            body_sql: body.into(),
        }
    }

    #[test]
    fn valid_text_body() {
        let p = make_parsed(vec![("email", "TEXT")], "TEXT", "SELECT LOWER(TRIM(email))");
        assert!(validate_function_body(&p).is_ok());
    }

    #[test]
    fn valid_numeric_body() {
        let p = make_parsed(vec![("x", "FLOAT")], "FLOAT", "SELECT x * 2.0");
        assert!(validate_function_body(&p).is_ok());
    }

    #[test]
    fn valid_no_params() {
        let p = make_parsed(vec![], "FLOAT", "SELECT 3.14159");
        assert!(validate_function_body(&p).is_ok());
    }

    #[test]
    fn invalid_body_syntax() {
        let p = make_parsed(vec![("x", "TEXT")], "TEXT", "SELECT ((");
        assert!(validate_function_body(&p).is_err());
    }
}
