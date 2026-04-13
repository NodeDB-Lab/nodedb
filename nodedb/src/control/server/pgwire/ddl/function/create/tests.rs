//! Parser tests for `CREATE [OR REPLACE] FUNCTION`.

use crate::control::security::catalog::FunctionVolatility;

use super::parse::parse_create_function;

#[test]
fn parse_simple_expression_function() {
    let sql =
        "CREATE FUNCTION normalize_email(email TEXT) RETURNS TEXT AS SELECT LOWER(TRIM(email))";
    let parsed = parse_create_function(sql).unwrap();
    assert_eq!(parsed.name, "normalize_email");
    assert!(!parsed.or_replace);
    assert_eq!(parsed.parameters.len(), 1);
    assert_eq!(parsed.parameters[0].name, "email");
    assert_eq!(parsed.parameters[0].data_type, "TEXT");
    assert_eq!(parsed.return_type, "TEXT");
    assert_eq!(parsed.body_sql, "SELECT LOWER(TRIM(email))");
    assert_eq!(parsed.volatility, FunctionVolatility::Immutable);
}

#[test]
fn parse_or_replace() {
    let sql = "CREATE OR REPLACE FUNCTION f(x INT) RETURNS INT AS SELECT x + 1";
    let parsed = parse_create_function(sql).unwrap();
    assert!(parsed.or_replace);
    assert_eq!(parsed.name, "f");
}

#[test]
fn parse_multi_param() {
    let sql = "CREATE FUNCTION add(a FLOAT, b FLOAT) RETURNS FLOAT AS SELECT a + b";
    let parsed = parse_create_function(sql).unwrap();
    assert_eq!(parsed.parameters.len(), 2);
    assert_eq!(parsed.parameters[0].name, "a");
    assert_eq!(parsed.parameters[1].name, "b");
    assert_eq!(parsed.return_type, "FLOAT");
}

#[test]
fn parse_no_params() {
    let sql = "CREATE FUNCTION pi() RETURNS FLOAT AS SELECT 3.14159";
    let parsed = parse_create_function(sql).unwrap();
    assert!(parsed.parameters.is_empty());
    assert_eq!(parsed.body_sql, "SELECT 3.14159");
}

#[test]
fn parse_explicit_volatility() {
    let sql = "CREATE FUNCTION f(x INT) RETURNS INT VOLATILE AS SELECT x";
    let parsed = parse_create_function(sql).unwrap();
    assert_eq!(parsed.volatility, FunctionVolatility::Volatile);
}

#[test]
fn parse_stable_volatility() {
    let sql = "CREATE FUNCTION f(x INT) RETURNS INT STABLE AS SELECT x";
    let parsed = parse_create_function(sql).unwrap();
    assert_eq!(parsed.volatility, FunctionVolatility::Stable);
}

#[test]
fn parse_with_semicolon() {
    let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS SELECT x + 1;";
    let parsed = parse_create_function(sql).unwrap();
    assert_eq!(parsed.body_sql, "SELECT x + 1");
}

#[test]
fn parse_error_no_returns() {
    let sql = "CREATE FUNCTION f(x INT) AS SELECT x";
    assert!(parse_create_function(sql).is_err());
}

#[test]
fn parse_error_bad_type() {
    let sql = "CREATE FUNCTION f(x FOOBAR) RETURNS INT AS SELECT x";
    assert!(parse_create_function(sql).is_err());
}

#[test]
fn parse_error_empty_body() {
    let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS";
    assert!(parse_create_function(sql).is_err());
}

#[test]
fn parse_procedural_body() {
    let sql = "CREATE FUNCTION classify(score INT) RETURNS TEXT AS \
                BEGIN \
                  IF score > 90 THEN RETURN 'excellent'; \
                  ELSIF score > 70 THEN RETURN 'good'; \
                  ELSE RETURN 'needs improvement'; \
                  END IF; \
                END";
    let parsed = parse_create_function(sql).unwrap();
    assert_eq!(parsed.name, "classify");
    assert!(parsed.body_sql.starts_with("BEGIN"));

    use crate::control::planner::procedural::ast::BodyKind;
    assert!(matches!(
        BodyKind::detect(&parsed.body_sql),
        BodyKind::Procedural
    ));
    let block = crate::control::planner::procedural::parse_block(&parsed.body_sql);
    assert!(block.is_ok(), "procedural parse failed: {:?}", block.err());
}

#[test]
fn parse_dml_in_procedural_body() {
    let sql = "CREATE FUNCTION bad_func(x INT) RETURNS INT AS \
                BEGIN INSERT INTO t (id) VALUES (x); RETURN x; END";
    let parsed = parse_create_function(sql).unwrap();

    use crate::control::planner::procedural::ast::BodyKind;
    assert!(matches!(
        BodyKind::detect(&parsed.body_sql),
        BodyKind::Procedural
    ));
    let block = crate::control::planner::procedural::parse_block(&parsed.body_sql).unwrap();

    let result = crate::control::planner::procedural::validate_function_block(&block);
    assert!(result.is_err(), "should reject DML: {:?}", result);
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("DML"),
        "error should mention DML, got: {err_msg}"
    );
}
