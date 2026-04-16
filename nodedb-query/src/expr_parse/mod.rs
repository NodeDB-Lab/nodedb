//! SQL expression text → SqlExpr AST parser.
//!
//! Parses the subset of SQL expressions used in `GENERATED ALWAYS AS (expr)`
//! column definitions. Supports:
//! - Column references: `price`, `tax_rate`
//! - Numeric literals: `42`, `3.14`, `-1`
//! - String literals: `'hello'`, `''escaped''`
//! - Binary operators: `+`, `-`, `*`, `/`, `%`
//! - Comparison: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
//! - Logical: `AND`, `OR`, `NOT`
//! - Parenthesized sub-expressions: `(a + b) * c`
//! - Function calls: `ROUND(price * 1.08, 2)`, `CONCAT(a, ' ', b)`
//! - COALESCE: `COALESCE(a, b, '')`
//! - CASE WHEN: `CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END`
//! - NULL literal
//!
//! Determinism validation: rejects `NOW()`, `RANDOM()`, `NEXTVAL()`, `UUID()`.

mod tokenizer;

use super::expr::{BinaryOp, SqlExpr};
use nodedb_types::Value;
use tokenizer::{Token, TokenKind, tokenize};

/// Parse a SQL expression string into an SqlExpr AST.
///
/// Returns the parsed expression and a list of column names it references
/// (the `depends_on` set for generated columns).
pub fn parse_generated_expr(text: &str) -> Result<(SqlExpr, Vec<String>), String> {
    let tokens = tokenize(text)?;
    let mut pos = 0;
    let expr = parse_expr(&tokens, &mut pos, &mut 0)?;
    if pos < tokens.len() {
        return Err(format!(
            "unexpected token after expression: '{}'",
            tokens[pos].text
        ));
    }

    // Validate determinism.
    validate_deterministic(&expr)?;

    // Collect column references.
    let mut deps = Vec::new();
    collect_columns(&expr, &mut deps);
    deps.sort();
    deps.dedup();

    Ok((expr, deps))
}

// ── Recursive descent parser ──────────────────────────────────────────

/// Maximum recursion depth for nested parentheses / sub-expressions.
const MAX_EXPR_DEPTH: usize = 128;

fn parse_expr(tokens: &[Token], pos: &mut usize, depth: &mut usize) -> Result<SqlExpr, String> {
    parse_or(tokens, pos, depth)
}

fn parse_or(tokens: &[Token], pos: &mut usize, depth: &mut usize) -> Result<SqlExpr, String> {
    let mut left = parse_and(tokens, pos, depth)?;
    while peek_keyword(tokens, *pos, "OR") {
        *pos += 1;
        let right = parse_and(tokens, pos, depth)?;
        left = SqlExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_and(tokens: &[Token], pos: &mut usize, depth: &mut usize) -> Result<SqlExpr, String> {
    let mut left = parse_comparison(tokens, pos, depth)?;
    while peek_keyword(tokens, *pos, "AND") {
        *pos += 1;
        let right = parse_comparison(tokens, pos, depth)?;
        left = SqlExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_comparison(
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
) -> Result<SqlExpr, String> {
    let left = parse_additive(tokens, pos, depth)?;
    if *pos < tokens.len() && tokens[*pos].kind == TokenKind::Op {
        let op = match tokens[*pos].text.as_str() {
            "=" => BinaryOp::Eq,
            "!=" | "<>" => BinaryOp::NotEq,
            "<" => BinaryOp::Lt,
            "<=" => BinaryOp::LtEq,
            ">" => BinaryOp::Gt,
            ">=" => BinaryOp::GtEq,
            _ => return Ok(left),
        };
        *pos += 1;
        let right = parse_additive(tokens, pos, depth)?;
        return Ok(SqlExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        });
    }
    Ok(left)
}

fn parse_additive(tokens: &[Token], pos: &mut usize, depth: &mut usize) -> Result<SqlExpr, String> {
    let mut left = parse_multiplicative(tokens, pos, depth)?;
    while *pos < tokens.len() && tokens[*pos].kind == TokenKind::Op {
        let op = match tokens[*pos].text.as_str() {
            "+" => BinaryOp::Add,
            "-" => BinaryOp::Sub,
            "||" => BinaryOp::Concat,
            _ => break,
        };
        *pos += 1;
        let right = parse_multiplicative(tokens, pos, depth)?;
        left = SqlExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_multiplicative(
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
) -> Result<SqlExpr, String> {
    let mut left = parse_unary(tokens, pos, depth)?;
    while *pos < tokens.len() && tokens[*pos].kind == TokenKind::Op {
        let op = match tokens[*pos].text.as_str() {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            "%" => BinaryOp::Mod,
            _ => break,
        };
        *pos += 1;
        let right = parse_unary(tokens, pos, depth)?;
        left = SqlExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_unary(tokens: &[Token], pos: &mut usize, depth: &mut usize) -> Result<SqlExpr, String> {
    if *pos < tokens.len() && tokens[*pos].kind == TokenKind::Op && tokens[*pos].text == "-" {
        *pos += 1;
        let expr = parse_primary(tokens, pos, depth)?;
        return Ok(SqlExpr::Negate(Box::new(expr)));
    }
    if peek_keyword(tokens, *pos, "NOT") {
        *pos += 1;
        let expr = parse_primary(tokens, pos, depth)?;
        return Ok(SqlExpr::Negate(Box::new(expr)));
    }
    parse_primary(tokens, pos, depth)
}

fn parse_primary(tokens: &[Token], pos: &mut usize, depth: &mut usize) -> Result<SqlExpr, String> {
    if *pos >= tokens.len() {
        return Err("unexpected end of expression".into());
    }

    let token = &tokens[*pos];

    match token.kind {
        TokenKind::LParen => {
            *depth += 1;
            if *depth > MAX_EXPR_DEPTH {
                return Err(format!(
                    "expression nesting depth exceeds maximum of {MAX_EXPR_DEPTH}"
                ));
            }
            *pos += 1;
            let expr = parse_expr(tokens, pos, depth)?;
            *depth -= 1;
            expect_token(tokens, pos, TokenKind::RParen, ")")?;
            Ok(expr)
        }

        TokenKind::Number => {
            *pos += 1;
            if let Ok(i) = token.text.parse::<i64>() {
                Ok(SqlExpr::Literal(Value::Integer(i)))
            } else if let Ok(f) = token.text.parse::<f64>() {
                Ok(SqlExpr::Literal(Value::Float(f)))
            } else {
                Err(format!("invalid number: '{}'", token.text))
            }
        }

        TokenKind::StringLit => {
            *pos += 1;
            Ok(SqlExpr::Literal(Value::String(token.text.clone())))
        }

        TokenKind::Ident => {
            let name = token.text.clone();
            let upper = name.to_uppercase();
            *pos += 1;

            match upper.as_str() {
                "NULL" => Ok(SqlExpr::Literal(Value::Null)),
                "TRUE" => Ok(SqlExpr::Literal(Value::Bool(true))),
                "FALSE" => Ok(SqlExpr::Literal(Value::Bool(false))),
                "CASE" => parse_case(tokens, pos, depth),
                "COALESCE" => {
                    let args = parse_arg_list(tokens, pos, depth)?;
                    Ok(SqlExpr::Coalesce(args))
                }
                _ => {
                    if *pos < tokens.len() && tokens[*pos].kind == TokenKind::LParen {
                        let args = parse_arg_list(tokens, pos, depth)?;
                        Ok(SqlExpr::Function {
                            name: name.to_lowercase(),
                            args,
                        })
                    } else {
                        Ok(SqlExpr::Column(name.to_lowercase()))
                    }
                }
            }
        }

        _ => Err(format!("unexpected token: '{}'", token.text)),
    }
}

fn parse_case(tokens: &[Token], pos: &mut usize, depth: &mut usize) -> Result<SqlExpr, String> {
    let mut when_thens = Vec::new();
    let mut else_expr = None;

    loop {
        if peek_keyword(tokens, *pos, "WHEN") {
            *pos += 1;
            let cond = parse_expr(tokens, pos, depth)?;
            expect_keyword(tokens, pos, "THEN")?;
            let then = parse_expr(tokens, pos, depth)?;
            when_thens.push((cond, then));
        } else if peek_keyword(tokens, *pos, "ELSE") {
            *pos += 1;
            else_expr = Some(Box::new(parse_expr(tokens, pos, depth)?));
        } else if peek_keyword(tokens, *pos, "END") {
            *pos += 1;
            break;
        } else {
            return Err("expected WHEN, ELSE, or END in CASE expression".into());
        }
    }

    if when_thens.is_empty() {
        return Err("CASE requires at least one WHEN clause".into());
    }

    Ok(SqlExpr::Case {
        operand: None,
        when_thens,
        else_expr,
    })
}

fn parse_arg_list(
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
) -> Result<Vec<SqlExpr>, String> {
    expect_token(tokens, pos, TokenKind::LParen, "(")?;
    let mut args = Vec::new();
    if *pos < tokens.len() && tokens[*pos].kind == TokenKind::RParen {
        *pos += 1;
        return Ok(args);
    }
    loop {
        args.push(parse_expr(tokens, pos, depth)?);
        if *pos < tokens.len() && tokens[*pos].kind == TokenKind::Comma {
            *pos += 1;
        } else {
            break;
        }
    }
    expect_token(tokens, pos, TokenKind::RParen, ")")?;
    Ok(args)
}

// ── Helpers ───────────────────────────────────────────────────────────

fn peek_keyword(tokens: &[Token], pos: usize, keyword: &str) -> bool {
    pos < tokens.len()
        && tokens[pos].kind == TokenKind::Ident
        && tokens[pos].text.eq_ignore_ascii_case(keyword)
}

fn expect_keyword(tokens: &[Token], pos: &mut usize, keyword: &str) -> Result<(), String> {
    if peek_keyword(tokens, *pos, keyword) {
        *pos += 1;
        Ok(())
    } else {
        let got = tokens.get(*pos).map_or("EOF", |t| &t.text);
        Err(format!("expected '{keyword}', got '{got}'"))
    }
}

fn expect_token(
    tokens: &[Token],
    pos: &mut usize,
    kind: TokenKind,
    expected: &str,
) -> Result<(), String> {
    if *pos < tokens.len() && tokens[*pos].kind == kind {
        *pos += 1;
        Ok(())
    } else {
        let got = tokens.get(*pos).map_or("EOF", |t| &t.text);
        Err(format!("expected '{expected}', got '{got}'"))
    }
}

// ── Validation ────────────────────────────────────────────────────────

const NON_DETERMINISTIC: &[&str] = &[
    "now",
    "current_timestamp",
    "random",
    "nextval",
    "uuid",
    "uuid_v4",
    "uuid_v7",
    "gen_random_uuid",
    "ulid",
    "cuid2",
    "nanoid",
];

fn validate_deterministic(expr: &SqlExpr) -> Result<(), String> {
    match expr {
        SqlExpr::Function { name, args } => {
            if NON_DETERMINISTIC.contains(&name.as_str()) {
                return Err(format!(
                    "non-deterministic function '{name}()' not allowed in GENERATED ALWAYS AS"
                ));
            }
            for arg in args {
                validate_deterministic(arg)?;
            }
            Ok(())
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            validate_deterministic(left)?;
            validate_deterministic(right)
        }
        SqlExpr::Negate(inner) => validate_deterministic(inner),
        SqlExpr::Coalesce(args) => {
            for arg in args {
                validate_deterministic(arg)?;
            }
            Ok(())
        }
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            if let Some(op) = operand {
                validate_deterministic(op)?;
            }
            for (cond, then) in when_thens {
                validate_deterministic(cond)?;
                validate_deterministic(then)?;
            }
            if let Some(e) = else_expr {
                validate_deterministic(e)?;
            }
            Ok(())
        }
        SqlExpr::Cast { expr, .. } => validate_deterministic(expr),
        SqlExpr::NullIf(a, b) => {
            validate_deterministic(a)?;
            validate_deterministic(b)
        }
        SqlExpr::IsNull { expr, .. } => validate_deterministic(expr),
        SqlExpr::Column(_) | SqlExpr::Literal(_) | SqlExpr::OldColumn(_) => Ok(()),
    }
}

fn collect_columns(expr: &SqlExpr, deps: &mut Vec<String>) {
    match expr {
        SqlExpr::Column(name) => deps.push(name.clone()),
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, deps);
            collect_columns(right, deps);
        }
        SqlExpr::Negate(inner) => collect_columns(inner, deps),
        SqlExpr::Function { args, .. } => {
            for arg in args {
                collect_columns(arg, deps);
            }
        }
        SqlExpr::Coalesce(args) => {
            for arg in args {
                collect_columns(arg, deps);
            }
        }
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_columns(op, deps);
            }
            for (cond, then) in when_thens {
                collect_columns(cond, deps);
                collect_columns(then, deps);
            }
            if let Some(e) = else_expr {
                collect_columns(e, deps);
            }
        }
        SqlExpr::Cast { expr, .. } => collect_columns(expr, deps),
        SqlExpr::NullIf(a, b) => {
            collect_columns(a, deps);
            collect_columns(b, deps);
        }
        SqlExpr::IsNull { expr, .. } => collect_columns(expr, deps),
        SqlExpr::Literal(_) | SqlExpr::OldColumn(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    fn parse_ok(text: &str) -> (SqlExpr, Vec<String>) {
        parse_generated_expr(text).unwrap()
    }

    #[test]
    fn simple_arithmetic() {
        let (expr, deps) = parse_ok("price * (1 + tax_rate)");
        assert_eq!(deps, vec!["price", "tax_rate"]);
        let doc = Value::from(serde_json::json!({"price": 100.0, "tax_rate": 0.08}));
        let result = expr.eval(&doc);
        assert_eq!(result.as_f64(), Some(108.0));
    }

    #[test]
    fn round_function() {
        let (expr, deps) = parse_ok("ROUND(price * (1 + tax_rate), 2)");
        assert_eq!(deps, vec!["price", "tax_rate"]);
        let doc = Value::from(serde_json::json!({"price": 99.99, "tax_rate": 0.08}));
        let result = expr.eval(&doc);
        assert_eq!(result, Value::Float(107.99));
    }

    #[test]
    fn concat_function() {
        let (expr, deps) = parse_ok("CONCAT(name, ' ', brand)");
        assert_eq!(deps, vec!["brand", "name"]);
        let doc = Value::from(serde_json::json!({"name": "Shoe", "brand": "Nike"}));
        assert_eq!(expr.eval(&doc), Value::String("Shoe Nike".into()));
    }

    #[test]
    fn coalesce() {
        let (expr, _) = parse_ok("COALESCE(description, '')");
        let doc = Value::from(serde_json::json!({"description": null}));
        assert_eq!(expr.eval(&doc), Value::String("".into()));
    }

    #[test]
    fn case_when() {
        let (expr, deps) =
            parse_ok("CASE WHEN discount > 0 THEN price * (1 - discount) ELSE price END");
        assert!(deps.contains(&"discount".to_string()));
        assert!(deps.contains(&"price".to_string()));

        let doc = Value::from(serde_json::json!({"price": 100.0, "discount": 0.2}));
        assert_eq!(expr.eval(&doc).as_f64(), Some(80.0));

        let doc2 = Value::from(serde_json::json!({"price": 100.0, "discount": 0}));
        assert_eq!(expr.eval(&doc2).as_f64(), Some(100.0));
    }

    #[test]
    fn rejects_now() {
        assert!(parse_generated_expr("NOW()").is_err());
    }

    #[test]
    fn rejects_random() {
        assert!(parse_generated_expr("RANDOM()").is_err());
    }

    #[test]
    fn rejects_uuid() {
        assert!(parse_generated_expr("UUID()").is_err());
    }

    #[test]
    fn string_literal() {
        let (expr, _) = parse_ok("CONCAT(name, ' - ', 'default')");
        let doc = Value::from(serde_json::json!({"name": "Product"}));
        assert_eq!(expr.eval(&doc), Value::String("Product - default".into()));
    }

    #[test]
    fn null_literal() {
        let (expr, _) = parse_ok("COALESCE(x, NULL, 0)");
        let doc = Value::from(serde_json::json!({"x": null}));
        assert_eq!(expr.eval(&doc), Value::Integer(0));
    }

    #[test]
    fn nested_functions() {
        let (expr, _) = parse_ok("ROUND(price * (1 - COALESCE(discount, 0)), 2)");
        let doc = Value::from(serde_json::json!({"price": 49.99}));
        assert_eq!(expr.eval(&doc), Value::Float(49.99));
    }

    #[test]
    fn deeply_nested_parentheses_return_error_not_stack_overflow() {
        let depth = 10_000;
        let input = format!("{}x{}", "(".repeat(depth), ")".repeat(depth));
        let result = parse_generated_expr(&input);
        assert!(
            result.is_err(),
            "parse_generated_expr must return Err for {depth}-deep nesting"
        );
    }

    #[test]
    fn cjk_string_in_concat() {
        let (expr, _) = parse_ok("CONCAT('你好', name)");
        let doc = Value::from(serde_json::json!({"name": "world"}));
        assert_eq!(expr.eval(&doc), Value::String("你好world".into()));
    }

    #[test]
    fn comparison_with_utf8_literal() {
        let (expr, deps) = parse_ok("name != '禁止'");
        assert_eq!(deps, vec!["name"]);
        let doc = Value::from(serde_json::json!({"name": "allowed"}));
        assert_eq!(expr.eval(&doc), Value::Bool(true));
    }
}
