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

use super::expr::{BinaryOp, SqlExpr};
use nodedb_types::Value;

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

// ── Tokenizer ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Token {
    text: String,
    kind: TokenKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TokenKind {
    Ident,
    Number,
    StringLit,
    LParen,
    RParen,
    Comma,
    Op,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];

        // Skip whitespace.
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Single-char tokens.
        if b == b'(' {
            tokens.push(Token {
                text: "(".into(),
                kind: TokenKind::LParen,
            });
            i += 1;
            continue;
        }
        if b == b')' {
            tokens.push(Token {
                text: ")".into(),
                kind: TokenKind::RParen,
            });
            i += 1;
            continue;
        }
        if b == b',' {
            tokens.push(Token {
                text: ",".into(),
                kind: TokenKind::Comma,
            });
            i += 1;
            continue;
        }

        // Two-char operators.
        if i + 1 < bytes.len() {
            let two = &input[i..i + 2];
            if matches!(two, "<=" | ">=" | "!=" | "<>") {
                tokens.push(Token {
                    text: two.into(),
                    kind: TokenKind::Op,
                });
                i += 2;
                continue;
            }
            if two == "||" {
                tokens.push(Token {
                    text: "||".into(),
                    kind: TokenKind::Op,
                });
                i += 2;
                continue;
            }
        }

        // Single-char operators.
        if matches!(b, b'+' | b'-' | b'*' | b'/' | b'%' | b'=' | b'<' | b'>') {
            tokens.push(Token {
                text: (b as char).to_string(),
                kind: TokenKind::Op,
            });
            i += 1;
            continue;
        }

        // String literal.
        if b == b'\'' {
            let mut s = String::new();
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(bytes[i] as char);
                i += 1;
            }
            tokens.push(Token {
                text: s,
                kind: TokenKind::StringLit,
            });
            continue;
        }

        // Number.
        if b.is_ascii_digit() || (b == b'.' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
        {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                i += 1;
            }
            tokens.push(Token {
                text: input[start..i].to_string(),
                kind: TokenKind::Number,
            });
            continue;
        }

        // Identifier or keyword.
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(Token {
                text: input[start..i].to_string(),
                kind: TokenKind::Ident,
            });
            continue;
        }

        return Err(format!("unexpected character: '{}'", b as char));
    }

    Ok(tokens)
}

// ── Recursive descent parser ──────────────────────────────────────────

/// Maximum recursion depth for nested parentheses / sub-expressions.
/// Exceeding this limit returns `Err` instead of overflowing the stack.
const MAX_EXPR_DEPTH: usize = 128;

/// Parse an expression (lowest precedence: OR).
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
    // Unary minus.
    if *pos < tokens.len() && tokens[*pos].kind == TokenKind::Op && tokens[*pos].text == "-" {
        *pos += 1;
        let expr = parse_primary(tokens, pos, depth)?;
        return Ok(SqlExpr::Negate(Box::new(expr)));
    }
    // NOT
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
        // Parenthesized expression.
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

        // Number literal.
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

        // String literal.
        TokenKind::StringLit => {
            *pos += 1;
            Ok(SqlExpr::Literal(Value::String(token.text.clone())))
        }

        // Identifier: column ref, function call, keyword (NULL, TRUE, FALSE, CASE, COALESCE).
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
                    // Function call: IDENT(args).
                    if *pos < tokens.len() && tokens[*pos].kind == TokenKind::LParen {
                        let args = parse_arg_list(tokens, pos, depth)?;
                        Ok(SqlExpr::Function {
                            name: name.to_lowercase(),
                            args,
                        })
                    } else {
                        // Column reference.
                        Ok(SqlExpr::Column(name.to_lowercase()))
                    }
                }
            }
        }

        _ => Err(format!("unexpected token: '{}'", token.text)),
    }
}

/// Parse `CASE WHEN cond THEN result [WHEN ... THEN ...] [ELSE result] END`.
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

/// Parse a parenthesized, comma-separated argument list: `(expr, expr, ...)`.
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

/// Non-deterministic functions that are rejected in GENERATED ALWAYS AS.
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
        // eval returns integer when result is whole number.
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
        // Spec: the parser must enforce a recursion depth limit so that
        // pathologically deep nesting returns Err rather than overflowing the
        // call stack and causing a process crash.
        let depth = 10_000;
        let input = format!("{}x{}", "(".repeat(depth), ")".repeat(depth),);
        let result = parse_generated_expr(&input);
        assert!(
            result.is_err(),
            "parse_generated_expr must return Err for {depth}-deep nesting, not stack overflow"
        );
    }
}
