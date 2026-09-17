// SPDX-License-Identifier: BUSL-1.1

//! RLS predicate parser: tokenization and recursive descent parsing.
//!
//! Converts predicate expression strings into compiled [`RlsPredicate`] AST.

use super::predicate::{CompareOp, PredicateValue, RlsPredicate};

/// Parse a predicate expression string into a compiled `RlsPredicate`.
///
/// Grammar:
/// ```text
/// expr      = or_expr
/// or_expr   = and_expr ("OR" and_expr)*
/// and_expr  = atom ("AND" atom)*
/// atom      = comparison | contains | intersects | "NOT" atom | "(" expr ")"
/// comparison = field_ref op value_ref
/// contains   = value_ref "CONTAINS" value_ref
/// intersects = value_ref "INTERSECTS" value_ref
/// field_ref  = identifier | "$auth." identifier
/// value_ref  = literal | field_ref
/// ```
pub fn parse_predicate(input: &str) -> Result<RlsPredicate, PredicateParseError> {
    let tokens = tokenize(input)?;
    let mut pos = 0;
    let result = parse_or_expr(&tokens, &mut pos)?;
    if pos < tokens.len() {
        return Err(PredicateParseError::UnexpectedToken {
            token: tokens[pos].clone(),
            position: pos,
        });
    }
    Ok(result)
}

/// Predicate parse errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PredicateParseError {
    #[error("unexpected token '{token}' at position {position}")]
    UnexpectedToken { token: String, position: usize },
    #[error("unexpected end of expression")]
    UnexpectedEnd,
    #[error("unknown operator: '{0}'")]
    UnknownOperator(String),
    #[error("invalid literal: '{0}'")]
    InvalidLiteral(String),
    #[error("unmatched parenthesis")]
    UnmatchedParen,
}

// ── Tokenizer ───────────────────────────────────────────────────────────

fn tokenize(input: &str) -> Result<Vec<String>, PredicateParseError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }

        if ch == '(' || ch == ')' {
            tokens.push(ch.to_string());
            chars.next();
            continue;
        }

        // Quoted string literal.
        if ch == '\'' {
            chars.next(); // consume opening quote
            let mut s = String::new();
            loop {
                match chars.next() {
                    Some('\'') => {
                        // Check for escaped quote ('')
                        if chars.peek() == Some(&'\'') {
                            s.push('\'');
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    Some(c) => s.push(c),
                    None => {
                        return Err(PredicateParseError::InvalidLiteral(
                            "unterminated string".into(),
                        ));
                    }
                }
            }
            tokens.push(format!("'{s}'"));
            continue;
        }

        // Operators: =, !=, <>, <, <=, >, >=
        if matches!(ch, '=' | '!' | '<' | '>') {
            let mut op = String::new();
            op.push(ch);
            chars.next();
            if let Some(&next) = chars.peek()
                && ((ch == '!' && next == '=')
                    || (ch == '<' && matches!(next, '=' | '>'))
                    || (ch == '>' && next == '='))
            {
                op.push(next);
                chars.next();
            }
            tokens.push(op);
            continue;
        }

        // Identifiers, keywords, $auth references, numbers.
        if ch.is_alphanumeric() || ch == '_' || ch == '$' || ch == '-' {
            let mut word = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_alphanumeric() || c == '_' || c == '.' || c == '$' || c == '-' {
                    word.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            tokens.push(word);
            continue;
        }

        // Skip unknown characters
        chars.next();
    }

    Ok(tokens)
}

// ── Recursive Descent Parser ────────────────────────────────────────────

fn parse_or_expr(tokens: &[String], pos: &mut usize) -> Result<RlsPredicate, PredicateParseError> {
    let mut left = parse_and_expr(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos].to_uppercase() == "OR" {
        *pos += 1;
        let right = parse_and_expr(tokens, pos)?;
        left = match left {
            RlsPredicate::Or(mut children) => {
                children.push(right);
                RlsPredicate::Or(children)
            }
            _ => RlsPredicate::Or(vec![left, right]),
        };
    }

    Ok(left)
}

fn parse_and_expr(tokens: &[String], pos: &mut usize) -> Result<RlsPredicate, PredicateParseError> {
    let mut left = parse_atom(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos].to_uppercase() == "AND" {
        *pos += 1;
        let right = parse_atom(tokens, pos)?;
        left = match left {
            RlsPredicate::And(mut children) => {
                children.push(right);
                RlsPredicate::And(children)
            }
            _ => RlsPredicate::And(vec![left, right]),
        };
    }

    Ok(left)
}

fn parse_atom(tokens: &[String], pos: &mut usize) -> Result<RlsPredicate, PredicateParseError> {
    if *pos >= tokens.len() {
        return Err(PredicateParseError::UnexpectedEnd);
    }

    // NOT prefix
    if tokens[*pos].to_uppercase() == "NOT" {
        *pos += 1;
        let inner = parse_atom(tokens, pos)?;
        return Ok(RlsPredicate::Not(Box::new(inner)));
    }

    // Parenthesized expression
    if tokens[*pos] == "(" {
        *pos += 1;
        let inner = parse_or_expr(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != ")" {
            return Err(PredicateParseError::UnmatchedParen);
        }
        *pos += 1;
        return Ok(inner);
    }

    // Look ahead for CONTAINS / INTERSECTS / comparison operator
    let left_value = parse_value_ref(tokens, pos)?;

    if *pos >= tokens.len() {
        // Standalone value — treat as boolean (truthy).
        let field = match &left_value {
            PredicateValue::Field(f) => f.clone(),
            PredicateValue::AuthRef(_) | PredicateValue::AuthFunc { .. } => String::new(),
            PredicateValue::Literal(_) | PredicateValue::Instant { .. } => {
                return Ok(RlsPredicate::AlwaysTrue);
            }
        };
        return Ok(RlsPredicate::Compare {
            field,
            op: CompareOp::IsNotNull,
            value: left_value,
        });
    }

    let op_token = tokens[*pos].to_uppercase();

    match op_token.as_str() {
        "CONTAINS" => {
            *pos += 1;
            let element = parse_value_ref(tokens, pos)?;
            Ok(RlsPredicate::Contains {
                set: left_value,
                element,
            })
        }
        "INTERSECTS" => {
            *pos += 1;
            let right = parse_value_ref(tokens, pos)?;
            Ok(RlsPredicate::Intersects {
                left: left_value,
                right,
            })
        }
        _ => {
            // Must be a comparison operator.
            let compare_op = parse_compare_op(tokens, pos)?;
            let right_value = parse_value_ref(tokens, pos)?;

            // Determine the doc field and value sides.
            match (&left_value, &right_value) {
                (PredicateValue::Field(f), _) => Ok(RlsPredicate::Compare {
                    field: f.clone(),
                    op: compare_op,
                    value: right_value,
                }),
                (_, PredicateValue::Field(f)) => {
                    // Flip: value op field → field flipped_op value
                    let flipped = match compare_op {
                        CompareOp::Gt => CompareOp::Lt,
                        CompareOp::Gte => CompareOp::Lte,
                        CompareOp::Lt => CompareOp::Gt,
                        CompareOp::Lte => CompareOp::Gte,
                        other => other, // Eq, Ne are symmetric
                    };
                    Ok(RlsPredicate::Compare {
                        field: f.clone(),
                        op: flipped,
                        value: left_value,
                    })
                }
                // Auth refs, literals, or functions → evaluate at plan time.
                _ => Ok(RlsPredicate::Compare {
                    field: String::new(), // sentinel: plan-time only
                    op: compare_op,
                    value: right_value,
                }),
            }
        }
    }
}

fn parse_value_ref(
    tokens: &[String],
    pos: &mut usize,
) -> Result<PredicateValue, PredicateParseError> {
    if *pos >= tokens.len() {
        return Err(PredicateParseError::UnexpectedEnd);
    }

    let token = &tokens[*pos];

    // $auth.* reference
    if let Some(field) = token.strip_prefix("$auth.") {
        *pos += 1;
        return Ok(PredicateValue::AuthRef(field.to_string()));
    }

    // Quoted string literal
    if token.starts_with('\'') && token.ends_with('\'') && token.len() >= 2 {
        *pos += 1;
        let inner = &token[1..token.len() - 1];
        return Ok(PredicateValue::Literal(serde_json::Value::String(
            inner.to_string(),
        )));
    }

    // Numeric literal
    if token.starts_with(|c: char| c.is_ascii_digit() || c == '-') {
        if let Ok(n) = token.parse::<i64>() {
            *pos += 1;
            return Ok(PredicateValue::Literal(serde_json::json!(n)));
        }
        if let Ok(f) = token.parse::<f64>() {
            *pos += 1;
            return Ok(PredicateValue::Literal(serde_json::json!(f)));
        }
    }

    // Boolean literals
    match token.to_lowercase().as_str() {
        "true" => {
            *pos += 1;
            return Ok(PredicateValue::Literal(serde_json::json!(true)));
        }
        "false" => {
            *pos += 1;
            return Ok(PredicateValue::Literal(serde_json::json!(false)));
        }
        "null" => {
            *pos += 1;
            return Ok(PredicateValue::Literal(serde_json::Value::Null));
        }
        _ => {}
    }

    // Document field identifier (anything that's not a keyword).
    let upper = token.to_uppercase();
    if !matches!(
        upper.as_str(),
        "AND" | "OR" | "NOT" | "CONTAINS" | "INTERSECTS"
    ) {
        *pos += 1;
        return Ok(PredicateValue::Field(token.clone()));
    }

    Err(PredicateParseError::UnexpectedToken {
        token: token.clone(),
        position: *pos,
    })
}

fn parse_compare_op(tokens: &[String], pos: &mut usize) -> Result<CompareOp, PredicateParseError> {
    if *pos >= tokens.len() {
        return Err(PredicateParseError::UnexpectedEnd);
    }

    let token = &tokens[*pos];
    if let Some(op) = CompareOp::from_str_sql(token) {
        *pos += 1;
        return Ok(op);
    }

    Err(PredicateParseError::UnknownOperator(token.clone()))
}

/// Validate that all `$auth.*` references in a predicate are known fields.
///
/// Called at policy creation time to reject typos like `$auth.usrname`.
pub fn validate_auth_refs(predicate: &RlsPredicate) -> crate::Result<()> {
    let known_fields = [
        "id",
        "username",
        "email",
        "tenant_id",
        "org_id",
        "org_ids",
        "roles",
        "groups",
        "permissions",
        "status",
        "auth_method",
        "auth_time",
        "session_id",
        "database_id",
    ];

    fn check(pred: &RlsPredicate, known: &[&str]) -> crate::Result<()> {
        match pred {
            RlsPredicate::Compare { value, .. } => check_value(value, known),
            RlsPredicate::Contains { set, element } => {
                check_value(set, known)?;
                check_value(element, known)
            }
            RlsPredicate::Intersects { left, right } => {
                check_value(left, known)?;
                check_value(right, known)
            }
            RlsPredicate::And(children) | RlsPredicate::Or(children) => {
                for child in children {
                    check(child, known)?;
                }
                Ok(())
            }
            RlsPredicate::Not(inner) => check(inner, known),
            RlsPredicate::AlwaysTrue | RlsPredicate::AlwaysFalse => Ok(()),
        }
    }

    fn check_value(val: &PredicateValue, known: &[&str]) -> crate::Result<()> {
        match val {
            PredicateValue::AuthRef(field) => {
                let base = field.split('.').next().unwrap_or(field);
                // Allow metadata.* sub-fields
                if base == "metadata" {
                    return Ok(());
                }
                if !known.contains(&base) {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "unknown $auth field: '{field}'. Valid fields: {}",
                            known.join(", ")
                        ),
                    });
                }
            }
            PredicateValue::AuthFunc { func, .. } => {
                const KNOWN_FUNCS: &[&str] = &[
                    "scope_status",
                    "scope_expires_at",
                    "quota_remaining",
                    "quota_pct",
                ];
                if !KNOWN_FUNCS.contains(&func.as_str()) {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "unknown $auth function: '{func}'. Valid functions: {}",
                            KNOWN_FUNCS.join(", ")
                        ),
                    });
                }
            }
            PredicateValue::Literal(_)
            | PredicateValue::Instant { .. }
            | PredicateValue::Field(_) => {}
        }
        Ok(())
    }

    check(predicate, &known_fields)
}
