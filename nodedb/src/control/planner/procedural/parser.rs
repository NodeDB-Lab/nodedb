//! Procedural SQL parser.
//!
//! Converts a token stream from the tokenizer into a `ProceduralBlock` AST.
//! Handles IF/ELSIF/ELSE, DECLARE, LOOP/WHILE/FOR, RETURN, BREAK/CONTINUE.
//! Detects DML (INSERT/UPDATE/DELETE/COMMIT/ROLLBACK) and emits them as
//! `Statement::Dml` / `Statement::Commit` / `Statement::Rollback` for
//! the validator to reject in function bodies.

use super::ast::*;
use super::tokenizer::Token;

/// Parse a procedural SQL body into a `ProceduralBlock`.
///
/// Input: raw SQL text starting with `BEGIN` and ending with `END`.
pub fn parse_block(input: &str) -> Result<ProceduralBlock, String> {
    let tokens = super::tokenizer::tokenize(input)?;
    let mut pos = 0;

    // Expect BEGIN.
    skip_token(&tokens, &mut pos, &Token::Begin)?;

    let statements = parse_statements(&tokens, &mut pos)?;

    // Expect END (with optional semicolon).
    expect_token(&tokens, &mut pos, &Token::End)?;
    skip_if(&tokens, &mut pos, &Token::Semicolon);

    Ok(ProceduralBlock { statements })
}

/// Parse a sequence of statements until we hit END, ELSE, ELSIF, or end of tokens.
fn parse_statements(tokens: &[Token], pos: &mut usize) -> Result<Vec<Statement>, String> {
    let mut stmts = Vec::new();

    while *pos < tokens.len() {
        // Check for block terminators.
        match tokens.get(*pos) {
            Some(Token::End | Token::EndIf | Token::EndLoop | Token::Else | Token::Elsif) => {
                break;
            }
            None => break,
            _ => {}
        }

        stmts.push(parse_statement(tokens, pos)?);
    }

    Ok(stmts)
}

/// Parse a single statement.
fn parse_statement(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    match tokens.get(*pos) {
        Some(Token::Declare) => parse_declare(tokens, pos),
        Some(Token::If) => parse_if(tokens, pos),
        Some(Token::While) => parse_while(tokens, pos),
        Some(Token::For) => parse_for(tokens, pos),
        Some(Token::Loop) => parse_loop(tokens, pos),
        Some(Token::Return) => parse_return(tokens, pos),
        Some(Token::ReturnQuery) => parse_return_query(tokens, pos),
        Some(Token::Break) => {
            *pos += 1;
            skip_if(tokens, pos, &Token::Semicolon);
            Ok(Statement::Break)
        }
        Some(Token::Continue) => {
            *pos += 1;
            skip_if(tokens, pos, &Token::Semicolon);
            Ok(Statement::Continue)
        }
        Some(Token::Raise) => parse_raise(tokens, pos),
        // DML detection — captured for validator to reject in function bodies.
        Some(Token::Insert | Token::Update | Token::Delete) => parse_dml(tokens, pos),
        Some(Token::Commit) => {
            *pos += 1;
            skip_if(tokens, pos, &Token::Semicolon);
            Ok(Statement::Commit)
        }
        Some(Token::Rollback) => {
            *pos += 1;
            skip_if(tokens, pos, &Token::Semicolon);
            Ok(Statement::Rollback)
        }
        // Assignment: `ident := expr;`
        Some(Token::Ident(_)) => {
            if *pos + 1 < tokens.len() && tokens[*pos + 1] == Token::Assign {
                parse_assign(tokens, pos)
            } else {
                Err(format!(
                    "unexpected token at position {}: {:?}",
                    *pos,
                    tokens.get(*pos)
                ))
            }
        }
        other => Err(format!("unexpected token at position {pos}: {other:?}")),
    }
}

// ─── Individual statement parsers ────────────────────────────────────────────

/// `DECLARE name TYPE [:= default];`
fn parse_declare(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip DECLARE

    let name = expect_ident(tokens, pos)?;
    let data_type = expect_ident(tokens, pos)?;

    let default = if matches!(tokens.get(*pos), Some(Token::Assign)) {
        *pos += 1; // skip :=
        let expr = collect_sql_until(tokens, pos, &[Token::Semicolon])?;
        Some(expr)
    } else {
        None
    };

    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Declare {
        name,
        data_type,
        default,
    })
}

/// `name := expr;`
fn parse_assign(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    let target = expect_ident(tokens, pos)?;
    expect_token(tokens, pos, &Token::Assign)?;
    let expr = collect_sql_until(tokens, pos, &[Token::Semicolon])?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Assign { target, expr })
}

/// `IF cond THEN ... [ELSIF cond THEN ...] [ELSE ...] END IF;`
fn parse_if(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip IF

    let condition = collect_sql_until(tokens, pos, &[Token::Then])?;
    expect_token(tokens, pos, &Token::Then)?;

    let then_block = parse_statements(tokens, pos)?;

    let mut elsif_branches = Vec::new();
    while matches!(tokens.get(*pos), Some(Token::Elsif)) {
        *pos += 1; // skip ELSIF
        let cond = collect_sql_until(tokens, pos, &[Token::Then])?;
        expect_token(tokens, pos, &Token::Then)?;
        let body = parse_statements(tokens, pos)?;
        elsif_branches.push(ElsIfBranch {
            condition: cond,
            body,
        });
    }

    let else_block = if matches!(tokens.get(*pos), Some(Token::Else)) {
        *pos += 1; // skip ELSE
        Some(parse_statements(tokens, pos)?)
    } else {
        None
    };

    expect_token(tokens, pos, &Token::EndIf)?;
    skip_if(tokens, pos, &Token::Semicolon);

    Ok(Statement::If {
        condition,
        then_block,
        elsif_branches,
        else_block,
    })
}

/// `WHILE cond LOOP ... END LOOP;`
fn parse_while(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip WHILE
    let condition = collect_sql_until(tokens, pos, &[Token::Loop])?;
    expect_token(tokens, pos, &Token::Loop)?;
    let body = parse_statements(tokens, pos)?;
    expect_token(tokens, pos, &Token::EndLoop)?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::While { condition, body })
}

/// `FOR var IN [REVERSE] start..end LOOP ... END LOOP;`
fn parse_for(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip FOR
    let var = expect_ident(tokens, pos)?;
    expect_token(tokens, pos, &Token::In)?;

    let reverse = if matches!(tokens.get(*pos), Some(Token::Reverse)) {
        *pos += 1;
        true
    } else {
        false
    };

    let start = collect_sql_until(tokens, pos, &[Token::DotDot])?;
    expect_token(tokens, pos, &Token::DotDot)?;
    let end = collect_sql_until(tokens, pos, &[Token::Loop])?;
    expect_token(tokens, pos, &Token::Loop)?;
    let body = parse_statements(tokens, pos)?;
    expect_token(tokens, pos, &Token::EndLoop)?;
    skip_if(tokens, pos, &Token::Semicolon);

    Ok(Statement::For {
        var,
        start,
        end,
        reverse,
        body,
    })
}

/// `LOOP ... END LOOP;`
fn parse_loop(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip LOOP
    let body = parse_statements(tokens, pos)?;
    expect_token(tokens, pos, &Token::EndLoop)?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Loop { body })
}

/// `RETURN expr;`
fn parse_return(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip RETURN
    let expr = collect_sql_until(tokens, pos, &[Token::Semicolon])?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Return { expr })
}

/// `RETURN QUERY sql;`
fn parse_return_query(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip RETURN QUERY
    let query = collect_raw_sql_until(tokens, pos, &[Token::Semicolon]);
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::ReturnQuery { query })
}

/// `RAISE [NOTICE|WARNING|EXCEPTION] 'message';`
fn parse_raise(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    *pos += 1; // skip RAISE
    let level = match tokens.get(*pos) {
        Some(Token::Notice) => {
            *pos += 1;
            RaiseLevel::Notice
        }
        Some(Token::Warning) => {
            *pos += 1;
            RaiseLevel::Warning
        }
        Some(Token::Exception) => {
            *pos += 1;
            RaiseLevel::Exception
        }
        _ => RaiseLevel::Exception, // default
    };
    let message = collect_sql_until(tokens, pos, &[Token::Semicolon])?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Raise { level, message })
}

/// Capture DML statement as raw SQL for later rejection.
fn parse_dml(tokens: &[Token], pos: &mut usize) -> Result<Statement, String> {
    let sql = collect_raw_sql_until(tokens, pos, &[Token::Semicolon]);
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Dml { sql })
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Collect tokens as a SQL expression until one of the terminator tokens is found.
fn collect_sql_until(
    tokens: &[Token],
    pos: &mut usize,
    terminators: &[Token],
) -> Result<SqlExpr, String> {
    let sql = collect_raw_sql_until(tokens, pos, terminators);
    if sql.is_empty() {
        return Err(format!(
            "expected SQL expression before {:?} at position {pos}",
            terminators
        ));
    }
    Ok(SqlExpr::new(sql))
}

/// Collect tokens as raw SQL text until a terminator is found.
fn collect_raw_sql_until(tokens: &[Token], pos: &mut usize, terminators: &[Token]) -> String {
    let mut parts = Vec::new();
    while *pos < tokens.len() {
        if terminators.iter().any(|t| token_matches(&tokens[*pos], t)) {
            break;
        }
        parts.push(token_to_sql(&tokens[*pos]));
        *pos += 1;
    }
    parts.join(" ").trim().to_string()
}

/// Convert a token back to its SQL text representation.
fn token_to_sql(token: &Token) -> String {
    match token {
        Token::Ident(s) => s.clone(),
        Token::StringLit(s) => format!("'{}'", s.replace('\'', "''")),
        Token::NumberLit(s) => s.clone(),
        Token::SqlFragment(s) => s.clone(),
        Token::Semicolon => ";".into(),
        Token::Assign => ":=".into(),
        Token::DotDot => "..".into(),
        Token::In => "IN".into(),
        Token::Reverse => "REVERSE".into(),
        // Keywords that might appear in SQL expressions.
        Token::If => "IF".into(),
        Token::Then => "THEN".into(),
        Token::Else => "ELSE".into(),
        Token::End => "END".into(),
        Token::Begin => "BEGIN".into(),
        Token::Loop => "LOOP".into(),
        Token::Return => "RETURN".into(),
        Token::Insert => "INSERT".into(),
        Token::Update => "UPDATE".into(),
        Token::Delete => "DELETE".into(),
        _ => format!("{token:?}"),
    }
}

/// Check if a token matches a pattern token (ignoring content for parameterized variants).
fn token_matches(token: &Token, pattern: &Token) -> bool {
    std::mem::discriminant(token) == std::mem::discriminant(pattern)
}

fn skip_token(tokens: &[Token], pos: &mut usize, expected: &Token) -> Result<(), String> {
    expect_token(tokens, pos, expected)
}

fn expect_token(tokens: &[Token], pos: &mut usize, expected: &Token) -> Result<(), String> {
    if *pos < tokens.len() && token_matches(&tokens[*pos], expected) {
        *pos += 1;
        Ok(())
    } else {
        Err(format!(
            "expected {expected:?} at position {pos}, got {:?}",
            tokens.get(*pos)
        ))
    }
}

fn expect_ident(tokens: &[Token], pos: &mut usize) -> Result<String, String> {
    match tokens.get(*pos) {
        Some(Token::Ident(s)) => {
            let name = s.clone();
            *pos += 1;
            Ok(name)
        }
        other => Err(format!(
            "expected identifier at position {pos}, got {other:?}"
        )),
    }
}

fn skip_if(tokens: &[Token], pos: &mut usize, token: &Token) {
    if *pos < tokens.len() && token_matches(&tokens[*pos], token) {
        *pos += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_return() {
        let block = parse_block("BEGIN RETURN 42; END").unwrap();
        assert_eq!(block.statements.len(), 1);
        assert!(matches!(&block.statements[0], Statement::Return { expr } if expr.sql == "42"));
    }

    #[test]
    fn parse_if_else() {
        let block =
            parse_block("BEGIN IF x > 0 THEN RETURN 1; ELSE RETURN 0; END IF; END").unwrap();
        assert_eq!(block.statements.len(), 1);
        let Statement::If {
            condition,
            then_block,
            else_block,
            ..
        } = &block.statements[0]
        else {
            panic!("expected If");
        };
        assert_eq!(condition.sql, "x > 0");
        assert_eq!(then_block.len(), 1);
        assert!(else_block.is_some());
    }

    #[test]
    fn parse_if_elsif_else() {
        let block = parse_block(
            "BEGIN \
             IF x > 10 THEN RETURN 'high'; \
             ELSIF x > 5 THEN RETURN 'mid'; \
             ELSE RETURN 'low'; \
             END IF; \
             END",
        )
        .unwrap();
        let Statement::If {
            elsif_branches,
            else_block,
            ..
        } = &block.statements[0]
        else {
            panic!("expected If");
        };
        assert_eq!(elsif_branches.len(), 1);
        assert!(else_block.is_some());
    }

    #[test]
    fn parse_declare_and_assign() {
        let block = parse_block("BEGIN DECLARE x INT := 0; x := x + 1; RETURN x; END").unwrap();
        assert_eq!(block.statements.len(), 3);
        assert!(matches!(&block.statements[0], Statement::Declare { name, .. } if name == "x"));
        assert!(matches!(&block.statements[1], Statement::Assign { target, .. } if target == "x"));
    }

    #[test]
    fn parse_while_loop() {
        let block = parse_block("BEGIN WHILE i < 10 LOOP i := i + 1; END LOOP; END").unwrap();
        assert_eq!(block.statements.len(), 1);
        assert!(matches!(&block.statements[0], Statement::While { .. }));
    }

    #[test]
    fn parse_for_loop() {
        let block = parse_block("BEGIN FOR i IN 1..10 LOOP BREAK; END LOOP; END").unwrap();
        let Statement::For {
            var, reverse, body, ..
        } = &block.statements[0]
        else {
            panic!("expected For");
        };
        assert_eq!(var, "i");
        assert!(!reverse);
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn parse_dml_detected() {
        let block = parse_block("BEGIN INSERT INTO users VALUES (1); END").unwrap();
        assert!(matches!(&block.statements[0], Statement::Dml { .. }));
    }

    #[test]
    fn parse_raise() {
        let block = parse_block("BEGIN RAISE EXCEPTION 'bad input'; END").unwrap();
        let Statement::Raise { level, message } = &block.statements[0] else {
            panic!("expected Raise");
        };
        assert_eq!(*level, RaiseLevel::Exception);
        assert!(message.sql.contains("bad input"));
    }

    #[test]
    fn parse_nested_if() {
        let block = parse_block(
            "BEGIN \
             IF x > 0 THEN \
               IF x > 10 THEN RETURN 'big'; \
               ELSE RETURN 'small'; \
               END IF; \
             END IF; \
             END",
        )
        .unwrap();
        let Statement::If { then_block, .. } = &block.statements[0] else {
            panic!("expected If");
        };
        assert!(matches!(&then_block[0], Statement::If { .. }));
    }
}
