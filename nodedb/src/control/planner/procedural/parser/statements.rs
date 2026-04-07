//! Individual statement parsers for the procedural SQL parser.

use super::super::ast::*;
use super::super::error::ProceduralError;
use super::super::tokenizer::Token;
use super::utils::*;

/// Parse a single statement.
pub(super) fn parse_statement(
    tokens: &[Token],
    pos: &mut usize,
) -> Result<Statement, ProceduralError> {
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
        Some(Token::Insert | Token::Update | Token::Delete) => parse_dml(tokens, pos),
        Some(Token::Commit) => {
            *pos += 1;
            skip_if(tokens, pos, &Token::Semicolon);
            Ok(Statement::Commit)
        }
        Some(Token::Rollback) => parse_rollback(tokens, pos),
        Some(Token::Savepoint) => parse_savepoint(tokens, pos),
        Some(Token::Release) => parse_release(tokens, pos),
        Some(Token::Ident(_)) => {
            // Check for assignment: `var := expr` or `NEW.field := expr`
            if is_assignment_ahead(tokens, *pos) {
                parse_assign(tokens, pos)
            } else {
                Err(ProceduralError::parse(format!(
                    "unexpected token at position {}: {:?}",
                    *pos,
                    tokens.get(*pos)
                )))
            }
        }
        other => Err(ProceduralError::parse(format!(
            "unexpected token at position {pos}: {other:?}"
        ))),
    }
}

/// `DECLARE name TYPE [:= default];`
fn parse_declare(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    let name = expect_ident(tokens, pos)?;
    let data_type = expect_ident(tokens, pos)?;
    let default = if matches!(tokens.get(*pos), Some(Token::Assign)) {
        *pos += 1;
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

/// Check if tokens at `pos` form an assignment: `ident := ...` or `ident.ident := ...`
fn is_assignment_ahead(tokens: &[Token], pos: usize) -> bool {
    let mut i = pos;
    // Skip ident
    if !matches!(tokens.get(i), Some(Token::Ident(_))) {
        return false;
    }
    i += 1;
    // Skip optional `.ident` chains (e.g., NEW.status)
    while i + 1 < tokens.len() {
        if let Some(Token::Ident(dot)) = tokens.get(i) {
            if dot == "." {
                i += 1; // skip dot
                if matches!(tokens.get(i), Some(Token::Ident(_))) {
                    i += 1; // skip field name
                } else {
                    break;
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
    matches!(tokens.get(i), Some(Token::Assign))
}

/// `name := expr;` or `NEW.field := expr;`
fn parse_assign(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    // Collect dotted target: `name` or `NEW.field`
    let mut target = expect_ident(tokens, pos)?;
    while let Some(Token::Ident(dot)) = tokens.get(*pos) {
        if dot == "." {
            *pos += 1; // skip dot
            let field = expect_ident(tokens, pos)?;
            target = format!("{target}.{field}");
        } else {
            break;
        }
    }
    expect_token(tokens, pos, &Token::Assign)?;
    let expr = collect_sql_until(tokens, pos, &[Token::Semicolon])?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Assign { target, expr })
}

/// `IF cond THEN ... [ELSIF cond THEN ...] [ELSE ...] END IF;`
fn parse_if(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    let condition = collect_sql_until(tokens, pos, &[Token::Then])?;
    expect_token(tokens, pos, &Token::Then)?;
    let then_block = super::parse_statements(tokens, pos)?;

    let mut elsif_branches = Vec::new();
    while matches!(tokens.get(*pos), Some(Token::Elsif)) {
        *pos += 1;
        let cond = collect_sql_until(tokens, pos, &[Token::Then])?;
        expect_token(tokens, pos, &Token::Then)?;
        let body = super::parse_statements(tokens, pos)?;
        elsif_branches.push(ElsIfBranch {
            condition: cond,
            body,
        });
    }

    let else_block = if matches!(tokens.get(*pos), Some(Token::Else)) {
        *pos += 1;
        Some(super::parse_statements(tokens, pos)?)
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
fn parse_while(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    let condition = collect_sql_until(tokens, pos, &[Token::Loop])?;
    expect_token(tokens, pos, &Token::Loop)?;
    let body = super::parse_statements(tokens, pos)?;
    expect_token(tokens, pos, &Token::EndLoop)?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::While { condition, body })
}

/// `FOR var IN [REVERSE] start..end LOOP ... END LOOP;`
fn parse_for(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
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
    let body = super::parse_statements(tokens, pos)?;
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
fn parse_loop(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    let body = super::parse_statements(tokens, pos)?;
    expect_token(tokens, pos, &Token::EndLoop)?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Loop { body })
}

/// `RETURN expr;`
fn parse_return(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    let expr = collect_sql_until(tokens, pos, &[Token::Semicolon])?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Return { expr })
}

/// `RETURN QUERY sql;`
fn parse_return_query(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    let query = collect_raw_sql_until(tokens, pos, &[Token::Semicolon]);
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::ReturnQuery { query })
}

/// `RAISE [NOTICE|WARNING|EXCEPTION] 'message';`
fn parse_raise(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
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
        _ => RaiseLevel::Exception,
    };
    let message = collect_sql_until(tokens, pos, &[Token::Semicolon])?;
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Raise { level, message })
}

/// Capture DML statement as raw SQL.
fn parse_dml(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    let sql = collect_raw_sql_until(tokens, pos, &[Token::Semicolon]);
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Dml { sql })
}

/// ROLLBACK [TO [SAVEPOINT] <name>]
fn parse_rollback(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    if *pos < tokens.len() && tokens[*pos] == Token::To {
        *pos += 1;
        if *pos < tokens.len() && tokens[*pos] == Token::Savepoint {
            *pos += 1;
        }
        let name = match tokens.get(*pos) {
            Some(Token::Ident(n)) => {
                let n = n.to_lowercase();
                *pos += 1;
                n
            }
            _ => {
                return Err(ProceduralError::parse(
                    "expected savepoint name after ROLLBACK TO",
                ));
            }
        };
        skip_if(tokens, pos, &Token::Semicolon);
        Ok(Statement::RollbackTo { name })
    } else {
        skip_if(tokens, pos, &Token::Semicolon);
        Ok(Statement::Rollback)
    }
}

/// SAVEPOINT <name>
fn parse_savepoint(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    let name = match tokens.get(*pos) {
        Some(Token::Ident(n)) => {
            let n = n.to_lowercase();
            *pos += 1;
            n
        }
        _ => {
            return Err(ProceduralError::parse(
                "expected savepoint name after SAVEPOINT",
            ));
        }
    };
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::Savepoint { name })
}

/// RELEASE [SAVEPOINT] <name>
fn parse_release(tokens: &[Token], pos: &mut usize) -> Result<Statement, ProceduralError> {
    *pos += 1;
    if *pos < tokens.len() && tokens[*pos] == Token::Savepoint {
        *pos += 1;
    }
    let name = match tokens.get(*pos) {
        Some(Token::Ident(n)) => {
            let n = n.to_lowercase();
            *pos += 1;
            n
        }
        _ => {
            return Err(ProceduralError::parse(
                "expected savepoint name after RELEASE",
            ));
        }
    };
    skip_if(tokens, pos, &Token::Semicolon);
    Ok(Statement::ReleaseSavepoint { name })
}
