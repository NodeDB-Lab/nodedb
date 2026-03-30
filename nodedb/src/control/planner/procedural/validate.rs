//! Procedural function body validation.
//!
//! Checks that a procedural block is legal for a `CREATE FUNCTION` body:
//! - No DML statements (INSERT/UPDATE/DELETE)
//! - No transaction control (COMMIT/ROLLBACK)
//! - All loops have bounded iteration counts (for plan compilation)
//! - No unsupported patterns (dynamic SQL, unbounded recursion)

use super::ast::*;

/// Maximum iterations a loop can be unrolled to during plan compilation.
/// Loops exceeding this threshold are rejected at CREATE FUNCTION time.
/// (Procedures and triggers use the statement executor instead.)
pub const MAX_LOOP_UNROLL: u64 = 16;

/// Validate a procedural block for use as a function body.
///
/// Returns `Ok(())` if the block is valid, or `Err(message)` with a clear
/// error message if it contains forbidden constructs.
pub fn validate_function_block(block: &ProceduralBlock) -> Result<(), String> {
    for stmt in &block.statements {
        validate_statement(stmt, 0)?;
    }
    Ok(())
}

fn validate_statement(stmt: &Statement, loop_depth: usize) -> Result<(), String> {
    match stmt {
        Statement::Dml { sql } => Err(format!(
            "DML is not allowed in function bodies: '{sql}'. \
                 Use CREATE PROCEDURE for side-effecting logic"
        )),
        Statement::Commit => Err("COMMIT is not allowed in function bodies. \
                 Use CREATE PROCEDURE for transaction control"
            .into()),
        Statement::Rollback => Err("ROLLBACK is not allowed in function bodies. \
                 Use CREATE PROCEDURE for transaction control"
            .into()),
        Statement::If {
            then_block,
            elsif_branches,
            else_block,
            ..
        } => {
            for s in then_block {
                validate_statement(s, loop_depth)?;
            }
            for branch in elsif_branches {
                for s in &branch.body {
                    validate_statement(s, loop_depth)?;
                }
            }
            if let Some(else_stmts) = else_block {
                for s in else_stmts {
                    validate_statement(s, loop_depth)?;
                }
            }
            Ok(())
        }
        Statement::While { .. } | Statement::Loop { .. } => {
            // Bare LOOP and WHILE in functions are rejected — we can't determine
            // bound at compile time without analyzing the condition + body.
            // Use FOR with known bounds instead.
            Err("LOOP/WHILE is not supported in function bodies \
                 (loop bounds cannot be determined at compile time). \
                 Use FOR i IN start..end LOOP for bounded iteration, \
                 or CREATE PROCEDURE for unbounded loops"
                .into())
        }
        Statement::For {
            body, start, end, ..
        } => {
            // Check if bounds are numeric literals (statically known).
            let start_val = parse_integer_literal(&start.sql);
            let end_val = parse_integer_literal(&end.sql);
            if let (Some(s), Some(e)) = (start_val, end_val) {
                let iterations = if e >= s { e - s + 1 } else { 0 };
                if iterations > MAX_LOOP_UNROLL as i64 {
                    return Err(format!(
                        "FOR loop has {iterations} iterations, exceeds unrolling \
                         threshold ({MAX_LOOP_UNROLL}). Reduce range or use \
                         CREATE PROCEDURE for large iterations"
                    ));
                }
            } else {
                return Err(
                    "FOR loop bounds must be integer literals in function bodies \
                     (required for compile-time unrolling). Use CREATE PROCEDURE \
                     for dynamic loop bounds"
                        .into(),
                );
            }
            for s in body {
                validate_statement(s, loop_depth + 1)?;
            }
            Ok(())
        }
        Statement::Break | Statement::Continue => {
            if loop_depth == 0 {
                Err("BREAK/CONTINUE outside of a loop".into())
            } else {
                Ok(())
            }
        }
        Statement::Declare { .. }
        | Statement::Assign { .. }
        | Statement::Return { .. }
        | Statement::ReturnQuery { .. }
        | Statement::Raise { .. } => Ok(()),
    }
}

/// Try to parse a string as an integer literal.
fn parse_integer_literal(s: &str) -> Option<i64> {
    s.trim().parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(stmts: Vec<Statement>) -> ProceduralBlock {
        ProceduralBlock { statements: stmts }
    }

    #[test]
    fn valid_if_else() {
        let block = make_block(vec![Statement::If {
            condition: SqlExpr::new("x > 0"),
            then_block: vec![Statement::Return {
                expr: SqlExpr::new("1"),
            }],
            elsif_branches: vec![],
            else_block: Some(vec![Statement::Return {
                expr: SqlExpr::new("0"),
            }]),
        }]);
        assert!(validate_function_block(&block).is_ok());
    }

    #[test]
    fn reject_dml() {
        let block = make_block(vec![Statement::Dml {
            sql: "INSERT INTO users VALUES (1)".into(),
        }]);
        assert!(validate_function_block(&block).is_err());
    }

    #[test]
    fn reject_commit() {
        let block = make_block(vec![Statement::Commit]);
        assert!(validate_function_block(&block).is_err());
    }

    #[test]
    fn reject_unbounded_loop() {
        let block = make_block(vec![Statement::While {
            condition: SqlExpr::new("true"),
            body: vec![Statement::Break],
        }]);
        assert!(validate_function_block(&block).is_err());
    }

    #[test]
    fn accept_bounded_for() {
        let block = make_block(vec![Statement::For {
            var: "i".into(),
            start: SqlExpr::new("1"),
            end: SqlExpr::new("5"),
            reverse: false,
            body: vec![Statement::Return {
                expr: SqlExpr::new("i"),
            }],
        }]);
        assert!(validate_function_block(&block).is_ok());
    }

    #[test]
    fn reject_for_exceeding_threshold() {
        let block = make_block(vec![Statement::For {
            var: "i".into(),
            start: SqlExpr::new("1"),
            end: SqlExpr::new("100"),
            reverse: false,
            body: vec![Statement::Return {
                expr: SqlExpr::new("i"),
            }],
        }]);
        assert!(validate_function_block(&block).is_err());
    }

    #[test]
    fn reject_dynamic_for_bounds() {
        let block = make_block(vec![Statement::For {
            var: "i".into(),
            start: SqlExpr::new("1"),
            end: SqlExpr::new("n"), // not a literal
            reverse: false,
            body: vec![],
        }]);
        assert!(validate_function_block(&block).is_err());
    }

    #[test]
    fn reject_dml_nested_in_if() {
        let block = make_block(vec![Statement::If {
            condition: SqlExpr::new("true"),
            then_block: vec![Statement::Dml {
                sql: "DELETE FROM t".into(),
            }],
            elsif_branches: vec![],
            else_block: None,
        }]);
        assert!(validate_function_block(&block).is_err());
    }
}
