//! Procedural SQL → expression compiler.
//!
//! Compiles a `ProceduralBlock` AST into an equivalent SQL expression string.
//! The output is a single SQL expression (not a statement) that can be registered
//! as a DataFusion ScalarUDF body.
//!
//! **Compilation rules:**
//! - `IF/ELSIF/ELSE` → `CASE WHEN ... THEN ... END`
//! - `DECLARE x := expr` → inlined at each use site
//! - `FOR i IN 1..N LOOP` → unrolled CASE WHEN chain
//! - `RETURN expr` → the expression value at that branch
//! - Subqueries in conditions → passed through (DataFusion plans as semi-joins)

use std::collections::HashMap;

use super::ast::*;
use super::validate::MAX_LOOP_UNROLL;

/// Compile a procedural block into a single SQL expression string.
///
/// The resulting expression can be used as the body of a DataFusion ScalarUDF.
/// Variables declared with DECLARE are inlined at each use site.
///
/// Returns the compiled SQL expression, or an error if the block cannot be
/// compiled to a single expression.
pub fn compile_to_sql(block: &ProceduralBlock) -> Result<String, String> {
    let mut ctx = CompileContext::new();
    compile_statements(&block.statements, &mut ctx)
}

/// Tracks variable bindings for inlining DECLARE/ASSIGN.
struct CompileContext {
    /// Variable name → SQL expression for inlining.
    variables: HashMap<String, String>,
}

impl CompileContext {
    fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    /// Substitute known variables in a SQL expression.
    fn substitute(&self, sql: &str) -> String {
        let mut result = sql.to_string();
        for (name, expr) in &self.variables {
            // Replace whole-word occurrences of the variable name.
            result = replace_identifier(&result, name, &format!("({expr})"));
        }
        result
    }
}

/// Compile a list of statements into a single SQL expression.
///
/// The last RETURN in the control flow determines the expression's value.
/// If no RETURN is reached, the expression evaluates to NULL.
fn compile_statements(stmts: &[Statement], ctx: &mut CompileContext) -> Result<String, String> {
    // Strategy: process statements sequentially. DECLARE and ASSIGN update
    // the variable context. IF and FOR produce CASE WHEN expressions.
    // RETURN sets the final expression value.
    //
    // For a sequence like:
    //   DECLARE x INT := 1;
    //   x := x + 1;
    //   IF x > 1 THEN RETURN 'yes'; ELSE RETURN 'no'; END IF;
    //
    // This compiles to:
    //   CASE WHEN ((1) + 1) > 1 THEN 'yes' ELSE 'no' END

    for stmt in stmts {
        match stmt {
            Statement::Declare { name, default, .. } => {
                let expr = match default {
                    Some(e) => ctx.substitute(&e.sql),
                    None => "NULL".into(),
                };
                ctx.variables.insert(name.clone(), expr);
            }
            Statement::Assign { target, expr } => {
                let substituted = ctx.substitute(&expr.sql);
                ctx.variables.insert(target.clone(), substituted);
            }
            Statement::If {
                condition,
                then_block,
                elsif_branches,
                else_block,
            } => {
                return compile_if(condition, then_block, elsif_branches, else_block, ctx);
            }
            Statement::Return { expr } => {
                return Ok(ctx.substitute(&expr.sql));
            }
            Statement::For {
                var,
                start,
                end,
                reverse,
                body,
            } => {
                return compile_for(var, start, end, *reverse, body, ctx);
            }
            Statement::Raise {
                level: RaiseLevel::Exception,
                ..
            } => {
                // RAISE EXCEPTION in a branch terminates that path.
                // We can't represent this directly in a CASE WHEN expression.
                // For now, raise in a non-branching position → compile error.
                return Err(
                    "RAISE EXCEPTION outside an IF branch cannot be compiled to an expression. \
                     Move it inside an IF block or use CREATE PROCEDURE"
                        .into(),
                );
            }
            Statement::ReturnQuery { .. } => {
                return Err(
                    "RETURN QUERY in procedural functions is not yet supported. \
                     Use a plain RETURN with a subquery expression"
                        .into(),
                );
            }
            // LOOP/WHILE rejected by validator for function bodies.
            Statement::Loop { .. } | Statement::While { .. } => {
                return Err("LOOP/WHILE cannot be compiled to an expression. \
                     Use FOR with literal bounds or CREATE PROCEDURE"
                    .into());
            }
            // Break/Continue should not appear outside loops (validator catches this).
            Statement::Break | Statement::Continue => {
                return Err("BREAK/CONTINUE outside of a loop".into());
            }
            // Raise NOTICE/WARNING — informational, skip (no expression effect).
            Statement::Raise { .. } => {}
            // DML/Commit/Rollback — should be caught by validator before we get here.
            Statement::Dml { .. } | Statement::Commit | Statement::Rollback => {
                return Err("DML/transaction control not allowed in function bodies".into());
            }
        }
    }

    // If we reach here with no RETURN, the function returns NULL.
    Ok("NULL".into())
}

/// Compile IF/ELSIF/ELSE to CASE WHEN expression.
fn compile_if(
    condition: &SqlExpr,
    then_block: &[Statement],
    elsif_branches: &[ElsIfBranch],
    else_block: &Option<Vec<Statement>>,
    ctx: &mut CompileContext,
) -> Result<String, String> {
    let mut parts = Vec::new();

    // WHEN condition THEN result
    let cond_sql = ctx.substitute(&condition.sql);
    let then_sql = compile_statements(then_block, &mut ctx.clone_vars())?;
    parts.push(format!("WHEN {cond_sql} THEN {then_sql}"));

    // ELSIF branches.
    for branch in elsif_branches {
        let cond = ctx.substitute(&branch.condition.sql);
        let body = compile_statements(&branch.body, &mut ctx.clone_vars())?;
        parts.push(format!("WHEN {cond} THEN {body}"));
    }

    // ELSE clause.
    let else_sql = if let Some(else_stmts) = else_block {
        compile_statements(else_stmts, &mut ctx.clone_vars())?
    } else {
        "NULL".into()
    };
    parts.push(format!("ELSE {else_sql}"));

    Ok(format!("CASE {} END", parts.join(" ")))
}

/// Compile FOR loop by unrolling into a nested CASE WHEN chain.
///
/// `FOR i IN 1..3 LOOP RETURN i * 2; END LOOP;`
/// compiles to: `CASE WHEN true THEN (1) * 2 END`
///
/// Since function FOR loops must contain a RETURN (otherwise value is undefined),
/// and the first iteration's RETURN determines the value, unrolling just evaluates
/// the body with each iteration's variable binding.
///
/// For FOR loops with conditional RETURNs:
/// `FOR i IN 1..3 LOOP IF arr[i] > threshold THEN RETURN i; END IF; END LOOP;`
/// → unrolled as nested CASE WHEN for each iteration.
fn compile_for(
    var: &str,
    start: &SqlExpr,
    end: &SqlExpr,
    reverse: bool,
    body: &[Statement],
    ctx: &mut CompileContext,
) -> Result<String, String> {
    let start_val = start
        .sql
        .trim()
        .parse::<i64>()
        .map_err(|_| "FOR loop start must be integer literal in function bodies")?;
    let end_val = end
        .sql
        .trim()
        .parse::<i64>()
        .map_err(|_| "FOR loop end must be integer literal in function bodies")?;

    let iterations: Vec<i64> = if reverse {
        (end_val..=start_val).rev().collect()
    } else {
        (start_val..=end_val).collect()
    };

    if iterations.len() as u64 > MAX_LOOP_UNROLL {
        return Err(format!(
            "FOR loop has {} iterations, exceeds unrolling threshold ({MAX_LOOP_UNROLL})",
            iterations.len()
        ));
    }

    if iterations.is_empty() {
        return Ok("NULL".into());
    }

    // Unroll: for each iteration, bind the variable and compile the body.
    // Use nested CASE WHEN to try each iteration in order.
    // The first RETURN hit determines the value.
    let mut case_parts = Vec::new();
    for val in &iterations {
        let mut iter_ctx = ctx.clone_vars();
        iter_ctx.variables.insert(var.to_string(), val.to_string());
        let body_sql = compile_statements(body, &mut iter_ctx)?;
        // Each iteration becomes a WHEN clause. If the body is unconditional
        // (always returns), it's `WHEN true THEN <body>`. If conditional,
        // the inner CASE WHEN handles it.
        if body_sql != "NULL" {
            case_parts.push(format!("WHEN true THEN {body_sql}"));
            // For unconditional returns, first iteration wins.
            if !body.iter().any(|s| matches!(s, Statement::If { .. })) {
                break;
            }
        }
    }

    if case_parts.is_empty() {
        return Ok("NULL".into());
    }

    case_parts.push("ELSE NULL".into());
    Ok(format!("CASE {} END", case_parts.join(" ")))
}

impl CompileContext {
    /// Create a snapshot of the current variable bindings.
    /// Used for branches (IF/ELSE) that shouldn't leak assignments to siblings.
    fn clone_vars(&self) -> Self {
        Self {
            variables: self.variables.clone(),
        }
    }
}

/// Replace whole-word occurrences of `name` in `sql` with `replacement`.
///
/// Only replaces when `name` is surrounded by non-alphanumeric characters
/// (or at string boundaries). This prevents replacing "x" in "max" or "text".
fn replace_identifier(sql: &str, name: &str, replacement: &str) -> String {
    if name.is_empty() || !sql.contains(name) {
        return sql.to_string();
    }

    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let name_bytes = name.as_bytes();
    let name_len = name_bytes.len();
    let mut i = 0;

    while i < bytes.len() {
        // Skip string literals.
        if bytes[i] == b'\'' {
            result.push('\'');
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    result.push('\'');
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'\'' {
                        result.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    result.push(bytes[i] as char);
                    i += 1;
                }
            }
            continue;
        }

        // Check for whole-word match.
        if i + name_len <= bytes.len()
            && sql[i..i + name_len].eq_ignore_ascii_case(name)
            && !is_ident_char(bytes.get(i.wrapping_sub(1)).copied(), i == 0)
            && !is_ident_continue(bytes.get(i + name_len).copied())
        {
            result.push_str(replacement);
            i += name_len;
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }

    result
}

fn is_ident_char(byte: Option<u8>, at_start: bool) -> bool {
    if at_start {
        return false;
    }
    byte.is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn is_ident_continue(byte: Option<u8>) -> bool {
    byte.is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(stmts: Vec<Statement>) -> ProceduralBlock {
        ProceduralBlock { statements: stmts }
    }

    #[test]
    fn compile_simple_return() {
        let block = make_block(vec![Statement::Return {
            expr: SqlExpr::new("42"),
        }]);
        assert_eq!(compile_to_sql(&block).unwrap(), "42");
    }

    #[test]
    fn compile_if_else() {
        let block = make_block(vec![Statement::If {
            condition: SqlExpr::new("x > 0"),
            then_block: vec![Statement::Return {
                expr: SqlExpr::new("'positive'"),
            }],
            elsif_branches: vec![],
            else_block: Some(vec![Statement::Return {
                expr: SqlExpr::new("'non-positive'"),
            }]),
        }]);
        let sql = compile_to_sql(&block).unwrap();
        assert!(sql.contains("CASE"));
        assert!(sql.contains("WHEN x > 0 THEN 'positive'"));
        assert!(sql.contains("ELSE 'non-positive'"));
    }

    #[test]
    fn compile_if_elsif_else() {
        let block = make_block(vec![Statement::If {
            condition: SqlExpr::new("score > 0.9"),
            then_block: vec![Statement::Return {
                expr: SqlExpr::new("'critical'"),
            }],
            elsif_branches: vec![
                ElsIfBranch {
                    condition: SqlExpr::new("score > 0.7"),
                    body: vec![Statement::Return {
                        expr: SqlExpr::new("'high'"),
                    }],
                },
                ElsIfBranch {
                    condition: SqlExpr::new("score > 0.4"),
                    body: vec![Statement::Return {
                        expr: SqlExpr::new("'medium'"),
                    }],
                },
            ],
            else_block: Some(vec![Statement::Return {
                expr: SqlExpr::new("'low'"),
            }]),
        }]);
        let sql = compile_to_sql(&block).unwrap();
        assert_eq!(
            sql,
            "CASE WHEN score > 0.9 THEN 'critical' \
             WHEN score > 0.7 THEN 'high' \
             WHEN score > 0.4 THEN 'medium' \
             ELSE 'low' END"
        );
    }

    #[test]
    fn compile_declare_and_inline() {
        let block = make_block(vec![
            Statement::Declare {
                name: "threshold".into(),
                data_type: "FLOAT".into(),
                default: Some(SqlExpr::new("0.5")),
            },
            Statement::If {
                condition: SqlExpr::new("score > threshold"),
                then_block: vec![Statement::Return {
                    expr: SqlExpr::new("'high'"),
                }],
                elsif_branches: vec![],
                else_block: Some(vec![Statement::Return {
                    expr: SqlExpr::new("'low'"),
                }]),
            },
        ]);
        let sql = compile_to_sql(&block).unwrap();
        // threshold should be inlined as (0.5)
        assert!(sql.contains("score > (0.5)"));
    }

    #[test]
    fn compile_variable_reassign() {
        let block = make_block(vec![
            Statement::Declare {
                name: "x".into(),
                data_type: "INT".into(),
                default: Some(SqlExpr::new("1")),
            },
            Statement::Assign {
                target: "x".into(),
                expr: SqlExpr::new("x + 1"),
            },
            Statement::Return {
                expr: SqlExpr::new("x"),
            },
        ]);
        let sql = compile_to_sql(&block).unwrap();
        // x starts as 1, then x := x+1 = (1)+1, then RETURN x = ((1) + 1)
        assert!(sql.contains("(1) + 1"));
    }

    #[test]
    fn compile_for_loop_unrolled() {
        let block = make_block(vec![Statement::For {
            var: "i".into(),
            start: SqlExpr::new("1"),
            end: SqlExpr::new("3"),
            reverse: false,
            body: vec![Statement::Return {
                expr: SqlExpr::new("i * 2"),
            }],
        }]);
        let sql = compile_to_sql(&block).unwrap();
        // First iteration: i=1, body = "(1) * 2" (variable inlined with parens).
        assert!(sql.contains("(1) * 2"), "actual: {sql}");
    }

    #[test]
    fn replace_whole_word() {
        assert_eq!(replace_identifier("x + 1", "x", "(5)"), "(5) + 1");
        assert_eq!(replace_identifier("max(x)", "x", "(5)"), "max((5))");
        // Should NOT replace "x" inside "text" or "max".
        assert_eq!(replace_identifier("text", "x", "(5)"), "text");
    }

    #[test]
    fn no_return_gives_null() {
        let block = make_block(vec![Statement::Declare {
            name: "x".into(),
            data_type: "INT".into(),
            default: Some(SqlExpr::new("1")),
        }]);
        assert_eq!(compile_to_sql(&block).unwrap(), "NULL");
    }
}
