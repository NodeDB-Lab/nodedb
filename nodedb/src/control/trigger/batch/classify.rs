//! Classify trigger body at CREATE TRIGGER time for batch eligibility.
//!
//! Analyzes the procedural AST to determine whether the trigger can safely
//! process rows in batches or must fall back to row-at-a-time execution.
//!
//! A trigger is `BatchSafe` if:
//! - Its body contains only DML targeting a single collection
//! - No row-dependent IF/ELSIF branching that dispatches to different collections
//! - No dynamic SQL or complex control flow affecting DML targets
//!
//! Otherwise it is `RowAtATime`.

use crate::control::planner::procedural::ast::Statement;
use crate::control::security::catalog::trigger_types::TriggerBatchMode;

/// Analyze a trigger body and return its batch mode.
///
/// Parses the body SQL into an AST and walks it to find DML targets.
/// If all DML targets the same collection and there is no conditional branching
/// that changes the target, returns `BatchSafe`. Otherwise `RowAtATime`.
pub fn classify_trigger_body(body_sql: &str) -> TriggerBatchMode {
    let block = match crate::control::planner::procedural::parse_block(body_sql) {
        Ok(b) => b,
        Err(_) => return TriggerBatchMode::RowAtATime,
    };

    let mut targets = Vec::new();
    let mut has_conditional_dml = false;
    collect_dml_info(
        &block.statements,
        false,
        &mut targets,
        &mut has_conditional_dml,
    );

    // If DML targets depend on control flow (IF/ELSIF branches with different targets),
    // we must fall back to row-at-a-time since each row may take a different branch.
    if has_conditional_dml {
        return TriggerBatchMode::RowAtATime;
    }

    // If all DML targets the same collection (or there's no DML), batch is safe.
    let unique_targets: std::collections::HashSet<&str> =
        targets.iter().map(|s| s.as_str()).collect();
    if unique_targets.len() <= 1 {
        TriggerBatchMode::BatchSafe
    } else {
        TriggerBatchMode::RowAtATime
    }
}

/// Recursively walk statements collecting DML target collections.
///
/// `in_conditional` tracks whether we're inside an IF/ELSIF branch — if
/// different branches target different collections, the trigger can't be batched.
fn collect_dml_info(
    stmts: &[Statement],
    in_conditional: bool,
    targets: &mut Vec<String>,
    has_conditional_dml: &mut bool,
) {
    for stmt in stmts {
        match stmt {
            Statement::Dml { sql } => {
                if let Some(target) = extract_dml_target(sql) {
                    if in_conditional && !targets.is_empty() && !targets.contains(&target) {
                        *has_conditional_dml = true;
                    }
                    targets.push(target);
                }
            }
            Statement::If {
                then_block,
                elsif_branches,
                else_block,
                ..
            } => {
                // DML inside IF branches is conditional on row data.
                collect_dml_info(then_block, true, targets, has_conditional_dml);
                for branch in elsif_branches {
                    collect_dml_info(&branch.body, true, targets, has_conditional_dml);
                }
                if let Some(else_stmts) = else_block {
                    collect_dml_info(else_stmts, true, targets, has_conditional_dml);
                }
            }
            Statement::Loop { body }
            | Statement::While { body, .. }
            | Statement::For { body, .. } => {
                collect_dml_info(body, in_conditional, targets, has_conditional_dml);
            }
            _ => {}
        }
    }
}

/// Extract the target collection name from a raw DML SQL string.
fn extract_dml_target(sql: &str) -> Option<String> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.trim().to_uppercase();

    if upper.starts_with("INSERT INTO") && tokens.len() >= 3 {
        Some(tokens[2].to_lowercase().trim_matches('(').to_string())
    } else if upper.starts_with("UPDATE") && tokens.len() >= 2 {
        Some(tokens[1].to_lowercase())
    } else if upper.starts_with("DELETE FROM") && tokens.len() >= 3 {
        Some(tokens[2].to_lowercase())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_insert_is_batch_safe() {
        let body = "BEGIN INSERT INTO audit (id) VALUES (NEW.id); END";
        assert_eq!(classify_trigger_body(body), TriggerBatchMode::BatchSafe);
    }

    #[test]
    fn multiple_inserts_same_target_batch_safe() {
        let body = "BEGIN \
            INSERT INTO audit (id, op) VALUES (NEW.id, 'insert'); \
            INSERT INTO audit (id, op) VALUES (NEW.id, 'log'); \
        END";
        assert_eq!(classify_trigger_body(body), TriggerBatchMode::BatchSafe);
    }

    #[test]
    fn multiple_different_targets_row_at_a_time() {
        let body = "BEGIN \
            INSERT INTO audit (id) VALUES (NEW.id); \
            INSERT INTO vectors (id) VALUES (NEW.id); \
        END";
        assert_eq!(classify_trigger_body(body), TriggerBatchMode::RowAtATime);
    }

    #[test]
    fn conditional_different_targets_row_at_a_time() {
        let body = "BEGIN \
            IF NEW.status = 'active' THEN \
                INSERT INTO active_users (id) VALUES (NEW.id); \
            ELSE \
                INSERT INTO inactive_users (id) VALUES (NEW.id); \
            END IF; \
        END";
        // Different collections in IF/ELSE branches → row-at-a-time.
        assert_eq!(classify_trigger_body(body), TriggerBatchMode::RowAtATime);
    }

    #[test]
    fn conditional_same_target_batch_safe() {
        let body = "BEGIN \
            IF NEW.total > 100 THEN \
                INSERT INTO audit (id, note) VALUES (NEW.id, 'high'); \
            ELSE \
                INSERT INTO audit (id, note) VALUES (NEW.id, 'low'); \
            END IF; \
        END";
        // Same collection in both branches → batch safe.
        assert_eq!(classify_trigger_body(body), TriggerBatchMode::BatchSafe);
    }

    #[test]
    fn no_dml_is_batch_safe() {
        let body = "BEGIN RETURN 1; END";
        assert_eq!(classify_trigger_body(body), TriggerBatchMode::BatchSafe);
    }

    #[test]
    fn unparseable_body_is_row_at_a_time() {
        assert_eq!(
            classify_trigger_body("not valid sql"),
            TriggerBatchMode::RowAtATime
        );
    }
}
