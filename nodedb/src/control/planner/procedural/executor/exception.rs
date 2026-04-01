//! Exception condition matching for procedural SQL exception handlers.
//!
//! Maps error messages to exception conditions (OTHERS, SQLSTATE, named).

use crate::control::planner::procedural::ast::ExceptionCondition;

/// Check if an exception condition matches an error message.
///
/// Matching rules:
/// - `Others` matches any error (catch-all).
/// - `SqlState(code)` matches if the error message contains the SQLSTATE code.
/// - `Named(name)` matches common named conditions against error patterns.
pub fn exception_matches(condition: &ExceptionCondition, error: &str) -> bool {
    match condition {
        ExceptionCondition::Others => true,
        ExceptionCondition::SqlState(code) => error.contains(code),
        ExceptionCondition::Named(name) => {
            let lower = error.to_lowercase();
            match name.as_str() {
                "UNIQUE_VIOLATION" => lower.contains("unique") || lower.contains("duplicate"),
                "FOREIGN_KEY_VIOLATION" => lower.contains("foreign key"),
                "CHECK_VIOLATION" => lower.contains("check constraint"),
                "NOT_NULL_VIOLATION" => {
                    lower.contains("not null") || lower.contains("null constraint")
                }
                "NO_DATA_FOUND" => lower.contains("not found") || lower.contains("no data"),
                "DIVISION_BY_ZERO" => lower.contains("division by zero"),
                _ => lower.contains(&name.to_lowercase()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn others_matches_everything() {
        assert!(exception_matches(&ExceptionCondition::Others, "any error"));
        assert!(exception_matches(&ExceptionCondition::Others, ""));
    }

    #[test]
    fn sqlstate_matches_code() {
        assert!(exception_matches(
            &ExceptionCondition::SqlState("23505".into()),
            "ERROR: duplicate key violates unique constraint (SQLSTATE 23505)"
        ));
        assert!(!exception_matches(
            &ExceptionCondition::SqlState("23505".into()),
            "some other error"
        ));
    }

    #[test]
    fn named_unique_violation() {
        let cond = ExceptionCondition::Named("UNIQUE_VIOLATION".into());
        assert!(exception_matches(
            &cond,
            "duplicate key value violates unique constraint"
        ));
        assert!(exception_matches(&cond, "UNIQUE constraint failed"));
        assert!(!exception_matches(&cond, "foreign key violation"));
    }

    #[test]
    fn named_no_data_found() {
        let cond = ExceptionCondition::Named("NO_DATA_FOUND".into());
        assert!(exception_matches(&cond, "document not found"));
        assert!(exception_matches(&cond, "no data returned"));
        assert!(!exception_matches(&cond, "success"));
    }

    #[test]
    fn named_division_by_zero() {
        let cond = ExceptionCondition::Named("DIVISION_BY_ZERO".into());
        assert!(exception_matches(&cond, "division by zero"));
        assert!(!exception_matches(&cond, "overflow"));
    }

    #[test]
    fn named_fallback_substring() {
        let cond = ExceptionCondition::Named("TIMEOUT".into());
        assert!(exception_matches(&cond, "operation timeout exceeded"));
        assert!(!exception_matches(&cond, "connection refused"));
    }
}
