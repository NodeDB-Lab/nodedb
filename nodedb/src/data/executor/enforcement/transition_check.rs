//! Transition check predicate enforcement.
//!
//! Evaluates predicates with `OLD.*` and `NEW.*` column access on UPDATE.
//! If any predicate returns FALSE, the UPDATE is rejected.
//! Not evaluated on INSERT — only CHECK constraints apply to inserts.

use crate::bridge::envelope::ErrorCode;
use crate::control::security::catalog::TransitionCheckDef;

/// Check all transition check predicates for an UPDATE operation.
///
/// `old_doc` and `new_doc` are the pre- and post-update JSON documents.
/// Each predicate is evaluated via `SqlExpr::eval_with_old()`, which resolves
/// `Column(name)` against `new_doc` and `OldColumn(name)` against `old_doc`.
///
/// Returns `Ok(())` if all predicates pass, or the first violation found.
pub fn check_transition_predicates(
    collection: &str,
    checks: &[TransitionCheckDef],
    old_doc: &serde_json::Value,
    new_doc: &serde_json::Value,
) -> Result<(), ErrorCode> {
    let old_val = nodedb_types::Value::from(old_doc.clone());
    let new_val = nodedb_types::Value::from(new_doc.clone());
    for check in checks {
        let result = check.predicate.eval_with_old(&new_val, &old_val);
        let passed = match result {
            nodedb_types::Value::Bool(b) => b,
            nodedb_types::Value::Null => false, // NULL treated as FALSE for constraint purposes.
            _ => true,                          // Non-boolean non-null values are truthy.
        };
        if !passed {
            return Err(ErrorCode::TransitionCheckViolation {
                collection: collection.to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::expr_eval::{BinaryOp, SqlExpr};

    fn make_check(name: &str, predicate: SqlExpr) -> TransitionCheckDef {
        TransitionCheckDef {
            name: name.to_string(),
            predicate,
        }
    }

    #[test]
    fn simple_true_predicate() {
        // Predicate: TRUE (literal)
        let check = make_check(
            "always_pass",
            SqlExpr::Literal(nodedb_types::Value::Bool(true)),
        );
        let old = serde_json::json!({"x": 1});
        let new = serde_json::json!({"x": 2});
        assert!(check_transition_predicates("coll", &[check], &old, &new).is_ok());
    }

    #[test]
    fn simple_false_predicate() {
        let check = make_check(
            "always_fail",
            SqlExpr::Literal(nodedb_types::Value::Bool(false)),
        );
        let old = serde_json::json!({"x": 1});
        let new = serde_json::json!({"x": 2});
        assert!(check_transition_predicates("coll", &[check], &old, &new).is_err());
    }

    #[test]
    fn old_equals_new_column_check() {
        // Predicate: OLD.sealed = FALSE (i.e., can only update if not sealed)
        let check = make_check(
            "not_sealed",
            SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::OldColumn("sealed".into())),
                op: BinaryOp::Eq,
                right: Box::new(SqlExpr::Literal(nodedb_types::Value::Bool(false))),
            },
        );

        // OLD.sealed = false → predicate passes
        let old = serde_json::json!({"sealed": false, "amount": 100});
        let new = serde_json::json!({"sealed": false, "amount": 200});
        assert!(
            check_transition_predicates("coll", std::slice::from_ref(&check), &old, &new).is_ok()
        );

        // OLD.sealed = true → predicate fails
        let old_sealed = serde_json::json!({"sealed": true, "amount": 100});
        assert!(
            check_transition_predicates("coll", std::slice::from_ref(&check), &old_sealed, &new)
                .is_err()
        );
    }

    #[test]
    fn amount_cannot_decrease() {
        // Predicate: NEW.amount >= OLD.amount
        let check = make_check(
            "no_decrease",
            SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Column("amount".into())),
                op: BinaryOp::GtEq,
                right: Box::new(SqlExpr::OldColumn("amount".into())),
            },
        );

        // 200 >= 100 → ok
        let old = serde_json::json!({"amount": 100});
        let new = serde_json::json!({"amount": 200});
        assert!(
            check_transition_predicates("coll", std::slice::from_ref(&check), &old, &new).is_ok()
        );

        // 50 >= 100 → fail
        let new_less = serde_json::json!({"amount": 50});
        assert!(check_transition_predicates("coll", &[check], &old, &new_less).is_err());
    }

    #[test]
    fn multiple_checks_all_must_pass() {
        let c1 = make_check("c1", SqlExpr::Literal(nodedb_types::Value::Bool(true)));
        let c2 = make_check("c2", SqlExpr::Literal(nodedb_types::Value::Bool(false)));
        let old = serde_json::json!({});
        let new = serde_json::json!({});
        // c1 passes but c2 fails → overall failure
        assert!(check_transition_predicates("coll", &[c1, c2], &old, &new).is_err());
    }

    #[test]
    fn empty_checks_passes() {
        let old = serde_json::json!({"x": 1});
        let new = serde_json::json!({"x": 2});
        assert!(check_transition_predicates("coll", &[], &old, &new).is_ok());
    }

    #[test]
    fn null_result_treated_as_false() {
        // Predicate references a missing column → evaluates to NULL → treated as FALSE
        let check = make_check("null_check", SqlExpr::OldColumn("nonexistent".into()));
        let old = serde_json::json!({"x": 1});
        let new = serde_json::json!({"x": 2});
        assert!(check_transition_predicates("coll", &[check], &old, &new).is_err());
    }
}
