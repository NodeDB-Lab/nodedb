//! State transition constraint enforcement.
//!
//! On UPDATE, if a constrained column's value changes, validates that
//! `(OLD.value → NEW.value)` is in the allowed transitions list.
//! If the transition requires a role, verifies the user holds it.

use crate::bridge::envelope::ErrorCode;
use crate::control::security::catalog::StateTransitionDef;

/// Check all state transition constraints for an UPDATE operation.
///
/// `old_doc` and `new_doc` are the pre- and post-update JSON documents.
/// `user_roles` are the roles held by the authenticated user.
///
/// Returns `Ok(())` if all constraints pass, or the first violation found.
pub fn check_state_transitions(
    collection: &str,
    constraints: &[StateTransitionDef],
    old_doc: &serde_json::Value,
    new_doc: &serde_json::Value,
    user_roles: &[String],
) -> Result<(), ErrorCode> {
    let old_obj = old_doc.as_object();
    let new_obj = new_doc.as_object();

    for constraint in constraints {
        let old_val = old_obj
            .and_then(|o| o.get(&constraint.column))
            .and_then(|v| v.as_str());
        let new_val = new_obj
            .and_then(|o| o.get(&constraint.column))
            .and_then(|v| v.as_str());

        // If neither document has the constrained column, skip.
        let (Some(old_str), Some(new_str)) = (old_val, new_val) else {
            continue;
        };

        // If the value didn't change, no transition — skip.
        if old_str == new_str {
            continue;
        }

        // Find a matching transition rule.
        let matching_rule = constraint
            .transitions
            .iter()
            .find(|r| r.from == old_str && r.to == new_str);

        match matching_rule {
            None => {
                return Err(ErrorCode::StateTransitionViolation {
                    collection: collection.to_string(),
                    detail: format!(
                        "constraint '{}': transition '{}' → '{}' not allowed on column '{}'",
                        constraint.name, old_str, new_str, constraint.column
                    ),
                });
            }
            Some(rule) => {
                // Check role guard if present.
                if let Some(ref required_role) = rule.required_role
                    && !user_roles
                        .iter()
                        .any(|r| r.eq_ignore_ascii_case(required_role))
                {
                    return Err(ErrorCode::StateTransitionViolation {
                        collection: collection.to_string(),
                        detail: format!(
                            "constraint '{}': transition '{}' → '{}' requires role '{}'",
                            constraint.name, old_str, new_str, required_role
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::TransitionRule;

    fn invoice_constraint() -> StateTransitionDef {
        StateTransitionDef {
            name: "status_flow".into(),
            column: "status".into(),
            transitions: vec![
                TransitionRule {
                    from: "draft".into(),
                    to: "submitted".into(),
                    required_role: None,
                },
                TransitionRule {
                    from: "draft".into(),
                    to: "cancelled".into(),
                    required_role: None,
                },
                TransitionRule {
                    from: "submitted".into(),
                    to: "approved".into(),
                    required_role: Some("manager".into()),
                },
                TransitionRule {
                    from: "submitted".into(),
                    to: "draft".into(),
                    required_role: None,
                },
                TransitionRule {
                    from: "approved".into(),
                    to: "posted".into(),
                    required_role: Some("accountant".into()),
                },
                TransitionRule {
                    from: "posted".into(),
                    to: "voided".into(),
                    required_role: Some("controller".into()),
                },
            ],
        }
    }

    #[test]
    fn allowed_transition_no_role() {
        let c = invoice_constraint();
        let old = serde_json::json!({"status": "draft"});
        let new = serde_json::json!({"status": "submitted"});
        assert!(check_state_transitions("invoices", &[c], &old, &new, &[]).is_ok());
    }

    #[test]
    fn disallowed_transition() {
        let c = invoice_constraint();
        let old = serde_json::json!({"status": "draft"});
        let new = serde_json::json!({"status": "posted"});
        assert!(check_state_transitions("invoices", &[c], &old, &new, &[]).is_err());
    }

    #[test]
    fn role_guarded_with_role() {
        let c = invoice_constraint();
        let old = serde_json::json!({"status": "submitted"});
        let new = serde_json::json!({"status": "approved"});
        let roles = vec!["manager".to_string()];
        assert!(check_state_transitions("invoices", &[c], &old, &new, &roles).is_ok());
    }

    #[test]
    fn role_guarded_without_role() {
        let c = invoice_constraint();
        let old = serde_json::json!({"status": "submitted"});
        let new = serde_json::json!({"status": "approved"});
        assert!(check_state_transitions("invoices", &[c], &old, &new, &[]).is_err());
    }

    #[test]
    fn role_guarded_wrong_role() {
        let c = invoice_constraint();
        let old = serde_json::json!({"status": "submitted"});
        let new = serde_json::json!({"status": "approved"});
        let roles = vec!["clerk".to_string()];
        assert!(check_state_transitions("invoices", &[c], &old, &new, &roles).is_err());
    }

    #[test]
    fn no_change_passes() {
        let c = invoice_constraint();
        let old = serde_json::json!({"status": "draft"});
        let new = serde_json::json!({"status": "draft"});
        assert!(check_state_transitions("invoices", &[c], &old, &new, &[]).is_ok());
    }

    #[test]
    fn missing_column_passes() {
        let c = invoice_constraint();
        let old = serde_json::json!({"amount": 100});
        let new = serde_json::json!({"amount": 200});
        assert!(check_state_transitions("invoices", &[c], &old, &new, &[]).is_ok());
    }

    #[test]
    fn empty_constraints_passes() {
        let old = serde_json::json!({"status": "draft"});
        let new = serde_json::json!({"status": "anything"});
        assert!(check_state_transitions("invoices", &[], &old, &new, &[]).is_ok());
    }
}
