//! Type guard enforcement: per-field type + CHECK validation at write time.
//!
//! Evaluated on the Data Plane before WAL append. If any guard fails,
//! the write is rejected with a descriptive error.

use nodedb_sql::parser::type_expr::{parse_type_expr, value_matches_type};
use nodedb_types::TypeGuardFieldDef;

use crate::bridge::envelope::ErrorCode;

/// Inject DEFAULT and VALUE expressions into document fields before validation.
///
/// - **DEFAULT**: injected only if the field is absent or null.
/// - **VALUE**: always injected, overwriting any user-provided value.
///
/// Evaluation order: DEFAULT/VALUE injection → REQUIRED → type → CHECK.
/// This means `REQUIRED + DEFAULT` = field is always present (DEFAULT fills the gap
/// before REQUIRED rejects absent fields).
///
/// Expressions are evaluated using `parse_generated_expr` + `SqlExpr::eval`,
/// which supports cross-field references (e.g., `LOWER(REPLACE(title, ' ', '-'))`).
pub fn inject_defaults(
    guards: &[TypeGuardFieldDef],
    fields: &mut std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<(), ErrorCode> {
    for guard in guards {
        // VALUE: always overwrite.
        if let Some(ref expr_str) = guard.value_expr {
            let (expr, _deps) =
                nodedb_query::expr_parse::parse_generated_expr(expr_str).map_err(|e| {
                    ErrorCode::TypeGuardViolation {
                        collection: String::new(),
                        detail: format!(
                            "field '{}': invalid VALUE expression '{}': {e}",
                            guard.field, expr_str
                        ),
                    }
                })?;
            let doc = nodedb_types::Value::Object(fields.clone());
            fields.insert(guard.field.clone(), expr.eval(&doc));
        }
        // DEFAULT: inject only if absent or null.
        else if let Some(ref expr_str) = guard.default_expr {
            let is_absent = fields
                .get(&guard.field)
                .is_none_or(|v| matches!(v, nodedb_types::Value::Null));
            if is_absent {
                let (expr, _deps) = nodedb_query::expr_parse::parse_generated_expr(expr_str)
                    .map_err(|e| ErrorCode::TypeGuardViolation {
                        collection: String::new(),
                        detail: format!(
                            "field '{}': invalid DEFAULT expression '{}': {e}",
                            guard.field, expr_str
                        ),
                    })?;
                let doc = nodedb_types::Value::Object(fields.clone());
                fields.insert(guard.field.clone(), expr.eval(&doc));
            }
        }
    }
    Ok(())
}

/// Combined inject + validate for INSERT/UPSERT (full document, no written_fields filter).
///
/// Runs DEFAULT/VALUE injection, then type guard validation. Returns the first error.
pub fn inject_and_validate(
    collection: &str,
    guards: &[TypeGuardFieldDef],
    fields: &mut std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<(), ErrorCode> {
    inject_defaults(guards, fields)?;
    let doc = nodedb_types::Value::Object(fields.clone());
    check_type_guards(collection, guards, &doc, None)
}

/// Validate a document against type guard field definitions.
///
/// `doc` is the document being written (INSERT/UPSERT: the full document,
/// UPDATE: the merged document with existing + updated fields).
/// `written_fields` is the set of field names being written in this operation.
/// For INSERT/UPSERT, this is `None` (all fields are in scope). For UPDATE,
/// it is `Some(&[...])` containing only the modified fields.
///
/// Checks in order:
/// 1. REQUIRED — field must be present and non-null
/// 2. Type check — value must match the type expression
/// 3. CHECK expression — SQL expression must evaluate to true (see TODO below)
///
/// Returns `Ok(())` if all guards pass, or the first violation.
pub fn check_type_guards(
    collection: &str,
    guards: &[TypeGuardFieldDef],
    doc: &nodedb_types::Value,
    written_fields: Option<&[String]>,
) -> Result<(), ErrorCode> {
    for guard in guards {
        let field_written = match written_fields {
            None => true,
            Some(fields) => fields.iter().any(|f| f == &guard.field),
        };

        // For UPDATE: skip type-only guards on fields not being written.
        // Guards with a check_expr always run (they may be cross-field).
        if !field_written && guard.check_expr.is_none() {
            continue;
        }

        let value = resolve_field(doc, &guard.field);

        if field_written {
            // REQUIRED check.
            if guard.required {
                match value {
                    None | Some(nodedb_types::Value::Null) => {
                        return Err(ErrorCode::TypeGuardViolation {
                            collection: collection.to_string(),
                            detail: format!(
                                "field '{}' is required but absent or null",
                                guard.field
                            ),
                        });
                    }
                    _ => {}
                }
            }

            // Type check: only when the field is present.
            if let Some(val) = value
                && (!matches!(val, nodedb_types::Value::Null) || guard.required)
            {
                let type_expr = parse_type_expr(&guard.type_expr).map_err(|e| {
                    ErrorCode::TypeGuardViolation {
                        collection: collection.to_string(),
                        detail: format!(
                            "field '{}': invalid type expression '{}': {e}",
                            guard.field, guard.type_expr
                        ),
                    }
                })?;
                if !value_matches_type(val, &type_expr) {
                    return Err(ErrorCode::TypeGuardViolation {
                        collection: collection.to_string(),
                        detail: format!(
                            "field '{}': expected type '{}', got {:?}",
                            guard.field, guard.type_expr, val
                        ),
                    });
                }
            }
            // If the field is absent and not required, it's valid — skip type check.
        }

        // CHECK expression evaluation.
        // Skip if the guarded field is absent and not required — absent optional fields
        // don't need CHECK validation (the field simply isn't there).
        let field_absent = resolve_field(doc, &guard.field).is_none()
            || matches!(
                resolve_field(doc, &guard.field),
                Some(nodedb_types::Value::Null)
            );
        if let Some(ref check_str) = guard.check_expr
            && (!field_absent || guard.required)
        {
            let (check_expr, _deps) = nodedb_query::expr_parse::parse_generated_expr(check_str)
                .map_err(|e| ErrorCode::TypeGuardViolation {
                    collection: collection.to_string(),
                    detail: format!(
                        "field '{}': invalid CHECK expression '{}': {e}",
                        guard.field, check_str
                    ),
                })?;
            let result = check_expr.eval(doc);
            match result {
                nodedb_types::Value::Bool(true) => {} // CHECK passed
                nodedb_types::Value::Null => {}       // NULL passes CHECK (SQL semantics)
                _ => {
                    return Err(ErrorCode::TypeGuardViolation {
                        collection: collection.to_string(),
                        detail: format!(
                            "CHECK failed on '{}': {} (got {:?})",
                            guard.field, check_str, result
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

/// Resolve a dot-path field name from a document value.
///
/// Supports nested paths like `"metadata.source"` by walking `Value::Object` maps.
/// Returns `None` if any segment is missing or if a non-object intermediate is encountered.
fn resolve_field<'a>(doc: &'a nodedb_types::Value, path: &str) -> Option<&'a nodedb_types::Value> {
    let mut current = doc;
    for segment in path.split('.') {
        match current {
            nodedb_types::Value::Object(map) => {
                current = map.get(segment)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::{TypeGuardFieldDef, Value};

    use super::*;

    fn make_guard(field: &str, type_expr: &str, required: bool) -> TypeGuardFieldDef {
        TypeGuardFieldDef {
            field: field.to_string(),
            type_expr: type_expr.to_string(),
            required,
            check_expr: None,
            default_expr: None,
            value_expr: None,
        }
    }

    fn obj(pairs: &[(&str, Value)]) -> Value {
        let mut map = HashMap::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), v.clone());
        }
        Value::Object(map)
    }

    #[test]
    fn passes_when_no_guards() {
        let doc = obj(&[("x", Value::Integer(1))]);
        assert!(check_type_guards("coll", &[], &doc, None).is_ok());
    }

    #[test]
    fn passes_correct_type() {
        let guard = make_guard("name", "STRING", false);
        let doc = obj(&[("name", Value::String("alice".into()))]);
        assert!(check_type_guards("coll", &[guard], &doc, None).is_ok());
    }

    #[test]
    fn rejects_wrong_type() {
        let guard = make_guard("name", "STRING", false);
        let doc = obj(&[("name", Value::Integer(42))]);
        let err = check_type_guards("coll", &[guard], &doc, None).unwrap_err();
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("name")
        ));
    }

    #[test]
    fn rejects_missing_required_field() {
        let guard = make_guard("email", "STRING", true);
        let doc = obj(&[("name", Value::String("bob".into()))]);
        let err = check_type_guards("coll", &[guard], &doc, None).unwrap_err();
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("email")
        ));
    }

    #[test]
    fn rejects_null_required_field() {
        let guard = make_guard("email", "STRING", true);
        let doc = obj(&[("email", Value::Null)]);
        let err = check_type_guards("coll", &[guard], &doc, None).unwrap_err();
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("email")
        ));
    }

    #[test]
    fn allows_null_for_optional_field() {
        let guard = make_guard("notes", "STRING|NULL", false);
        let doc = obj(&[("notes", Value::Null)]);
        assert!(check_type_guards("coll", &[guard], &doc, None).is_ok());
    }

    #[test]
    fn allows_absent_optional_field() {
        let guard = make_guard("bio", "STRING", false);
        let doc = obj(&[("name", Value::String("carol".into()))]);
        // Field is absent and not required → passes.
        assert!(check_type_guards("coll", &[guard], &doc, None).is_ok());
    }

    #[test]
    fn update_skips_type_guard_for_non_written_field() {
        // Guard on "score" but the UPDATE only writes "name".
        let guard = make_guard("score", "INT", true);
        let doc = obj(&[
            ("name", Value::String("dave".into())),
            ("score", Value::String("bad".into())), // wrong type, but not written
        ]);
        let written = vec!["name".to_string()];
        // Should pass because "score" is not in written_fields and has no check_expr.
        assert!(check_type_guards("coll", &[guard], &doc, Some(&written)).is_ok());
    }

    #[test]
    fn update_enforces_type_for_written_field() {
        let guard = make_guard("score", "INT", false);
        let doc = obj(&[("score", Value::String("bad".into()))]);
        let written = vec!["score".to_string()];
        let err = check_type_guards("coll", &[guard], &doc, Some(&written)).unwrap_err();
        assert!(matches!(err, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn dot_path_resolution_nested() {
        let inner = obj(&[("source", Value::String("web".into()))]);
        let doc = obj(&[("metadata", inner)]);
        let guard = make_guard("metadata.source", "STRING", true);
        assert!(check_type_guards("coll", &[guard], &doc, None).is_ok());
    }

    #[test]
    fn dot_path_resolution_nested_wrong_type() {
        let inner = obj(&[("source", Value::Integer(99))]);
        let doc = obj(&[("metadata", inner)]);
        let guard = make_guard("metadata.source", "STRING", false);
        let err = check_type_guards("coll", &[guard], &doc, None).unwrap_err();
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("metadata.source")
        ));
    }

    #[test]
    fn dot_path_absent_required() {
        let doc = obj(&[("metadata", obj(&[]))]);
        let guard = make_guard("metadata.source", "STRING", true);
        let err = check_type_guards("coll", &[guard], &doc, None).unwrap_err();
        assert!(matches!(err, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn union_type_passes_either_variant() {
        let guard = make_guard("count", "INT|NULL", false);
        let doc1 = obj(&[("count", Value::Integer(5))]);
        let doc2 = obj(&[("count", Value::Null)]);
        assert!(check_type_guards("coll", std::slice::from_ref(&guard), &doc1, None).is_ok());
        assert!(check_type_guards("coll", std::slice::from_ref(&guard), &doc2, None).is_ok());
    }

    #[test]
    fn invalid_type_expr_returns_violation() {
        let guard = make_guard("x", "NOTATYPE", false);
        let doc = obj(&[("x", Value::String("foo".into()))]);
        let err = check_type_guards("coll", &[guard], &doc, None).unwrap_err();
        assert!(matches!(err, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn multiple_guards_first_violation_returned() {
        let g1 = make_guard("a", "STRING", false);
        let g2 = make_guard("b", "INT", true); // required but absent
        let doc = obj(&[("a", Value::String("ok".into()))]);
        let err = check_type_guards("coll", &[g1, g2], &doc, None).unwrap_err();
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("'b'")
        ));
    }

    // ── DEFAULT/VALUE injection ──

    #[test]
    fn default_injects_when_absent() {
        let guard = TypeGuardFieldDef {
            field: "status".to_string(),
            type_expr: "STRING".to_string(),
            required: false,
            check_expr: None,
            default_expr: Some("'draft'".to_string()),
            value_expr: None,
        };
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String("Alice".into()));
        inject_defaults(&[guard], &mut fields).unwrap();
        assert_eq!(fields.get("status"), Some(&Value::String("draft".into())));
    }

    #[test]
    fn default_does_not_overwrite() {
        let guard = TypeGuardFieldDef {
            field: "status".to_string(),
            type_expr: "STRING".to_string(),
            required: false,
            check_expr: None,
            default_expr: Some("'draft'".to_string()),
            value_expr: None,
        };
        let mut fields = HashMap::new();
        fields.insert("status".to_string(), Value::String("active".into()));
        inject_defaults(&[guard], &mut fields).unwrap();
        assert_eq!(
            fields.get("status"),
            Some(&Value::String("active".into())),
            "DEFAULT should not overwrite existing value"
        );
    }

    #[test]
    fn value_always_overwrites() {
        let guard = TypeGuardFieldDef {
            field: "updated".to_string(),
            type_expr: "STRING".to_string(),
            required: false,
            check_expr: None,
            default_expr: None,
            value_expr: Some("'computed'".to_string()),
        };
        let mut fields = HashMap::new();
        fields.insert("updated".to_string(), Value::String("user_input".into()));
        inject_defaults(&[guard], &mut fields).unwrap();
        assert_eq!(
            fields.get("updated"),
            Some(&Value::String("computed".into())),
            "VALUE should overwrite user input"
        );
    }

    #[test]
    fn default_plus_required_always_present() {
        let guard = TypeGuardFieldDef {
            field: "version".to_string(),
            type_expr: "INT".to_string(),
            required: true,
            check_expr: None,
            default_expr: Some("1".to_string()),
            value_expr: None,
        };
        let mut fields = HashMap::new();
        // version absent — DEFAULT fills it before REQUIRED check
        inject_defaults(std::slice::from_ref(&guard), &mut fields).unwrap();
        assert_eq!(fields.get("version"), Some(&Value::Integer(1)));
        // Now validate — should pass because DEFAULT filled the gap
        let doc = obj(&[("version", Value::Integer(1))]);
        assert!(check_type_guards("coll", std::slice::from_ref(&guard), &doc, None).is_ok());
    }

    #[test]
    fn value_cross_field_reference() {
        let guard = TypeGuardFieldDef {
            field: "greeting".to_string(),
            type_expr: "STRING".to_string(),
            required: false,
            check_expr: None,
            default_expr: None,
            value_expr: Some("name".to_string()), // references another field
        };
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String("Alice".into()));
        inject_defaults(&[guard], &mut fields).unwrap();
        // SqlExpr::eval resolves "name" as a column reference → Value::String("Alice")
        assert_eq!(
            fields.get("greeting"),
            Some(&Value::String("Alice".into())),
            "VALUE should resolve cross-field references"
        );
    }
}
