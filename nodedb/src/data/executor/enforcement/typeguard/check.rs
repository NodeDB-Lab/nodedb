// SPDX-License-Identifier: BUSL-1.1

//! Typeguard validation: REQUIRED, type and CHECK against one document.

use nodedb_sql::parser::type_expr::value_matches_type;
use nodedb_types::Value;

use super::compiled::CompiledTypeGuard;
use crate::bridge::envelope::ErrorCode;

/// Validate a document against compiled typeguards.
///
/// `doc` is the document being written (INSERT/UPSERT: the full document,
/// UPDATE: the merged document with existing + updated fields).
/// `written_fields` is the set of field names in scope. For INSERT/UPSERT,
/// this is `None` (all fields are in scope). For UPDATE, it is `Some(&[...])`
/// containing the assigned and injected fields.
///
/// Checks in order:
/// 1. REQUIRED — field must be present and non-null
/// 2. Type check — value must match the type expression
/// 3. CHECK expression — SQL expression must evaluate to true
///
/// REQUIRED and type run only for a field in scope. A CHECK runs whenever its
/// field is present, since it may read other fields.
///
/// Returns `Ok(())` if all guards pass, or the first violation. A violation
/// names the collection each guard was compiled for.
pub fn check_type_guards(
    guards: &[CompiledTypeGuard],
    doc: &Value,
    written_fields: Option<&[String]>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<(), ErrorCode> {
    for guard in guards {
        let field_written = match written_fields {
            None => true,
            Some(fields) => fields.iter().any(|f| f == &guard.field),
        };

        // For UPDATE: skip type-only guards on fields not being written.
        if !field_written && !guard.has_check() {
            continue;
        }

        let clauses = guard.clauses()?;
        let value = resolve_field(doc, &guard.field);

        if field_written {
            if guard.required && matches!(value, None | Some(Value::Null)) {
                return Err(ErrorCode::TypeGuardViolation {
                    collection: guard.collection.clone(),
                    detail: format!("field '{}' is required but absent or null", guard.field),
                });
            }

            // Type check: only when the field is present.
            if let Some(val) = value
                && (!matches!(val, Value::Null) || guard.required)
                && !value_matches_type(val, &clauses.type_expr)
            {
                return Err(ErrorCode::TypeGuardViolation {
                    collection: guard.collection.clone(),
                    detail: format!(
                        "field '{}' must be {}, got {}",
                        guard.field,
                        guard.type_text,
                        value_type_name(val)
                    ),
                });
            }
        }

        // An absent optional field has nothing for its CHECK to validate.
        let field_absent = matches!(value, None | Some(Value::Null));
        if let Some((check_text, check_expr)) = clauses.check.as_ref()
            && (!field_absent || guard.required)
        {
            // An evaluation error keeps its own SQLSTATE, not this guard's
            // violation code, matching CHECK-constraint enforcement elsewhere.
            let result = check_expr.eval(doc, crs).map_err(ErrorCode::from)?;
            match result {
                Value::Bool(true) | Value::Null => {}
                _ => {
                    return Err(ErrorCode::TypeGuardViolation {
                        collection: guard.collection.clone(),
                        detail: format!(
                            "CHECK failed on '{}': {} (got {})",
                            guard.field,
                            check_text,
                            value_display(value)
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

/// Human-readable type name for a Value (used in error messages).
fn value_type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOL",
        Value::Integer(_) => "INT",
        Value::Float(_) => "FLOAT",
        Value::String(_) => "STRING",
        Value::Array(_) => "ARRAY",
        Value::Object(_) => "OBJECT",
        Value::Bytes(_) => "BYTES",
        Value::DateTime(_) => "TIMESTAMPTZ",
        Value::NaiveDateTime(_) => "TIMESTAMP",
        Value::Uuid(_) => "UUID",
        Value::Ulid(_) => "ULID",
        Value::Decimal(_) => "DECIMAL",
        Value::Duration(_) => "DURATION",
        Value::Geometry(_) => "GEOMETRY",
        Value::Set(_) => "SET",
        Value::Regex(_) => "REGEX",
        Value::Range { .. } => "RANGE",
        Value::Record { .. } => "RECORD",
        Value::ArrayCell(_) => "ARRAY_CELL",
        // Value is #[non_exhaustive]; future variants report as "UNKNOWN".
        _ => "UNKNOWN",
    }
}

/// Human-readable display of a field value for error messages.
fn value_display(val: Option<&Value>) -> String {
    match val {
        None => "absent".to_string(),
        Some(Value::Null) => "NULL".to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Integer(i)) => i.to_string(),
        Some(Value::Float(f)) => format!("{f}"),
        Some(Value::String(s)) => {
            // Truncate by characters: a byte index can split a multi-byte
            // character.
            if s.chars().count() > 50 {
                let head: String = s.chars().take(47).collect();
                format!("'{head}'...")
            } else {
                format!("'{s}'")
            }
        }
        Some(other) => format!("{} value", value_type_name(other)),
    }
}

/// Resolve a dot-path field name from a document value.
///
/// Supports nested paths like `"metadata.source"` by walking `Value::Object` maps.
/// Returns `None` if any segment is missing or if a non-object intermediate is encountered.
fn resolve_field<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = doc;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => {
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

    use nodedb_types::TypeGuardFieldDef;

    use super::super::inject::inject_defaults;
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

    fn compiled(defs: &[TypeGuardFieldDef]) -> Vec<CompiledTypeGuard> {
        CompiledTypeGuard::compile_all("coll", defs)
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
        assert!(
            check_type_guards(&[], &doc, None, &nodedb_crs::CrsContext::builtin_only()).is_ok()
        );
    }

    #[test]
    fn passes_correct_type() {
        let guards = compiled(&[make_guard("name", "STRING", false)]);
        let doc = obj(&[("name", Value::String("alice".into()))]);
        assert!(
            check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only()).is_ok()
        );
    }

    #[test]
    fn rejects_wrong_type() {
        let guards = compiled(&[make_guard("name", "STRING", false)]);
        let doc = obj(&[("name", Value::Integer(42))]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("wrong type");
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("name")
        ));
    }

    #[test]
    fn rejects_missing_required_field() {
        let guards = compiled(&[make_guard("email", "STRING", true)]);
        let doc = obj(&[("name", Value::String("bob".into()))]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("email missing");
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("email")
        ));
    }

    #[test]
    fn rejects_null_required_field() {
        let guards = compiled(&[make_guard("email", "STRING", true)]);
        let doc = obj(&[("email", Value::Null)]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("email null");
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("email")
        ));
    }

    #[test]
    fn allows_null_for_optional_field() {
        let guards = compiled(&[make_guard("notes", "STRING|NULL", false)]);
        let doc = obj(&[("notes", Value::Null)]);
        assert!(
            check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only()).is_ok()
        );
    }

    #[test]
    fn allows_absent_optional_field() {
        let guards = compiled(&[make_guard("bio", "STRING", false)]);
        let doc = obj(&[("name", Value::String("carol".into()))]);
        assert!(
            check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only()).is_ok()
        );
    }

    #[test]
    fn update_skips_type_guard_for_non_written_field() {
        // Guard on "score" but the UPDATE only writes "name".
        let guards = compiled(&[make_guard("score", "INT", true)]);
        let doc = obj(&[
            ("name", Value::String("dave".into())),
            ("score", Value::String("bad".into())),
        ]);
        let written = vec!["name".to_string()];
        assert!(
            check_type_guards(
                &guards,
                &doc,
                Some(&written),
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
        );
    }

    #[test]
    fn update_enforces_type_for_written_field() {
        let guards = compiled(&[make_guard("score", "INT", false)]);
        let doc = obj(&[("score", Value::String("bad".into()))]);
        let written = vec!["score".to_string()];
        let err = check_type_guards(
            &guards,
            &doc,
            Some(&written),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("bad");
        assert!(matches!(err, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn dot_path_resolution_nested() {
        let inner = obj(&[("source", Value::String("web".into()))]);
        let doc = obj(&[("metadata", inner)]);
        let guards = compiled(&[make_guard("metadata.source", "STRING", true)]);
        assert!(
            check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only()).is_ok()
        );
    }

    #[test]
    fn dot_path_resolution_nested_wrong_type() {
        let inner = obj(&[("source", Value::Integer(99))]);
        let doc = obj(&[("metadata", inner)]);
        let guards = compiled(&[make_guard("metadata.source", "STRING", false)]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("wrong type");
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("metadata.source")
        ));
    }

    #[test]
    fn dot_path_absent_required() {
        let doc = obj(&[("metadata", obj(&[]))]);
        let guards = compiled(&[make_guard("metadata.source", "STRING", true)]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("absent");
        assert!(matches!(err, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn union_type_passes_either_variant() {
        let guards = compiled(&[make_guard("count", "INT|NULL", false)]);
        let doc1 = obj(&[("count", Value::Integer(5))]);
        let doc2 = obj(&[("count", Value::Null)]);
        assert!(
            check_type_guards(
                &guards,
                &doc1,
                None,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
        );
        assert!(
            check_type_guards(
                &guards,
                &doc2,
                None,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
        );
    }

    #[test]
    fn invalid_type_expr_returns_violation() {
        let guards = compiled(&[make_guard("x", "NOTATYPE", false)]);
        let doc = obj(&[("x", Value::String("foo".into()))]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("NOTATYPE");
        assert!(matches!(err, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn multiple_guards_first_violation_returned() {
        let guards = compiled(&[
            make_guard("a", "STRING", false),
            make_guard("b", "INT", true), // required but absent
        ]);
        let doc = obj(&[("a", Value::String("ok".into()))]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("b missing");
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("'b'")
        ));
    }

    // ── DEFAULT/VALUE injection ──

    fn with_clauses(
        field: &str,
        type_expr: &str,
        default_expr: Option<&str>,
        value_expr: Option<&str>,
    ) -> TypeGuardFieldDef {
        TypeGuardFieldDef {
            default_expr: default_expr.map(str::to_string),
            value_expr: value_expr.map(str::to_string),
            ..make_guard(field, type_expr, false)
        }
    }

    #[test]
    fn default_injects_when_absent() {
        let guards = compiled(&[with_clauses("status", "STRING", Some("'draft'"), None)]);
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String("Alice".into()));
        let injected = inject_defaults(
            &guards,
            &mut fields,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("injects");
        assert_eq!(fields.get("status"), Some(&Value::String("draft".into())));
        assert_eq!(injected, ["status"]);
    }

    #[test]
    fn default_does_not_overwrite() {
        let guards = compiled(&[with_clauses("status", "STRING", Some("'draft'"), None)]);
        let mut fields = HashMap::new();
        fields.insert("status".to_string(), Value::String("active".into()));
        let injected = inject_defaults(
            &guards,
            &mut fields,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("injects");
        assert_eq!(fields.get("status"), Some(&Value::String("active".into())));
        assert!(injected.is_empty());
    }

    #[test]
    fn value_always_overwrites() {
        let guards = compiled(&[with_clauses("updated", "STRING", None, Some("'computed'"))]);
        let mut fields = HashMap::new();
        fields.insert("updated".to_string(), Value::String("user_input".into()));
        inject_defaults(
            &guards,
            &mut fields,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("injects");
        assert_eq!(
            fields.get("updated"),
            Some(&Value::String("computed".into()))
        );
    }

    #[test]
    fn default_plus_required_always_present() {
        let mut def = with_clauses("version", "INT", Some("1"), None);
        def.required = true;
        let guards = compiled(&[def]);
        let mut fields = HashMap::new();
        inject_defaults(
            &guards,
            &mut fields,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("injects");
        assert_eq!(fields.get("version"), Some(&Value::Integer(1)));
        let doc = obj(&[("version", Value::Integer(1))]);
        assert!(
            check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only()).is_ok()
        );
    }

    #[test]
    fn value_cross_field_reference() {
        let guards = compiled(&[with_clauses("greeting", "STRING", None, Some("name"))]);
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String("Alice".into()));
        inject_defaults(
            &guards,
            &mut fields,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("injects");
        assert_eq!(fields.get("greeting"), Some(&Value::String("Alice".into())));
    }

    /// A long multi-byte value in a violation message is cut on a character
    /// boundary.
    #[test]
    fn a_long_multibyte_value_is_truncated_by_character() {
        let mut def = make_guard("tag", "STRING", false);
        def.check_expr = Some("tag = 'x'".to_string());
        let guards = compiled(&[def]);
        let long: String = "é".repeat(60);
        let doc = obj(&[("tag", Value::String(long))]);
        let err = check_type_guards(&guards, &doc, None, &nodedb_crs::CrsContext::builtin_only())
            .expect_err("CHECK fails");
        match err {
            ErrorCode::TypeGuardViolation { detail, .. } => {
                assert!(detail.contains(&"é".repeat(47)), "{detail}");
                assert!(detail.ends_with("...)"), "{detail}");
            }
            other => panic!("expected TypeGuardViolation, got {other:?}"),
        }
    }

    /// A violation names the collection the guard was compiled for.
    #[test]
    fn a_violation_names_the_compiled_collection() {
        let guards = CompiledTypeGuard::compile_all("orders", &[make_guard("a", "INT", true)]);
        let err = check_type_guards(
            &guards,
            &obj(&[]),
            None,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("a missing");
        assert!(matches!(
            err,
            ErrorCode::TypeGuardViolation { ref collection, .. } if collection == "orders"
        ));
    }

    // ── CHECK predicates ──

    fn with_check(field: &str, type_expr: &str, check: &str) -> Vec<CompiledTypeGuard> {
        let mut def = make_guard(field, type_expr, false);
        def.check_expr = Some(check.to_string());
        compiled(&[def])
    }

    /// Whether `value` in `field` passes the guard's CHECK.
    fn passes(guards: &[CompiledTypeGuard], field: &str, value: Value) -> bool {
        check_type_guards(
            guards,
            &obj(&[(field, value)]),
            None,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .is_ok()
    }

    #[test]
    fn check_like_and_not_like() {
        let like = with_check("email", "STRING", "email LIKE '%@%'");
        assert!(passes(&like, "email", Value::String("a@b.c".into())));
        assert!(!passes(&like, "email", Value::String("nobody".into())));
        let not_like = with_check("email", "STRING", "email NOT LIKE '%@%'");
        assert!(passes(&not_like, "email", Value::String("nobody".into())));
        assert!(!passes(&not_like, "email", Value::String("a@b.c".into())));
    }

    #[test]
    fn check_ilike() {
        let guards = with_check("name", "STRING", "name ILIKE 'ali%'");
        assert!(passes(&guards, "name", Value::String("ALICE".into())));
        assert!(!passes(&guards, "name", Value::String("bob".into())));
    }

    #[test]
    fn check_in_and_not_in() {
        let guards = with_check("role", "STRING", "role IN ('admin', 'user')");
        assert!(passes(&guards, "role", Value::String("user".into())));
        assert!(!passes(&guards, "role", Value::String("guest".into())));
        let not_in = with_check("role", "STRING", "role NOT IN ('banned')");
        assert!(passes(&not_in, "role", Value::String("user".into())));
        assert!(!passes(&not_in, "role", Value::String("banned".into())));
    }

    #[test]
    fn check_between_and_not_between() {
        let guards = with_check("age", "INT", "age BETWEEN 0 AND 150");
        assert!(passes(&guards, "age", Value::Integer(30)));
        assert!(!passes(&guards, "age", Value::Integer(200)));
        let outside = with_check("age", "INT", "age NOT BETWEEN 0 AND 17");
        assert!(passes(&outside, "age", Value::Integer(30)));
        assert!(!passes(&outside, "age", Value::Integer(10)));
    }

    #[test]
    fn check_is_null_and_is_not_null() {
        // A CHECK may read another field: `code` is set only while `legacy`
        // is absent.
        let is_null = with_check("code", "STRING", "legacy IS NULL");
        let code = ("code", Value::String("x".into()));
        assert!(
            check_type_guards(
                &is_null,
                &obj(std::slice::from_ref(&code)),
                None,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
        );
        let both = obj(&[code, ("legacy", Value::Integer(1))]);
        assert!(
            check_type_guards(
                &is_null,
                &both,
                None,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_err()
        );
        let not_null = with_check("code", "STRING", "code IS NOT NULL AND code <> ''");
        assert!(passes(&not_null, "code", Value::String("x".into())));
        assert!(!passes(&not_null, "code", Value::String(String::new())));
    }
}
