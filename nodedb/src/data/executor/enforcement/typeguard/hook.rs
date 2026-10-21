// SPDX-License-Identifier: BUSL-1.1

//! The one call every write path makes to apply a schemaless collection's
//! typeguards to the body it stores.
//!
//! Every write form reaches it on the Data Plane: SQL `INSERT … VALUES`, the
//! `{ }` form, `INSERT … SELECT`, UPSERT, UPDATE, MERGE, native puts and
//! batches, and transaction staging. The Data Plane is the one place that holds
//! every row a write stores, an UPDATE's post-image included.

use std::collections::HashMap;

use nodedb_types::Value;

use super::check::check_type_guards;
use super::compiled::CompiledTypeGuard;
use super::inject::inject_defaults;
use crate::bridge::envelope::ErrorCode;

/// Which fields one write puts in scope for validation.
#[derive(Debug, Clone, Copy)]
pub enum GuardScope<'a> {
    /// INSERT, PUT, the UPSERT insert arm: the whole document.
    Document,
    /// UPDATE, the UPSERT merge arm, the MERGE update arm: the assigned
    /// fields plus every field a guard injected. A row written before a guard
    /// existed stays updatable on its other fields.
    Assigned(&'a [String]),
    /// Redo replay and derived writes: inject only. The write's commit already
    /// judged it, and a redo record carries the submitted body, so the stored
    /// row is derived the same way again.
    InjectOnly,
}

/// Apply `guards` to `doc`, the body one write stores.
///
/// DEFAULT fills an absent or null field and VALUE always overwrites. Then
/// REQUIRED, type and CHECK run over the scope. A non-object body carries no
/// field a guard names and is left for the encoder to judge.
pub fn enforce_type_guards(
    guards: &[CompiledTypeGuard],
    doc: &mut serde_json::Value,
    scope: GuardScope<'_>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<(), ErrorCode> {
    if guards.is_empty() {
        return Ok(());
    }
    let serde_json::Value::Object(map) = doc else {
        return Ok(());
    };
    let mut fields: HashMap<String, Value> = map
        .iter()
        .map(|(name, value)| (name.clone(), Value::from(value.clone())))
        .collect();
    let injected = inject_defaults(guards, &mut fields, crs)?;
    let in_scope: Option<Vec<String>> = match scope {
        GuardScope::Document => None,
        GuardScope::Assigned(assigned) => Some(
            assigned
                .iter()
                .cloned()
                .chain(injected.iter().cloned())
                .collect(),
        ),
        GuardScope::InjectOnly => {
            write_back(map, &fields, &injected);
            return Ok(());
        }
    };
    check_type_guards(
        guards,
        &Value::Object(fields.clone()),
        in_scope.as_deref(),
        crs,
    )?;
    write_back(map, &fields, &injected);
    Ok(())
}

/// Write only the injected fields back, so every other field keeps its
/// original form.
fn write_back(
    map: &mut serde_json::Map<String, serde_json::Value>,
    fields: &HashMap<String, Value>,
    injected: &[String],
) {
    for name in injected {
        if let Some(value) = fields.get(name) {
            map.insert(name.clone(), serde_json::Value::from(value.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::TypeGuardFieldDef;

    use super::*;

    fn guards() -> Vec<CompiledTypeGuard> {
        let def = |field: &str, required: bool| TypeGuardFieldDef {
            field: field.to_string(),
            type_expr: "STRING".to_string(),
            required,
            check_expr: None,
            default_expr: None,
            value_expr: None,
        };
        let mut role = def("role", false);
        role.default_expr = Some("'user'".to_string());
        let mut slug = def("slug", false);
        slug.value_expr = Some("LOWER(title)".to_string());
        CompiledTypeGuard::compile_all("c", &[def("email", true), role, slug])
    }

    #[test]
    fn document_scope_injects_then_validates() {
        let mut doc = serde_json::json!({ "title": "Hi", "email": "a@b.c" });
        enforce_type_guards(
            &guards(),
            &mut doc,
            GuardScope::Document,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("valid");
        assert_eq!(doc["role"], "user");
        assert_eq!(doc["slug"], "hi");

        let mut missing = serde_json::json!({ "title": "Hi" });
        let error = enforce_type_guards(
            &guards(),
            &mut missing,
            GuardScope::Document,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("email is required");
        assert!(matches!(error, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn assigned_scope_skips_unwritten_required_fields() {
        let mut doc = serde_json::json!({ "title": "New X" });
        let assigned = vec!["title".to_string()];
        enforce_type_guards(
            &guards(),
            &mut doc,
            GuardScope::Assigned(&assigned),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("email is not assigned");
        assert_eq!(doc["slug"], "new x");
    }

    #[test]
    fn assigned_scope_validates_injected_fields() {
        let def = TypeGuardFieldDef {
            field: "n".to_string(),
            type_expr: "INT".to_string(),
            required: false,
            check_expr: None,
            default_expr: None,
            value_expr: Some("'text'".to_string()),
        };
        let guards = CompiledTypeGuard::compile_all("c", &[def]);
        let mut doc = serde_json::json!({ "other": 1 });
        let assigned = vec!["other".to_string()];
        let error = enforce_type_guards(
            &guards,
            &mut doc,
            GuardScope::Assigned(&assigned),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("the injected value is not an INT");
        assert!(matches!(error, ErrorCode::TypeGuardViolation { .. }));
    }

    #[test]
    fn inject_only_never_refuses() {
        let mut doc = serde_json::json!({ "title": "Hi" });
        enforce_type_guards(
            &guards(),
            &mut doc,
            GuardScope::InjectOnly,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("replay never refuses");
        assert_eq!(doc["slug"], "hi");
    }

    #[test]
    fn value_overwrites_an_assigned_value() {
        let mut doc = serde_json::json!({ "title": "Hi", "email": "a@b.c", "slug": "zzz" });
        enforce_type_guards(
            &guards(),
            &mut doc,
            GuardScope::Document,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("valid");
        assert_eq!(doc["slug"], "hi");
    }

    #[test]
    fn default_refills_a_null_assignment() {
        let mut doc = serde_json::json!({ "title": "Hi", "role": null });
        let assigned = vec!["role".to_string()];
        enforce_type_guards(
            &guards(),
            &mut doc,
            GuardScope::Assigned(&assigned),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("valid");
        assert_eq!(doc["role"], "user");
    }
}
