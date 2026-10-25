// SPDX-License-Identifier: Apache-2.0

//! Constraint-checking methods for the [`Validator`].
//!
//! All methods are `impl Validator` blocks and belong logically to the
//! validator, but live here to respect file-size guidelines.

use crate::constraint::{Constraint, ConstraintKind};
use crate::dead_letter::CompensationHint;
use crate::row_lookup::RowLookup;
use crate::validator::{ProposedChange, Validator, Violation};
use loro::LoroValue;

/// Render a field value as a clean, client-facing string.
///
/// Scalars render as their bare value (`x@y.com`, `42`) rather than the Rust
/// debug form (`String(LoroStringValue("x@y.com"))`), so violation messages and
/// compensation hints carry a value the caller can act on directly.
fn render_value(value: &LoroValue) -> String {
    match value {
        LoroValue::String(s) => s.to_string(),
        LoroValue::I64(n) => n.to_string(),
        LoroValue::Double(f) => f.to_string(),
        LoroValue::Bool(b) => b.to_string(),
        LoroValue::Null => "null".to_string(),
        other => format!("{other:?}"),
    }
}

impl Validator {
    ///
    /// Returns `Err(EvalError)` when a CHECK predicate cannot be *evaluated*
    /// (division/modulo by zero). This is distinct from an
    /// ordinary constraint failure (`Ok(Some(Violation))`): the caller lifts it
    /// to [`ValidationOutcome::EvalError`], a hard statement failure that
    /// declarative conflict policies must never "resolve".
    pub(crate) fn check_constraint(
        &self,
        state: &impl RowLookup,
        change: &ProposedChange,
        constraint: &Constraint,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> Result<Option<Violation>, nodedb_query::EvalError> {
        match &constraint.kind {
            ConstraintKind::Unique => Ok(self.check_unique(state, change, constraint)),
            ConstraintKind::ForeignKey {
                ref_collection,
                ref_key,
            }
            | ConstraintKind::BiTemporalFK {
                ref_collection,
                ref_key,
            } => Ok(self.check_foreign_key(state, change, constraint, ref_collection, ref_key)),
            ConstraintKind::NotNull => Ok(self.check_not_null(change, constraint)),
            ConstraintKind::Check { expr, .. } => self.check_expr(change, constraint, expr, crs),
        }
    }

    /// Evaluate a stored CHECK predicate against a proposed row.
    ///
    /// Mirrors the Data-Plane CHECK semantics: `Bool(true)` and `Null` PASS,
    /// anything else FAILS. A malformed stored expression rejects loudly
    /// (never silently passes) so a corrupt catalog entry can't bypass the
    /// invariant it claims to enforce.
    pub(crate) fn check_expr(
        &self,
        change: &ProposedChange,
        constraint: &Constraint,
        expr: &str,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> Result<Option<Violation>, nodedb_query::EvalError> {
        // Build a row Value::Object from the proposed change's fields so the
        // expression evaluator can resolve column references by name.
        let mut row = std::collections::HashMap::with_capacity(change.fields.len());
        for (name, val) in &change.fields {
            row.insert(name.clone(), crate::loro_value::loro_to_value(val));
        }
        let row_value = nodedb_types::Value::Object(row);

        let parsed = match nodedb_query::expr_parse::parse_generated_expr(expr) {
            Ok((expr, _deps)) => expr,
            Err(e) => {
                // A malformed *stored* predicate is a catalog-integrity failure,
                // not an evaluation error — it stays an ordinary Violation
                // (fails closed) so a corrupt entry can't bypass its invariant.
                return Ok(Some(Violation {
                    constraint_name: constraint.name.clone(),
                    reason: format!("invalid CHECK expression `{expr}`: {e}"),
                    hint: CompensationHint::ManualIntervention {
                        reason: format!(
                            "CHECK constraint `{}` has an unparseable predicate",
                            constraint.name
                        ),
                    },
                }));
            }
        };

        // A division/modulo-by-zero inside the predicate
        // is neither a PASS nor an ordinary FAIL — it's an evaluation error,
        // propagated as `Err(EvalError)` and lifted by `validate` to
        // `ValidationOutcome::EvalError`. That keeps it out of the conflict-
        // policy machinery: an unevaluable predicate is a hard SQLSTATE-22012
        // failure, never something LastWriterWins et al. may "resolve".
        match parsed.eval(&row_value, crs) {
            Ok(nodedb_types::Value::Bool(true)) => Ok(None),
            Ok(nodedb_types::Value::Null) => Ok(None),
            Ok(_) => Ok(Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!("CHECK `{}` failed: {expr}", constraint.name),
                hint: CompensationHint::ManualIntervention {
                    reason: format!("row violates CHECK predicate `{expr}`"),
                },
            })),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn check_unique(
        &self,
        state: &impl RowLookup,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        let value = &field_value.1;

        // Bitemporal collections: only consider live (non-superseded) rows,
        // so a new version of the same logical row with the same value does
        // not spuriously collide with its prior version.
        // Exclude the row's own already-committed version so re-validating a
        // committed row does not falsely collide with itself; a second, distinct
        // row carrying the same value still collides.
        let exclude = Some(change.row_id.as_str());
        let exists = if self.is_bitemporal(&change.collection) {
            state.field_value_exists_live(&change.collection, &constraint.field, value, exclude)
        } else {
            state.field_value_exists(&change.collection, &constraint.field, value, exclude)
        };

        if exists {
            let value_str = render_value(value);
            Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!(
                    "value {} for field `{}` already exists in `{}`",
                    value_str, constraint.field, constraint.collection
                ),
                hint: CompensationHint::RetryWithDifferentValue {
                    field: constraint.field.clone(),
                    conflicting_value: value_str.clone(),
                    suggestion: format!("{value_str}-dedup"),
                },
            })
        } else {
            None
        }
    }

    pub(crate) fn check_foreign_key(
        &self,
        state: &impl RowLookup,
        change: &ProposedChange,
        constraint: &Constraint,
        ref_collection: &str,
        ref_key: &str,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        // The FK value should reference an existing row_id in the ref collection.
        let ref_id = render_value(&field_value.1);

        if !state.row_exists(ref_collection, &ref_id) {
            Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!(
                    "foreign key `{}` references `{}.{}` = `{}` which does not exist",
                    constraint.field, ref_collection, ref_key, ref_id
                ),
                hint: CompensationHint::CreateReferencedRow {
                    ref_collection: ref_collection.to_string(),
                    ref_key: ref_key.to_string(),
                    missing_value: ref_id,
                },
            })
        } else {
            None
        }
    }

    pub(crate) fn check_not_null(
        &self,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field);

        match field_value {
            None => Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!("field `{}` is required but not provided", constraint.field),
                hint: CompensationHint::ProvideRequiredField {
                    field: constraint.field.clone(),
                },
            }),
            Some((_, LoroValue::Null)) => Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!("field `{}` must not be null", constraint.field),
                hint: CompensationHint::ProvideRequiredField {
                    field: constraint.field.clone(),
                },
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod bitemporal_fk_tests {
    use super::*;
    use crate::constraint::ConstraintKind;
    use crate::state::CrdtState;
    use crate::validator::Validator;
    use loro::LoroValue;

    fn make_btfk_constraint(ref_collection: &str, ref_key: &str) -> Constraint {
        Constraint {
            name: "test_btfk".to_string(),
            collection: "referrer".to_string(),
            field: "ref_id".to_string(),
            kind: ConstraintKind::BiTemporalFK {
                ref_collection: ref_collection.to_string(),
                ref_key: ref_key.to_string(),
            },
        }
    }

    fn make_change(ref_value: &str) -> ProposedChange {
        ProposedChange {
            collection: "referrer".to_string(),
            row_id: "row1".to_string(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("ref_id".to_string(), LoroValue::String(ref_value.into()))],
        }
    }

    /// Test-only `RowLookup` that treats a fixed set of ids as live array
    /// surrogates, mirroring the tenant-level cross-engine FK registry that now
    /// lives outside `CrdtState`.
    struct ArraySurrogateLookup<'a> {
        state: &'a CrdtState,
        surrogates: std::collections::HashSet<String>,
    }

    impl crate::row_lookup::RowLookup for ArraySurrogateLookup<'_> {
        fn row_exists(&self, collection: &str, row_id: &str) -> bool {
            self.state.row_exists(collection, row_id) || self.surrogates.contains(row_id)
        }
        fn field_value_exists(
            &self,
            collection: &str,
            field: &str,
            value: &LoroValue,
            exclude_row_id: Option<&str>,
        ) -> bool {
            self.state
                .field_value_exists(collection, field, value, exclude_row_id)
        }
        fn field_value_exists_live(
            &self,
            collection: &str,
            field: &str,
            value: &LoroValue,
            exclude_row_id: Option<&str>,
        ) -> bool {
            self.state
                .field_value_exists_live(collection, field, value, exclude_row_id)
        }
    }

    #[test]
    fn bitemporal_fk_passes_when_array_surrogate_exists() {
        let state = CrdtState::new(1).unwrap();
        let lookup = ArraySurrogateLookup {
            state: &state,
            surrogates: std::iter::once("surr-42".to_string()).collect(),
        };

        let validator = Validator::new(Default::default(), 16);
        let constraint = make_btfk_constraint("variants", "id");
        let change = make_change("surr-42");

        let violation =
            validator.check_foreign_key(&lookup, &change, &constraint, "variants", "id");
        assert!(violation.is_none());
    }

    #[test]
    fn bitemporal_fk_fails_when_array_surrogate_missing() {
        let state = CrdtState::new(1).unwrap();
        let validator = Validator::new(Default::default(), 16);
        let constraint = make_btfk_constraint("variants", "id");
        let change = make_change("surr-99");

        let violation = validator.check_foreign_key(&state, &change, &constraint, "variants", "id");
        assert!(violation.is_some());
        let v = violation.unwrap();
        assert_eq!(v.constraint_name, "test_btfk");
        assert!(v.reason.contains("surr-99"));
    }
}
