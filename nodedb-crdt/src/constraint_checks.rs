//! Constraint-checking methods for the [`Validator`].
//!
//! All methods are `impl Validator` blocks and belong logically to the
//! validator, but live here to respect file-size guidelines.

use crate::constraint::{Constraint, ConstraintKind};
use crate::dead_letter::CompensationHint;
use crate::state::CrdtState;
use crate::validator::{ProposedChange, Validator, Violation};
use loro::LoroValue;

impl Validator {
    pub(crate) fn check_constraint(
        &self,
        state: &CrdtState,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        match &constraint.kind {
            ConstraintKind::Unique => self.check_unique(state, change, constraint),
            ConstraintKind::ForeignKey {
                ref_collection,
                ref_key,
            } => self.check_foreign_key(state, change, constraint, ref_collection, ref_key),
            ConstraintKind::NotNull => self.check_not_null(change, constraint),
            ConstraintKind::Check { .. } => {
                // Custom checks are application-defined; we can't evaluate them
                // generically. They'd be registered as closures in a real impl.
                None
            }
        }
    }

    pub(crate) fn check_unique(
        &self,
        state: &CrdtState,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        let value = &field_value.1;

        // Check if this value already exists in another row.
        if state.field_value_exists(&change.collection, &constraint.field, value) {
            let value_str = format!("{:?}", value);
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
        state: &CrdtState,
        change: &ProposedChange,
        constraint: &Constraint,
        ref_collection: &str,
        ref_key: &str,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        // The FK value should reference an existing row_id in the ref collection.
        let ref_id = match &field_value.1 {
            LoroValue::String(s) => s.to_string(),
            LoroValue::I64(n) => n.to_string(),
            other => format!("{:?}", other),
        };

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
