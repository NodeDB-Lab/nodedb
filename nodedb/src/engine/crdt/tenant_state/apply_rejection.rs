// SPDX-License-Identifier: BUSL-1.1

//! How a validated apply refuses a delta.
//!
//! A refused delta goes to the dead-letter queue. The caller gets the
//! deterministic wire [`ViolationType`], derived from the internal
//! compensation hint.

use nodedb_crdt::validator::Violation;
use nodedb_types::sync::violation::ViolationType;

use super::apply_validated::ValidatedApplyOutcome;
use super::core::TenantCrdtEngine;

/// Name the dead-letter entry of a delta that breaks the collection shape.
const ROW_SHAPE_CONSTRAINT: &str = "crdt_row_shape";

impl TenantCrdtEngine {
    /// Dead-letter a delta whose writes break the collection shape. The
    /// caller gets the fault as a schema violation.
    pub(super) fn reject_shape_fault(
        &mut self,
        collection: &str,
        delta: &[u8],
        peer_id: u64,
        fault: &nodedb_crdt::CrdtError,
    ) -> ValidatedApplyOutcome {
        let reason = fault.to_string();
        let violation = Violation {
            constraint_name: ROW_SHAPE_CONSTRAINT.to_string(),
            reason: reason.clone(),
            hint: nodedb_crdt::CompensationHint::ManualIntervention {
                reason: reason.clone(),
            },
        };
        let wire = ViolationType::SchemaViolation {
            field: shape_fault_target(fault),
            reason,
        };
        self.dead_letter(collection, delta, peer_id, &violation, wire)
    }

    /// Enqueue a rejected delta to the DLQ and report it as `wire`.
    ///
    /// The DLQ entry carries the INTERNAL compensation hint verbatim. The wire
    /// hint the client sees derives from `wire`, never from the DLQ. The DLQ
    /// id and timestamp are node-local and non-deterministic, so they are not
    /// returned. A delta the DLQ cannot hold is recorded nowhere, so the apply
    /// reports [`ValidatedApplyOutcome::DeadLetterRefused`].
    pub(super) fn dead_letter(
        &mut self,
        collection: &str,
        delta: &[u8],
        peer_id: u64,
        violation: &Violation,
        wire: ViolationType,
    ) -> ValidatedApplyOutcome {
        // No authenticated user identity is threaded to the apply path in this
        // layer, so the DLQ records `0` (unauthenticated).
        let user_id = 0u64;
        let tenant_id = self.tenant_id().as_u64();

        // The violated constraint by name, so the DLQ entry records the real
        // collection and field. An unresolved name records a
        // ManualIntervention entry under a placeholder CHECK constraint.
        let resolved = self
            .constraints_for_collection(collection)
            .into_iter()
            .find(|c| c.name == violation.constraint_name);
        let (constraint, hint) = match resolved {
            Some(constraint) => (constraint, violation.hint.clone()),
            None => (
                nodedb_crdt::Constraint {
                    name: violation.constraint_name.clone(),
                    collection: collection.to_string(),
                    field: String::new(),
                    kind: nodedb_crdt::ConstraintKind::Check {
                        expr: String::new(),
                        description: "unresolved constraint".to_string(),
                    },
                },
                nodedb_crdt::CompensationHint::ManualIntervention {
                    reason: violation.reason.clone(),
                },
            ),
        };
        let enqueued = self
            .validator
            .dlq_mut()
            .enqueue(nodedb_crdt::EnqueueDeadLetterArgs {
                peer_id,
                user_id,
                tenant_id,
                delta: delta.to_vec(),
                constraint: &constraint,
                reason: violation.reason.clone(),
                hint,
            });
        match enqueued {
            Ok(id) => {
                self.last_dead_letter = Some(id);
                ValidatedApplyOutcome::Rejected(wire)
            }
            Err(error) => {
                crate::diag::crdt_dead_letter_not_enqueued(
                    &error,
                    tenant_id,
                    collection,
                    &constraint.name,
                );
                ValidatedApplyOutcome::DeadLetterRefused {
                    violation: wire,
                    error,
                }
            }
        }
    }
}

/// The container or row a shape fault names, as `collection/row` for a row.
fn shape_fault_target(fault: &nodedb_crdt::CrdtError) -> String {
    match fault {
        nodedb_crdt::CrdtError::NonMapRootContainer { container, .. } => container.clone(),
        nodedb_crdt::CrdtError::NonMapRowValue {
            collection, row_id, ..
        } => format!("{collection}/{row_id}"),
        other => other.to_string(),
    }
}

/// Translate an internal [`Violation`] into the deterministic wire
/// [`ViolationType`] by matching the internal compensation hint.
///
/// `ViolationType` is `#[non_exhaustive]`. A hint with no wire variant maps to
/// the generic `ConstraintViolation`, which carries the human-readable reason.
pub(super) fn violation_to_type(violation: &Violation) -> ViolationType {
    use nodedb_crdt::CompensationHint;
    match &violation.hint {
        CompensationHint::RetryWithDifferentValue {
            field,
            conflicting_value,
            ..
        } => ViolationType::UniqueViolation {
            field: field.clone(),
            value: conflicting_value.clone(),
        },
        CompensationHint::CreateReferencedRow { missing_value, .. } => {
            ViolationType::ForeignKeyMissing {
                referenced_id: missing_value.clone(),
            }
        }
        CompensationHint::ProvideRequiredField { field } => ViolationType::NotNullViolation {
            field: field.clone(),
        },
        CompensationHint::DeleteThenRetry { .. } | CompensationHint::ManualIntervention { .. } => {
            ViolationType::ConstraintViolation {
                detail: violation.reason.clone(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crdt::CompensationHint;

    use super::*;

    fn violation_with(hint: CompensationHint) -> Violation {
        Violation {
            constraint_name: "c".into(),
            reason: "boom".into(),
            hint,
        }
    }

    #[test]
    fn translator_maps_each_hint() {
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::RetryWithDifferentValue {
                field: "email".into(),
                conflicting_value: "x".into(),
                suggestion: "x2".into(),
            })),
            ViolationType::UniqueViolation {
                field: "email".into(),
                value: "x".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::CreateReferencedRow {
                ref_collection: "orgs".into(),
                ref_key: "id".into(),
                missing_value: "org-7".into(),
            })),
            ViolationType::ForeignKeyMissing {
                referenced_id: "org-7".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::ProvideRequiredField {
                field: "name".into(),
            })),
            ViolationType::NotNullViolation {
                field: "name".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::ManualIntervention {
                reason: "nope".into(),
            })),
            ViolationType::ConstraintViolation {
                detail: "boom".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::DeleteThenRetry {
                collection: "users".into(),
                conflicting_key: "a".into(),
            })),
            ViolationType::ConstraintViolation {
                detail: "boom".into(),
            }
        );
    }
}
