// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_sql::SqlError` to Control-Plane `crate::Error` mapping.

/// Map a planner error onto its Control-Plane equivalent.
///
/// One mapping for every site that surfaces a `nodedb_sql` error: the
/// `plan_sql*` calls and the `SqlPlan` -> `PhysicalPlan` converters that
/// evaluate column DEFAULTs. Four copies of this match existed and had
/// already drifted — only one of them mapped `RetryableSchemaChanged`, so the
/// same condition was retryable on one path and a flat plan error on the
/// others. A new variant added to `SqlError` reaches every site through here
/// or none.
pub(crate) fn map_plan_error(
    error: nodedb_sql::SqlError,
    tenant_id: crate::types::TenantId,
) -> crate::Error {
    match error {
        nodedb_sql::SqlError::RetryableSchemaChanged { descriptor } => {
            crate::Error::RetryableSchemaChanged { descriptor }
        }
        nodedb_sql::SqlError::CollectionDeactivated {
            name,
            retention_expires_at_ns,
            ..
        } => crate::Error::CollectionDeactivated {
            tenant_id,
            collection: name,
            retention_expires_at_ns,
        },
        nodedb_sql::SqlError::UnknownTable { name } => crate::Error::CollectionNotFound {
            tenant_id,
            collection: name,
        },
        nodedb_sql::SqlError::UndefinedFunction { name } => {
            crate::Error::UndefinedFunction { name }
        }
        // A per-row sequence accessor and a search function outside its
        // search plan are refusals, not syntax errors, so they keep SQLSTATE
        // `0A000` rather than the syntax class `42601`.
        nodedb_sql::SqlError::SequencePerRowUnsupported { .. }
        | nodedb_sql::SqlError::SearchFunctionOutsideSearch { .. } => {
            crate::Error::FeatureNotSupported {
                detail: error.to_string(),
            }
        }
        nodedb_sql::SqlError::UndefinedObject { kind, name } => {
            crate::Error::UndefinedObject { kind, name }
        }
        nodedb_sql::SqlError::ObjectNotInPrerequisiteState { object, detail } => {
            crate::Error::ObjectNotInPrerequisiteState { object, detail }
        }
        // A constant expression that divides by zero is the same condition the
        // row-scope evaluator raises, so it carries the same code.
        nodedb_sql::SqlError::DivisionByZero => crate::Error::DivisionByZero,
        nodedb_sql::SqlError::DataException { detail } => crate::Error::DataException { detail },
        nodedb_sql::SqlError::InvalidLimitValue { clause, value } => {
            crate::Error::InvalidLimitValue { clause, value }
        }
        nodedb_sql::SqlError::UnknownColumn { column, .. } => {
            crate::Error::UndefinedColumn { column }
        }
        nodedb_sql::SqlError::AmbiguousColumn { column } => {
            crate::Error::AmbiguousColumn { column }
        }
        nodedb_sql::SqlError::TextColumn {
            collection,
            column,
            fault,
            ..
        } => crate::Error::TextColumn {
            collection,
            column,
            fault,
        },
        // A target/expression count mismatch is a syntax error in PostgreSQL,
        // so it renders 42601 through `BadRequest`.
        nodedb_sql::SqlError::Arity { detail } => crate::Error::BadRequest { detail },
        // Refusals of a constraint or clause NodeDB does not implement. The
        // DDL router renders both as `0A000`, so the planner path does too.
        nodedb_sql::SqlError::UnsupportedConstraint { .. }
        | nodedb_sql::SqlError::ConflictingEngineClause { .. } => {
            crate::Error::FeatureNotSupported {
                detail: error.to_string(),
            }
        }
        // A value out of range for its type is a data exception (class `22`),
        // the class PostgreSQL and the DDL DEFAULT gate give it.
        nodedb_sql::SqlError::ConstantOverflow { .. }
        | nodedb_sql::SqlError::NumericLiteralOutOfRange { .. }
        | nodedb_sql::SqlError::IntegerOutOfRange { .. }
        | nodedb_sql::SqlError::FloatOutOfRange { .. } => crate::Error::DataException {
            detail: error.to_string(),
        },
        // The executor's recursion cap: the program-limit class (`54000`) the
        // Data-Plane verdict for the same condition renders.
        nodedb_sql::SqlError::RecursionDepthExceeded {
            cte_name,
            max_depth,
        } => crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::RecursionDepthExceeded {
            cte_name,
            max_depth,
        }),
        // Statement errors the client must fix: the syntax class `42601`.
        other @ (nodedb_sql::SqlError::Parse { .. }
        | nodedb_sql::SqlError::TypeMismatch { .. }
        | nodedb_sql::SqlError::Unsupported { .. }
        | nodedb_sql::SqlError::UnevaluableDefault { .. }
        | nodedb_sql::SqlError::SetvalInColumnDefault { .. }
        | nodedb_sql::SqlError::InvalidFunction { .. }
        | nodedb_sql::SqlError::InvalidWindowFrame { .. }
        | nodedb_sql::SqlError::MissingField { .. }
        | nodedb_sql::SqlError::InsertColumnArityMismatch { .. }
        | nodedb_sql::SqlError::PositionalKvInsertUnsupported { .. }
        | nodedb_sql::SqlError::InvalidIdentifier { .. }
        | nodedb_sql::SqlError::ReservedIdentifier { .. }
        | nodedb_sql::SqlError::InvalidRecursiveSetOp { .. }
        | nodedb_sql::SqlError::InvalidRecursiveSelfRef { .. }
        | nodedb_sql::SqlError::RecursiveColumnMismatch { .. }
        | nodedb_sql::SqlError::DuplicateRecursiveColumn { .. }) => crate::Error::PlanError {
            detail: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    #[test]
    fn a_value_out_of_range_is_a_data_exception() {
        let error = nodedb_sql::SqlError::IntegerOutOfRange {
            column: "qty".into(),
            value: 1 << 40,
            declared_type: "integer",
        };
        match map_plan_error(error, TenantId::new(1)) {
            crate::Error::DataException { detail } => assert!(detail.contains("out of range")),
            other => panic!("expected a data exception, got {other:?}"),
        }
    }

    #[test]
    fn an_unsupported_constraint_is_feature_not_supported() {
        let error = nodedb_sql::SqlError::UnsupportedConstraint {
            feature: "EXCLUDE".into(),
            hint: "use a unique index".into(),
        };
        assert!(matches!(
            map_plan_error(error, TenantId::new(1)),
            crate::Error::FeatureNotSupported { .. }
        ));
    }

    #[test]
    fn a_recursion_cap_is_a_program_limit() {
        let error = nodedb_sql::SqlError::RecursionDepthExceeded {
            cte_name: "walk".into(),
            max_depth: 100,
        };
        assert!(matches!(
            map_plan_error(error, TenantId::new(1)),
            crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::RecursionDepthExceeded {
                max_depth: 100,
                ..
            })
        ));
    }
}
