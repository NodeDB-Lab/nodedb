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
        nodedb_sql::SqlError::UndefinedObject { kind, name } => {
            crate::Error::UndefinedObject { kind, name }
        }
        nodedb_sql::SqlError::ObjectNotInPrerequisiteState { object, detail } => {
            crate::Error::ObjectNotInPrerequisiteState { object, detail }
        }
        // A constant expression that divides by zero is the same condition the
        // row-scope evaluator raises, so it carries the same code.
        nodedb_sql::SqlError::DivisionByZero => crate::Error::DivisionByZero,
        nodedb_sql::SqlError::InvalidLimitValue { clause, value } => {
            crate::Error::InvalidLimitValue { clause, value }
        }
        nodedb_sql::SqlError::UnknownColumn { column, .. } => {
            crate::Error::UndefinedColumn { column }
        }
        nodedb_sql::SqlError::AmbiguousColumn { column } => {
            crate::Error::AmbiguousColumn { column }
        }
        // A target/expression count mismatch is a syntax error in PostgreSQL,
        // so it renders 42601 through `BadRequest`.
        nodedb_sql::SqlError::Arity { detail } => crate::Error::BadRequest { detail },
        other => crate::Error::PlanError {
            detail: other.to_string(),
        },
    }
}
