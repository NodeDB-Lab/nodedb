// SPDX-License-Identifier: BUSL-1.1

//! Entry point: dispatch each CHECK constraint to the simple or subquery
//! evaluator depending on whether it references a subquery.

use std::collections::HashMap;

use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

use super::simple::enforce_simple_check;
use super::subquery::enforce_subquery_check;

/// Evaluate all general CHECK constraints for a document being written.
///
/// Returns `Ok(())` if all constraints pass, or a [`DdlError`] with the
/// constraint name and expression on failure.
///
/// Two evaluation paths:
/// - **Simple CHECK** (no subquery): strip `NEW.` prefixes, parse into `SqlExpr`,
///   evaluate directly against the document — same evaluator as typeguard CHECK.
/// - **Subquery CHECK**: substitute `NEW.field` with literal SQL values, plan and
///   dispatch a `SELECT` query, check the result.
pub async fn enforce_check_constraints(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    constraints: &[CheckConstraintDef],
    fields: &HashMap<String, nodedb_types::Value>,
) -> Result<(), DdlError> {
    for constraint in constraints {
        if constraint.has_subquery {
            enforce_subquery_check(state, identity, database_id, constraint, fields).await?;
        } else {
            enforce_simple_check(
                constraint,
                fields,
                &crate::control::crs_context::crs_context_for(state),
            )?;
        }
    }

    Ok(())
}

/// Build a [`DdlError`] with the given SQLSTATE + message.
pub(super) fn ddl_err(sqlstate: &str, msg: &str) -> DdlError {
    DdlError::new(sqlstate, msg)
}
