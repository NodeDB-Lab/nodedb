// SPDX-License-Identifier: BUSL-1.1

//! CHECK and enum-label enforcement over every statement of one request.
//!
//! Admission runs this for every transport that plans through it. The request
//! text splits into statements, and each statement is checked alone on its
//! preprocessed text, so an UPSERT and an object-literal INSERT reach the
//! check in their INSERT form.

use std::borrow::Cow;

use nodedb_types::{DatabaseId, starts_with_ascii_case_insensitive};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::sql_split::split_sql_statements;
use crate::control::state::SharedState;
use crate::types::{TenantId, TxnId};

use super::statement::{enforce_statement_checks, enforce_statement_enum_labels};

/// The request-wide inputs of [`enforce_request_checks`].
pub struct RequestChecks<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub auth: &'a AuthContext,
    /// The open transaction an UPDATE's current row is read in. `None`
    /// outside a transaction block.
    pub txn_id: Option<TxnId>,
}

/// Enforce the CHECK constraints and enum labels of every statement in `sql`.
///
/// The first violation refuses the request. A statement that is neither an
/// INSERT, an UPSERT nor an UPDATE passes.
pub async fn enforce_request_checks(checks: &RequestChecks<'_>, sql: &str) -> crate::Result<()> {
    for statement in split_sql_statements(sql) {
        let text = preprocessed(&statement);
        enforce_statement_checks(
            checks.state,
            checks.identity,
            checks.tenant_id,
            checks.database_id,
            checks.auth,
            checks.txn_id,
            &text,
        )
        .await?;
        enforce_statement_enum_labels(checks.state, checks.tenant_id, checks.database_id, &text)?;
    }
    Ok(())
}

/// `statement` in the form the planner parses. An UPSERT reads as an INSERT,
/// and an object literal as a column list.
///
/// Only an INSERT, or an UPSERT that opens with `UPSERT INTO `, is rewritten.
/// Another statement, and a text the rewrite refuses, is checked as written.
/// The planner refuses such text itself.
fn preprocessed(statement: &str) -> Cow<'_, str> {
    let head = nodedb_sql::parser::preprocess::lex::first_sql_word(statement).unwrap_or_default();
    let rewritable = head.eq_ignore_ascii_case("INSERT")
        || starts_with_ascii_case_insensitive(statement.trim(), "UPSERT INTO ");
    if !rewritable {
        return Cow::Borrowed(statement);
    }
    match nodedb_sql::parser::preprocess::preprocess(statement) {
        Ok(Some(rewritten)) => Cow::Owned(rewritten.sql),
        Ok(None) | Err(_) => Cow::Borrowed(statement),
    }
}
