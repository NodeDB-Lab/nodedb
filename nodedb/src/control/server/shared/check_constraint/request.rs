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
use crate::control::server::shared::ddl::DdlError;
use crate::control::server::shared::sql_split::split_sql_statements;
use crate::control::server::shared::statement_exec::RequestError;
use crate::control::server::shared::txn_control::wrapper_offset;
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
///
/// A CHECK or enum-label error of statement `k` in a request of several
/// statements carries `k`. In a request that `BEGIN` and an end wrap, `k`
/// counts the inner statements, and the wrapper statements pass.
pub async fn enforce_request_checks(
    checks: &RequestChecks<'_>,
    sql: &str,
) -> Result<(), RequestError> {
    let statements = split_sql_statements(sql);
    let first = wrapper_offset(&statements);
    let end = statements.len().saturating_sub(first);
    let inner = statements.get(first..end).unwrap_or_default();
    let count = inner.len();
    for (index, statement) in inner.iter().enumerate() {
        // Both checks answer a `DdlError`. It converts as it does alone.
        let in_statement =
            |error: DdlError| RequestError::in_statement(crate::Error::from(error), index, count);
        let text = preprocessed(statement);
        enforce_statement_checks(
            checks.state,
            checks.identity,
            checks.tenant_id,
            checks.database_id,
            checks.auth,
            checks.txn_id,
            &text,
        )
        .await
        .map_err(in_statement)?;
        enforce_statement_enum_labels(checks.state, checks.tenant_id, checks.database_id, &text)
            .map_err(in_statement)?;
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
