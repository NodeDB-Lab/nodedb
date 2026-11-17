// SPDX-License-Identifier: BUSL-1.1

//! CHECK and enum-label enforcement over every statement of one request.
//!
//! Admission runs this for every transport that plans through it. The request
//! text splits into statements, and each statement is checked alone on its
//! preprocessed text, so an UPSERT and an object-literal INSERT reach the
//! check in their INSERT form.
//!
//! An UPDATE by primary key of a collection an earlier statement of the
//! request writes is not checked here. The stored row is not the row the
//! earlier statement leaves. The transaction route checks its NEW row
//! instead, against the row the transaction holds.

use std::borrow::Cow;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::{
    DatabaseId, starts_with_ascii_case_insensitive, strip_prefix_ascii_case_insensitive,
};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::sql_split::split_sql_statements;
use crate::control::state::SharedState;
use crate::control::target_identity::{TargetPk, resolve_target_pk};
use crate::types::{TenantId, TxnId};

use super::statement::{
    enforce_statement_checks, enforce_statement_enum_labels, extract_collection_from_sql,
};

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
/// INSERT, an UPSERT nor an UPDATE passes. The CHECK of an UPDATE by primary
/// key of a collection an earlier statement writes is left to the
/// transaction route.
pub async fn enforce_request_checks(checks: &RequestChecks<'_>, sql: &str) -> crate::Result<()> {
    // The collections the statements before the current one write.
    let mut written: Vec<String> = Vec::new();
    for statement in split_sql_statements(sql) {
        let text = preprocessed(&statement);
        let target = written_collection(&text);
        let route_checks = target
            .as_ref()
            .is_some_and(|collection| written.contains(collection))
            && checked_by_route(checks, &text)?;
        if !route_checks {
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
        }
        enforce_statement_enum_labels(checks.state, checks.tenant_id, checks.database_id, &text)?;
        if let Some(collection) = target {
            written.push(collection);
        }
    }
    Ok(())
}

/// The collection an INSERT, UPDATE or DELETE in `sql` writes. `None` for
/// any other statement.
fn written_collection(sql: &str) -> Option<String> {
    if let Some((collection, _)) = extract_collection_from_sql(sql) {
        return Some(collection);
    }
    let after = strip_prefix_ascii_case_insensitive(sql, "DELETE FROM ")?.trim_start();
    let end = after
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(after.len());
    Some(after[..end].to_lowercase())
}

/// Whether the transaction route checks the UPDATE in `sql` against the row
/// the transaction holds: its collection has CHECK constraints, and its only
/// filter is its primary key equal to one literal. The route plans that
/// UPDATE as a point write, which it checks. Any other UPDATE is checked
/// here.
fn checked_by_route(checks: &RequestChecks<'_>, sql: &str) -> crate::Result<bool> {
    let Some((collection, false)) = extract_collection_from_sql(sql) else {
        return Ok(false);
    };
    let catalog = checks.state.credentials.catalog();
    let Some(stored) =
        catalog.get_collection(checks.database_id, checks.tenant_id.as_u64(), &collection)?
    else {
        return Ok(false);
    };
    if stored.check_constraints.is_empty() {
        return Ok(false);
    }
    Ok(match resolve_target_pk(&stored, "UPDATE") {
        Ok(TargetPk::Field { name, .. }) => filters_one_key(sql, &name),
        Ok(_) | Err(_) => false,
    })
}

/// Whether the `WHERE` clause of `sql` is `column = <literal>` and nothing
/// else.
fn filters_one_key(sql: &str, column: &str) -> bool {
    let Some(at) = find_ascii_case_insensitive(sql, " WHERE ") else {
        return false;
    };
    let filter = sql[at + " WHERE ".len()..]
        .trim()
        .trim_end_matches(';')
        .trim_end();
    let Some(rest) = strip_prefix_ascii_case_insensitive(filter, column) else {
        return false;
    };
    let Some(value) = rest.trim_start().strip_prefix('=') else {
        return false;
    };
    is_one_literal(value.trim())
}

/// Whether `value` is one quoted string with no quote inside, or one bare
/// number or word.
fn is_one_literal(value: &str) -> bool {
    match value.strip_prefix('\'') {
        Some(inner) => inner
            .strip_suffix('\'')
            .is_some_and(|body| !body.contains('\'')),
        None => {
            !value.is_empty()
                && value
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        }
    }
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
