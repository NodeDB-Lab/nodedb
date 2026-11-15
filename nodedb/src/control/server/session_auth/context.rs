// SPDX-License-Identifier: BUSL-1.1

//! `AuthContext` construction, scope enrichment, and per-query `ON DENY`
//! extraction.

use nodedb_sql::parser::preprocess::lex::rfind_ascii_case_insensitive;

use crate::control::security::auth_context::{AuthContext, generate_session_id};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;

pub use crate::control::security::scope::enrichment::enrich_auth_context_with_scopes;

/// Build an `AuthContext` from an `AuthenticatedIdentity`.
///
/// This is the centralized factory used by password, API-key, certificate,
/// and trust flows. JWT flows use the opaque verified-claims constructor so
/// unverified token fields cannot enrich authorization context.
pub fn build_auth_context(identity: &AuthenticatedIdentity) -> crate::Result<AuthContext> {
    let mut ctx = AuthContext::from_identity(identity, generate_session_id()?);
    // Stamp the per-user default database so `$auth.database_id` is available
    // for RLS predicates even before a `USE DATABASE` command.
    ctx.database_id = identity.default_database;
    Ok(ctx)
}

/// Read the `SET LOCAL nodedb.on_deny = '...'` session parameter, parsed into
/// a [`DenyMode`](crate::control::security::deny::DenyMode).
///
/// This is the one piece of `build_auth_context_with_session`'s old work that
/// [`RequestAuthScope`](crate::control::security::request_scope::RequestAuthScope)
/// cannot absorb by itself: unlike the session database (which every builder
/// call site already threads through `with_session_database`), the
/// session-level `ON DENY` override lives only in session parameters. Callers
/// pass the result to `RequestAuthScopeBuilder::with_on_deny` instead of
/// mutating an `AuthContext` field directly.
pub fn session_on_deny_override(
    sessions: &crate::control::server::shared::session::SessionStore,
    session_id: impl Into<SessionId>,
) -> Option<crate::control::security::deny::DenyMode> {
    let on_deny_val = sessions.get_parameter(session_id, "nodedb.on_deny")?;
    crate::control::security::deny::parse_on_deny(&[&on_deny_val]).ok()
}

/// Extract a per-query `ON DENY` clause from SQL and apply it to the auth context.
///
/// Parses: `SELECT ... ON DENY ERROR 'CODE' MESSAGE '...'`
/// Strips the `ON DENY` clause from the SQL and sets `auth_ctx.on_deny_override`.
/// Returns the cleaned SQL.
pub fn extract_and_apply_on_deny(
    sql: &str,
    auth_ctx: &mut crate::control::security::auth_context::AuthContext,
) -> String {
    let (clean_sql, mode) = extract_on_deny(sql);
    if let Some(mode) = mode {
        auth_ctx.on_deny_override = Some(mode);
    }
    clean_sql
}

/// Extract a per-query `ON DENY` clause from SQL without an `AuthContext` to
/// mutate.
///
/// Parses: `SELECT ... ON DENY ERROR 'CODE' MESSAGE '...'`
/// Returns the SQL with the clause stripped (unchanged if none was found or
/// parseable) alongside the parsed override, if any. Callers that hold a
/// [`RequestAuthScope`](crate::control::security::request_scope::RequestAuthScope)
/// rather than a mutable `AuthContext` apply the override via
/// `RequestAuthScope::with_on_deny_override` instead of mutating a field
/// directly.
///
/// A text of several statements comes back unchanged with no override. The
/// request planner reads each statement's own clause. A clause read off the
/// joined text swallows every later statement.
pub fn extract_on_deny(sql: &str) -> (String, Option<crate::control::security::deny::DenyMode>) {
    let Some(idx) = rfind_ascii_case_insensitive(sql, "on deny ") else {
        return (sql.to_string(), None);
    };
    if crate::control::server::shared::sql_split::holds_several_statements(sql) {
        return (sql.to_string(), None);
    }

    // Only strip ON DENY from SELECT/WITH queries (not CREATE RLS POLICY).
    let trimmed = sql.trim_start();
    if !trimmed
        .get(.."select".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        && !trimmed
            .get(.."with".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("with"))
    {
        return (sql.to_string(), None);
    }

    let on_deny_part = &sql[idx + "on deny ".len()..];
    let parts: Vec<&str> = on_deny_part.split_whitespace().collect();
    match crate::control::security::deny::parse_on_deny(&parts) {
        Ok(mode) => (sql[..idx].trim_end().to_string(), Some(mode)),
        Err(_) => (sql.to_string(), None),
    }
}

/// [`extract_on_deny`] followed by applying the extracted override (if any)
/// to `scope`. Per-query `ON DENY` always wins over any header- or
/// session-level override already baked into `scope`, since it is applied
/// last, after `scope` was built.
///
/// Returns the cleaned SQL alongside the (possibly overridden) scope.
pub fn apply_per_query_on_deny<'a>(
    sql: &str,
    scope: crate::control::security::request_scope::RequestAuthScope<'a>,
) -> (
    String,
    crate::control::security::request_scope::RequestAuthScope<'a>,
) {
    let (clean_sql, mode) = extract_on_deny(sql);
    let scope = match mode {
        Some(mode) => scope.with_on_deny_override(Some(mode)),
        None => scope,
    };
    (clean_sql, scope)
}
